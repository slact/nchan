/*
 *  Copyright 2009 Leo Ponomarev.
 */

#include <assert.h>
#include <nchan_module.h>

#include <subscribers/longpoll.h>
#include <subscribers/websocket.h>
#include <store/memory/store.h>
#include <store/redis/store.h>
#include <nchan_setup.c>
#include <store/memory/ipc.h>
#include <store/memory/shmem.h>
#include <store/memory/store-private.h>
#include <nchan_output.h>

ngx_int_t           nchan_worker_processes;
ngx_pool_t         *nchan_pool;
ngx_module_t        nchan_module;

//nchan_store_t *nchan_store = &nchan_store_redis;
nchan_store_t *nchan_store = &nchan_store_memory;




static ngx_str_t *nchan_get_channel_id(ngx_http_request_t *r, nchan_loc_conf_t *cf) {
  static const ngx_str_t          TEXT_PLAIN = ngx_string("text/plain");
  static const ngx_str_t          NO_CHANNEL_ID_MESSAGE = ngx_string("No channel id provided.");
  
  ngx_http_variable_value_t      *vv = ngx_http_get_indexed_variable(r, cf->index);
  ngx_str_t                      *group = &cf->channel_group;
  size_t                          group_len = group->len;
  size_t                          var_len;
  size_t                          len;
  ngx_str_t                      *id;
  if (vv == NULL || vv->not_found || vv->len == 0) {
    nchan_respond_string(r, NGX_HTTP_NOT_FOUND, &TEXT_PLAIN, &NO_CHANNEL_ID_MESSAGE, 0);
    ngx_log_error(NGX_LOG_WARN, r->connection->log, 0, 
            "nchan: the $push_channel_id variable is required but is not set");
    return NULL;
  }
  //maximum length limiter for channel id
  var_len = vv->len <= cf->max_channel_id_length ? vv->len : cf->max_channel_id_length; 
  len = group_len + 1 + var_len;
  if((id = ngx_palloc(r->pool, sizeof(*id) + len))==NULL) {
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "nchan: unable to allocate memory for $push_channel_id string");
    return NULL;
  }
  id->len=len;
  id->data=(u_char *)(id+1);
  ngx_memcpy(id->data, group->data, group_len);
  id->data[group_len]='/';
  ngx_memcpy(id->data + group_len + 1, vv->data, var_len);
  return id;
}

ngx_str_t * nchan_get_header_value(ngx_http_request_t * r, ngx_str_t header_name) {
  ngx_uint_t                       i;
  ngx_list_part_t                 *part = &r->headers_in.headers.part;
  ngx_table_elt_t                 *header= part->elts;
  
  for (i = 0; /* void */ ; i++) {
    if (i >= part->nelts) {
      if (part->next == NULL) {
        break;
      }
      part = part->next;
      header = part->elts;
      i = 0;
    }
    if (header[i].key.len == header_name.len
      && ngx_strncasecmp(header[i].key.data, header_name.data, header[i].key.len) == 0) {
      return &header[i].value;
      }
  }
  return NULL;
}

static ngx_int_t nchan_detect_websocket_handshake(ngx_http_request_t *r) {
  ngx_str_t *tmp;
  if((tmp = nchan_get_header_value(r, NCHAN_HEADER_CONNECTION))) {
    if(ngx_strncasecmp(tmp->data, NCHAN_UPGRADE.data, NCHAN_UPGRADE.len) != 0) return 0;
  }
  else return 0;
  
  if((tmp = nchan_get_header_value(r, NCHAN_HEADER_UPGRADE))) {
    if(ngx_strncasecmp(tmp->data, NCHAN_WEBSOCKET.data, NCHAN_WEBSOCKET.len) != 0) return 0;
  }
  else return 0;

  return 1;
}

/*
ngx_int_t NCHAN_allow_caching(ngx_http_request_t * r) {
  ngx_str_t *tmp_header;
  ngx_str_t header_checks[2] = { NCHAN_HEADER_CACHE_CONTROL, NCHAN_HEADER_PRAGMA };
  ngx_int_t i = 0;
  
  for(; i < 2; i++) {
    tmp_header = nchan_get_header_value(r, header_checks[i]);
    
    if (tmp_header != NULL) {
      return !!ngx_strncasecmp(tmp_header->data, NCHAN_CACHE_CONTROL_VALUE.data, tmp_header->len);
    }
  }
  
  return 1;
}
*/

ngx_str_t * nchan_subscriber_get_etag(ngx_http_request_t * r) {
  ngx_uint_t                       i;
  ngx_list_part_t                 *part = &r->headers_in.headers.part;
  ngx_table_elt_t                 *header= part->elts;
  
  for (i = 0; /* void */ ; i++) {
    if (i >= part->nelts) {
      if (part->next == NULL) {
        break;
      }
      part = part->next;
      header = part->elts;
      i = 0;
    }
    if (header[i].key.len == NCHAN_HEADER_IF_NONE_MATCH.len
      && ngx_strncasecmp(header[i].key.data, NCHAN_HEADER_IF_NONE_MATCH.data, header[i].key.len) == 0) {
      return &header[i].value;
      }
  }
  return NULL;
}

ngx_int_t nchan_subscriber_get_msg_id(ngx_http_request_t *r, nchan_msg_id_t *id) {
  ngx_str_t                      *if_none_match = nchan_subscriber_get_etag(r);
  ngx_int_t                       tag=0;
  id->time=(r->headers_in.if_modified_since == NULL) ? 0 : ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
  if(if_none_match==NULL || (if_none_match!=NULL && (tag = ngx_atoi(if_none_match->data, if_none_match->len))==NGX_ERROR)) {
    tag=0;
  }
  id->tag=ngx_abs(tag);
  return NGX_OK;
}


static void nchan_match_channel_info_subtype(size_t off, u_char *cur, size_t rem, u_char **priority, const ngx_str_t **format, ngx_str_t *content_type) {
  static nchan_content_subtype_t subtypes[] = {
    { "json"  , 4, &NCHAN_CHANNEL_INFO_JSON },
    { "yaml"  , 4, &NCHAN_CHANNEL_INFO_YAML },
    { "xml"   , 3, &NCHAN_CHANNEL_INFO_XML  },
    { "x-json", 6, &NCHAN_CHANNEL_INFO_JSON },
    { "x-yaml", 6, &NCHAN_CHANNEL_INFO_YAML }
  };
  u_char                         *start = cur + off;
  ngx_uint_t                      i;
  
  for(i=0; i<(sizeof(subtypes)/sizeof(nchan_content_subtype_t)); i++) {
    if(ngx_strncmp(start, subtypes[i].subtype, rem<subtypes[i].len ? rem : subtypes[i].len)==0) {
      if(*priority>start) {
        *format = subtypes[i].format;
        *priority = start;
        content_type->data=cur;
        content_type->len= off + 1 + subtypes[i].len;
      }
    }
  }
}

//print information about a channel
static ngx_int_t nchan_channel_info(ngx_http_request_t *r, ngx_uint_t messages, ngx_uint_t subscribers, time_t last_seen) {
  ngx_buf_t                      *b;
  ngx_uint_t                      len;
  ngx_str_t                       content_type = ngx_string("text/plain");
  const ngx_str_t                *format = &NCHAN_CHANNEL_INFO_PLAIN;
  time_t                          time_elapsed = ngx_time() - last_seen;
  
  if(r->headers_in.accept) {
    //lame content-negotiation (without regard for qvalues)
    u_char                    *accept = r->headers_in.accept->value.data;
    size_t                     len = r->headers_in.accept->value.len;
    size_t                     rem;
    u_char                    *cur = accept;
    u_char                    *priority=&accept[len-1];
    for(rem=len; (cur = ngx_strnstr(cur, "text/", rem))!=NULL; cur += sizeof("text/")-1) {
      rem=len - ((size_t)(cur-accept)+sizeof("text/")-1);
      if(ngx_strncmp(cur+sizeof("text/")-1, "plain", rem<5 ? rem : 5)==0) {
        if(priority) {
          format = &NCHAN_CHANNEL_INFO_PLAIN;
          priority = cur+sizeof("text/")-1;
          //content-type is already set by default
        }
      }
      nchan_match_channel_info_subtype(sizeof("text/")-1, cur, rem, &priority, &format, &content_type);
    }
    cur = accept;
    for(rem=len; (cur = ngx_strnstr(cur, "application/", rem))!=NULL; cur += sizeof("application/")-1) {
      rem=len - ((size_t)(cur-accept)+sizeof("application/")-1);
      nchan_match_channel_info_subtype(sizeof("application/")-1, cur, rem, &priority, &format, &content_type);
    }
  }
  
  len = format->len - 8 - 1 + 3*NGX_INT_T_LEN; //minus 8 sprintf
  if ((b = ngx_create_temp_buf(r->pool, len)) == NULL) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }
  b->last = ngx_sprintf(b->last, (char *)format->data, messages, last_seen==0 ? -1 : (ngx_int_t) time_elapsed ,subscribers);
  b->end = b->last;
  
  return nchan_respond_membuf(r, NGX_HTTP_OK, &content_type, b, 0);
}


// this function adapted from push stream module. thanks Wandenberg Peixoto <wandenberg@gmail.com> and Rogério Carvalho Schneider <stockrt@gmail.com>
static ngx_buf_t * nchan_request_body_to_single_buffer(ngx_http_request_t *r) {
  ngx_buf_t *buf = NULL;
  ngx_chain_t *chain;
  ssize_t n;
  off_t len;

  chain = r->request_body->bufs;
  if (chain->next == NULL) {
    return chain->buf;
  }
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "nchan: multiple buffers in request, need memcpy :(");
  if (chain->buf->in_file) {
    if (ngx_buf_in_memory(chain->buf)) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "nchan: can't handle a buffer in a temp file and in memory ");
    }
    if (chain->next != NULL) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "nchan: error reading request body with multiple ");
    }
    return chain->buf;
  }
  buf = ngx_create_temp_buf(r->pool, r->headers_in.content_length_n + 1);
  if (buf != NULL) {
    ngx_memset(buf->start, '\0', r->headers_in.content_length_n + 1);
    while ((chain != NULL) && (chain->buf != NULL)) {
      len = ngx_buf_size(chain->buf);
      // if buffer is equal to content length all the content is in this buffer
      if (len >= r->headers_in.content_length_n) {
        buf->start = buf->pos;
        buf->last = buf->pos;
        len = r->headers_in.content_length_n;
      }
      if (chain->buf->in_file) {
        n = ngx_read_file(chain->buf->file, buf->start, len, 0);
        if (n == NGX_FILE_ERROR) {
          ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "nchan: cannot read file with request body");
          return NULL;
        }
        buf->last = buf->last + len;
        ngx_delete_file(chain->buf->file->name.data);
        chain->buf->file->fd = NGX_INVALID_FILE;
      } else {
        buf->last = ngx_copy(buf->start, chain->buf->pos, len);
      }

      chain = chain->next;
      buf->start = buf->last;
    }
    buf->last_buf = 1;
  }
  return buf;
}

static ngx_int_t nchan_response_channel_ptr_info(nchan_channel_t *channel, ngx_http_request_t *r, ngx_int_t status_code) {
  static const ngx_str_t CREATED_LINE = ngx_string("201 Created");
  static const ngx_str_t ACCEPTED_LINE = ngx_string("201 Created");
  
  time_t             last_seen = 0;
  ngx_uint_t         subscribers = 0;
  ngx_uint_t         messages = 0;
  if(channel!=NULL) {
    //nchan_store->lock();
    subscribers = channel->subscribers;
    last_seen = channel->last_seen;
    messages  = channel->messages;
    //nchan_store->unlock();
    r->headers_out.status = status_code == (ngx_int_t) NULL ? NGX_HTTP_OK : status_code;
    if (status_code == NGX_HTTP_CREATED) {
      ngx_memcpy(&r->headers_out.status_line, &CREATED_LINE, sizeof(ngx_str_t));
    }
    else if (status_code == NGX_HTTP_ACCEPTED) {
      ngx_memcpy(&r->headers_out.status_line, &ACCEPTED_LINE, sizeof(ngx_str_t));
    }
    nchan_channel_info(r, messages, subscribers, last_seen);
  }
  else {
    //404!
    nchan_respond_status(r, NGX_HTTP_NOT_FOUND, NULL, 0);
  }
  return NGX_OK;
}

static ngx_int_t subscribe_longpoll_callback(ngx_int_t status, void *_, ngx_http_request_t *r) {
  return NGX_OK;
}
static ngx_int_t subscribe_websocket_callback(ngx_int_t status, void *_, ngx_http_request_t *r) {
  return NGX_OK;
}

static ngx_int_t subscribe_intervalpoll_callback(ngx_int_t msg_search_outcome, nchan_msg_t *msg, ngx_http_request_t *r) {
  //inefficient, but close enough for now
  subscriber_t            *sub;
  ngx_str_t               *etag;
  switch(msg_search_outcome) {
    case NCHAN_MESSAGE_EXPECTED:
      //interval-polling subscriber requests get a 304 with their entity tags preserved.
      if (r->headers_in.if_modified_since != NULL) {
        r->headers_out.last_modified_time=ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
      }
      if ((etag=nchan_subscriber_get_etag(r)) != NULL) {
        nchan_add_response_header(r, &NCHAN_HEADER_ETAG, etag);
      }
      ngx_http_finalize_request(r, NGX_HTTP_NOT_MODIFIED);
      return NGX_OK;
      
    case NCHAN_MESSAGE_FOUND:
      sub = longpoll_subscriber_create(r);
      sub->respond_message(sub, msg);
      longpoll_subscriber_destroy(sub);
      return NGX_OK;

    case NCHAN_MESSAGE_NOTFOUND:
    case NCHAN_MESSAGE_EXPIRED:
      r->headers_out.status=NGX_HTTP_NOT_FOUND;
      //just the headers, please. we don't care to describe the situation or
      //respond with an html page
      r->headers_out.content_length_n=0;
      r->header_only = 1;
      //ngx_http_send_header(r);
      ngx_http_finalize_request(r, NGX_HTTP_NOT_FOUND);
      return NGX_OK;

    default:
      ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return NGX_ERROR;
  }
}

ngx_int_t nchan_subscriber_handler(ngx_http_request_t *r) {
  nchan_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, nchan_module);
  subscriber_t                   *sub;
  ngx_str_t                      *channel_id;
  nchan_msg_id_t          msg_id;
  
  if((channel_id=nchan_get_channel_id(r, cf)) == NULL) {
    return r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR;
  }
  
  switch(r->method) {
    case NGX_HTTP_GET:
      
#if FAKESHARD  
  #ifdef SUB_FAKE_WORKER
      memstore_fakeprocess_push(SUB_FAKE_WORKER);
  #else
      memstore_fakeprocess_push_random();
  #endif
#endif      

      if(nchan_detect_websocket_handshake(r)) {
        //do you want a websocket?
        if((sub = websocket_subscriber_create(r)) == NULL) {
          ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "unable to create websocket subscriber");
          return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
        nchan_store->subscribe(channel_id, &msg_id, sub, (callback_pt )&subscribe_websocket_callback, (void *)r);
      }
      else {
        nchan_subscriber_get_msg_id(r, &msg_id);
        
        switch(cf->subscriber_poll_mechanism) {
          case NCHAN_MECHANISM_INTERVALPOLL:
            nchan_store->get_message(channel_id, &msg_id, (callback_pt )&subscribe_intervalpoll_callback, (void *)r);
            break;
            
          case NCHAN_MECHANISM_LONGPOLL:
            if((sub = longpoll_subscriber_create(r)) == NULL) {
              ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "unable to create longpoll subscriber");
              return NGX_HTTP_INTERNAL_SERVER_ERROR;
            }
            nchan_store->subscribe(channel_id, &msg_id, sub, (callback_pt )&subscribe_longpoll_callback, (void *)r);
            break;
        }
      }
      
#if FAKESHARD
      memstore_fakeprocess_pop();
#endif
      
      return NGX_DONE;
    
    case NGX_HTTP_OPTIONS:
      return nchan_OPTIONS_respond(r, &NCHAN_ANYSTRING, &NCHAN_ACCESS_CONTROL_ALLOWED_SUBSCRIBER_HEADERS, &NCHAN_ALLOW_GET_OPTIONS);
      
    default:
      nchan_add_response_header(r, &NCHAN_HEADER_ALLOW, &NCHAN_ALLOW_GET_OPTIONS); //valid HTTP for the win
      return NGX_HTTP_NOT_ALLOWED;
  }

}

static ngx_int_t channel_info_callback(ngx_int_t status, void *rptr, ngx_http_request_t *r) {
  
  ngx_http_finalize_request(r, nchan_response_channel_ptr_info( (nchan_channel_t *)rptr, r, 0));
  return NGX_OK;
}

static ngx_int_t publish_callback(ngx_int_t status, void *rptr, ngx_http_request_t *r) {
  nchan_channel_t *ch = rptr;
  switch(status) {
    case NCHAN_MESSAGE_QUEUED:
      //message was queued successfully, but there were no subscribers to receive it.
      ngx_http_finalize_request(r, nchan_response_channel_ptr_info(ch, r, NGX_HTTP_ACCEPTED));
      return NGX_OK;
      
    case NCHAN_MESSAGE_RECEIVED:
      //message was queued successfully, and it was already sent to at least one subscriber
      ngx_http_finalize_request(r, nchan_response_channel_ptr_info(ch, r, NGX_HTTP_CREATED));
      return NGX_OK;
      
    case NGX_ERROR:
    case NGX_HTTP_INTERNAL_SERVER_ERROR:
      //WTF?
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "nchan: error publishing message");
      ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return NGX_ERROR;
      
    default:
      //for debugging, mostly. I don't expect this branch to behit during regular operation
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "nchan: TOTALLY UNEXPECTED error publishing message, status code %i", status);
      ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return NGX_ERROR;
  }
}

#define NGX_REQUEST_VAL_CHECK(val, fail, r, errormessage)             \
if (val == fail) {                                                        \
  ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, errormessage);    \
  ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);         \
  return;                                                          \
  }

static void nchan_publisher_body_handler(ngx_http_request_t * r) {
  ngx_str_t                      *channel_id;
  nchan_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, nchan_module);
  ngx_uint_t                      method = r->method;
  ngx_buf_t                      *buf;
  size_t                          content_type_len;
  nchan_msg_t            *msg;
  struct timeval                  tv;
  if((channel_id = nchan_get_channel_id(r, cf))==NULL) {
    ngx_http_finalize_request(r, r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR);
    return;
  }
#if FAKESHARD
  #ifdef PUB_FAKE_WORKER
  memstore_fakeprocess_push(PUB_FAKE_WORKER);
  #else
  memstore_fakeprocess_push_random();
  #endif
#endif
  switch(method) {
    case NGX_HTTP_POST:
    case NGX_HTTP_PUT:
      msg = ngx_pcalloc(r->pool, sizeof(*msg));
      msg->shared = 0;
      NGX_REQUEST_VAL_CHECK(msg, NULL, r, "nchan: can't allocate msg in request pool");
      //buf = ngx_create_temp_buf(r->pool, 0);
      //NGX_REQUEST_VAL_CHECK(buf, NULL, r, "nchan: can't allocate buf in request pool");
      
      //content type
      content_type_len = (r->headers_in.content_type!=NULL ? r->headers_in.content_type->value.len : 0);
      if(content_type_len > 0) {
        msg->content_type.len = content_type_len;
        msg->content_type.data = r->headers_in.content_type->value.data;
      }

      if(r->headers_in.content_length_n == -1 || r->headers_in.content_length_n == 0) {
        buf = ngx_create_temp_buf(r->pool, 0);
      }
      else if(r->request_body->bufs!=NULL) {
        buf = nchan_request_body_to_single_buffer(r);
      }
      else {
        ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, "nchan: unexpected publisher message request body buffer location. please report this to the push module developers.");
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
      }

      ngx_gettimeofday(&tv);
      msg->message_time = tv.tv_sec;
      
      msg->buf = buf;

      nchan_store->publish(channel_id, msg, cf, (callback_pt) &publish_callback, r);
      break;
      
    case NGX_HTTP_DELETE:
      nchan_store->delete_channel(channel_id, (callback_pt) &channel_info_callback, (void *)r);
      break;
      
    case NGX_HTTP_GET:
      nchan_store->find_channel(channel_id, (callback_pt) &channel_info_callback, (void *)r);
      break;
      
    case NGX_HTTP_OPTIONS:
      nchan_OPTIONS_respond(r, &NCHAN_ANYSTRING, &NCHAN_ACCESS_CONTROL_ALLOWED_PUBLISHER_HEADERS, &NCHAN_ALLOW_GET_POST_PUT_DELETE_OPTIONS);
      break;
      
    default:
      //some other weird request method
      nchan_add_response_header(r, &NCHAN_HEADER_ALLOW, &NCHAN_ALLOW_GET_POST_PUT_DELETE_OPTIONS);
      nchan_respond_status(r, NGX_HTTP_NOT_ALLOWED, NULL, 0);
      break;
  }
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}


ngx_int_t nchan_publisher_handler(ngx_http_request_t * r) {
  ngx_int_t                       rc;
  
  /* Instruct ngx_http_read_subscriber_request_body to store the request
     body entirely in a memory buffer or in a file */
  r->request_body_in_single_buf = 1;
  r->request_body_in_persistent_file = 1;
  r->request_body_in_clean_file = 0;
  r->request_body_file_log_level = 0;
  
  //don't buffer the request body --send it right on through
  //r->request_body_no_buffering = 1;

  rc = ngx_http_read_client_request_body(r, nchan_publisher_body_handler);
  if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
    return rc;
  }
  return NGX_DONE;
}
