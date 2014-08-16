/*
 *  Copyright 2009 Leo Ponomarev.
 */

#include <ngx_http_push_module.h>

#include <store/memory/store.h>
#include <ngx_http_push_module_setup.c>

ngx_int_t           ngx_http_push_worker_processes;
ngx_pool_t         *ngx_http_push_pool;
ngx_module_t        ngx_http_push_module;

ngx_http_push_store_t *ngx_http_push_store = &ngx_http_push_store_memory;


ngx_int_t ngx_http_push_respond_status_only(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *statusline) {
  r->headers_out.status=status_code;
  if(statusline!=NULL) {
    r->headers_out.status_line.len =statusline->len;
    r->headers_out.status_line.data=statusline->data;
  }
  r->headers_out.content_length_n = 0;
  r->header_only = 1;
  return ngx_http_send_header(r);
}

void ngx_http_push_clean_timeouted_subscriber(ngx_event_t *ev)
{
  ngx_http_push_subscriber_t *subscriber = NULL;
  ngx_http_request_t *r = NULL;

  subscriber = ev->data;
  r = subscriber->request;
  
  if (r->connection->destroyed) {
    return;
  }

  ngx_int_t rc = ngx_http_push_respond_status_only(r, NGX_HTTP_NOT_MODIFIED, NULL);
  ngx_http_finalize_request(r, rc);
  //the subscriber and channel counter will be freed by the pool cleanup callback
}

void ngx_http_push_subscriber_del_timer(ngx_http_push_subscriber_t *sb) {
  if (sb->event.timer_set) {
    ngx_del_timer(&sb->event);
  }
}

void ngx_http_push_subscriber_clear_ctx(ngx_http_push_subscriber_t *sb) {
  ngx_http_push_subscriber_del_timer(sb);
  sb->clndata->subscriber = NULL;
  sb->clndata->channel = NULL;
}


#define NGX_HTTP_BUF_ALLOC_SIZE(buf)                                          \
(sizeof(*buf) +                                                           \
(((buf)->temporary || (buf)->memory) ? ngx_buf_size(buf) : 0) +          \
(((buf)->file!=NULL) ? (sizeof(*(buf)->file) + (buf)->file->name.len + 1) : 0))

//buffer is _copied_
ngx_chain_t * ngx_http_push_create_output_chain(ngx_buf_t *buf, ngx_pool_t *pool, ngx_log_t *log) {
  ngx_chain_t                    *out;
  ngx_file_t                     *file;
  
  if((out = ngx_pcalloc(pool, sizeof(*out)))==NULL) {
    return NULL;
  }
  ngx_buf_t                      *buf_copy;
  
  if((buf_copy = ngx_pcalloc(pool, NGX_HTTP_BUF_ALLOC_SIZE(buf)))==NULL) {
    return NULL;
  }
  ngx_http_push_copy_preallocated_buffer(buf, buf_copy);
  
  if (buf->file!=NULL) {
    file = buf_copy->file;
    file->log=log;
    if(file->fd==NGX_INVALID_FILE) {
      file->fd=ngx_open_file(file->name.data, NGX_FILE_RDONLY, NGX_FILE_OPEN, NGX_FILE_OWNER_ACCESS);
    }
    if(file->fd==NGX_INVALID_FILE) {
      return NULL;
    }
  }
  buf_copy->last_buf = 1;
  out->buf = buf_copy;
  out->next = NULL;
  return out;
}

#define NGX_HTTP_PUSH_NO_CHANNEL_ID_MESSAGE "No channel id provided."
static ngx_str_t * ngx_http_push_get_channel_id(ngx_http_request_t *r, ngx_http_push_loc_conf_t *cf) {
  ngx_http_variable_value_t      *vv = ngx_http_get_indexed_variable(r, cf->index);
  ngx_str_t                      *group = &cf->channel_group;
  size_t                          group_len = group->len;
  size_t                          var_len;
  size_t                          len;
  ngx_str_t                      *id;
    if (vv == NULL || vv->not_found || vv->len == 0) {
        ngx_buf_t *buf = ngx_create_temp_buf(r->pool, sizeof(NGX_HTTP_PUSH_NO_CHANNEL_ID_MESSAGE));
    ngx_chain_t *chain;
    if(buf==NULL) {
      return NULL;
    }
    buf->pos=(u_char *)NGX_HTTP_PUSH_NO_CHANNEL_ID_MESSAGE;
    buf->last=buf->pos + sizeof(NGX_HTTP_PUSH_NO_CHANNEL_ID_MESSAGE)-1;
    chain = ngx_http_push_create_output_chain(buf, r->pool, r->connection->log);
    buf->last_buf=1;
    r->headers_out.content_length_n=ngx_buf_size(buf);
    r->headers_out.status=NGX_HTTP_NOT_FOUND;
    r->headers_out.content_type.len = sizeof("text/plain") - 1;
    r->headers_out.content_type.data = (u_char *) "text/plain";
    r->headers_out.content_type_len = r->headers_out.content_type.len;
    ngx_http_send_header(r);
    ngx_http_output_filter(r, chain);
    ngx_log_error(NGX_LOG_WARN, r->connection->log, 0,
            "push module: the $push_channel_id variable is required but is not set");
    return NULL;
    }
  //maximum length limiter for channel id
  var_len = vv->len <= cf->max_channel_id_length ? vv->len : cf->max_channel_id_length; 
  len = group_len + 1 + var_len;
  if((id = ngx_palloc(r->pool, sizeof(*id) + len))==NULL) {
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "push module: unable to allocate memory for $push_channel_id string");
    return NULL;
  }
  id->len=len;
  id->data=(u_char *)(id+1);
  ngx_memcpy(id->data, group->data, group_len);
  id->data[group_len]='/';
  ngx_memcpy(id->data + group_len + 1, vv->data, var_len);
  return id;
}

ngx_table_elt_t * ngx_http_push_add_response_header(ngx_http_request_t *r, const ngx_str_t *header_name, const ngx_str_t *header_value) {
  ngx_table_elt_t                *h = ngx_list_push(&r->headers_out.headers);
  if (h == NULL) {
    return NULL;
  }
  h->hash = 1;
  h->key.len = header_name->len;
  h->key.data = header_name->data;
  h->value.len = header_value->len;
  h->value.data = header_value->data;
  return h;
}

static ngx_str_t * ngx_http_push_find_in_header_value(ngx_http_request_t * r, ngx_str_t header_name) {
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

ngx_int_t ngx_http_push_allow_caching(ngx_http_request_t * r) {
  ngx_str_t *tmp_header;
  ngx_str_t header_checks[2] = { NGX_HTTP_PUSH_HEADER_CACHE_CONTROL, NGX_HTTP_PUSH_HEADER_PRAGMA };
  ngx_int_t i = 0;
  
  for(; i < 2; i++) {
    tmp_header = ngx_http_push_find_in_header_value(r, header_checks[i]);
    
    if (tmp_header != NULL) {
      return !!ngx_strncasecmp(tmp_header->data, NGX_HTTP_PUSH_CACHE_CONTROL_VALUE.data, tmp_header->len);
    }
  }
  
  return 1;
}

void ngx_http_push_subscriber_cleanup(ngx_http_push_subscriber_cleanup_t *data) {
  if(data->subscriber!=NULL) { //still queued up
    ngx_http_push_subscriber_t* sb = data->subscriber;
    ngx_http_push_subscriber_del_timer(sb);
    ngx_queue_remove(&data->subscriber->queue);
    ngx_pfree(ngx_http_push_pool, data->subscriber); //was there an error? oh whatever.
  }
  if (data->rchain != NULL) {
    ngx_pfree(data->rpool, data->rchain->buf);
    ngx_pfree(data->rpool, data->rchain);
    data->rchain=NULL;
  }
  if(data->buf_use_count != NULL && --(*data->buf_use_count) <= 0) {
    ngx_buf_t                      *buf;
    ngx_pfree(ngx_http_push_pool, data->buf_use_count);
    buf=data->buf;
    if(buf->file) {
      ngx_close_file(buf->file->fd);
    }
    ngx_pfree(ngx_http_push_pool, buf);
  }
  
  if(data->channel!=NULL) { //we're expected to decrement the subscriber count
    ngx_http_push_store->lock();
    data->channel->subscribers--;
    ngx_http_push_store->unlock();
  }
}

ngx_str_t * ngx_http_push_subscriber_get_etag(ngx_http_request_t * r) {
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
    if (header[i].key.len == NGX_HTTP_PUSH_HEADER_IF_NONE_MATCH.len
      && ngx_strncasecmp(header[i].key.data, NGX_HTTP_PUSH_HEADER_IF_NONE_MATCH.data, header[i].key.len) == 0) {
      return &header[i].value;
      }
  }
  return NULL;
}

ngx_int_t ngx_http_push_subscriber_get_msg_id(ngx_http_request_t *r, ngx_http_push_msg_id_t *id) {
  ngx_str_t                      *if_none_match = ngx_http_push_subscriber_get_etag(r);
  ngx_int_t                       tag=0;
  id->time=(r->headers_in.if_modified_since == NULL) ? 0 : ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
  if(if_none_match==NULL || (if_none_match!=NULL && (tag = ngx_atoi(if_none_match->data, if_none_match->len))==NGX_ERROR)) {
    tag=0;
  }
  id->tag=ngx_abs(tag);
  return NGX_OK;
}

//allocates nothing
ngx_int_t ngx_http_push_prepare_response_to_subscriber_request(ngx_http_request_t *r, ngx_chain_t *chain, ngx_str_t *content_type, ngx_str_t *etag, time_t last_modified) {
  ngx_int_t                      res;
  if (content_type!=NULL) {
    r->headers_out.content_type.len=content_type->len;
    r->headers_out.content_type.data = content_type->data;
    r->headers_out.content_type_len = r->headers_out.content_type.len;
  }
  if(last_modified) {
    //if-modified-since header
    r->headers_out.last_modified_time=last_modified;
  }
  if(etag!=NULL) {
    //etag, if we need one
    if ((ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ETAG, etag))==NULL) {
      return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
  }
  //Vary header needed for proper HTTP caching.
  ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_VARY, &NGX_HTTP_PUSH_VARY_HEADER_VALUE);
  
  r->headers_out.status=NGX_HTTP_OK;
  //we know the entity length, and we're using just one buffer. so no chunking please.
  r->headers_out.content_length_n=ngx_buf_size(chain->buf);
  if((res = ngx_http_send_header(r)) >= NGX_HTTP_SPECIAL_RESPONSE) {
    return res;
  }
  
  return ngx_http_output_filter(r, chain);
}

//allocates message and responds to subscriber
ngx_int_t ngx_http_push_alloc_for_subscriber_response(ngx_pool_t *pool, ngx_int_t shared, ngx_http_push_msg_t *msg, ngx_chain_t **chain, ngx_str_t **content_type, ngx_str_t **etag, time_t *last_modified) {
  if(etag != NULL && (*etag = ngx_http_push_store->message_etag(msg, pool))==NULL) {
    //oh, nevermind...
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate memory for Etag header");
    return NGX_ERROR;
  }
  if(content_type != NULL && (*content_type= ngx_http_push_store->message_content_type(msg, pool))==NULL) {
    //oh, nevermind...
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate memory for Content Type header");
    if(pool == NULL) {
      ngx_free(*etag);
    }
    else {
      ngx_pfree(pool, *etag);
    }
    return NGX_ERROR;
  }
  
  //preallocate output chain. yes, same one for every waiting subscriber
  if(chain != NULL && (*chain = ngx_http_push_create_output_chain(msg->buf, pool, ngx_cycle->log))==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate buffer chain while responding to subscriber request");
    if(pool == NULL) {
      ngx_free(*etag);
      ngx_free(*content_type);
    }
    else {
      ngx_pfree(pool, *etag);
      ngx_pfree(pool, *content_type);
    }
    return NGX_ERROR;
  }
  
  if(last_modified != NULL) {
    *last_modified = msg->message_time;
  }
  ngx_http_push_store->unlock();
  
  
  if(pool!=NULL && shared == 0 && ((*chain)->buf->file!=NULL)) {
    //close file when we're done with it
    ngx_pool_cleanup_t *cln;
    ngx_pool_cleanup_file_t *clnf;
    
    if((cln = ngx_pool_cleanup_add(pool, sizeof(ngx_pool_cleanup_file_t)))==NULL) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate buffer chain pool cleanup while responding to subscriber request");
      ngx_pfree(pool, *etag);
      ngx_pfree(pool, *content_type);
      ngx_pfree(pool, *chain);
      return NGX_ERROR;
    }
    cln->handler = ngx_pool_cleanup_file;
    clnf = cln->data;
    clnf->fd = (*chain)->buf->file->fd;
    clnf->name = (*chain)->buf->file->name.data;
    clnf->log = ngx_cycle->log;
  }
  return NGX_OK;
}



static void ngx_http_push_match_channel_info_subtype(size_t off, u_char *cur, size_t rem, u_char **priority, const ngx_str_t **format, ngx_str_t *content_type) {
  static ngx_http_push_content_subtype_t subtypes[] = {
    { "json"  , 4, &NGX_HTTP_PUSH_CHANNEL_INFO_JSON },
    { "yaml"  , 4, &NGX_HTTP_PUSH_CHANNEL_INFO_YAML },
    { "xml"   , 3, &NGX_HTTP_PUSH_CHANNEL_INFO_XML  },
    { "x-json", 6, &NGX_HTTP_PUSH_CHANNEL_INFO_JSON },
    { "x-yaml", 6, &NGX_HTTP_PUSH_CHANNEL_INFO_YAML }
  };
  u_char                         *start = cur + off;
  ngx_uint_t                      i;
  
  for(i=0; i<(sizeof(subtypes)/sizeof(ngx_http_push_content_subtype_t)); i++) {
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
static ngx_int_t ngx_http_push_channel_info(ngx_http_request_t *r, ngx_uint_t messages, ngx_uint_t subscribers, time_t last_seen) {
  ngx_buf_t                      *b;
  ngx_uint_t                      len;
  ngx_str_t                       content_type = ngx_string("text/plain");
  const ngx_str_t                *format = &NGX_HTTP_PUSH_CHANNEL_INFO_PLAIN;
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
          format = &NGX_HTTP_PUSH_CHANNEL_INFO_PLAIN;
          priority = cur+sizeof("text/")-1;
          //content-type is already set by default
        }
      }
      ngx_http_push_match_channel_info_subtype(sizeof("text/")-1, cur, rem, &priority, &format, &content_type);
    }
    cur = accept;
    for(rem=len; (cur = ngx_strnstr(cur, "application/", rem))!=NULL; cur += sizeof("application/")-1) {
      rem=len - ((size_t)(cur-accept)+sizeof("application/")-1);
      ngx_http_push_match_channel_info_subtype(sizeof("application/")-1, cur, rem, &priority, &format, &content_type);
    }
  }
  
  r->headers_out.content_type.len = content_type.len;
  r->headers_out.content_type.data = content_type.data;
  r->headers_out.content_type_len = r->headers_out.content_type.len;
  
  len = format->len - 8 - 1 + 3*NGX_INT_T_LEN; //minus 8 sprintf
  
  if ((b = ngx_create_temp_buf(r->pool, len)) == NULL) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }
  b->last = ngx_sprintf(b->last, (char *)format->data, messages, last_seen==0 ? -1 : (ngx_int_t) time_elapsed ,subscribers);
  
  //lastly, set the content-length, because if the status code isn't 200, nginx may not do so automatically
  r->headers_out.content_length_n = ngx_buf_size(b);
  
  if (ngx_http_send_header(r) > NGX_HTTP_SPECIAL_RESPONSE) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }
  
  return ngx_http_output_filter(r, ngx_http_push_create_output_chain(b, r->pool, r->connection->log));
}



#define NGX_HTTP_PUSH_MAKE_CONTENT_TYPE(content_type, content_type_len, msg, pool)  \
    if(((content_type) = ngx_palloc(pool, sizeof(*content_type)+content_type_len))!=NULL) { \
        (content_type)->len=content_type_len;                                        \
        (content_type)->data=(u_char *)((content_type)+1);                           \
        ngx_memcpy(content_type->data, (msg)->content_type.data, content_type_len);  \
    }

#define NGX_HTTP_PUSH_OPTIONS_OK_MESSAGE "Go ahead"


ngx_int_t ngx_push_longpoll_subscriber_enqueue(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *subscriber, ngx_int_t subscriber_timeout) {
  ngx_http_cleanup_t                 *cln;
  ngx_http_push_subscriber_cleanup_t *clndata;
  ngx_http_request_t                 *r = subscriber->request;
  //attach a cleaner to remove the request from the channel and handle shared buffer deallocation.
  if ((cln=ngx_http_cleanup_add(r, sizeof(*clndata))) == NULL) { //make sure we can.
    return NGX_ERROR;
  }
  cln->handler = (ngx_http_cleanup_pt) ngx_http_push_subscriber_cleanup;
  clndata = (ngx_http_push_subscriber_cleanup_t *) cln->data;
  clndata->channel=channel;
  clndata->subscriber=subscriber;
  clndata->buf_use_count=0;
  clndata->buf=NULL;
  clndata->rchain=NULL;
  clndata->rpool=NULL;
  subscriber->clndata=clndata;
  
  //set up subscriber timeout event
  ngx_memzero(&subscriber->event, sizeof(subscriber->event));
  if (subscriber_timeout > 0) {
    subscriber->event.handler = ngx_http_push_clean_timeouted_subscriber;  
    subscriber->event.data = subscriber;
    subscriber->event.log = r->connection->log;
    ngx_add_timer(&subscriber->event, subscriber_timeout * 1000);
  }
  
  r->read_event_handler = ngx_http_test_reading;
  r->write_event_handler = ngx_http_request_empty_handler;
  r->main->count++; //this is the right way to hold and finalize the request... maybe
  //r->keepalive = 1; //stayin' alive!!
  return NGX_OK;
}

/*
ngx_int_t ngx_push_longpoll_subscriber_dequeue(ngx_http_push_subscriber_t *subscriber) {
  return NGX_OK;
}
*/

static ngx_int_t ngx_http_push_response_channel_ptr_info(ngx_http_push_channel_t *channel, ngx_http_request_t *r, ngx_int_t status_code) {
  time_t             last_seen = 0;
  ngx_uint_t         subscribers = 0;
  ngx_uint_t         messages = 0;
  if(channel!=NULL) {
    ngx_http_push_store->lock();
    subscribers = channel->subscribers;
    last_seen = channel->last_seen;
    messages  = channel->messages;
    ngx_http_push_store->unlock();
    r->headers_out.status = status_code == (ngx_int_t) NULL ? NGX_HTTP_OK : status_code;
    if (status_code == NGX_HTTP_CREATED) {
      r->headers_out.status_line.len =sizeof("201 Created")- 1;
      r->headers_out.status_line.data=(u_char *) "201 Created";
    }
    else if (status_code == NGX_HTTP_ACCEPTED) {
      r->headers_out.status_line.len =sizeof("202 Accepted")- 1;
      r->headers_out.status_line.data=(u_char *) "202 Accepted";
    }
    ngx_http_push_channel_info(r, messages, subscribers, last_seen);
  }
  else {
    //404!
    r->headers_out.status=NGX_HTTP_NOT_FOUND;
    //just the headers, please. we don't care to describe the situation or
    //respond with an html page
    r->headers_out.content_length_n=0;
    r->header_only = 1;
    ngx_http_send_header(r);
  }
  return NGX_OK;
}

static ngx_int_t ngx_http_push_response_channel_info(ngx_str_t *channel_id, ngx_http_request_t *r, ngx_int_t status_code) {
  ngx_http_push_channel_t        *channel;
  ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  channel = channel_id == NULL ? NULL : ngx_http_push_store->find_channel(channel_id, cf->channel_timeout, NULL);
  return ngx_http_push_response_channel_ptr_info(channel, r, status_code);
}

static ngx_int_t subscribe_longpoll_callback(ngx_int_t status, ngx_http_request_t *r) {
  ngx_http_finalize_request(r, status);
  return NGX_OK;
}

static ngx_int_t subscribe_intervalpoll_callback(ngx_http_push_msg_t *msg, ngx_int_t msg_search_outcome, ngx_http_request_t *r) {
  ngx_chain_t                *chain;
  ngx_str_t                  *content_type, *etag;
  time_t                     last_modified;
  switch(msg_search_outcome) {
    case NGX_HTTP_PUSH_MESSAGE_EXPECTED:
      //interval-polling subscriber requests get a 304 with their entity tags preserved.
      if (r->headers_in.if_modified_since != NULL) {
        r->headers_out.last_modified_time=ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
      }
      if ((etag=ngx_http_push_subscriber_get_etag(r)) != NULL) {
        ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ETAG, etag);
      }
      ngx_http_finalize_request(r, NGX_HTTP_NOT_MODIFIED);
      return NGX_OK;
      
    case NGX_HTTP_PUSH_MESSAGE_FOUND:
      ngx_http_push_alloc_for_subscriber_response(r->pool, 0, msg, &chain, &content_type, &etag, &last_modified);
      ngx_http_push_prepare_response_to_subscriber_request(r, chain, content_type, etag, last_modified);
      ngx_http_push_store->release_message(NULL, msg);
      ngx_http_finalize_request(r, NGX_OK);
      return NGX_OK;
      
    default:
      ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return NGX_ERROR;
  }
}

ngx_int_t ngx_http_push_subscriber_handler(ngx_http_request_t *r) {
  ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  ngx_str_t                      *channel_id;
  ngx_http_push_msg_id_t          msg_id;
  
  if((channel_id=ngx_http_push_get_channel_id(r, cf)) == NULL) {
    return r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR;
  }
  
  switch(r->method) {
    case NGX_HTTP_GET:
      ngx_http_push_subscriber_get_msg_id(r, &msg_id);

      r->main->count++; //let it linger until callback
      switch(cf->subscriber_poll_mechanism) {
        ngx_int_t                       msg_search_outcome;
        
        //for NGX_HTTP_PUSH_MECHANISM_LONGPOLL
        case NGX_HTTP_PUSH_MECHANISM_INTERVALPOLL:
          ngx_http_push_store->get_message(channel_id, &msg_id, &msg_search_outcome, r, &subscribe_intervalpoll_callback);
          break;
          
        case NGX_HTTP_PUSH_MECHANISM_LONGPOLL:
          ngx_http_push_store->subscribe(channel_id, &msg_id, r, &subscribe_longpoll_callback);
          break;
      }
      return NGX_DONE;
    
    case NGX_HTTP_OPTIONS:
      ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN,  &NGX_HTTP_PUSH_ANYSTRING);
      ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ACCESS_CONTROL_ALLOW_HEADERS, &NGX_HTTP_PUSH_ACCESS_CONTROL_ALLOWED_SUBSCRIBER_HEADERS);
      ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ACCESS_CONTROL_ALLOW_METHODS, &NGX_HTTP_PUSH_ALLOW_GET_OPTIONS);
      r->headers_out.content_length_n = 0;
      r->header_only = 1;
      r->headers_out.status=NGX_HTTP_OK;
      ngx_http_send_header(r);
      return NGX_OK;
      
    default:
      ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ALLOW, &NGX_HTTP_PUSH_ALLOW_GET_OPTIONS); //valid HTTP for the win
      return NGX_HTTP_NOT_ALLOWED;
  }
}

static ngx_int_t publish_callback(ngx_int_t status, ngx_http_push_channel_t *ch, ngx_http_request_t *r) {
  switch(status) {
    case NGX_HTTP_PUSH_MESSAGE_QUEUED:
      //message was queued successfully, but there were no subscribers to receive it.
      ngx_http_finalize_request(r, ngx_http_push_response_channel_ptr_info(ch, r, NGX_HTTP_ACCEPTED));
      return NGX_OK;
      
    case NGX_HTTP_PUSH_MESSAGE_RECEIVED:
      //message was queued successfully, and it was already sent to at least one subscriber
      ngx_http_finalize_request(r, ngx_http_push_response_channel_ptr_info(ch, r, NGX_HTTP_CREATED));
      return NGX_OK;
      
    case NGX_ERROR:
      //WTF?
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: error broadcasting message to workers");
      ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return NGX_ERROR;
      
    default:
      //for debugging, mostly. I don't expect this branch to behit during regular operation
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: TOTALLY UNEXPECTED error broadcasting message to workers");
      ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return NGX_ERROR;
  }
}

static void ngx_http_push_publisher_body_handler(ngx_http_request_t * r) {
  ngx_str_t                      *channel_id;
  ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  ngx_uint_t                      method = r->method;
  
  if((channel_id = ngx_http_push_get_channel_id(r, cf))==NULL) {
    ngx_http_finalize_request(r, r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR);
    return;
  }
  
  switch(method) {
    case NGX_HTTP_POST:
    case NGX_HTTP_PUT:
      ngx_http_push_store->publish(channel_id, r, &publish_callback);
      break;
      
    case NGX_HTTP_DELETE:
      ngx_http_finalize_request(r, ngx_http_push_response_channel_info(channel_id, r, NGX_HTTP_OK));
      ngx_http_push_store->delete_channel(channel_id);
      break;
      
    case NGX_HTTP_GET:
      ngx_http_finalize_request(r, ngx_http_push_response_channel_info(channel_id, r, NGX_HTTP_OK));
      break;
      
    case NGX_HTTP_OPTIONS:
      ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN, &NGX_HTTP_PUSH_ANYSTRING);
      ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ACCESS_CONTROL_ALLOW_HEADERS,  &NGX_HTTP_PUSH_ACCESS_CONTROL_ALLOWED_PUBLISHER_HEADERS);
      ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ACCESS_CONTROL_ALLOW_METHODS, &NGX_HTTP_PUSH_ALLOW_GET_POST_PUT_DELETE_OPTIONS);
      r->header_only = 1;
      r->headers_out.content_length_n = 0;
      r->headers_out.status=NGX_HTTP_OK;
      ngx_http_send_header(r);
      ngx_http_finalize_request(r, NGX_HTTP_OK);
      break;
      
    default:
      //some other weird request method
      ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ALLOW, &NGX_HTTP_PUSH_ALLOW_GET_POST_PUT_DELETE_OPTIONS);
      ngx_http_finalize_request(r, NGX_HTTP_NOT_ALLOWED);
      break;
  }
}

ngx_int_t ngx_http_push_respond_to_subscribers(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *sentinel, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line) {

  //copy everything we need first
  ngx_str_t                  *content_type=NULL;
  ngx_str_t                  *etag=NULL;
  time_t                      last_modified;
  ngx_chain_t                *chain=NULL;
  ngx_http_request_t         *r;
  ngx_buf_t                  *buffer;
  ngx_chain_t                *rchain;
  ngx_buf_t                  *rbuffer;
  ngx_int_t                  *buf_use_count;
  ngx_http_push_subscriber_cleanup_t *clndata;
  ngx_http_push_subscriber_t *cur=NULL;
  ngx_int_t                   responded_subscribers=0;

  if(sentinel==NULL) {
    //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "respond_to_subscribers with sentinel==NULL");
    return NGX_OK;
  }
  
  if(msg!=NULL) {
    if(ngx_http_push_alloc_for_subscriber_response(ngx_http_push_pool, 1, msg, &chain, &content_type, &etag, &last_modified)==NGX_ERROR) {
      ngx_http_push_store->release_message(channel, msg);
      return NGX_ERROR;
    }
    
    buffer = chain->buf;
    buffer->recycled = 1;

    buf_use_count = ngx_pcalloc(ngx_http_push_pool, sizeof(*buf_use_count));
    *buf_use_count = ngx_http_push_store->channel_worker_subscribers(sentinel);
  }
    
  while((cur=ngx_http_push_store->next_subscriber(channel, sentinel, cur, 1))!=NULL) {
    //in this block, nothing in shared memory should be dereferenced.
    r=cur->request;

    if(msg!=NULL) {
      //chain and buffer for this request
      rchain = ngx_pcalloc(r->pool, sizeof(*rchain));
      rchain->next = NULL;
      rbuffer = ngx_pcalloc(r->pool, sizeof(*rbuffer));
      rchain->buf = rbuffer;
      ngx_memcpy(rbuffer, buffer, sizeof(*buffer));

      //request buffer cleanup
      clndata = cur->clndata;
      clndata->buf = buffer;
      clndata->buf_use_count = buf_use_count;
      clndata->rchain = rchain;
      clndata->rpool = r->pool;

      if (rbuffer->in_file && (fcntl(rbuffer->file->fd, F_GETFD) == -1)) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: buffer in invalid file descriptor");
      }
      //cleanup oughtn't dequeue anything. or decrement the subscriber count, for that matter
      ngx_http_push_subscriber_clear_ctx(cur);
      ngx_http_finalize_request(r, ngx_http_push_prepare_response_to_subscriber_request(r, rchain, content_type, etag, last_modified)); //BAM!
    }
    else {
      ngx_http_push_subscriber_clear_ctx(cur);
      ngx_http_finalize_request(r, ngx_http_push_respond_status_only(r, status_code, status_line));
    }
    responded_subscribers++;
  }
  if(msg!=NULL) {
    ngx_http_push_store->release_message(channel, msg);
    ngx_pfree(ngx_http_push_pool, etag);
    ngx_pfree(ngx_http_push_pool, content_type);
    ngx_pfree(ngx_http_push_pool, chain);
  }
  
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "respond_to_subscribers with msg %p finished", msg);
  ngx_http_push_store->lock();
  channel->subscribers-=responded_subscribers;
  //is the message still needed?
  ngx_http_push_store->unlock();
  ngx_http_push_store->release_subscriber_sentinel(channel, sentinel);
  return NGX_OK;
}

ngx_int_t ngx_http_push_publisher_handler(ngx_http_request_t * r) {
  ngx_int_t                       rc;
  
  /* Instruct ngx_http_read_subscriber_request_body to store the request
     body entirely in a memory buffer or in a file */
  r->request_body_in_single_buf = 1;
  r->request_body_in_persistent_file = 1;
  r->request_body_in_clean_file = 0;
  r->request_body_file_log_level = 0;

  rc = ngx_http_read_client_request_body(r, ngx_http_push_publisher_body_handler);
  if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
    return rc;
  }
  return NGX_DONE;
}

void ngx_http_push_copy_preallocated_buffer(ngx_buf_t *buf, ngx_buf_t *cbuf) {
  if (cbuf!=NULL) {
    ngx_memcpy(cbuf, buf, sizeof(*buf)); //overkill?
    if(buf->temporary || buf->memory) { //we don't want to copy mmpapped memory, so no ngx_buf_in_momory(buf)
      cbuf->pos = (u_char *) (cbuf+1);
      cbuf->last = cbuf->pos + ngx_buf_size(buf);
      cbuf->start=cbuf->pos;
      cbuf->end = cbuf->start + ngx_buf_size(buf);
      ngx_memcpy(cbuf->pos, buf->pos, ngx_buf_size(buf));
      cbuf->memory=ngx_buf_in_memory_only(buf) ? 1 : 0;
    }
    if (buf->file!=NULL) {
      cbuf->file = (ngx_file_t *) (cbuf+1) + ((buf->temporary || buf->memory) ? ngx_buf_size(buf) : 0);
      cbuf->file->fd=NGX_INVALID_FILE;
      cbuf->file->log=NULL;
      cbuf->file->offset=buf->file->offset;
      cbuf->file->sys_offset=buf->file->sys_offset;
      cbuf->file->name.len=buf->file->name.len;
      cbuf->file->name.data=(u_char *) (cbuf->file+1);
      ngx_memcpy(cbuf->file->name.data, buf->file->name.data, buf->file->name.len);
    }
  }
}
