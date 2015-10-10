/*
 *  Copyright 2009 Leo Ponomarev.
 */

#include <assert.h>
#include <ngx_http_push_module.h>

#include <subscribers/longpoll.h>
#include <store/memory/store.h>
#include <store/redis/store.h>
#include <ngx_http_push_module_setup.c>
#include <store/memory/ipc.h>
#include <store/memory/shmem.h>
#include <store/memory/store-private.h>

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



#define NGX_HTTP_BUF_ALLOC_SIZE(buf)                                          \
(sizeof(*buf) +                                                           \
(((buf)->temporary || (buf)->memory) ? ngx_buf_size(buf) : 0) +          \
(((buf)->file!=NULL) ? (sizeof(*(buf)->file) + (buf)->file->name.len + 1) : 0))

//buffer is _copied_
ngx_chain_t * ngx_http_push_create_output_chain(ngx_buf_t *buf, ngx_pool_t *pool, ngx_log_t *log) {
  ngx_chain_t                    *out;
  ngx_file_t                     *file;
  ngx_pool_cleanup_t             *cln = NULL;
  ngx_pool_cleanup_file_t        *clnf = NULL;
  if((out = ngx_pcalloc(pool, sizeof(*out)))==NULL) {
    ngx_log_error(NGX_LOG_ERR, log, 0, "push module: can't create output chain, can't allocate chain  in pool");
    return NULL;
  }
  ngx_buf_t                      *buf_copy;
  
  if((buf_copy = ngx_pcalloc(pool, NGX_HTTP_BUF_ALLOC_SIZE(buf)))==NULL) {
    ngx_log_error(NGX_LOG_ERR, log, 0, "push module: can't create output chain, can't allocate buffer copy in pool");
    return NULL;
  }
  ngx_http_push_copy_preallocated_buffer(buf, buf_copy);
  
  if (buf->file!=NULL) {
    if(buf->mmap) { //just the mmap, please
      buf->in_file=0;
      buf->file=NULL;
      buf->file_pos=0;
      buf->file_last=0;
    }
    else {
      file = buf_copy->file;
      file->log=log;
      if(file->fd==NGX_INVALID_FILE) {
        //ngx_log_error(NGX_LOG_ERR, log, 0, "opening invalid file at %s", file->name.data);
        file->fd=ngx_open_file(file->name.data, NGX_FILE_RDONLY, NGX_FILE_OPEN, NGX_FILE_OWNER_ACCESS);
      }
      if(file->fd==NGX_INVALID_FILE) {
        ngx_log_error(NGX_LOG_ERR, log, 0, "push module: can't create output chain, file in buffer is invalid");
        return NULL;
      }
      else {
        //close file on cleanup
        if((cln = ngx_pool_cleanup_add(pool, sizeof(*clnf))) == NULL) {
          ngx_close_file(file->fd);
          file->fd=NGX_INVALID_FILE;
          ngx_log_error(NGX_LOG_ERR, log, 0, "push module: can't create output chain file cleanup.");
          return NULL;
        }
        cln->handler = ngx_pool_cleanup_file;
        clnf = cln->data;
        clnf->fd = file->fd;
        clnf->name = file->name.data;
        clnf->log = pool->log;
      }
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
  //ngx_http_push_store->unlock();
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



// this function adapted from push stream module. thanks Wandenberg Peixoto <wandenberg@gmail.com> and Rogério Carvalho Schneider <stockrt@gmail.com>
static ngx_buf_t * ngx_http_push_request_body_to_single_buffer(ngx_http_request_t *r) {
  ngx_buf_t *buf = NULL;
  ngx_chain_t *chain;
  ssize_t n;
  off_t len;

  chain = r->request_body->bufs;
  if (chain->next == NULL) {
    return chain->buf;
  }
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "push module: multiple buffers in request, need memcpy :(");
  if (chain->buf->in_file) {
    if (ngx_buf_in_memory(chain->buf)) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: can't handle a buffer in a temp file and in memory ");
    }
    if (chain->next != NULL) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: error reading request body with multiple ");
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
          ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: cannot read file with request body");
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
  }
  buf->last_buf = 1;
  return buf;
}

static ngx_int_t ngx_http_push_response_channel_ptr_info(ngx_http_push_channel_t *channel, ngx_http_request_t *r, ngx_int_t status_code) {
  time_t             last_seen = 0;
  ngx_uint_t         subscribers = 0;
  ngx_uint_t         messages = 0;
  if(channel!=NULL) {
    //ngx_http_push_store->lock();
    subscribers = channel->subscribers;
    last_seen = channel->last_seen;
    messages  = channel->messages;
    //ngx_http_push_store->unlock();
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

static ngx_int_t subscribe_longpoll_callback(ngx_int_t status, void *_, ngx_http_request_t *r) {
  return NGX_OK;
}

static ngx_int_t subscribe_intervalpoll_callback(ngx_int_t msg_search_outcome, ngx_http_push_msg_t *msg, ngx_http_request_t *r) {
  //inefficient, but close enough for now
  subscriber_t            *sub;
  ngx_str_t               *etag;
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
      sub = longpoll_subscriber_create(r);
      sub->respond_message(sub, msg);
      longpoll_subscriber_destroy(sub);
      return NGX_OK;

    case NGX_HTTP_PUSH_MESSAGE_NOTFOUND:
    case NGX_HTTP_PUSH_MESSAGE_EXPIRED:
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

ngx_int_t ngx_http_push_subscriber_handler(ngx_http_request_t *r) {
  ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  subscriber_t                   *sub;
  ngx_str_t                      *channel_id;
  ngx_http_push_msg_id_t          msg_id;
  
#if FAKESHARD  
  #ifdef SUB_FAKE_WORKER
  memstore_fakeprocess_push(SUB_FAKE_WORKER);
  #else
  memstore_fakeprocess_push_random();
  #endif
#endif
  
  if((channel_id=ngx_http_push_get_channel_id(r, cf)) == NULL) {
    return r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR;
  }
  
  switch(r->method) {
    case NGX_HTTP_GET:
      ngx_http_push_subscriber_get_msg_id(r, &msg_id);
      switch(cf->subscriber_poll_mechanism) {
        case NGX_HTTP_PUSH_MECHANISM_INTERVALPOLL:
          ngx_http_push_store->get_message(channel_id, &msg_id, (callback_pt )&subscribe_intervalpoll_callback, (void *)r);
          break;
          
        case NGX_HTTP_PUSH_MECHANISM_LONGPOLL:
          if((sub = longpoll_subscriber_create(r)) == NULL) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "unable to create longpoll subscriber");
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
          }
          ngx_http_push_store->subscribe(channel_id, &msg_id, sub, (callback_pt )&subscribe_longpoll_callback, (void *)r);
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
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}

static ngx_int_t channel_info_callback(ngx_int_t status, void *rptr, ngx_http_request_t *r) {
  
  ngx_http_finalize_request(r, ngx_http_push_response_channel_ptr_info( (ngx_http_push_channel_t *)rptr, r, 0));
  return NGX_OK;
}

static ngx_int_t publish_callback(ngx_int_t status, void *rptr, ngx_http_request_t *r) {
  ngx_http_push_channel_t *ch = rptr;
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
    case NGX_HTTP_INTERNAL_SERVER_ERROR:
      //WTF?
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: error publishing message");
      ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return NGX_ERROR;
      
    default:
      //for debugging, mostly. I don't expect this branch to behit during regular operation
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: TOTALLY UNEXPECTED error publishing message, status code %i", status);
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

static void ngx_http_push_publisher_body_handler(ngx_http_request_t * r) {
  ngx_str_t                      *channel_id;
  ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  ngx_uint_t                      method = r->method;
  ngx_buf_t                      *buf;
  size_t                          content_type_len;
  ngx_http_push_msg_t            *msg;
  struct timeval                  tv;
  if((channel_id = ngx_http_push_get_channel_id(r, cf))==NULL) {
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
      NGX_REQUEST_VAL_CHECK(msg, NULL, r, "push module: can't allocate msg in request pool");
      //buf = ngx_create_temp_buf(r->pool, 0);
      //NGX_REQUEST_VAL_CHECK(buf, NULL, r, "push module: can't allocate buf in request pool");
      
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
        buf = ngx_http_push_request_body_to_single_buffer(r);
      }
      else {
        ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, "push module: unexpected publisher message request body buffer location. please report this to the push module developers.");
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
      }

      ngx_gettimeofday(&tv);
      msg->message_time = tv.tv_sec;
      
      msg->buf = buf;

      ngx_http_push_store->publish(channel_id, msg, cf, (callback_pt) &publish_callback, r);
      break;
      
    case NGX_HTTP_DELETE:
      ngx_http_push_store->delete_channel(channel_id, (callback_pt) &channel_info_callback, (void *)r);
      break;
      
    case NGX_HTTP_GET:
      ngx_http_push_store->find_channel(channel_id, (callback_pt) &channel_info_callback, (void *)r);
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
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}


ngx_int_t ngx_http_push_publisher_handler(ngx_http_request_t * r) {
  ngx_int_t                       rc;
  
  /* Instruct ngx_http_read_subscriber_request_body to store the request
     body entirely in a memory buffer or in a file */
  r->request_body_in_single_buf = 1;
  r->request_body_in_persistent_file = 1;
  r->request_body_in_clean_file = 0;
  r->request_body_file_log_level = 0;
  
  //don't buffer the request body --send it right on through
  //r->request_body_no_buffering = 1;

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
      cbuf->file->fd=buf->file->fd;
      cbuf->file->log=ngx_cycle->log;
      cbuf->file->offset=buf->file->offset;
      cbuf->file->sys_offset=buf->file->sys_offset;
      cbuf->file->name.len=buf->file->name.len;
      cbuf->file->name.data=(u_char *) (cbuf->file+1);
      ngx_memcpy(cbuf->file->name.data, buf->file->name.data, buf->file->name.len);
    }
  }
}
