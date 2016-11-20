/*
 *  Written by Leo Ponomarev 2009-2015
 */

#include <nchan_module.h>
#include <util/nchan_subrequest.h>
#include <assert.h>

#include <subscribers/longpoll.h>
#include <subscribers/intervalpoll.h>
#include <subscribers/eventsource.h>
#include <subscribers/http-chunked.h>
#include <subscribers/http-multipart-mixed.h>
#include <subscribers/http-raw-stream.h>
#include <subscribers/websocket.h>
#include <store/memory/store.h>
#include <store/redis/store.h>

#include <nchan_setup.c>

#if FAKESHARD
#include <store/memory/ipc.h>
#include <store/memory/shmem.h>
//#include <store/memory/store-private.h> //for debugging
#endif
#include <util/nchan_output.h>
#include <nchan_websocket_publisher.h>

ngx_int_t           nchan_worker_processes;
int                 nchan_stub_status_enabled = 0;


//#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG

//#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "NCHAN:" fmt, ##args)

typedef struct {
  ngx_http_request_t    *r;
  ngx_http_cleanup_t    *cln;
} safe_request_ptr_t;
static safe_request_ptr_t *nchan_set_safe_request_ptr(ngx_http_request_t *r);
static ngx_http_request_t *nchan_get_safe_request_ptr(safe_request_ptr_t *pd);

ngx_int_t nchan_maybe_send_channel_event_message(ngx_http_request_t *r, channel_event_type_t event_type) {
  static nchan_loc_conf_t            evcf_data;
  static nchan_loc_conf_t           *evcf = NULL;
  
  static ngx_str_t group =           ngx_string("meta");
  
  static ngx_str_t evt_sub_enqueue = ngx_string("subscriber_enqueue");
  static ngx_str_t evt_sub_dequeue = ngx_string("subscriber_dequeue");
  static ngx_str_t evt_sub_recvmsg = ngx_string("subscriber_receive_message");
  static ngx_str_t evt_sub_recvsts = ngx_string("subscriber_receive_status");
  static ngx_str_t evt_chan_publish= ngx_string("channel_publish");
  static ngx_str_t evt_chan_delete = ngx_string("channel_delete");

  struct timeval             tv;
  
  nchan_loc_conf_t          *cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  ngx_http_complex_value_t  *cv = cf->channel_events_channel_id;
  if(cv==NULL) {
    //nothing to send
    return NGX_OK;
  }
  
  nchan_request_ctx_t       *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  ngx_str_t                  tmpid;
  size_t                     sz;
  ngx_str_t                 *id;
  u_char                    *cur;
  ngx_str_t                  evstr;
  ngx_buf_t                  buf;
  nchan_msg_t                msg;
  
  switch(event_type) {
    case SUB_ENQUEUE:
      ctx->channel_event_name = &evt_sub_enqueue;
      break;
    case SUB_DEQUEUE:
      ctx->channel_event_name = &evt_sub_dequeue;
      break;
    case SUB_RECEIVE_MESSAGE:
      ctx->channel_event_name = &evt_sub_recvmsg;
      break;
    case SUB_RECEIVE_STATUS:
      ctx->channel_event_name = &evt_sub_recvsts;
      break;
    case CHAN_PUBLISH:
      ctx->channel_event_name = &evt_chan_publish;
      break;
    case CHAN_DELETE:
      ctx->channel_event_name = &evt_chan_delete;
      break;
  }
  
  //the id
  ngx_http_complex_value(r, cv, &tmpid); 
  sz = group.len + 1 + tmpid.len;
  if((id = ngx_palloc(r->pool, sizeof(*id) + sz)) == NULL) {
    nchan_log_request_error(r, "can't allocate space for legacy channel id");
    return NGX_ERROR;
  }
  id->len = sz;
  id->data = (u_char *)&id[1];
  cur = id->data;  
  ngx_memcpy(cur, group.data, group.len);
  cur += group.len;
  cur[0]='/';
  cur++;
  ngx_memcpy(cur, tmpid.data, tmpid.len);
  
  
  //the event message
  ngx_http_complex_value(r, cf->channel_event_string, &evstr);
  ngx_memzero(&buf, sizeof(buf)); //do we really need this?...
  buf.temporary = 1;
  buf.memory = 1;
  buf.last_buf = 1;
  buf.pos = evstr.data;
  buf.last = evstr.data + evstr.len;
  buf.start = buf.pos;
  buf.end = buf.last;
  
  ngx_memzero(&msg, sizeof(msg));
  ngx_gettimeofday(&tv);
  msg.id.time = tv.tv_sec;
  msg.id.tagcount = 1;
  msg.buf = &buf;
  
  
  if(evcf == NULL) {
    evcf = &evcf_data;
    ngx_memzero(evcf, sizeof(*evcf));

    evcf->message_timeout = NCHAN_META_CHANNEL_MESSAGE_TTL;
    evcf->max_messages = NCHAN_META_CHANNEL_MAX_MESSAGES;
    evcf->complex_max_messages = NULL;
    evcf->complex_message_timeout = NULL;
    evcf->subscriber_first_message = 0;
    evcf->channel_timeout = NCHAN_META_CHANNEL_TIMEOUT;
  }
  evcf->storage_engine = cf->storage_engine;
  evcf->redis = cf->redis;
  
  evcf->storage_engine->publish(id, &msg, evcf, NULL, NULL);
  
  return NGX_OK;
}

#if FAKESHARD 
static void memstore_sub_debug_start() {
  #ifdef SUB_FAKE_WORKER
  memstore_fakeprocess_push(SUB_FAKE_WORKER);
  #else
  memstore_fakeprocess_push_random();
  #endif
}
static void memstore_sub_debug_end() {
  memstore_fakeprocess_pop();
}
static void memstore_pub_debug_start() {
  #ifdef PUB_FAKE_WORKER
  memstore_fakeprocess_push(PUB_FAKE_WORKER);
  #else
  memstore_fakeprocess_push_random();
  #endif
}
static void memstore_pub_debug_end() {
  memstore_fakeprocess_pop();
}
#endif

time_t nchan_loc_conf_message_timeout(nchan_loc_conf_t *cf) {
  time_t                        timeout;
  nchan_loc_conf_shared_data_t *shcf;
  
  if(!cf->complex_message_timeout) {
    timeout = cf->message_timeout;
  }
  else {
    shcf = memstore_get_conf_shared_data(cf);
    timeout = shcf->message_timeout;
  }
  
  return timeout != 0 ? timeout : 525600 * 60;
}

ngx_int_t nchan_loc_conf_max_messages(nchan_loc_conf_t *cf) {  
  ngx_int_t                     num;
  nchan_loc_conf_shared_data_t *shcf;
  
  if(!cf->complex_max_messages) {
    num = cf->max_messages;
  }
  else {
    shcf = memstore_get_conf_shared_data(cf);
    num = shcf->max_messages;
  }
  
  return num;
}

static void nchan_publisher_body_handler(ngx_http_request_t *r);

static ngx_int_t nchan_http_publisher_handler(ngx_http_request_t * r) {
  ngx_int_t                       rc;
  nchan_request_ctx_t            *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  
  static ngx_str_t                publisher_name = ngx_string("http");
  
  if(ctx) ctx->publisher_type = &publisher_name;
  
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

ngx_int_t nchan_stub_status_handler(ngx_http_request_t *r) {
  ngx_buf_t           *b;
  ngx_chain_t          out;
  nchan_stub_status_t *stats;
  
  float                shmem_used;
  
  char     *buf_fmt = "total published messages: %ui\n"
                      "stored messages: %ui\n"
                      "shared memory used: %fK\n"
                      "channels: %ui\n"
                      "subscribers: %ui\n"
                      "redis pending commands: %ui\n"
                      "redis connected servers: %ui\n"
                      "total interprocess alerts received: %ui\n"
                      "interprocess alerts in transit: %ui\n"
                      "interprocess queued alerts: %ui\n"
                      "total interprocess send delay: %ui\n"
                      "total interprocess receive delay: %ui\n";
  
  if ((b = ngx_pcalloc(r->pool, sizeof(*b) + 800)) == NULL) {
    nchan_log_request_error(r, "Failed to allocate response buffer for nchan_stub_status.");
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }
  
  shmem_used = (float )((float )nchan_get_used_shmem() / 1024.0);
  
  stats = nchan_get_stub_status_stats();
  
  b->start = (u_char *)&b[1];
  b->pos = b->start;
  
  b->end = ngx_snprintf(b->start, 800, buf_fmt, stats->total_published_messages, stats->messages, shmem_used, stats->channels, stats->subscribers, stats->redis_pending_commands, stats->redis_connected_servers, stats->ipc_total_alerts_received, stats->ipc_total_alerts_sent - stats->ipc_total_alerts_received, stats->ipc_queue_size, stats->ipc_total_send_delay, stats->ipc_total_receive_delay);
  b->last = b->end;

  b->memory = 1;
  b->last_buf = 1;
  
  r->headers_out.status = NGX_HTTP_OK;
  r->headers_out.content_type.len = sizeof("text/plain") - 1;
  r->headers_out.content_type.data = (u_char *) "text/plain";
  
  r->headers_out.content_length_n = b->end - b->start;
  ngx_http_send_header(r);
  
  out.buf = b;
  out.next = NULL;
  
  return ngx_http_output_filter(r, &out);
}

int nchan_parse_message_buffer_config(ngx_http_request_t *r, nchan_loc_conf_t *cf, char **err) {
  ngx_str_t                      val;
  nchan_loc_conf_shared_data_t  *shcf;
  
  if(!cf->complex_message_timeout && !cf->complex_max_messages) {
    return 1;
  }
  
  if(cf->complex_message_timeout) {
    time_t    timeout;
    if(ngx_http_complex_value(r, cf->complex_message_timeout, &val) != NGX_OK) {
      nchan_log_request_error(r, "cannot evaluate nchan_message_timeout value");
      *err = NULL;
      return 0;
    }
    if(val.len == 0) {
      *err = "missing nchan_message_timeout value";
      nchan_log_request_error(r, "%s", *err);
      return 0;
    }
    
    if((timeout = ngx_parse_time(&val, 1)) == (time_t )NGX_ERROR) {
      *err = "invalid nchan_message_timeout value";
      nchan_log_request_error(r, "%s '%V'", *err, &val);
      return 0;
    }
    
    shcf = memstore_get_conf_shared_data(cf);
    shcf->message_timeout = timeout;
  }
  if(cf->complex_max_messages) {
    ngx_int_t                      num;
    if(ngx_http_complex_value(r, cf->complex_max_messages, &val) != NGX_OK) {
      nchan_log_request_error(r, "cannot evaluate nchan_message_buffer_length value");
      *err = NULL;
      return 0;
    }
    
    if(val.len == 0) {
      *err = "missing nchan_message_buffer_length value";
      nchan_log_request_error(r, "%s", *err);
      return 0;
    }
    
    num = ngx_atoi(val.data, val.len);
    if(num == NGX_ERROR || num < 0) {
      *err = "invalid nchan_message_buffer_length value";
      nchan_log_request_error(r, "%s %V", *err, &val);
      return 0;
    }
    
    shcf = memstore_get_conf_shared_data(cf);
    shcf->max_messages = num;
  }
  return 1;
}

ngx_int_t nchan_pubsub_handler(ngx_http_request_t *r) {
  nchan_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  ngx_str_t              *channel_id;
  subscriber_t           *sub;
  nchan_msg_id_t         *msg_id;
  ngx_int_t               rc = NGX_DONE;
  nchan_request_ctx_t    *ctx;
  ngx_str_t              *origin_header;
  
#if NCHAN_BENCHMARK
  struct timeval          tv;
  ngx_gettimeofday(&tv);
#endif
  
  if((ctx = ngx_pcalloc(r->pool, sizeof(nchan_request_ctx_t))) == NULL) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }
  ngx_http_set_ctx(r, ctx, ngx_nchan_module);

#if NCHAN_BENCHMARK
  ctx->start_tv = tv;
#endif
  
  //X-Accel-Redirected requests get their method mangled to GET. De-mangle it if necessary
  if(r->upstream && r->upstream->headers_in.x_accel_redirect) {
    //yep, we got x-accel-redirected. what was the original method?...
    nchan_recover_x_accel_redirected_request_method(r);
  }
  
  if((origin_header = nchan_get_header_value(r, NCHAN_HEADER_ORIGIN)) != NULL) {
    ctx->request_origin_header = *origin_header;
    if(!(cf->allow_origin.len == 1 && cf->allow_origin.data[0] == '*')) {
      if(!(origin_header->len == cf->allow_origin.len && ngx_strnstr(origin_header->data, (char *)cf->allow_origin.data, origin_header->len) != NULL)) {
        //CORS origin match failed! return a 403 forbidden
        goto forbidden;
      }
    }
  }
  else {
    ctx->request_origin_header.len=0;
    ctx->request_origin_header.data=NULL;
  }
  
  if((channel_id = nchan_get_channel_id(r, SUB, 1)) == NULL) {
    //just get the subscriber_channel_id for now. the publisher one is handled elsewhere
    return r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  if(cf->pub.websocket || cf->pub.http) {
    char *err;
    if(!nchan_parse_message_buffer_config(r, cf, &err)) {
      if(err) {
        nchan_respond_cstring(r, NGX_HTTP_FORBIDDEN, &NCHAN_CONTENT_TYPE_TEXT_PLAIN, err, 0);
        return NGX_OK;
      }
      else {
        nchan_respond_status(r, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, 0);
        return NGX_OK;
      }
    }
  }
  
  if(nchan_detect_websocket_request(r)) {
    //want websocket?
    if(cf->sub.websocket) {
      //we prefer to subscribe
#if FAKESHARD
      memstore_sub_debug_start();
#endif
      if((msg_id = nchan_subscriber_get_msg_id(r)) == NULL) {
        goto bad_msgid;
      }
      if((sub = websocket_subscriber_create(r, msg_id)) == NULL) {
        nchan_log_request_error(r, "unable to create websocket subscriber");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
      }
      sub->fn->subscribe(sub, channel_id);
#if FAKESHARD      
      memstore_sub_debug_end();
#endif
    }
    else if(cf->pub.websocket) {
      //no need to subscribe, but keep a connection open for publishing
      nchan_create_websocket_publisher(r);
    }
    else goto forbidden;
    return NGX_DONE;
  }
  else {
    subscriber_t *(*sub_create)(ngx_http_request_t *r, nchan_msg_id_t *msg_id) = NULL;
    
    switch(r->method) {
      case NGX_HTTP_GET:
        if(cf->sub.eventsource && nchan_detect_eventsource_request(r)) {
          sub_create = eventsource_subscriber_create;
        }
        else if(cf->sub.http_chunked && nchan_detect_chunked_subscriber_request(r)) {
          sub_create = http_chunked_subscriber_create;
        }
        else if(cf->sub.http_multipart && nchan_detect_multipart_subscriber_request(r)) {
          sub_create = http_multipart_subscriber_create;
        }
        else if(cf->sub.poll) {
          sub_create = intervalpoll_subscriber_create;
        }
        else if(cf->sub.http_raw_stream) {
          sub_create = http_raw_stream_subscriber_create;
        }
        else if(cf->sub.longpoll) {
          sub_create = longpoll_subscriber_create;
        }
        else if(cf->pub.http) {
          nchan_http_publisher_handler(r);
        }
        else {
          goto forbidden;
        }
        
        if(sub_create) {
#if FAKESHARD
          memstore_sub_debug_start();
#endif
          if((msg_id = nchan_subscriber_get_msg_id(r)) == NULL) {
            goto bad_msgid;
          }
          if((sub = sub_create(r, msg_id)) == NULL) {
            nchan_log_request_error(r, "unable to create subscriber");
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
          }
          
          sub->fn->subscribe(sub, channel_id);
#if FAKESHARD
          memstore_sub_debug_end();
#endif
        }
        
        break;
      
      case NGX_HTTP_POST:
      case NGX_HTTP_PUT:
        if(cf->pub.http) {
          nchan_http_publisher_handler(r);
        }
        else goto forbidden;
        break;
      
      case NGX_HTTP_DELETE:
        if(cf->pub.http) {
          nchan_http_publisher_handler(r);
        }
        else goto forbidden;
        break;
      
      case NGX_HTTP_OPTIONS:
        if(cf->pub.http) {
          nchan_OPTIONS_respond(r, &cf->allow_origin, &NCHAN_ACCESS_CONTROL_ALLOWED_PUBLISHER_HEADERS, &NCHAN_ALLOW_GET_POST_PUT_DELETE);
        }
        else if(cf->sub.poll || cf->sub.longpoll || cf->sub.eventsource || cf->sub.websocket) {
          nchan_OPTIONS_respond(r, &cf->allow_origin, &NCHAN_ACCESS_CONTROL_ALLOWED_SUBSCRIBER_HEADERS, &NCHAN_ALLOW_GET);
        }
        else goto forbidden;
        break;
    }
  }
  ctx->request_ran_content_handler = 1;
  return rc;
  
forbidden:
  nchan_respond_status(r, NGX_HTTP_FORBIDDEN, NULL, 0);
  ctx->request_ran_content_handler = 1;
  return NGX_OK;

bad_msgid:
  nchan_respond_cstring(r, NGX_HTTP_BAD_REQUEST, &NCHAN_CONTENT_TYPE_TEXT_PLAIN, "Message ID invalid", 0);
  ctx->request_ran_content_handler = 1;
  return NGX_OK;
  
}

static ngx_int_t channel_info_callback(ngx_int_t status, void *rptr, void *pd) {
  ngx_http_request_t *r = nchan_get_safe_request_ptr(pd);
  if(r == NULL) {
    return NGX_ERROR;
  }
  nchan_http_finalize_request(r, nchan_response_channel_ptr_info( (nchan_channel_t *)rptr, r, 0));
  return NGX_OK;
}

static void clear_request_pointer(safe_request_ptr_t *pdata) {
  if(pdata) {
    pdata->r = NULL;
  }
}

static safe_request_ptr_t *nchan_set_safe_request_ptr(ngx_http_request_t *r) {
  safe_request_ptr_t           *data = ngx_alloc(sizeof(*data), ngx_cycle->log);
  ngx_http_cleanup_t           *cln = ngx_http_cleanup_add(r, 0);
  
  if(!data || !cln) {
    nchan_log_request_error(r, "couldn't allocate request cleanup stuff.");
    if(cln) {
      cln->data = NULL;
      cln->handler = (ngx_http_cleanup_pt )clear_request_pointer;
    }
    nchan_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
    return NULL;
  }
  
  data->cln = cln;
  data->r = r;
  
  cln->data = data;
  cln->handler = (ngx_http_cleanup_pt )clear_request_pointer;
  
  return data;
}

static ngx_http_request_t *nchan_get_safe_request_ptr(safe_request_ptr_t *d) {
  ngx_http_request_t    *r = d->r;
  ngx_http_cleanup_t    *cln = d->cln;
  
  ngx_free(d);
  
  if(r) {
    cln->data = NULL;
  }
  
  return r;
}


static ngx_int_t publish_callback(ngx_int_t status, nchan_channel_t *ch, safe_request_ptr_t *pd) {
  nchan_request_ctx_t   *ctx;
  static nchan_msg_id_t  empty_msgid = NCHAN_ZERO_MSGID;
  
  ngx_http_request_t    *r = nchan_get_safe_request_ptr(pd);
  
  if(r == NULL) { // the request has since disappered
    return NGX_ERROR;
  }
  ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  
  //DBG("publish_callback %V owner %i status %i", ch_id, memstore_channel_owner(ch_id), status);
  switch(status) {
    case NCHAN_MESSAGE_QUEUED:
      //message was queued successfully, but there were no subscribers to receive it.
      ctx->prev_msg_id = ctx->msg_id;
      ctx->msg_id = ch != NULL ? ch->last_published_msg_id : empty_msgid;
      
      nchan_maybe_send_channel_event_message(r, CHAN_PUBLISH);
      nchan_http_finalize_request(r, nchan_response_channel_ptr_info(ch, r, NGX_HTTP_ACCEPTED));
      return NGX_OK;
      
    case NCHAN_MESSAGE_RECEIVED:
      //message was queued successfully, and it was already sent to at least one subscriber
      ctx->prev_msg_id = ctx->msg_id;
      ctx->msg_id = ch != NULL ? ch->last_published_msg_id : empty_msgid;
      
      nchan_maybe_send_channel_event_message(r, CHAN_PUBLISH);
      nchan_http_finalize_request(r, nchan_response_channel_ptr_info(ch, r, NGX_HTTP_CREATED));
      return NGX_OK;
      
    case NGX_ERROR:
    case NGX_HTTP_INTERNAL_SERVER_ERROR:
      //WTF?
      nchan_log_request_error(r, "error publishing message");
      ctx->prev_msg_id = empty_msgid;;
      ctx->msg_id = empty_msgid;
      nchan_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return NGX_ERROR;
      
    default:
      //for debugging, mostly. I don't expect this branch to behit during regular operation
      ctx->prev_msg_id = empty_msgid;;
      ctx->msg_id = empty_msgid;
      nchan_log_request_error(r, "TOTALLY UNEXPECTED error publishing message, status code %i", status);
      nchan_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return NGX_ERROR;
  }
}

static void nchan_publisher_post_request(ngx_http_request_t *r, ngx_str_t *content_type, size_t content_length, ngx_chain_t *request_body_chain, ngx_str_t *channel_id, nchan_loc_conf_t *cf) {
  ngx_buf_t                      *buf;
  struct timeval                  tv;
  nchan_msg_t                    *msg;
  ngx_str_t                      *eventsource_event;
  
  safe_request_ptr_t             *pd;

#if FAKESHARD
  memstore_pub_debug_start();
#endif
  if((msg = ngx_pcalloc(r->pool, sizeof(*msg))) == NULL) {
    nchan_log_request_error(r, "can't allocate msg in request pool");
    nchan_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
    return; 
  }
  msg->shared = 0;
  
  
  if(cf->eventsource_event.len > 0) {
    msg->eventsource_event = cf->eventsource_event;
  }
  else if((eventsource_event = nchan_get_header_value(r, NCHAN_HEADER_EVENTSOURCE_EVENT)) != NULL) {
    msg->eventsource_event = *eventsource_event;
  }
  
  //content type
  if(content_type) {
    msg->content_type = *content_type;
  }
  
  if(content_length == 0) {
    buf = ngx_create_temp_buf(r->pool, 0);
  }
  else if(request_body_chain!=NULL) {
    buf = nchan_chain_to_single_buffer(r->pool, request_body_chain, content_length);
  }
  else {
    nchan_log_request_error(r, "unexpected publisher message request body buffer location. please report this to the nchan developers.");
    nchan_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
    return;
  }
  
  ngx_gettimeofday(&tv);
  msg->id.time = tv.tv_sec;
  msg->id.tag.fixed[0] = 0;
  msg->id.tagactive = 0;
  msg->id.tagcount = 1;
  
  msg->buf = buf;
#if NCHAN_MSG_LEAK_DEBUG
  msg->lbl = r->uri;
#endif
#if NCHAN_BENCHMARK
  nchan_request_ctx_t            *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  msg->start_tv = ctx->start_tv;
#endif
  
  
  if((pd = nchan_set_safe_request_ptr(r)) == NULL) {
    return;
  }
  
  cf->storage_engine->publish(channel_id, msg, cf, (callback_pt) &publish_callback, pd);
  nchan_update_stub_status(total_published_messages, 1);
#if FAKESHARD
  memstore_pub_debug_end();
#endif
}

typedef struct {
  ngx_str_t       *ch_id;
} nchan_pub_upstream_data_t;

typedef struct {
  ngx_http_post_subrequest_t    psr;
  nchan_pub_upstream_data_t   psr_data;
} nchan_pub_upstream_stuff_t;

static ngx_int_t nchan_publisher_upstream_handler(ngx_http_request_t *sr, void *data, ngx_int_t rc) {
  ngx_http_request_t         *r = sr->parent;
  nchan_pub_upstream_data_t  *d = (nchan_pub_upstream_data_t *)data;
  
  //switch(r->headers_out
  if(rc == NGX_OK) {
    nchan_loc_conf_t          *cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
    ngx_int_t                 code = sr->headers_out.status;
    ngx_str_t                *content_type;
    ngx_int_t                 content_length;
    ngx_chain_t              *request_chain;
    
    switch(code) {
      case NGX_HTTP_OK:
      case NGX_HTTP_CREATED:
      case NGX_HTTP_ACCEPTED:
        if(sr->upstream) {
          content_type = (sr->upstream->headers_in.content_type ? &sr->upstream->headers_in.content_type->value : NULL);
          content_length = nchan_subrequest_content_length(sr);
          request_chain = sr->upstream->out_bufs;
        }
        else {
          content_type = NULL;
          content_length = 0;
          request_chain = NULL;
        }
        nchan_publisher_post_request(r, content_type, content_length, request_chain, d->ch_id, cf);
        break;
      
      case NGX_HTTP_NOT_MODIFIED:
        content_type = (r->headers_in.content_type ? &r->headers_in.content_type->value : NULL);
        content_length = r->headers_in.content_length_n > 0 ? r->headers_in.content_length_n : 0;
        nchan_publisher_post_request(r, content_type, content_length, r->request_body->bufs, d->ch_id, cf);
        break;
        
      case NGX_HTTP_NO_CONTENT:
        //cancel publication
        nchan_http_finalize_request(r, NGX_HTTP_NO_CONTENT);
        break;
      
      default:
        nchan_http_finalize_request(r, NGX_HTTP_FORBIDDEN);
    }
  }
  else {
    nchan_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
  }
  
  return NGX_OK;
}

static void nchan_publisher_body_handler_continued(ngx_http_request_t *r, ngx_str_t *channel_id, nchan_loc_conf_t *cf) {
  ngx_http_complex_value_t       *publisher_upstream_request_url_ccv;
  static ngx_str_t                POST_REQUEST_STRING = {4, (u_char *)"POST "};
  safe_request_ptr_t             *pd;
  
  switch(r->method) {
    case NGX_HTTP_GET:
      if((pd = nchan_set_safe_request_ptr(r)) == NULL){
        return;
      }
      cf->storage_engine->find_channel(channel_id, cf, (callback_pt) &channel_info_callback, pd);
      break;
    
    case NGX_HTTP_PUT:
    case NGX_HTTP_POST:
      publisher_upstream_request_url_ccv = cf->publisher_upstream_request_url;
      if(publisher_upstream_request_url_ccv == NULL) {
        ngx_str_t    *content_type = (r->headers_in.content_type ? &r->headers_in.content_type->value : NULL);
        ngx_int_t     content_length = r->headers_in.content_length_n > 0 ? r->headers_in.content_length_n : 0;
        // no need to check for chunked transfer-encoding, nginx automatically sets the 
        // content-length either way.
        
        nchan_publisher_post_request(r, content_type, content_length, r->request_body->bufs, channel_id, cf);
      }
      else {
        nchan_pub_upstream_stuff_t    *psr_stuff;
        
        if((psr_stuff = ngx_palloc(r->pool, sizeof(*psr_stuff))) == NULL) {
          nchan_log_request_error(r, "can't allocate memory for publisher auth subrequest");
          nchan_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
          return;
        }
        
        ngx_http_post_subrequest_t    *psr = &psr_stuff->psr;
        nchan_pub_upstream_data_t     *psrd = &psr_stuff->psr_data;
        ngx_http_request_t            *sr;
        ngx_str_t                      publisher_upstream_request_url;
        
        ngx_http_complex_value(r, publisher_upstream_request_url_ccv, &publisher_upstream_request_url);
        
        psr->handler = nchan_publisher_upstream_handler;
        psr->data = psrd;
        
        psrd->ch_id = channel_id;
        
        ngx_http_subrequest(r, &publisher_upstream_request_url, NULL, &sr, psr, NGX_HTTP_SUBREQUEST_IN_MEMORY);
        nchan_adjust_subrequest(sr, NGX_HTTP_POST, &POST_REQUEST_STRING, r->request_body, r->headers_in.content_length_n, NULL);
      }
      break;
      
    case NGX_HTTP_DELETE:
      if((pd = nchan_set_safe_request_ptr(r)) == NULL){
        return;
      }
      cf->storage_engine->delete_channel(channel_id, cf, (callback_pt) &channel_info_callback, pd);
      nchan_maybe_send_channel_event_message(r, CHAN_DELETE);
      break;
      
    default: 
      nchan_respond_status(r, NGX_HTTP_FORBIDDEN, NULL, 0);
  }
  
}

typedef struct {
  ngx_str_t       *ch_id;
} nchan_pub_subrequest_data_t;

typedef struct {
  ngx_http_post_subrequest_t    psr;
  nchan_pub_subrequest_data_t   psr_data;
} nchan_pub_subrequest_stuff_t;


static ngx_int_t nchan_publisher_body_authorize_handler(ngx_http_request_t *r, void *data, ngx_int_t rc) {
  nchan_pub_subrequest_data_t  *d = data;
  
  if(rc == NGX_OK) {
    nchan_loc_conf_t    *cf = ngx_http_get_module_loc_conf(r->parent, ngx_nchan_module);
    ngx_int_t            code = r->headers_out.status;
    if(code >= 200 && code <299) {
      //authorized. proceed as planned
      nchan_publisher_body_handler_continued(r->parent, d->ch_id, cf);
    }
    else { //anything else means forbidden
      nchan_http_finalize_request(r->parent, NGX_HTTP_FORBIDDEN);
    }
  }
  else {
    nchan_http_finalize_request(r->parent, rc);
  }
  return NGX_OK;
}

static void nchan_publisher_body_handler(ngx_http_request_t *r) {
  ngx_str_t                      *channel_id;
  nchan_loc_conf_t               *cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  ngx_table_elt_t                *content_length_elt;
  ngx_http_complex_value_t       *authorize_request_url_ccv = cf->authorize_request_url;
  
  if((channel_id = nchan_get_channel_id(r, PUB, 1))==NULL) {
    nchan_http_finalize_request(r, r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR);
    return;
  }
  
  if(!authorize_request_url_ccv) {
    nchan_publisher_body_handler_continued(r, channel_id, cf);
  }
  else {
    nchan_pub_subrequest_stuff_t   *psr_stuff;
    
    if((psr_stuff = ngx_palloc(r->pool, sizeof(*psr_stuff))) == NULL) {
      nchan_log_request_error(r, "can't allocate memory for publisher auth subrequest");
      nchan_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return;
    }
    
    ngx_http_post_subrequest_t    *psr = &psr_stuff->psr;
    nchan_pub_subrequest_data_t   *psrd = &psr_stuff->psr_data;
    ngx_http_request_t            *sr;
    ngx_str_t                      auth_request_url;
    
    ngx_http_complex_value(r, authorize_request_url_ccv, &auth_request_url);
    
    psr->handler = nchan_publisher_body_authorize_handler;
    psr->data = psrd;
    
    psrd->ch_id = channel_id;
    
    ngx_http_subrequest(r, &auth_request_url, NULL, &sr, psr, 0);
    
    if((sr->request_body = ngx_pcalloc(r->pool, sizeof(ngx_http_request_body_t))) == NULL) {
      nchan_log_request_error(r, "can't allocate memory for publisher auth subrequest body");
      nchan_http_finalize_request(r, r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR);
      return;
    }
    if((content_length_elt = ngx_palloc(r->pool, sizeof(*content_length_elt))) == NULL) {
      nchan_log_request_error(r, "can't allocate memory for publisher auth subrequest content-length header");
      nchan_http_finalize_request(r, r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR);
      return;
    }
    
    if(sr->headers_in.content_length) {
      *content_length_elt = *sr->headers_in.content_length;
      content_length_elt->value.len=1;
      content_length_elt->value.data=(u_char *)"0";
      sr->headers_in.content_length = content_length_elt;
    }
    
    sr->headers_in.content_length_n = 0;
    sr->args = r->args;
    sr->header_only = 1;
  }
}

#if NCHAN_BENCHMARK
int nchan_timeval_subtract(struct timeval *result, struct timeval *x, struct timeval *y) {
  /* Perform the carry for the later subtraction by updating y. */
  if (x->tv_usec < y->tv_usec) {
    int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
    y->tv_usec -= 1000000 * nsec;
    y->tv_sec += nsec;
  }
  if (x->tv_usec - y->tv_usec > 1000000) {
    int nsec = (x->tv_usec - y->tv_usec) / 1000000;
    y->tv_usec += 1000000 * nsec;
    y->tv_sec -= nsec;
  }

  /* Compute the time remaining to wait.
     tv_usec is certainly positive. */
  result->tv_sec = x->tv_sec - y->tv_sec;
  result->tv_usec = x->tv_usec - y->tv_usec;

  /* Return 1 if result is negative. */
  return x->tv_sec < y->tv_sec;
}
#endif


#if NCHAN_SUBSCRIBER_LEAK_DEBUG

subscriber_t *subdebug_head = NULL;

void subscriber_debug_add(subscriber_t *sub) {
  if(subdebug_head == NULL) {
    sub->dbg_next = NULL;
    sub->dbg_prev = NULL;
  }
  else {
    sub->dbg_next = subdebug_head;
    sub->dbg_prev = NULL;
    assert(subdebug_head->dbg_prev == NULL);
    subdebug_head->dbg_prev = sub;
  }
  subdebug_head = sub;
}
void subscriber_debug_remove(subscriber_t *sub) {
  subscriber_t *prev, *next;
  prev = sub->dbg_prev;
  next = sub->dbg_next;
  if(subdebug_head == sub) {
    assert(sub->dbg_prev == NULL);
    if(next) {
      next->dbg_prev = NULL;
    }
    subdebug_head = next;
  }
  else {
    if(prev) {
      prev->dbg_next = next;
    }
    if(next) {
      next->dbg_prev = prev;
    }
  }
  
  sub->dbg_next = NULL;
  sub->dbg_prev = NULL;
}
void subscriber_debug_assert_isempty(void) {
  assert(subdebug_head == NULL);
}
#endif
