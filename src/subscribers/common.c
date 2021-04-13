#include <nchan_module.h>
#include <assert.h>
#include "common.h"
//#include <util/nchan_fake_request.h>
#include <util/nchan_subrequest.h>

#include <util/nchan_fake_request.h>

#if FAKESHARD
#include <store/memory/store.h>
#endif

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:COMMON:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:COMMON:" fmt, ##arg)

typedef struct {
  subscriber_t       *sub;
  ngx_str_t          *ch_id;
  nchan_fakereq_subrequest_data_t *subrequest;
} nchan_subscribe_auth_request_data_t;

static ngx_int_t subscriber_authorize_callback(ngx_int_t rc, ngx_http_request_t *sr, void *data) {
  nchan_subscribe_auth_request_data_t *d = data;
  subscriber_t                        *sub = d->sub;
  
  if(sub->status == DEAD) {
    nchan_requestmachine_request_cleanup_manual(d->subrequest);
    sub->fn->release(d->sub, 0);
  }
  else if (rc == NGX_HTTP_CLIENT_CLOSED_REQUEST) {
    //this shouldn't happen, but if it does, no big deal
    nchan_requestmachine_request_cleanup_manual(d->subrequest);
    sub->fn->release(d->sub, 1);
    sub->fn->respond_status(sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, NULL); //couldn't reach upstream
  }
  else if(rc == NGX_OK) {
    ngx_int_t        code = sr->headers_out.status;
    sub->fn->release(sub, 1);
    if(code >= 200 && code <299) {
      nchan_requestmachine_request_cleanup_manual(d->subrequest);
      nchan_subscriber_subscribe(sub, d->ch_id);
    }
    else {
      //forbidden, but with some data to forward to the subscriber
      ngx_http_request_t       *r = d->sub->request;
      ngx_str_t                *content_type;
      ngx_int_t                 content_length;
      ngx_chain_t              *request_chain = NULL;
      content_type = (sr->upstream->headers_in.content_type ? &sr->upstream->headers_in.content_type->value : NULL);
      content_length = nchan_subrequest_content_length(sr);
      if(content_length > 0) {
#if nginx_version >= 1013010
        request_chain = sr->out;
#else
        request_chain = sr->upstream->out_bufs;
#endif
      }
      //copy headers
      ngx_uint_t                       i;
      ngx_list_part_t                 *part = &sr->headers_out.headers.part;
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
        if (!nchan_strmatch(&header[i].key, 4, "Content-Type", "Server", "Content-Length", "Connection")) {
          //copy header to main request's response
          nchan_add_response_header(r, &header[i].key, &header[i].value);
        }
      }
      
      if(content_type) {
        r->headers_out.content_type = *content_type;
      }
      r->headers_out.content_length_n = content_length;
      nchan_requestmachine_request_cleanup_on_request_finalize(d->subrequest, r);
      sub->fn->respond_status(sub, code, NULL, request_chain); //auto-closes subscriber
    }
  }
  else if(rc >= 500 && rc < 600) {
    nchan_requestmachine_request_cleanup_manual(d->subrequest);
    sub->fn->release(d->sub, 1);
    sub->fn->respond_status(sub, rc, NULL, NULL); //auto-closes subscriber
  }
  else {
    nchan_requestmachine_request_cleanup_manual(d->subrequest);
    sub->fn->release(d->sub, 1);
    d->sub->fn->respond_status(d->sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, NULL); //auto-closes subscriber
  }
  
  return NGX_OK;
}

ngx_int_t nchan_subscriber_authorize_subscribe_request(subscriber_t *sub, ngx_str_t *ch_id) {
  
  ngx_http_complex_value_t  *authorize_request_url_ccv = sub->cf->authorize_request_url;
  //DBG("%p (req %p) nchan_subscriber_authorize_subscribe_request", sub, sub->request);
  
  if(!authorize_request_url_ccv) {
    return nchan_subscriber_subscribe(sub, ch_id);
  }
  else {
    nchan_requestmachine_request_params_t param;
    param.url.cv = authorize_request_url_ccv;
    param.url_complex = 1;
    param.pool = ngx_create_pool(1024, ngx_cycle->log);
    param.body = NULL;
    param.response_headers_only = 0;
    param.manual_cleanup = 1;
    
    nchan_subscribe_auth_request_data_t  *d = ngx_palloc(param.pool, sizeof(*d));
    if(!d) {
      ngx_destroy_pool(param.pool);
      return NGX_ERROR;
    }
    param.cb = (callback_pt )subscriber_authorize_callback;
    param.pd = d;
    
    d->sub = sub;
    d->ch_id = ch_id;
    d->subrequest = nchan_subscriber_subrequest(sub, &param);
    if(d->subrequest != NULL) {
      sub->fn->reserve(sub);
      return NGX_OK;
    }
    else {
      ngx_destroy_pool(param.pool);
      return NGX_ERROR;
    }
  }
}

static ngx_int_t nchan_subscriber_subrequest_fire_and_forget(subscriber_t *sub, ngx_http_complex_value_t *url_cv) {
  //DBG("%p (req %p) nchan_subscriber_unsubscribe_request", sub, sub->request);
    
  nchan_requestmachine_request_params_t param;
  param.url.cv = url_cv;
  param.url_complex = 1;
  param.cb = NULL;
  param.pd = NULL;
  param.pool = NULL;
  param.body = NULL;
  param.response_headers_only = 1;
  param.manual_cleanup = 0;
  
  return nchan_subscriber_subrequest(sub, &param) == NULL ? NGX_ERROR : NGX_OK;
}

ngx_int_t nchan_subscriber_unsubscribe_request(subscriber_t *sub) {
  nchan_request_ctx_t         *ctx;
  //DBG("%p (req %p) nchan_subscriber_unsubscribe_request", sub, sub->request);
  
  if(!sub->enable_sub_unsub_callbacks) {
    return NGX_OK;
  }
  
  ctx = ngx_http_get_module_ctx(sub->request, ngx_nchan_module);
  if(ctx->sent_unsubscribe_request) {
    return NGX_OK;
  }
  
  ctx->sent_unsubscribe_request = 1;
  return nchan_subscriber_subrequest_fire_and_forget(sub, sub->cf->unsubscribe_request_url);
}

ngx_int_t nchan_subscriber_subscribe_request(subscriber_t *sub) {
  if(!sub->enable_sub_unsub_callbacks) {
    return NGX_OK;
  }
  return nchan_subscriber_subrequest_fire_and_forget(sub, sub->cf->subscribe_request_url);
}



ngx_int_t nchan_subscriber_subscribe(subscriber_t *sub, ngx_str_t *ch_id) {
  ngx_int_t             ret;
  nchan_request_ctx_t  *ctx;
  nchan_loc_conf_t     *cf = sub->cf;
  int                   enable_sub_unsub_callbacks = sub->enable_sub_unsub_callbacks;
  
  //DBG("%p (req %p) nchan_subscriber_subscribe", sub, sub->request);
  
  ctx = sub->request ? ngx_http_get_module_ctx(sub->request, ngx_nchan_module) : NULL;
  ret = sub->cf->storage_engine->subscribe(ch_id, sub);
  //don't access sub directly, it might have already been freed
  if(ret == NGX_OK && enable_sub_unsub_callbacks && cf->subscribe_request_url && ctx && ctx->sub == sub) {
    nchan_subscriber_subscribe_request(sub);
  }
  return ret;
}

ngx_int_t nchan_cleverly_output_headers_only_for_later_response(ngx_http_request_t *r) {
  ngx_int_t                rc;
  static const ngx_str_t   everything_ok = ngx_string("200 OK");
  
  r->headers_out.status_line = everything_ok; //but in reality, we're returning a 200 OK
#if (NGX_HTTP_V2)
  if(r->stream) {
    r->headers_out.status=NGX_HTTP_OK; //no need to fool chunking module
    r->header_only = 0;
  }
  else {
    r->headers_out.status=NGX_HTTP_NO_CONTENT; //fake it to fool the chunking module (mostly);  
    r->header_only = 1;
  }
#elif (NGX_HTTP_SPDY)
   if(r->spdy_stream) {
    r->headers_out.status=NGX_HTTP_OK; //no need to fool chunking module
    r->header_only = 0;
  }
  else {
    r->headers_out.status=NGX_HTTP_NO_CONTENT; //fake it to fool the chunking module (mostly);  
    r->header_only = 1;
  }
#else
  r->headers_out.status=NGX_HTTP_NO_CONTENT; //fake it to fool the chunking module (mostly);  
  r->header_only = 1;
#endif
  nchan_include_access_control_if_needed(r, NULL);
  rc = ngx_http_send_header(r);
  
  if(r->headers_out.status == NGX_HTTP_OK) {
    r->keepalive = 1;
  }
  
  return rc;
}


static void nchan_generate_random_boundary(u_char *buf, int sz) {
  //use the shitty-ass LFSR-based ngx_random. we're not looking for cryptographic randomness, 
  //just something unlikely
  static u_char   itoa64[] ="./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  int             i;
  u_char         *p = buf;
  
  for(i=0; i < sz; i++) {
    *p++ = itoa64[ngx_random() % 64];
  }
}

ngx_str_t *nchan_request_multipart_boundary(ngx_http_request_t *r, nchan_request_ctx_t *ctx) {
  if(ctx) {
    if(!ctx->multipart_boundary) {
      if((ctx->multipart_boundary = ngx_palloc(r->pool, sizeof(ngx_str_t) + 32)) == NULL) {
        //unable to allocate multipart boundary;
        return NULL;
      }
      ctx->multipart_boundary->data=(u_char *)&ctx->multipart_boundary[1];
      ctx->multipart_boundary->len = 32;
      nchan_generate_random_boundary(ctx->multipart_boundary->data, 32);
    }
    return ctx->multipart_boundary;
  }
  else {
    return NULL;
  }
}

ngx_int_t nchan_request_set_content_type_multipart_boundary_header(ngx_http_request_t *r, nchan_request_ctx_t *ctx) {
  u_char                        *cur;
  u_char                        *cbuf;
  ngx_str_t                      val;
  
  if((cbuf = ngx_palloc(r->pool, sizeof(u_char)*100)) == NULL) {
    return NGX_ERROR;
  }
  
  val.data = cbuf;
  cur = ngx_snprintf(cbuf, 100, "multipart/mixed; boundary=%V", nchan_request_multipart_boundary(r, ctx));
  val.len = cur - cbuf;
  
  r->headers_out.content_type = val;
  
  return NGX_OK;
}

void nchan_subscriber_timeout_ev_handler(ngx_event_t *ev) {
  subscriber_t *sub = (subscriber_t *)ev->data;
#if FAKESHARD
  memstore_fakeprocess_push(sub->owner);
#endif
  sub->dequeue_after_response = 1;
  sub->fn->respond_status(sub, NGX_HTTP_REQUEST_TIMEOUT, &NCHAN_HTTP_STATUS_408, NULL);
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}


void nchan_subscriber_init_timeout_timer(subscriber_t *sub, ngx_event_t *ev) {
  ngx_memzero(ev, sizeof(*ev));
  nchan_init_timer(ev, nchan_subscriber_timeout_ev_handler, sub);
}

nchan_fakereq_subrequest_data_t *nchan_subscriber_subrequest(subscriber_t *sub, nchan_requestmachine_request_params_t *params) {
  if(sub->upstream_requestmachine == NULL) {
    sub->upstream_requestmachine = ngx_calloc(sizeof(nchan_requestmachine_t), ngx_cycle->log);
    if(sub->upstream_requestmachine == NULL) {
      nchan_log_error("failed to allocate upstream_requestmachine for subscriber %p", sub);
      return NULL;
    }
    else {
      nchan_requestmachine_initialize(sub->upstream_requestmachine, sub->request);
    }
  }
  
  return nchan_requestmachine_request(sub->upstream_requestmachine, params);
}

ngx_int_t nchan_subscriber_subrequest_cleanup(subscriber_t *sub) {
  if(sub->upstream_requestmachine != NULL) {
    nchan_requestmachine_shutdown(sub->upstream_requestmachine);
    ngx_free(sub->upstream_requestmachine);
    sub->upstream_requestmachine = NULL;
  }
  return NGX_OK;
}

void nchan_subscriber_init(subscriber_t *sub, const subscriber_t *tmpl, ngx_http_request_t *r, nchan_msg_id_t *msgid) {
  nchan_request_ctx_t  *ctx = NULL;
  *sub = *tmpl;
  sub->request = r;
  sub->upstream_requestmachine = NULL;
  if(r) {
    ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
    sub->cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  }
  sub->reserved = 0;
  sub->enqueued = 0;
  sub->status = ALIVE;
  
  if(msgid) {
    nchan_copy_new_msg_id(&sub->last_msgid, msgid);
  }
  else {
    sub->last_msgid.time = 0;
    sub->last_msgid.tag.fixed[0] = 0;
    sub->last_msgid.tagcount = 1;
  }
  
  if(ctx) {
    ctx->prev_msg_id = sub->last_msgid;
    ctx->sub = sub;
    ctx->subscriber_type = sub->name;
  }
  
#if FAKESHARD
  sub->owner = memstore_slot();
#endif
  
}

void nchan_subscriber_common_setup(subscriber_t *sub, subscriber_type_t type, ngx_str_t *name, subscriber_fn_t *fn, ngx_int_t enable_sub_unsub_callbacks, ngx_int_t dequeue_after_response) {
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(sub->request, ngx_nchan_module);
  sub->type = type;
  sub->name = name;
  sub->fn = fn;
  sub->enable_sub_unsub_callbacks = enable_sub_unsub_callbacks;
  sub->dequeue_after_response = dequeue_after_response;
  if(ctx) {
    ctx->subscriber_type = sub->name;
  }
}

ngx_int_t nchan_subscriber_receive_notice(subscriber_t *self, ngx_int_t code, void *data) {
  if(code == NCHAN_NOTICE_SUBSCRIBER_INFO_REQUEST) {
    nchan_loc_conf_t     *cf = self->cf;
    ngx_str_t             result;
    int                   result_allocd = 0;
    ngx_str_t             content_type = ngx_string("text/plain");
    ngx_str_t             bad_info_string = ngx_string("bad subscriber info string");
    intptr_t              response_id = (intptr_t )data;
    if(!cf->subscriber_info_string) {
      result = bad_info_string;
    }
    else {
      if(ngx_http_complex_value_alloc(self->request, cf->subscriber_info_string, &result, 4096) == NGX_ERROR) {
        result = bad_info_string;
      }
      else {
        result_allocd = 1;
      }
    }
    
    ngx_str_t *response_channel_id = nchan_get_subscriber_info_response_channel_id(self->request, response_id);
    
    nchan_msg_t             msg;
    ngx_memzero(&msg, sizeof(msg));
    msg.id.time = 0;
    msg.id.tag.fixed[0]=0;
    msg.id.tagcount=1;
    msg.id.tagactive=0;
    
    msg.content_type=&content_type;
    
    msg.storage = NCHAN_MSG_STACK;
    
    msg.buf.temporary = 1;
    msg.buf.memory = 1;
    msg.buf.last_buf = 1;
    msg.buf.pos = result.data;
    msg.buf.last = result.data + result.len;
    msg.buf.start = msg.buf.pos;
    msg.buf.end = msg.buf.last;
    
    cf->storage_engine->publish(response_channel_id, &msg, cf, NULL, NULL);
    
    if(result_allocd) {
      ngx_http_complex_value_free(&result);
    }
  }
  return NGX_OK;
}


#define MSGID_BUF_LEN (10*255)
typedef struct msgidbuf_s msgidbuf_t;
struct msgidbuf_s {
  u_char       chr[MSGID_BUF_LEN];
  msgidbuf_t  *prev;
  msgidbuf_t  *next;
};

static void *msgidbuf_alloc(void *pd) {
  return ngx_palloc((ngx_pool_t *)pd, sizeof(msgidbuf_t));
}

ngx_int_t nchan_subscriber_init_msgid_reusepool(nchan_request_ctx_t *ctx, ngx_pool_t *request_pool) {
  ctx->output_str_queue = ngx_palloc(request_pool, sizeof(*ctx->output_str_queue));
  nchan_reuse_queue_init(ctx->output_str_queue, offsetof(msgidbuf_t, prev), offsetof(msgidbuf_t, next), msgidbuf_alloc, NULL, request_pool);
  return NGX_OK;
}

ngx_str_t nchan_subscriber_set_recyclable_msgid_str(nchan_request_ctx_t *ctx, nchan_msg_id_t *msgid) {
  ngx_str_t               ret;
  msgidbuf_t             *msgidbuf;
  
  msgidbuf = nchan_reuse_queue_push(ctx->output_str_queue);
  ret.data = &msgidbuf->chr[0];
  
  nchan_strcpy(&ret, msgid_to_str(msgid), MSGID_BUF_LEN);
  
  return ret;
}

ngx_int_t nchan_subscriber_publish_info(subscriber_t *sub, uintptr_t destination_channel_id_number) {
  return NGX_OK;
}
