#include <nchan_module.h>
#include <assert.h>
#include "common.h"

#if FAKESHARD
#include <store/memory/store.h>
#endif

ngx_int_t nchan_subscriber_subscribe(subscriber_t *sub, ngx_str_t *ch_id) {
  return sub->cf->storage_engine->subscribe(ch_id, sub);
}

typedef struct {
  subscriber_t    *sub;
  ngx_str_t       *ch_id;
  ngx_int_t        rc;
  ngx_int_t        http_response_code;
} nchan_auth_subrequest_data_t;

typedef struct {
  ngx_http_post_subrequest_t     psr;
  nchan_auth_subrequest_data_t   psr_data;
} nchan_auth_subrequest_stuff_t;

static void subscriber_authorize_timer_callback_handler(ngx_event_t *ev) {
  nchan_auth_subrequest_data_t *d = ev->data;
  
  d->sub->fn->release(d->sub, 1);
  
  if(d->rc == NGX_OK) {
    ngx_int_t code = d->http_response_code;
    if(code >= 200 && code <299) {
      //authorized. proceed as planned
      nchan_subscriber_subscribe(d->sub, d->ch_id);
    }
    else { //anything else means forbidden
      d->sub->fn->respond_status(d->sub, NGX_HTTP_FORBIDDEN, NULL); //auto-closes subscriber
    }
  }
  else {
    d->sub->fn->respond_status(d->sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL); //auto-closes subscriber
  }

}

static ngx_int_t subscriber_authorize_callback(ngx_http_request_t *r, void *data, ngx_int_t rc) {
  nchan_auth_subrequest_data_t  *d = data;
  ngx_event_t                   *timer;
  
  if (rc == NGX_HTTP_CLIENT_CLOSED_REQUEST) {
    d->sub->fn->release(d->sub, 1);
    //subscriber will be cleaned up and destroyed because this happens before the 
    //subscriber's sudden_abort_handler is called
  }
  else {
    d->rc = rc;
    d->http_response_code = r->headers_out.status;
    if((timer = ngx_pcalloc(r->pool, sizeof(*timer))) == NULL) {
      return NGX_ERROR;
    }
    timer->handler = subscriber_authorize_timer_callback_handler;
    timer->log = d->sub->request->connection->log;
    timer->data = data;
    
    ngx_add_timer(timer, 0); //not sure if this needs to be done like this, but i'm just playing it safe here.
  }
  
  return NGX_OK;
}

ngx_int_t nchan_subscriber_authorize_subscribe(subscriber_t *sub, ngx_str_t *ch_id) {
  
  ngx_http_complex_value_t  *authorize_request_url_ccv = sub->cf->authorize_request_url;
  ngx_str_t                  auth_request_url;
  
  if(!authorize_request_url_ccv) {
    return nchan_subscriber_subscribe(sub, ch_id);
  }
  else {
    nchan_auth_subrequest_stuff_t *psr_stuff = ngx_palloc(sub->request->pool, sizeof(*psr_stuff));
    assert(psr_stuff != NULL);
    
    //ngx_http_request_t            *fake_parent_req = fake_cloned_parent_request(sub->request);
    
    ngx_http_post_subrequest_t    *psr = &psr_stuff->psr;
    nchan_auth_subrequest_data_t  *psrd = &psr_stuff->psr_data;
    ngx_http_request_t            *sr;
    
    ngx_http_complex_value(sub->request, authorize_request_url_ccv, &auth_request_url);
    
    sub->fn->reserve(sub);
    
    psr->handler = subscriber_authorize_callback;
    psr->data = psrd;
    
    psrd->sub = sub;
    psrd->ch_id = ch_id;
    
    ngx_http_subrequest(sub->request, &auth_request_url, NULL, &sr, psr, 0);
    
    sr->request_body = ngx_pcalloc(sub->request->pool, sizeof(ngx_http_request_body_t)); //dummy request body 
    if (sr->request_body == NULL) {
      return NGX_ERROR;
    }
    
    sr->header_only = 1;
    
    sr->args = sub->request->args;
    
    return NGX_OK;
  }
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
  sub->fn->respond_status(sub, NGX_HTTP_REQUEST_TIMEOUT, &NCHAN_HTTP_STATUS_408);
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}


void nchan_subscriber_init_timeout_timer(subscriber_t *sub, ngx_event_t *ev) {
  ngx_memzero(ev, sizeof(*ev));
  nchan_init_timer(ev, nchan_subscriber_timeout_ev_handler, sub);
}

void nchan_subscriber_init(subscriber_t *sub, const subscriber_t *tmpl, ngx_http_request_t *r, nchan_msg_id_t *msgid) {
  nchan_request_ctx_t  *ctx = NULL;
  *sub = *tmpl;
  sub->request = r;
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

void nchan_subscriber_common_setup(subscriber_t *sub, subscriber_type_t type, ngx_str_t *name, subscriber_fn_t *fn, ngx_int_t dequeue_after_response) {
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(sub->request, ngx_nchan_module);
  sub->type = type;
  sub->name = name;
  sub->fn = fn;
  
  sub->dequeue_after_response = dequeue_after_response;
  if(ctx) {
    ctx->subscriber_type = sub->name;
  }
}

ngx_int_t nchan_subscriber_empty_notify(subscriber_t *self, ngx_int_t code, void *data) {
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

void ngx_init_set_membuf(ngx_buf_t *buf, u_char *start, u_char *end) {
  ngx_memzero(buf, sizeof(*buf));
  buf->start = start;
  buf->pos = start;
  buf->end = end;
  buf->last = end;
  buf->memory = 1;
}

void ngx_init_set_membuf_str(ngx_buf_t *buf, ngx_str_t *str) {
  ngx_memzero(buf, sizeof(*buf));
  buf->start = str->data;
  buf->pos = str->data;
  buf->end = str->data + str->len;
  buf->last = buf->end;
  buf->memory = 1;
}


