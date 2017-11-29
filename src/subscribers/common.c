#include <nchan_module.h>
#include <assert.h>
#include "common.h"
//#include <util/nchan_fake_request.h>
#include <util/nchan_subrequest.h>

#if FAKESHARD
#include <store/memory/store.h>
#endif

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:COMMON:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:COMMON:" fmt, ##arg)

static ngx_int_t flush_postponed_data_if_needed(ngx_http_request_t *r);

typedef struct {
  subscriber_t       *sub;
  ngx_str_t          *ch_id;
  ngx_int_t           rc;
  ngx_http_request_t *subrequest;
  ngx_int_t           http_response_code;
  ngx_http_cleanup_t *timer_cleanup;
} nchan_subrequest_data_t;


typedef struct {
  subscriber_t            *sub;
  subrequest_callback_pt   cb;
  void                    *cb_data;
} nchan_subrequest_data_cb_t;

typedef struct {
  ngx_http_post_subrequest_t     psr;
  nchan_subrequest_data_t        psr_data;
} nchan_subrequest_stuff_t;

static ngx_int_t subscriber_subrequest_handler(ngx_http_request_t *sr, void *pd, ngx_int_t rc) {
  nchan_subrequest_data_cb_t    *psrd = (nchan_subrequest_data_cb_t *)pd;
  ngx_http_request_t            *r = psrd->sub->request;
  
  flush_postponed_data_if_needed(r);
  
  psrd->sub->fn->release(psrd->sub, 1);
  
  if(psrd->cb) {
    psrd->cb(psrd->sub, sr, rc, psrd->cb_data);
  }
  return NGX_OK;
}


ngx_http_request_t *subscriber_cv_subrequest(subscriber_t *sub, ngx_http_complex_value_t *url_ccv, ngx_buf_t *body, subrequest_callback_pt cb, void *cb_data) {
  ngx_str_t                     request_url;
  ngx_http_complex_value(sub->request, url_ccv, &request_url);
  return subscriber_subrequest(sub, &request_url, body, cb, cb_data);
}


ngx_http_request_t *subscriber_subrequest(subscriber_t *sub, ngx_str_t *url, ngx_buf_t *body, subrequest_callback_pt cb, void *cb_data) {
  ngx_http_request_t            *r = sub->request;
  ngx_http_post_subrequest_t    *psr = ngx_pcalloc(r->pool, sizeof(*psr));
  nchan_subrequest_data_cb_t    *psrd = ngx_pcalloc(r->pool, sizeof(*psrd));
  ngx_http_request_t            *sr;
  
  //DBG("%p (req %p) subrequest", sub, r);

  sub->fn->reserve(sub);
  
  psr->handler = subscriber_subrequest_handler;
  psr->data = psrd;
  psrd->sub = sub;
  psrd->cb_data = cb_data;
  psrd->cb = cb;
  
  ngx_http_subrequest(r, url, NULL, &sr, psr, NGX_HTTP_SUBREQUEST_IN_MEMORY);
  
  if((sr->request_body = ngx_pcalloc(r->pool, sizeof(*sr->request_body))) == NULL) { //dummy request body 
    return NULL;
  }
  
  if(body && ngx_buf_size(body) > 0) {
    static ngx_str_t                   POST_REQUEST_STRING = {4, (u_char *)"POST "};
    size_t                             sz;
    ngx_http_request_body_t           *sr_body = sr->request_body;
    ngx_chain_t                       *fakebody_chain;
    ngx_buf_t                         *fakebody_buf;
    
    fakebody_chain = ngx_palloc(r->pool, sizeof(*fakebody_chain));
    fakebody_buf = ngx_palloc(r->pool, sizeof(*fakebody_buf));
    sr_body->bufs = fakebody_chain;
    fakebody_chain->next = NULL;
    fakebody_chain->buf = fakebody_buf;
    ngx_memzero(fakebody_buf, sizeof(*fakebody_buf));
    fakebody_buf->last_buf = 1;
    fakebody_buf->last_in_chain = 1;
    fakebody_buf->flush = 1;
    fakebody_buf->memory = 1;
    
    //just copy the buffer contents. it's inefficient but I don't care at the moment.
    //this can and should be optimized later
    sz = ngx_buf_size(body);
    fakebody_buf->start = ngx_palloc(r->pool, sz); //huuh?
    ngx_memcpy(fakebody_buf->start, body->start, sz);
    fakebody_buf->end = fakebody_buf->start + sz;
    fakebody_buf->pos = fakebody_buf->start;
    fakebody_buf->last = fakebody_buf->end;
    
    nchan_adjust_subrequest(sr, NGX_HTTP_POST, &POST_REQUEST_STRING, sr_body, sz, NULL);
  }
  else {
    sr->header_only = 1;
  }
  sr->args = sub->request->args;
  
  return sr;
}

static ngx_int_t generic_subscriber_subrequest_old(subscriber_t *sub, ngx_http_complex_value_t *url_ccv, ngx_int_t (*handler)(ngx_http_request_t *, void *, ngx_int_t), ngx_http_request_t **subrequest, ngx_str_t *chid) {
  ngx_str_t                  request_url;
  nchan_subrequest_stuff_t  *psr_stuff = ngx_palloc(sub->request->pool, sizeof(*psr_stuff));
  assert(psr_stuff != NULL);
  
  //DBG("%p (req %p) generic_subscriber_subrequest_old", sub, sub->request);
  
  //ngx_http_request_t            *fake_parent_req = fake_cloned_parent_request(sub->request);
  
  ngx_http_post_subrequest_t    *psr = &psr_stuff->psr;
  nchan_subrequest_data_t       *psrd = &psr_stuff->psr_data;
  ngx_http_request_t            *sr;
  
  ngx_http_complex_value(sub->request, url_ccv, &request_url);
  
  sub->fn->reserve(sub);
  
  psr->handler = handler;
  psr->data = psrd;
  
  psrd->sub = sub;
  if(chid) {
    psrd->ch_id = chid;
  }
  
  ngx_http_subrequest(sub->request, &request_url, NULL, &sr, psr, NGX_HTTP_SUBREQUEST_IN_MEMORY);
  
  sr->request_body = ngx_pcalloc(sub->request->pool, sizeof(ngx_http_request_body_t)); //dummy request body 
  if (sr->request_body == NULL) {
    return NGX_ERROR;
  }
  
  sr->header_only = 1;
  
  sr->args = sub->request->args;
  if(subrequest) {
    *subrequest = sr;
  }
  
  return NGX_OK;
}

static void subscriber_authorize_timer_callback_handler(ngx_event_t *ev) {
  nchan_subrequest_data_t *d = ev->data;
  
  d->timer_cleanup->data = NULL;
  
  //DBG("%p (req %p) subscriber_authorize callback handler", d->sub, d->sub->request);
  d->sub->fn->release(d->sub, 1);
  
  if(d->rc == NGX_OK) {
    ngx_int_t code = d->http_response_code;
    if(code >= 200 && code <299) {
      //authorized. proceed as planned
      
      //get subscribe callback data from sub in advance, in case it is destroyed during nchan_subscriber_subscribe()
      ngx_connection_t  *c = NULL;
      int                enabled_subscribe_callback = d->sub->enable_sub_unsub_callbacks;
      if(enabled_subscribe_callback) {
        c = d->sub->request->connection;
      }
      
      nchan_subscriber_subscribe(d->sub, d->ch_id);
      if(enabled_subscribe_callback) {
        //there might be a subscribe subrequest we need to run
        //because we're in a timer event outside the request processing loop,
        //the subrequest handling must be initiated manually
        ngx_http_run_posted_requests(c);
      }
    }
    else if(d->sub->status != DEAD && d->subrequest && d->subrequest->upstream) {
      //forbidden, but with some data to forward to the subscriber
      ngx_http_request_t       *sr = d->subrequest;
      ngx_http_request_t       *r = d->sub->request;
      ngx_str_t                *content_type;
      ngx_int_t                 content_length;
      ngx_chain_t              *request_chain;
      content_type = (sr->upstream->headers_in.content_type ? &sr->upstream->headers_in.content_type->value : NULL);
      content_length = nchan_subrequest_content_length(sr);
      request_chain = sr->upstream->out_bufs;
      
      if(content_type) {
        r->headers_out.content_type = *content_type;
      }
      r->headers_out.content_length_n = content_length;
      
      d->sub->fn->respond_status(d->sub, code, NULL, request_chain); //auto-closes subscriber
    }
    else {
      //forbidden. leave me alone, no data for you
      d->sub->fn->respond_status(d->sub, code, NULL, NULL); //auto-closes subscriber
    }
  }
  else if(d->rc >= 500 && d->rc < 600) {
    d->sub->fn->respond_status(d->sub, d->rc, NULL, NULL); //auto-closes subscriber
  }
  else {
    d->sub->fn->respond_status(d->sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, NULL); //auto-closes subscriber
  }

}

static void subscriber_authorize_timer_callback_cleanup(ngx_event_t *timer) {
  if(timer) {
    ngx_del_timer(timer);
  }
}

static ngx_int_t subscriber_authorize_callback(ngx_http_request_t *r, void *data, ngx_int_t rc) {
  nchan_subrequest_data_t       *d = data;
  ngx_event_t                   *timer;
  //DBG("%p (req %p) generic_subscriber_subrequest_old", d->sub, d->sub->request);
  
  if (rc == NGX_HTTP_CLIENT_CLOSED_REQUEST) {
    d->sub->fn->release(d->sub, 1);
    //subscriber will be cleaned up and destroyed because this happens before the 
    //subscriber's sudden_abort_handler is called
  }
  else {
    ngx_http_cleanup_t           *cln = ngx_http_cleanup_add(r, 0);
    if(!cln) {
      return NGX_ERROR;
    }
    
    d->rc = rc;
    d->http_response_code = r->headers_out.status;
    d->timer_cleanup = cln;
    if(r->pool == d->sub->request->pool) {
      d->subrequest = r;
    }
    else {
      //different pools -- not safe to use the subrequest later.
      d->subrequest = NULL;
    }
    
    //copy headers
    ngx_uint_t                       i;
    ngx_list_part_t                 *part = &r->headers_out.headers.part;
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
        nchan_add_response_header(d->sub->request, &header[i].key, &header[i].value);
      }
    }
    
    if((timer = ngx_pcalloc(r->pool, sizeof(*timer))) == NULL) {
      return NGX_ERROR;
    }
    timer->handler = subscriber_authorize_timer_callback_handler;
    timer->log = d->sub->request->connection->log;
    timer->data = data;
    
    cln->data = timer;
    cln->handler = (ngx_http_cleanup_pt )subscriber_authorize_timer_callback_cleanup;
    
    ngx_add_timer(timer, 0); //not sure if this needs to be done like this, but i'm just playing it safe here.
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
    return generic_subscriber_subrequest_old(sub, authorize_request_url_ccv, subscriber_authorize_callback, NULL, ch_id);
  }
}

static ngx_int_t subscriber_unsubscribe_request_callback(ngx_http_request_t *r, void *data, ngx_int_t rc) {
  nchan_subrequest_data_t       *d = data;
  nchan_request_ctx_t           *ctx = ngx_http_get_module_ctx(d->sub->request, ngx_nchan_module);
  ngx_int_t                      finalize_code = ctx->unsubscribe_request_callback_finalize_code;
  
  //DBG("%p (req %p) subscriber_unsubscribe_request_callback", d->sub, d->sub->request);
  
  if(d->sub->request->main->blocked) {
    d->sub->request->main->blocked = 0;
  }
  if(finalize_code != NGX_DONE) {
    nchan_http_finalize_request(d->sub->request, finalize_code);
  }
  
  ctx->unsubscribe_request_callback_finalize_code = NGX_OK;
  d->sub->fn->release(d->sub, 0);
  return NGX_OK;
}

ngx_int_t nchan_subscriber_unsubscribe_request(subscriber_t *sub, ngx_int_t finalize_code) {
  ngx_int_t                    ret;
  //ngx_http_upstream_conf_t    *ucf;
  
  //DBG("%p (req %p) nchan_subscriber_unsubscribe_request", sub, sub->request);
  
  if(!sub->enable_sub_unsub_callbacks) {
    return NGX_OK;
  }
  
  nchan_request_ctx_t         *ctx = ngx_http_get_module_ctx(sub->request, ngx_nchan_module);
  ngx_http_request_t          *subrequest;
  if(!ctx->sent_unsubscribe_request) {
    ctx->unsubscribe_request_callback_finalize_code = finalize_code;
    ret = generic_subscriber_subrequest_old(sub, sub->cf->unsubscribe_request_url, subscriber_unsubscribe_request_callback, &subrequest, NULL);
    ctx->sent_unsubscribe_request = 1;
  }
  else {
    ret = NGX_OK;
  }
  
  //ucf = ngx_http_get_module_loc_conf(subrequest, ngx_http_upstream_module);
  //ucf->ignore_client_abort = 1;
  
  return ret;
}

typedef struct {
  ngx_http_request_t *r;
  ngx_event_t         timer;
} subrequest_flush_postponed_data_t;

static void subrequest_callback_flush_postponed_data(ngx_event_t *ev) {
  subrequest_flush_postponed_data_t *d = ev->data;
  nchan_flush_pending_output(d->r);
}

static void subscriber_subscribe_post_data_cleanup_abort_timer(subrequest_flush_postponed_data_t *d) {
  if(d->timer.timer_set) {
    ngx_del_timer(&d->timer);
  }
}

static ngx_int_t flush_postponed_data_if_needed(ngx_http_request_t *r) {
  if(r->postponed && r->postponed->request && r->postponed->next && r->postponed->next->out) {
    subrequest_flush_postponed_data_t *post_data;
    ngx_event_t                  *timer;
    ngx_http_cleanup_t           *cln = ngx_http_cleanup_add(r, sizeof(*post_data));
    if(!cln) {
      return NGX_ERROR;
    }
    cln->handler = (ngx_http_cleanup_pt )subscriber_subscribe_post_data_cleanup_abort_timer;
    post_data = cln->data;
    post_data->r = r;
    timer = &post_data->timer;
    ngx_memzero(timer, sizeof(*timer));
    nchan_init_timer(timer, subrequest_callback_flush_postponed_data, post_data);
    ngx_add_timer(timer, 0);
  }
  return NGX_OK;
}

static ngx_int_t subscriber_subscribe_callback(ngx_http_request_t *sr, void *data, ngx_int_t rc) {
  nchan_subrequest_data_t       *d = data;
  ngx_http_request_t            *r = d->sub->request;

  flush_postponed_data_if_needed(r);

  d->sub->fn->release(d->sub, 0);
  return NGX_OK;
}


ngx_int_t nchan_subscriber_subscribe_request(subscriber_t *sub) {
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(sub->request, ngx_nchan_module);
  //DBG("%p (req %p) nchan_subscriber_subscribe_request", sub, sub->request);
  if(!ctx->sent_unsubscribe_request) {
    return generic_subscriber_subrequest_old(sub, sub->cf->subscribe_request_url, subscriber_subscribe_callback, NULL, NULL);
  }
  else {
    return NGX_OK;
  }
}


ngx_int_t nchan_subscriber_subscribe(subscriber_t *sub, ngx_str_t *ch_id) {
  ngx_int_t             ret;
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(sub->request, ngx_nchan_module);
  nchan_loc_conf_t     *cf = sub->cf;
  int                   enable_sub_unsub_callbacks = sub->enable_sub_unsub_callbacks;
  
  //DBG("%p (req %p) nchan_subscriber_subscribe", sub, sub->request);
  
  ret = sub->cf->storage_engine->subscribe(ch_id, sub);
  //don't access sub directly, it might have already been freed
  if(ret == NGX_OK && enable_sub_unsub_callbacks && cf->subscribe_request_url && ctx->sub == sub) {
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

#if nginx_version >= 1003015

static void ngx_http_close_request_dup(ngx_http_request_t *r, ngx_int_t rc) {
  ngx_connection_t  *c;

  r = r->main;
  c = r->connection;

  ngx_log_debug2(NGX_LOG_DEBUG_HTTP, c->log, 0,
                  "http request count:%d blk:%d", r->count, r->blocked);

  if (r->count == 0) {
    ngx_log_error(NGX_LOG_ALERT, c->log, 0, "http request count is zero");
  }

  r->count--;

  if (r->count || r->blocked) {
    return;
  }

#if (NGX_HTTP_V2)
  if (r->stream) {
    ngx_http_v2_close_stream(r->stream, rc);
    return;
  }
#endif

  ngx_http_free_request(r, rc);
  ngx_http_close_connection(c);
}

void nchan_subscriber_unsubscribe_callback_http_test_reading(ngx_http_request_t *r) {
  int                n;
  char               buf[1];
  ngx_err_t          err;
  ngx_event_t       *rev;
  ngx_connection_t  *c;
  
  nchan_request_ctx_t  *nchan_ctx;

  c = r->connection;
  rev = c->read;

  ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0, "http test reading");

#if (NGX_HTTP_V2)

  if (r->stream) {
    if (c->error) {
      err = 0;
      goto closed;
    }

    return;
  }

#endif
#if (NGX_HTTP_SPDY)

    if (r->spdy_stream) {
        if (c->error) {
            err = 0;
            goto closed;
        }

        return;
    }

#endif

#if (NGX_HAVE_KQUEUE)

  if (ngx_event_flags & NGX_USE_KQUEUE_EVENT) {

    if (!rev->pending_eof) {
        return;
    }

    rev->eof = 1;
    c->error = 1;
    err = rev->kq_errno;

    goto closed;
  }

#endif

#if (NGX_HAVE_EPOLLRDHUP)
#if nginx_version >= 1011000
  if ((ngx_event_flags & NGX_USE_EPOLL_EVENT) && ngx_use_epoll_rdhup) {
    socklen_t  len;

    if (!rev->pending_eof) {
        return;
    }
#else
  if ((ngx_event_flags & NGX_USE_EPOLL_EVENT) && rev->pending_eof) {
    socklen_t  len;
#endif

    rev->eof = 1;
    c->error = 1;

    err = 0;
    len = sizeof(ngx_err_t);

    /*
      * BSDs and Linux return 0 and set a pending error in err
      * Solaris returns -1 and sets errno
      */

    if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, (void *) &err, &len)
        == -1)
    {
        err = ngx_socket_errno;
    }

    goto closed;
  }

#endif

  n = recv(c->fd, buf, 1, MSG_PEEK);

  if (n == 0) {
    rev->eof = 1;
    c->error = 1;
    err = 0;

    goto closed;

  } else if (n == -1) {
    err = ngx_socket_errno;

    if (err != NGX_EAGAIN) {
      rev->eof = 1;
      c->error = 1;

      goto closed;
    }
  }

  /* aio does not call this handler */

  if ((ngx_event_flags & NGX_USE_LEVEL_EVENT) && rev->active) {

    if (ngx_del_event(rev, NGX_READ_EVENT, 0) != NGX_OK) {
      ngx_http_close_request_dup(r, 0);
    }
  }

  return;

closed:

  if (err) {
    rev->error = 1;
  }

  ngx_log_error(NGX_LOG_INFO, c->log, err,
                "client prematurely closed connection");

  //send the unsubscribe upstream request before finalize the main request.
  //otherwise, main request pool will have been wiped by the time we need it.
  nchan_ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  if(!nchan_ctx->sent_unsubscribe_request && nchan_ctx->sub) {
    nchan_subscriber_unsubscribe_request(nchan_ctx->sub, NGX_HTTP_CLIENT_CLOSED_REQUEST);
  }
}

#endif

