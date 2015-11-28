#include <nchan_module.h>
#include <assert.h>

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
  ngx_event_t                   *timer = ngx_pcalloc(r->pool, sizeof(*timer));
  
  if(timer == NULL) {
    return NGX_ERROR;
  }
  
  d->rc = rc;
  d->http_response_code = r->headers_out.status;
  
  timer->handler = subscriber_authorize_timer_callback_handler;
  timer->log = d->sub->request->connection->log;
  timer->data = data;
  
  ngx_add_timer(timer, 0); //not sure if this needs to be done like this, but i'm just playing it safe here.
  
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
    
    return NGX_OK;
  }
}