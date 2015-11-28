#include <nchan_module.h>
#include <assert.h>

ngx_int_t nchan_subscriber_subscribe(subscriber_t *sub, nchan_store_t *store, ngx_str_t *ch_id, callback_pt callback, void *privdata) {
  return store->subscribe(ch_id, sub, callback, privdata);
}

typedef struct {
  subscriber_t    *sub;
  nchan_store_t   *store;
  ngx_str_t       *ch_id;
  callback_pt      callback;
  void            *privdata;
} nchan_auth_subrequest_data_t;

typedef struct {
  ngx_http_post_subrequest_t     psr;
  nchan_auth_subrequest_data_t   psr_data;
} nchan_auth_subrequest_stuff_t;

static ngx_int_t subscriber_authorize_callback(ngx_http_request_t *r, void *data, ngx_int_t rc) {
  nchan_auth_subrequest_data_t *d = data;
  
  if(rc == NGX_OK) {
    ngx_int_t code = r->headers_out.status;
    if(code >= 200 && code <299) {
      //authorized. proceed as planned
      nchan_subscriber_subscribe(d->sub, d->store, d->ch_id, d->callback, d->privdata);
    }
    else { //anything else means forbidden
      d->sub->fn->respond_status(d->sub, NGX_HTTP_FORBIDDEN, NULL); //auto-closes subscriber
    }
  }
  else {
    d->sub->fn->respond_status(d->sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL); //auto-closes subscriber
  }
  
  return NGX_OK;
}

ngx_int_t nchan_subscriber_authorize_subscribe(subscriber_t *sub, nchan_store_t *store, ngx_str_t *ch_id, callback_pt callback, void *privdata) {
  
  ngx_http_complex_value_t  *authorize_request_url_ccv = sub->cf->authorize_request_url;
  ngx_str_t                  auth_request_url;
  
  if(!authorize_request_url_ccv) {
    return nchan_subscriber_subscribe(sub, store, ch_id, callback, privdata);
  }
  else {
    nchan_auth_subrequest_stuff_t *psr_stuff = ngx_palloc(sub->request->pool, sizeof(*psr_stuff));
    assert(psr_stuff != NULL);
    
    sub->request->count++;
    
    ngx_http_post_subrequest_t    *psr = &psr_stuff->psr;
    nchan_auth_subrequest_data_t  *psrd = &psr_stuff->psr_data;
    ngx_http_request_t            *sr;
    
    ngx_http_complex_value(sub->request, authorize_request_url_ccv, &auth_request_url);
    
    psr->handler = subscriber_authorize_callback;
    psr->data = psrd;
    
    psrd->sub = sub;
    psrd->store = store;
    psrd->ch_id = ch_id;
    psrd->callback = callback;
    psrd->privdata = privdata;
    
    return ngx_http_subrequest(sub->request, &auth_request_url, NULL, &sr, psr, NGX_HTTP_SUBREQUEST_IN_MEMORY);
  }
}