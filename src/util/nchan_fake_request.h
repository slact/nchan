#ifndef NCHAN_FAKE_REQUEST_H
#define NCHAN_FAKE_REQUEST_H

#include <nchan_module.h>
#include <util/nchan_slist.h>

ngx_http_request_t *nchan_create_derivative_fake_request(ngx_connection_t *c, ngx_http_request_t *rsrc);
void nchan_finalize_fake_request(ngx_http_request_t *r, ngx_int_t rc);
void nchan_free_fake_request(ngx_http_request_t *r);

typedef struct nchan_fakereq_subrequest_data_s nchan_fakereq_subrequest_data_t;
typedef struct nchan_requestmachine_s nchan_requestmachine_t;

struct nchan_fakereq_subrequest_data_s {
  ngx_http_request_t *r;
  ngx_http_request_t *sr;
  void          *pd;
  callback_pt    cb;
  nchan_requestmachine_t *rm;
  ngx_event_t    cleanup_timer;
  unsigned       manual_cleanup:1;
  unsigned       running:1;
  unsigned       aborted:1;
  struct {
    nchan_fakereq_subrequest_data_t *prev;
    nchan_fakereq_subrequest_data_t *next;
  }              slist;
};// nchan_fakereq_subrequest_data_t

struct nchan_requestmachine_s {
  ngx_http_request_t *template_request;
  nchan_slist_t       request_queue;
  unsigned            shutdown_when_finished;
};// nchan_requestmachine_t;

typedef struct {
  union {
    ngx_str_t                 *str;
    ngx_http_complex_value_t  *cv;
  }                          url;
  ngx_pool_t                *pool;
  ngx_buf_t                 *body;
  
  callback_pt                cb;
  void                      *pd;
  unsigned                   manual_cleanup:1;
  unsigned                   response_headers_only:1;
  unsigned                   url_complex:1;
} nchan_requestmachine_request_params_t;

ngx_int_t nchan_requestmachine_initialize(nchan_requestmachine_t *rm, ngx_http_request_t *template_request);
nchan_fakereq_subrequest_data_t *nchan_requestmachine_request(nchan_requestmachine_t *rm, nchan_requestmachine_request_params_t *params);
ngx_int_t nchan_requestmachine_request_cleanup_manual(nchan_fakereq_subrequest_data_t *d);
ngx_int_t nchan_requestmachine_request_cleanup_on_request_finalize(nchan_fakereq_subrequest_data_t *d, ngx_http_request_t *r);
ngx_int_t nchan_requestmachine_abort(nchan_requestmachine_t *rm);
ngx_int_t nchan_requestmachine_shutdown(nchan_requestmachine_t *rm);

#endif //NCHAN_FAKE_REQUEST_H
