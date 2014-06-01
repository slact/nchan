#include <ngx_http.h>
#include <ngx_http_push_types.h>
#include <ngx_http_push_defs.h>


extern ngx_pool_t *ngx_http_push_pool;
extern ngx_int_t ngx_http_push_worker_processes;
extern ngx_module_t ngx_http_push_module;
extern ngx_http_push_store_t *ngx_http_push_store;

//garbage-collecting shared memory slab allocation
void * ngx_http_push_slab_alloc(size_t size);
void * ngx_http_push_slab_alloc_locked(size_t size);


ngx_int_t ngx_http_push_subscriber_handler(ngx_http_request_t *r);
ngx_int_t ngx_http_push_publisher_handler(ngx_http_request_t *r);
ngx_int_t ngx_http_push_respond_to_subscribers(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *sentinel, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line);
ngx_int_t ngx_http_push_subscriber_get_etag_int(ngx_http_request_t * r);
void ngx_http_push_copy_preallocated_buffer(ngx_buf_t *buf, ngx_buf_t *cbuf);
