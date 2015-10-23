#include <ngx_http.h>
#include <nchan_types.h>
#include <nchan_defs.h>
#include <nchan_output.h>

//debugging config
//#define FAKESHARD 1
#if FAKESHARD
//#define PUB_FAKE_WORKER 1
//#define SUB_FAKE_WORKER 2
//#define ONE_FAKE_CHANNEL_OWNER 0
#define MAX_FAKE_WORKERS 5
#endif


extern ngx_pool_t *nchan_pool;
extern ngx_int_t nchan_worker_processes;
extern ngx_module_t nchan_module;
extern nchan_store_t *nchan_store;

ngx_int_t nchan_pubsub_handler(ngx_http_request_t *r);

ngx_str_t * nchan_get_header_value(ngx_http_request_t *r, ngx_str_t header_name);
ngx_int_t nchan_subscriber_get_etag_int(ngx_http_request_t * r);
ngx_str_t *nchan_get_channel_id(ngx_http_request_t *r, pub_or_sub_t what, ngx_int_t fail_hard);
ngx_buf_t *nchan_channel_info_buf(ngx_str_t *accept_header, ngx_uint_t messages, ngx_uint_t subscribers, time_t last_seen, ngx_str_t **generated_content_type);