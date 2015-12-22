//#define NCHAN_SUBSCRIBER_LEAK_DEBUG 1
//#define NCHAN_MSG_RESERVE_DEBUG 1
//#define NCHAN_MSG_LEAK_DEBUG 1
//#define NCHAN_BENCHMARK 1

#include <ngx_http.h>
#include <nchan_types.h>
#include <nchan_defs.h>
#include <nchan_output.h>

//debugging config
//#define FAKESHARD 1
#if FAKESHARD
//#define PUB_FAKE_WORKER 0
//#define SUB_FAKE_WORKER 1
//#define ONE_FAKE_CHANNEL_OWNER 2
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
ngx_buf_t *nchan_channel_info_buf(ngx_str_t *accept_header, ngx_uint_t messages, ngx_uint_t subscribers, time_t last_seen, nchan_msg_id_t *last_msgid, ngx_str_t **generated_content_type);

ngx_int_t update_subscriber_last_msg_id(subscriber_t *sub, nchan_msg_t *msg);

ngx_int_t nchan_copy_msg_id(nchan_msg_id_t *dst, nchan_msg_id_t *src, int16_t *largetags);
ngx_int_t nchan_copy_new_msg_id(nchan_msg_id_t *dst, nchan_msg_id_t *src);
ngx_int_t nchan_free_msg_id(nchan_msg_id_t *id);
void nchan_update_multi_msgid(nchan_msg_id_t *oldid, nchan_msg_id_t *newid);
void nchan_expand_msg_id_multi_tag(nchan_msg_id_t *id, uint8_t in_n, uint8_t out_n, int16_t fill);
ngx_int_t nchan_maybe_send_channel_event_message(ngx_http_request_t *, channel_event_type_t);

#if NCHAN_SUBSCRIBER_LEAK_DEBUG
void subscriber_debug_add(subscriber_t *);
void subscriber_debug_remove(subscriber_t *);
void subscriber_debug_assert_isempty(void);
#endif

#if NCHAN_BENCHMARK
int nchan_timeval_subtract(struct timeval *result, struct timeval *x, struct timeval *y);
#endif