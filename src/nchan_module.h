//#define NCHAN_SUBSCRIBER_LEAK_DEBUG 1
//#define NCHAN_MSG_LEAK_DEBUG 1

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
ngx_buf_t *nchan_channel_info_buf(ngx_str_t *accept_header, ngx_uint_t messages, ngx_uint_t subscribers, time_t last_seen, ngx_str_t **generated_content_type);

void nchan_decode_msg_id_multi_tag(uint64_t tag, uint8_t count, uint64_t tag_out[]);
uint64_t nchan_encode_msg_id_multi_tag(uint64_t tag, uint8_t n, uint8_t count, int8_t blankval);
uint64_t nchan_update_msg_id_multi_tag(uint64_t multitag, uint8_t count, uint8_t n, uint64_t tag);
ngx_int_t *verify_subscriber_last_msg_id(subscriber_t *sub, nchan_msg_t *msg);

#if NCHAN_SUBSCRIBER_LEAK_DEBUG
void subscriber_debug_add(subscriber_t *);
void subscriber_debug_remove(subscriber_t *);
void subscriber_debug_assert_isempty(void);
#endif

#if NCHAN_MSG_LEAK_DEBUG
void msg_debug_add(nchan_msg_t *);
void msg_debug_remove(nchan_msg_t *);
void msg_debug_assert_isempty(void);
#endif