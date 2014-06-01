#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_channel.h>


extern ngx_pool_t *ngx_http_push_pool;
extern ngx_int_t ngx_http_push_worker_processes;
extern ngx_module_t ngx_http_push_module;

//garbage-collecting shared memory slab allocation
void * ngx_http_push_slab_alloc(size_t size);
void * ngx_http_push_slab_alloc_locked(size_t size);

//channel messages
static ngx_http_push_msg_t *ngx_http_push_get_latest_message_locked(ngx_http_push_channel_t * channel);
static ngx_http_push_msg_t *ngx_http_push_get_oldest_message_locked(ngx_http_push_channel_t * channel);
static ngx_int_t ngx_http_push_delete_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t force);

static ngx_inline void ngx_http_push_free_message_locked(ngx_http_push_msg_t *msg, ngx_slab_pool_t *shpool);
static ngx_http_push_msg_t * ngx_http_push_find_message_locked(ngx_http_push_channel_t *channel, ngx_http_request_t *r, ngx_int_t *status);

//channel
static ngx_str_t * ngx_http_push_get_channel_id(ngx_http_request_t *r, ngx_http_push_loc_conf_t *cf);
static ngx_int_t ngx_http_push_channel_info(ngx_http_request_t *r, ngx_uint_t message_queue_size, ngx_uint_t subscriber_queue_size, time_t last_seen);

//subscriber
static ngx_int_t ngx_http_push_subscriber_handler(ngx_http_request_t *r);
static ngx_int_t ngx_http_push_handle_subscriber_concurrency(ngx_http_request_t *r, ngx_http_push_channel_t *channel, ngx_http_push_loc_conf_t *loc_conf);
#define ngx_http_push_broadcast_status(channel, status_code, status_line, log) ngx_http_push_store_local.publish(channel, NULL, status_code, status_line, log)
static ngx_int_t ngx_http_push_respond_to_subscribers(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *sentinel, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line);
static ngx_int_t ngx_http_push_allow_caching(ngx_http_request_t * r);
static ngx_int_t ngx_http_push_subscriber_get_etag_int(ngx_http_request_t * r);
static ngx_str_t * ngx_http_push_subscriber_get_etag(ngx_http_request_t * r);
static void ngx_http_push_subscriber_cleanup(ngx_http_push_subscriber_cleanup_t *data);
static ngx_int_t ngx_http_push_prepare_response_to_subscriber_request(ngx_http_request_t *r, ngx_chain_t *chain, ngx_str_t *content_type, ngx_str_t *etag, time_t last_modified);

//publisher
static ngx_int_t ngx_http_push_publisher_handler(ngx_http_request_t * r);
static void ngx_http_push_publisher_body_handler(ngx_http_request_t * r);

//utilities
//general request handling
static void ngx_http_push_copy_preallocated_buffer(ngx_buf_t *buf, ngx_buf_t *cbuf);
static ngx_table_elt_t * ngx_http_push_add_response_header(ngx_http_request_t *r, const ngx_str_t *header_name, const ngx_str_t *header_value);
static ngx_int_t ngx_http_push_respond_status_only(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *statusline);
static ngx_chain_t * ngx_http_push_create_output_chain(ngx_buf_t *buf, ngx_pool_t *pool, ngx_log_t *log);


