#include <ngx_http.h>
#include <ngx_http_push_types.h>
#include <ngx_http_push_defs.h>
#include <store/ngx_http_push_store.h>


extern ngx_pool_t *ngx_http_push_pool;
extern ngx_int_t ngx_http_push_worker_processes;
extern ngx_module_t ngx_http_push_module;
extern ngx_http_push_store_t *ngx_http_push_store;

ngx_int_t ngx_http_push_respond_status_only(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *statusline);
void ngx_http_push_clean_timeouted_subscriber(ngx_event_t *ev);
ngx_int_t ngx_http_push_allow_caching(ngx_http_request_t *r);
ngx_int_t ngx_http_push_subscriber_get_msg_id(ngx_http_request_t *r, ngx_http_push_msg_id_t *id);
void ngx_http_push_subscriber_cleanup(ngx_http_push_subscriber_cleanup_t *data);
ngx_str_t * ngx_http_push_subscriber_get_etag(ngx_http_request_t * r);
ngx_table_elt_t * ngx_http_push_add_response_header(ngx_http_request_t *r, const ngx_str_t *header_name, const ngx_str_t *header_value);
ngx_chain_t * ngx_http_push_create_output_chain(ngx_buf_t *buf, ngx_pool_t *pool, ngx_log_t *log);
ngx_int_t ngx_http_push_prepare_response_to_subscriber_request(ngx_http_request_t *r, ngx_chain_t *chain, ngx_str_t *content_type, ngx_str_t *etag, time_t last_modified);
ngx_int_t ngx_push_longpoll_subscriber_enqueue(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *subscriber, ngx_int_t subscriber_timeout);
ngx_int_t ngx_push_longpoll_subscriber_dequeue(ngx_http_push_subscriber_t *subscriber);
ngx_int_t ngx_http_push_alloc_for_subscriber_response(ngx_pool_t *pool, ngx_int_t shared, ngx_http_push_msg_t *msg, ngx_chain_t **chain, ngx_str_t **content_type, ngx_str_t **etag, time_t *last_modified);


ngx_int_t ngx_http_push_subscriber_handler(ngx_http_request_t *r);
ngx_int_t ngx_http_push_publisher_handler(ngx_http_request_t *r);
ngx_int_t ngx_http_push_respond_to_subscribers(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *sentinel, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line);
ngx_int_t ngx_http_push_respond_status_only(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *statusline);
ngx_int_t ngx_http_push_subscriber_get_etag_int(ngx_http_request_t * r);
void ngx_http_push_copy_preallocated_buffer(ngx_buf_t *buf, ngx_buf_t *cbuf);
