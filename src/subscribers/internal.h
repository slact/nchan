extern subscriber_t nhpm_internal_subscriber;

extern subscriber_t *internal_subscriber_create(ngx_http_request_t *r);
extern ngx_int_t internal_subscriber_destroy(subscriber_t *sub);

ngx_int_t internal_subscriber_set_name(subscriber_t *sub, const char *name);
ngx_int_t internal_subscriber_set_enqueue_handler(subscriber_t *sub, callback_pt handler);
ngx_int_t internal_subscriber_set_dequeue_handler(subscriber_t *sub, callback_pt handler);
ngx_int_t internal_subscriber_set_respond_message_handler(subscriber_t *sub, callback_pt handler);
ngx_int_t internal_subscriber_set_respond_status_handler(subscriber_t *sub, callback_pt handler);
