extern subscriber_t nhpm_websocket_subscriber;

subscriber_t *websocket_subscriber_create(ngx_http_request_t *r);
ngx_int_t websocket_subscriber_destroy(subscriber_t *sub);