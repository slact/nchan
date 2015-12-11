subscriber_t *websocket_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id);
ngx_int_t websocket_subscriber_destroy(subscriber_t *sub);

ngx_int_t nchan_detect_websocket_request(ngx_http_request_t *r);