subscriber_t *http_chunked_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id);
//ngx_int_t http_chunked_subscriber_destroy(subscriber_t *sub);

ngx_int_t nchan_detect_chunked_subscriber_request(ngx_http_request_t *r);