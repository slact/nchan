subscriber_t *http_multipart_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id);

ngx_int_t nchan_detect_multipart_subscriber_request(ngx_http_request_t *r);