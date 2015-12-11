subscriber_t *eventsource_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id);
//ngx_int_t eventsource_subscriber_destroy(subscriber_t *sub);

ngx_int_t nchan_detect_eventsource_request(ngx_http_request_t *r);