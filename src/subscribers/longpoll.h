extern subscriber_t nhpm_longpoll_subscriber;

subscriber_t *longpoll_subscriber_create(ngx_http_request_t *r);
ngx_int_t longpoll_subscriber_destroy(subscriber_t *sub);