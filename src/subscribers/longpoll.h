extern subscriber_t nhpm_longpoll_subscriber;

extern subscriber_t *longpoll_subscriber_create(ngx_http_request_t *r);
extern ngx_int_t longpoll_subscriber_destroy(subscriber_t *sub);