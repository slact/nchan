ngx_int_t nchan_subscriber_subscribe(subscriber_t *sub, ngx_str_t *ch_id);

ngx_int_t nchan_subscriber_authorize_subscribe(subscriber_t *sub, ngx_str_t *ch_id);

ngx_int_t nchan_cleverly_output_headers_only_for_later_response(ngx_http_request_t *r);

ngx_str_t *nchan_request_multipart_boundary(ngx_http_request_t *r, nchan_request_ctx_t *ctx);

ngx_int_t nchan_request_set_content_type_multipart_boundary_header(ngx_http_request_t *r, nchan_request_ctx_t *ctx);

void nchan_subscriber_timeout_ev_handler(ngx_event_t *ev);
void nchan_subscriber_init(subscriber_t *sub, const subscriber_t *tmpl, ngx_http_request_t *r, nchan_msg_id_t *msgid);
void nchan_subscriber_init_timeout_timer(subscriber_t *sub, ngx_event_t *ev);
void nchan_subscriber_common_setup(subscriber_t *sub, subscriber_type_t type, ngx_str_t *name, subscriber_fn_t *fn, ngx_int_t dequeue_after_response);
