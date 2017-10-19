ngx_int_t nchan_subscriber_subscribe(subscriber_t *sub, ngx_str_t *ch_id);

ngx_int_t nchan_subscriber_authorize_subscribe_request(subscriber_t *sub, ngx_str_t *ch_id);
ngx_int_t nchan_subscriber_subscribe_request(subscriber_t *sub);
ngx_int_t nchan_subscriber_unsubscribe_request(subscriber_t *sub, ngx_int_t finalize_code);

ngx_int_t nchan_cleverly_output_headers_only_for_later_response(ngx_http_request_t *r);

ngx_str_t *nchan_request_multipart_boundary(ngx_http_request_t *r, nchan_request_ctx_t *ctx);

ngx_int_t nchan_request_set_content_type_multipart_boundary_header(ngx_http_request_t *r, nchan_request_ctx_t *ctx);

ngx_int_t nchan_subscriber_empty_notify(subscriber_t *, ngx_int_t code, void *data);

typedef ngx_int_t (*subrequest_callback_pt)(subscriber_t *sub, ngx_http_request_t *r, ngx_int_t rc, void *);
ngx_http_request_t *subscriber_cv_subrequest(subscriber_t *sub, ngx_http_complex_value_t *url_ccv, ngx_buf_t *body, subrequest_callback_pt cb, void *cb_data);
ngx_http_request_t *subscriber_subrequest(subscriber_t *sub, ngx_str_t *url, ngx_buf_t *body, subrequest_callback_pt cb, void *cb_data);

void nchan_subscriber_timeout_ev_handler(ngx_event_t *ev);
void nchan_subscriber_init(subscriber_t *sub, const subscriber_t *tmpl, ngx_http_request_t *r, nchan_msg_id_t *msgid);
void nchan_subscriber_init_timeout_timer(subscriber_t *sub, ngx_event_t *ev);
void nchan_subscriber_common_setup(subscriber_t *sub, subscriber_type_t type, ngx_str_t *name, subscriber_fn_t *fn, ngx_int_t enable_sub_unsub_callbacks, ngx_int_t dequeue_after_response);
ngx_int_t nchan_subscriber_init_msgid_reusepool(nchan_request_ctx_t *ctx, ngx_pool_t *request_pool);
ngx_str_t nchan_subscriber_set_recyclable_msgid_str(nchan_request_ctx_t *ctx, nchan_msg_id_t *msgid);
void nchan_subscriber_unsubscribe_callback_http_test_reading(ngx_http_request_t *r);

