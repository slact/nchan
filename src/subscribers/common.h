ngx_int_t nchan_subscriber_subscribe(subscriber_t *sub, ngx_str_t *ch_id);

ngx_int_t nchan_subscriber_authorize_subscribe(subscriber_t *sub, ngx_str_t *ch_id);

ngx_int_t nchan_cleverly_output_headers_only_for_later_response(ngx_http_request_t *r);