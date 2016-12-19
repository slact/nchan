ngx_buf_t *nchan_channel_info_buf(ngx_str_t *accept_header, ngx_uint_t messages, ngx_uint_t subscribers, time_t last_seen, nchan_msg_id_t *last_msgid, ngx_str_t **generated_content_type);

ngx_int_t nchan_channel_info(ngx_http_request_t *r, ngx_int_t status_code, ngx_uint_t messages, ngx_uint_t subscribers, time_t last_seen, nchan_msg_id_t *msgid);
ngx_int_t nchan_group_info(ngx_http_request_t *r, const nchan_group_t *group);

ngx_int_t nchan_response_channel_ptr_info(nchan_channel_t *channel, ngx_http_request_t *r, ngx_int_t status_code);
