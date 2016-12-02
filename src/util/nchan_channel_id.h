ngx_str_t *nchan_get_channel_id(ngx_http_request_t *r, pub_or_sub_t what, ngx_int_t fail_hard);
ngx_int_t nchan_channel_id_is_multi(ngx_str_t *id);
ngx_str_t nchan_get_group_from_channel_id(ngx_str_t *id);
