void nchan_expand_msg_id_multi_tag(nchan_msg_id_t *id, uint8_t in_n, uint8_t out_n, int16_t fill);
ngx_int_t nchan_copy_msg_id(nchan_msg_id_t *dst, nchan_msg_id_t *src, int16_t *largetags);
ngx_int_t nchan_copy_new_msg_id(nchan_msg_id_t *dst, nchan_msg_id_t *src);
ngx_int_t nchan_free_msg_id(nchan_msg_id_t *id);

void nchan_update_multi_msgid(nchan_msg_id_t *oldid, nchan_msg_id_t *newid, int16_t *largetags);
ngx_int_t update_subscriber_last_msg_id(subscriber_t *sub, nchan_msg_t *msg);
nchan_msg_id_t *nchan_subscriber_get_msg_id(ngx_http_request_t *r);
