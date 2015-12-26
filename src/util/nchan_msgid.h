void nchan_expand_msg_id_multi_tag(nchan_msg_id_t *id, uint8_t in_n, uint8_t out_n, int16_t fill);
ngx_int_t nchan_copy_msg_id(nchan_msg_id_t *dst, nchan_msg_id_t *src, int16_t *largetags);
ngx_int_t nchan_copy_new_msg_id(nchan_msg_id_t *dst, nchan_msg_id_t *src);
ngx_int_t nchan_free_msg_id(nchan_msg_id_t *id);