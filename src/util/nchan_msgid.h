void nchan_expand_msg_id_multi_tag(nchan_msg_id_t *id, uint8_t in_n, uint8_t out_n, int16_t fill);
ngx_int_t nchan_copy_msg_id(nchan_msg_id_t *dst, nchan_msg_id_t *src, int16_t *largetags);
ngx_int_t nchan_copy_new_msg_id(nchan_msg_id_t *dst, nchan_msg_id_t *src);
ngx_int_t nchan_free_msg_id(nchan_msg_id_t *id);

ngx_int_t nchan_parse_compound_msgid(nchan_msg_id_t *id, ngx_str_t *str, ngx_int_t expected_tag_count);
void nchan_update_multi_msgid(nchan_msg_id_t *oldid, nchan_msg_id_t *newid, int16_t *largetags);
ngx_int_t update_subscriber_last_msg_id(subscriber_t *sub, nchan_msg_t *msg);
nchan_msg_id_t *nchan_subscriber_get_msg_id(ngx_http_request_t *r);
ngx_int_t nchan_extract_from_multi_msgid(nchan_msg_id_t *src, uint16_t n, nchan_msg_id_t *dst);

int8_t nchan_compare_msgid_tags(nchan_msg_id_t *id1, nchan_msg_id_t *id2);
int8_t nchan_compare_msgids(nchan_msg_id_t *id1, nchan_msg_id_t *id2);

void nchan_expand_tiny_msgid(nchan_msg_tiny_id_t *tinyid, nchan_msg_id_t *id);
void nchan_shrink_normal_msgid(nchan_msg_id_t *id, nchan_msg_tiny_id_t *tinyid);
