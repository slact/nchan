
ngx_connection_t *nchan_create_fake_connection(ngx_pool_t *pool);
void nchan_close_fake_connection(ngx_connection_t *c);
ngx_http_request_t *nchan_create_fake_request(ngx_connection_t *c);
void nchan_finalize_fake_request(ngx_http_request_t *r, ngx_int_t rc);
void nchan_free_fake_request(ngx_http_request_t *r);
