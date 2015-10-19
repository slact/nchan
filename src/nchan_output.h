ngx_int_t ngx_http_push_output_filter(ngx_http_request_t *r, ngx_chain_t *in);
ngx_int_t ngx_http_push_respond_status(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *status_line, ngx_int_t finalize);
ngx_int_t ngx_http_push_respond_string(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *content_type, const ngx_str_t *body, ngx_int_t finalize);
ngx_int_t ngx_http_push_respond_membuf(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *content_type, ngx_buf_t *body, ngx_int_t finalize);