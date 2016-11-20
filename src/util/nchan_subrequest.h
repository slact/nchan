ngx_int_t nchan_adjust_subrequest(ngx_http_request_t *sr, ngx_uint_t method, ngx_str_t *method_name, ngx_http_request_body_t *request_body, size_t content_length_n, u_char *content_len_str);
size_t nchan_subrequest_content_length(ngx_http_request_t *sr);
ngx_int_t nchan_recover_x_accel_redirected_request_method(ngx_http_request_t *r);
