#include <nchan_defs.h>
#include <nchan_types.h>

void nchan_output_init(void);
void nchan_output_shutdown(void);

ngx_int_t nchan_output_filter(ngx_http_request_t *r, ngx_chain_t *in);
ngx_int_t nchan_output_msg_filter(ngx_http_request_t *r, nchan_msg_t *msg, ngx_chain_t *in);

ngx_int_t nchan_respond_status(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *status_line, ngx_chain_t *body, ngx_int_t finalize);
ngx_int_t nchan_respond_string(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *content_type, const ngx_str_t *body, ngx_int_t finalize);
ngx_int_t nchan_respond_sprintf(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *content_type, const ngx_int_t finalize, char *fmt, ...);
ngx_int_t nchan_respond_cstring(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *content_type, char *body, ngx_int_t finalize);
ngx_int_t nchan_respond_membuf(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *content_type, ngx_buf_t *body, ngx_int_t finalize);
ngx_table_elt_t * nchan_add_response_header(ngx_http_request_t *r, const ngx_str_t *header_name, const ngx_str_t *header_value);
ngx_int_t nchan_set_msgid_http_response_headers(ngx_http_request_t *r, nchan_request_ctx_t *ctx, nchan_msg_id_t *msgid);
ngx_int_t nchan_OPTIONS_respond(ngx_http_request_t *r, const ngx_str_t *allowed_headers, const ngx_str_t *allowed_methods);
ngx_int_t nchan_respond_msg(ngx_http_request_t *r, nchan_msg_t *msg, nchan_msg_id_t *msgid, ngx_int_t finalize, char **err);
void nchan_include_access_control_if_needed(ngx_http_request_t *r, nchan_request_ctx_t *ctx);
void nchan_flush_pending_output(ngx_http_request_t *r);

void nchan_http_finalize_request(ngx_http_request_t *r, ngx_int_t code);

ngx_fd_t nchan_fdcache_get(ngx_str_t *filename);

ngx_int_t nchan_msg_buf_open_fd_if_needed(ngx_buf_t *buf, ngx_file_t *file, ngx_http_request_t *r);

ngx_str_t *msgtag_to_str(nchan_msg_id_t *id);
ngx_str_t *msgid_to_str(nchan_msg_id_t *id);
size_t msgtag_to_strptr(nchan_msg_id_t *id, char *ch);
