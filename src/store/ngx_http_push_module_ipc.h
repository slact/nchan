ngx_int_t ngx_http_push_alert_worker(ngx_pid_t pid,ngx_int_t slot);
ngx_int_t ngx_http_push_ipc_init_worker(ngx_cycle_t *cycle);
void ngx_http_push_ipc_exit_worker(ngx_cycle_t *cycle);
ngx_int_t ngx_http_push_shutdown_ipc(ngx_cycle_t *cycle);
ngx_int_t ngx_http_push_init_ipc(ngx_cycle_t *cycle,ngx_int_t workers);