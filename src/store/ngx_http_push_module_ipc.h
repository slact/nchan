ngx_int_t ngx_http_push_init_ipc(ngx_cycle_t *cycle, ngx_int_t workers);
void ngx_http_push_ipc_exit_worker(ngx_cycle_t *cycle);
ngx_int_t ngx_http_push_register_worker_message_handler(ngx_cycle_t *cycle);
ngx_int_t ngx_http_push_alert_worker(ngx_pid_t pid, ngx_int_t slot, ngx_log_t *log);
