ngx_int_t ngx_http_push_init_ipc(ngx_cycle_t *cycle, ngx_int_t workers);
void ngx_http_push_ipc_exit_worker(ngx_cycle_t *cycle);
ngx_int_t   ngx_http_push_init_ipc_shm(ngx_int_t workers);
ngx_int_t ngx_http_push_register_worker_message_handler(ngx_cycle_t *cycle);
ngx_int_t ngx_http_push_alert_worker(ngx_pid_t pid, ngx_int_t slot, ngx_log_t *log);
ngx_int_t ngx_http_push_send_worker_message(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *subscriber_sentinel, ngx_pid_t pid, ngx_int_t worker_slot, ngx_http_push_msg_t *msg, ngx_int_t status_code, ngx_log_t *log);
