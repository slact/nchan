ngx_int_t memstore_ipc_send_subscribe_channel(ipc_t *ipc, ngx_int_t owner, ngx_str_t *shm_chid, void* privdata);
ngx_int_t memstore_ipc_send_publish_message(ipc_t *ipc, ngx_int_t owner, ngx_str_t *shm_chid, ngx_http_push_msg_t *shm_msg, void *privdata);
ngx_int_t memstore_ipc_send_get_message(ipc_t *ipc, ngx_int_t owner, ngx_str_t *shm_chid, void * privdata);
ngx_int_t memstore_ipc_send_delete(ipc_t *ipc, ngx_int_t owner, ngx_str_t *shm_chid, void *privdata);
void memstore_ipc_alert_handler(ngx_uint_t code, void *data);