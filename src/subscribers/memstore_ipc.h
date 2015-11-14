#define MEMSTORE_IPC_SUBSCRIBER_TIMEOUT 5
subscriber_t *memstore_ipc_subscriber_create(ngx_int_t originator_slot, ngx_str_t *chid, uint8_t use_redis, void* foreign_chanhead);