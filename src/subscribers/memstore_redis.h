#define MEMSTORE_REDIS_SUBSCRIBER_TIMEOUT 10
subscriber_t *memstore_redis_subscriber_create(memstore_channel_head_t *chanhead);
ngx_int_t memstore_redis_subscriber_destroy(subscriber_t *sub);
ngx_int_t nchan_memstore_redis_subscriber_notify_on_MSG_EXPECTED(subscriber_t *sub, nchan_msg_id_t *id, void (*cb)(nchan_msg_status_t, void*), size_t pd_sz, void *pd);
