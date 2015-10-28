#define MEMSTORE_REDIS_SUBSCRIBER_TIMEOUT 10
subscriber_t *memstore_redis_subscriber_create(ngx_int_t originator_slot, ngx_str_t *chid, void* foreign_chanhead);