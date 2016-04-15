#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "uthash.h"

extern nchan_store_t  nchan_store_redis;

ngx_int_t nchan_store_redis_connection_close_handler(redisAsyncContext *ac);

ngx_int_t nchan_store_redis_fakesub_add(ngx_str_t *channel_id, ngx_int_t count, uint8_t shutting_down);
void redis_store_prepare_to_exit_worker(); // hark! a hack!!
