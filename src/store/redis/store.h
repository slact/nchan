#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "uthash.h"

extern nchan_store_t  nchan_store_redis;

ngx_int_t nchan_store_redis_connection_close_handler(redisAsyncContext *ac);