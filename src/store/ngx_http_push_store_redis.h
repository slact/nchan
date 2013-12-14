#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "hiredis/adapters/libevent.h"

redisAsyncContext *ngx_http_push_redis=NULL;
