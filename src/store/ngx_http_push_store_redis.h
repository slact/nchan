#include "hiredis/hiredis.h"
#include "hiredis/async.h"

redisAsyncContext *ngx_http_push_redis=NULL;
