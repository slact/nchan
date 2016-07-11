//Copyright (c) 2014 Wandenberg Peixoto under the MIT Licence
#ifndef __REDIS_NGINX_ADAPTER_H
#define __REDIS_NGINX_ADAPTER_H

#include <hiredis/hiredis.h>
#include <hiredis/async.h>

void redis_nginx_init(void);
redisAsyncContext *redis_nginx_open_context(ngx_str_t *host, int port, int database, ngx_str_t *password, void *privdata, redisAsyncContext **context);
redisContext *redis_nginx_open_sync_context(ngx_str_t *host, int port, int database, ngx_str_t *password, redisContext **context);
void redis_nginx_force_close_context(redisAsyncContext **context);
void redis_nginx_close_context(redisAsyncContext **context);
void redis_nginx_ping_callback(redisAsyncContext *ac, void *rep, void *privdata);

#endif // __REDIS_NGINX_ADAPTER_H
