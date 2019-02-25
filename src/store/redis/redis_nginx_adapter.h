//Copyright (c) 2014 Wandenberg Peixoto under the MIT Licence
#ifndef __REDIS_NGINX_ADAPTER_H
#define __REDIS_NGINX_ADAPTER_H

#if NCHAN_HAVE_HIREDIS_WITH_SOCKADDR
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#else
#include <store/redis/hiredis/hiredis.h>
#include <store/redis/hiredis/async.h>
#endif

void redis_nginx_init(void);
redisAsyncContext *redis_nginx_open_context(ngx_str_t *host, int port, void *privdata);
redisContext *redis_nginx_open_sync_context(ngx_str_t *host, int port, int database, ngx_str_t *password, redisContext **context);
void redis_nginx_force_close_context(redisAsyncContext **context);
void redis_nginx_close_context(redisAsyncContext **context);
void redis_nginx_ping_callback(redisAsyncContext *ac, void *rep, void *privdata);

#endif // __REDIS_NGINX_ADAPTER_H
