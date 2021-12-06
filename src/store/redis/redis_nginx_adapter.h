//Copyright (c) 2014 Wandenberg Peixoto under the MIT Licence
//Coptright (c) 2015-2021 slact
#ifndef __REDIS_NGINX_ADAPTER_H
#define __REDIS_NGINX_ADAPTER_H

#if NCHAN_HAVE_HIREDIS_WITH_SOCKADDR
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#else
#include <store/redis/hiredis/hiredis.h>
#include <store/redis/hiredis/async.h>
#endif

#include <nchan_module.h>
#include <store/redis/redis_nodeset.h>

void redis_nginx_init(void);
int redis_nginx_event_attach(redisAsyncContext *ac);
void redis_nginx_force_close_context(redisAsyncContext **context);
void redis_nginx_close_context(redisAsyncContext **context);

#endif // __REDIS_NGINX_ADAPTER_H
