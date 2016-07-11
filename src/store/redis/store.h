#ifndef NCHAN_REDIS_STORE_H
#define NCHAN_REDIS_STORE_H
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "uthash.h"

extern nchan_store_t  nchan_store_redis;

typedef enum {DISCONNECTED, CONNECTING, AUTHENTICATING, LOADING, LOADING_SCRIPTS, CONNECTED} redis_connection_status_t;
redis_connection_status_t redis_connection_status(nchan_loc_conf_t *cf);

ngx_int_t nchan_store_redis_fakesub_add(ngx_str_t *channel_id, ngx_int_t count, uint8_t shutting_down);
void redis_store_prepare_to_exit_worker(); // hark! a hack!!

ngx_int_t nchan_store_redis_add_server_conf(ngx_conf_t *cf, nchan_redis_conf_t *rcf);

ngx_int_t redis_store_callback_on_connected(nchan_loc_conf_t *cf, callback_pt cb, void *privdata);
#endif // NCHAN_REDIS_STORE_H
