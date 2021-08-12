#ifndef NCHAN_REDIS_STORE_H
#define NCHAN_REDIS_STORE_H

#define NCHAN_REDIS_DEFAULT_PING_INTERVAL_TIME 4*60
#define NCHAN_REDIS_DEFAULT_CLUSTER_CHECK_INTERVAL_TIME 5
#define NCHAN_REDIS_DEFAULT_PUBSUB_MESSAGE_MSGKEY_SIZE 1024*5

extern nchan_store_t  nchan_store_redis;

ngx_int_t nchan_store_redis_fakesub_add(ngx_str_t *channel_id, nchan_loc_conf_t *cf, ngx_int_t count, uint8_t shutting_down);
void redis_store_prepare_to_exit_worker(); // hark! a hack!!

ngx_int_t nchan_store_redis_add_active_loc_conf(ngx_conf_t *cf, nchan_loc_conf_t *loc_conf);
ngx_int_t nchan_store_redis_remove_active_loc_conf(ngx_conf_t *cf, nchan_loc_conf_t *loc_conf);

int nchan_store_redis_ready(nchan_loc_conf_t *cf);
int nchan_store_redis_validate_url(ngx_str_t *url);



ngx_int_t redis_store_callback_on_connected(nchan_loc_conf_t *cf, ngx_msec_t max_wait, callback_pt cb, void *privdata);
#endif // NCHAN_REDIS_STORE_H
