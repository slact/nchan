#ifndef NCHAN_REDIS_CLUSTER_H
#define NCHAN_REDIS_CLUSTER_H

#include "store-private.h"
#include "store.h"

void redis_cluster_init_postconfig(ngx_conf_t *cf);
void redis_cluster_exit_worker(ngx_cycle_t *cycle);

void redis_get_cluster_info(rdstore_data_t *rdata);
void redis_get_cluster_nodes(rdstore_data_t *rdata);

rdstore_data_t *redis_cluster_rdata(rdstore_data_t *rdata, ngx_str_t *str);
rdstore_data_t *redis_cluster_rdata_from_cstr(rdstore_data_t *rdata, u_char *str);
rdstore_data_t *redis_cluster_rdata_from_key(rdstore_data_t *rdata, ngx_str_t *key);
rdstore_data_t *redis_cluster_rdata_from_channel_id(rdstore_data_t *rdata, ngx_str_t *str);
rdstore_data_t *redis_cluster_rdata_from_channel(rdstore_channel_head_t *ch);
ngx_int_t redis_cluster_associate_chanhead_with_rdata(rdstore_channel_head_t *ch);

void *cluster_retry_palloc(redis_cluster_t *, size_t);
void  cluster_retry_pfree(redis_cluster_t *, void *);
ngx_int_t cluster_add_retry_command_with_chanhead(rdstore_channel_head_t *, void (*)(rdstore_data_t *, void *), void *);
ngx_int_t cluster_add_retry_command_with_key(redis_cluster_t *, ngx_str_t *, void (*)(rdstore_data_t *, void *pd), void *pd);
ngx_int_t cluster_add_retry_command_with_channel_id(redis_cluster_t *, ngx_str_t *, void (*)(rdstore_data_t *, void *pd), void *pd);
ngx_int_t cluster_add_retry_command_with_cstr(redis_cluster_t *, u_char *, void (*)(rdstore_data_t *, void *pd), void *pd);

int clusterKeySlotOk(redisAsyncContext *c, void *r);

ngx_int_t redis_cluster_node_change_status(rdstore_data_t *rdata, redis_connection_status_t status);
#endif //NCHAN_REDIS_CLUSTER_H
