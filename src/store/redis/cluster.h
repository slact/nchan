#ifndef NCHAN_REDIS_CLUSTER_H
#define NCHAN_REDIS_CLUSTER_H

#include "store-private.h"
#include "store.h"

void redis_cluster_init_postconfig(ngx_conf_t *cf);

void redis_get_cluster_info(rdstore_data_t *rdata);
void redis_get_cluster_nodes(rdstore_data_t *rdata);
void redis_cluster_drop_node(rdstore_data_t *rdata);
rdstore_data_t *redis_cluster_rdata(rdstore_data_t *rdata, ngx_str_t *str);
rdstore_data_t *redis_cluster_rdata_from_cstr(rdstore_data_t *rdata, u_char *str);
rdstore_data_t *redis_cluster_rdata_from_channel_id(rdstore_data_t *rdata, ngx_str_t *str);
rdstore_data_t *redis_cluster_rdata_from_channel(rdstore_channel_head_t *ch);

#endif //NCHAN_REDIS_CLUSTER_H
