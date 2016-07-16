#ifndef NCHAN_REDIS_CLUSTER_H
#define NCHAN_REDIS_CLUSTER_H

#include "store-private.h"
#include "store.h"

void redis_cluster_init_postconfig(ngx_conf_t *cf);

void redis_get_cluster_info(rdstore_data_t *rdata);
void redis_get_cluster_nodes(rdstore_data_t *rdata);


#endif //NCHAN_REDIS_CLUSTER_H
