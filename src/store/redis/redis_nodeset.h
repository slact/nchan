#ifndef NCHAN_REDIS_NODESET_H
#define NCHAN_REDIS_NODESET_H

#include <nchan_module.h>
#include "store-private.h"

#define NCHAN_MAX_NODESETS 1024

redis_nodeset_t *nodeset_create(nchan_redis_conf_t *rcf);
redis_nodeset_t *nodeset_find(nchan_redis_conf_t *rcf);
ngx_int_t nodeset_check_status(redis_nodeset_t *nodeset);

ngx_int_t nodeset_node_destroy(redis_node_t *node);


void node_set_role(redis_node_t *node, redis_node_role_t role);
int node_set_master_node(redis_node_t *node, redis_node_t *master);
redis_node_t *node_find_slave_node(redis_node_t *node, redis_node_t *slave);
int node_add_slave_node(redis_node_t *node, redis_node_t *slave);
int node_remove_slave_node(redis_node_t *node, redis_node_t *slave);

ngx_int_t nodeset_connect_all(void);

redis_node_t *nodeset_node_find(redis_nodeset_t *ns, redis_connect_params_t *rcp);
redis_node_t *nodeset_node_create(redis_nodeset_t *ns, redis_connect_params_t *rcp);



#endif /* NCHAN_REDIS_NODESET_H */
