#ifndef NCHAN_REDIS_NODESET_H
#define NCHAN_REDIS_NODESET_H

#include <nchan_module.h>
#include "store-private.h"

#define node_log(node, lvl, fmt, args...) \
  ngx_log_error(lvl, ngx_cycle->log, 0, "nchan: Redis node %V:%d " fmt, &node->connect_params.hostname, node->connect_params.port, ##args)
#define node_log_error(node, fmt, args...)    node_log(node, NGX_LOG_ERR, fmt, ##args)
#define node_log_warning(node, fmt, args...)  node_log(node, NGX_LOG_WARN, fmt, ##args)
#define node_log_notice(node, fmt, args...)   node_log(node, NGX_LOG_NOTICE, fmt, ##args)
#define node_log_info(node, fmt, args...)     node_log(node, NGX_LOG_INFO, fmt, ##args)
#define node_log_debug(node, fmt, args...)    node_log(node, NGX_LOG_DEBUG, fmt, ##args)

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

int nodeset_node_deduplicate_by_connect_params(redis_node_t *node);
int nodeset_node_deduplicate_by_run_id(redis_node_t *node);
int nodeset_node_deduplicate_by_cluster_id(redis_node_t *node);

redis_node_t *nodeset_node_find_by_connect_params(redis_nodeset_t *ns, redis_connect_params_t *rcp);
redis_node_t *nodeset_node_find_by_run_id(redis_nodeset_t *ns, ngx_str_t *run_id);
redis_node_t *nodeset_node_find_by_cluster_id(redis_nodeset_t *ns, ngx_str_t *cluster_id);

redis_node_t *nodeset_node_create(redis_nodeset_t *ns, redis_connect_params_t *rcp);

void node_discover_master(redis_node_t  *slave, redis_connect_params_t *rcp);
void node_discover_slave(redis_node_t *master, redis_connect_params_t *rcp);


#endif /* NCHAN_REDIS_NODESET_H */
