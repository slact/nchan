#ifndef NCHAN_REDIS_NODESET_H
#define NCHAN_REDIS_NODESET_H

#include <nchan_module.h>
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include <util/nchan_reaper.h>
#include <util/nchan_rbtree.h>
#include <util/nchan_list.h>

//#include "store-private.h"

#define node_log(node, lvl, fmt, args...) \
  ngx_log_error(lvl, ngx_cycle->log, 0, "nchan: Redis node %V:%d " fmt, &(node)->connect_params.hostname, node->connect_params.port, ##args)
#define node_log_error(node, fmt, args...)    node_log((node), NGX_LOG_ERR, fmt, ##args)
#define node_log_warning(node, fmt, args...)  node_log((node), NGX_LOG_WARN, fmt, ##args)
#define node_log_notice(node, fmt, args...)   node_log((node), NGX_LOG_NOTICE, fmt, ##args)
#define node_log_info(node, fmt, args...)     node_log((node), NGX_LOG_INFO, fmt, ##args)
#define node_log_debug(node, fmt, args...)    node_log((node), NGX_LOG_DEBUG, fmt, ##args)

#define NCHAN_MAX_NODESETS 1024
#define REDIS_NODESET_STATUS_CHECK_TIME_MSEC 4000
#define REDIS_NODESET_MAX_CONNECTING_TIME_SEC 5
#define REDIS_NODESET_RECONNECT_WAIT_TIME_SEC 5
#define REDIS_NODESET_MAX_FAILING_TIME_SEC 2

#define REDIS_NODE_DEDUPLICATED        -100
#define REDIS_NODE_FAILED                -1
#define REDIS_NODE_DISCONNECTED           0
#define REDIS_NODE_CMD_CONNECTING         1
#define REDIS_NODE_PUBSUB_CONNECTING      2
#define REDIS_NODE_CONNECTED              3
#define REDIS_NODE_CMD_AUTHENTICATING     4
#define REDIS_NODE_PUBSUB_AUTHENTICATING  5
#define REDIS_NODE_AUTHENTICATED          6
#define REDIS_NODE_CMD_SELECTING_DB       7
#define REDIS_NODE_PUBSUB_SELECTING_DB    8
#define REDIS_NODE_DB_SELECTED            9
#define REDIS_NODE_GETTING_INFO           10
#define REDIS_NODE_GET_CLUSTERINFO        11
#define REDIS_NODE_GETTING_CLUSTERINFO    12
#define REDIS_NODE_GET_CLUSTER_NODES      13
#define REDIS_NODE_GETTING_CLUSTER_NODES  14
#define REDIS_NODE_SCRIPTS_LOAD           15
#define REDIS_NODE_SCRIPTS_LOADING        16
#define REDIS_NODE_READY                  100
  
typedef struct redis_nodeset_s redis_nodeset_t;
typedef struct redis_node_s redis_node_t;

typedef struct { //redis_nodeset_cluster_t
  unsigned                    enabled:1;
  unsigned                    ready:1;
  rbtree_seed_t               keyslots; //cluster rbtree seed
} redis_nodeset_cluster_t;

typedef struct {
  unsigned         min:16;
  unsigned         max:16;
} redis_slot_range_t;

typedef enum {
  REDIS_NODE_ROLE_UNKNOWN = 0, REDIS_NODE_ROLE_MASTER, REDIS_NODE_ROLE_SLAVE
} redis_node_role_t;

typedef enum {
  REDIS_NODESET_FAILED = -3,
  REDIS_NODESET_FAILING = -2,
  REDIS_NODESET_INVALID = -1,
  REDIS_NODESET_DISCONNECTED = 0,
  REDIS_NODESET_CONNECTING,
  REDIS_NODESET_READY
} redis_nodeset_status_t;

struct redis_nodeset_s {
  //a set of redis nodes
  //  maybe just 1 master
  //  maybe a master and its slaves
  //  maybe a cluster of masters and their slaves
  //slaves of slaves not included
  
  redis_nodeset_status_t      status;
  time_t                      current_status_start;
  ngx_int_t                   current_status_times_checked;
  ngx_int_t                   generation;
  ngx_event_t                 status_check_ev;
  nchan_list_t                urls;
  ngx_http_upstream_srv_conf_t *upstream;
  nchan_list_t                nodes;
  redis_nodeset_cluster_t     cluster;
  struct {
    nchan_redis_storage_mode_t  storage_mode;
    struct {
      unsigned                    master:1;
      unsigned                    slave:1;
    }                           pubsub_subscribe_to;
  }                           settings;
  
  nchan_list_t                channels;
  nchan_reaper_t              chanhead_reaper;
  time_t                      reconnect_delay_sec;
}; //redis_nodeset_t

struct redis_node_s {
  int8_t                    state;
  unsigned                  discovered:1;
  redis_node_role_t         role;
  redis_connect_params_t    connect_params;
  redis_nodeset_t          *nodeset;
  ngx_str_t                 run_id;
  ngx_str_t                 version;
  int                       scripts_loaded;
  struct {
    unsigned                  enabled:1;
    unsigned                  ok:1;
    ngx_str_t                 id;
    struct {
      redis_slot_range_t         *range;
      size_t                      n;
      unsigned                    indexed:1;
    }                         slot_range; 
    char                     *cluster_nodes;
  }                         cluster;
  struct {
    redis_node_t              *master;
    nchan_list_t               slaves;
  }                         peers;
  struct {
    redisAsyncContext         *cmd;
    redisAsyncContext         *pubsub;
    redisContext              *sync;
  }                         ctx;
}; //redis_node_t

typedef struct {
  redis_slot_range_t      range;
  redis_node_t           *node;
} redis_nodeset_slot_range_node_t;

redis_nodeset_t *nodeset_create(nchan_redis_conf_t *rcf);
redis_nodeset_t *nodeset_find(nchan_redis_conf_t *rcf);
ngx_int_t nodeset_check_status(redis_nodeset_t *nodeset);

ngx_int_t nodeset_node_destroy(redis_node_t *node);


int node_disconnect(redis_node_t *node);
int node_connect(redis_node_t *node);
void node_set_role(redis_node_t *node, redis_node_role_t role);
int node_set_master_node(redis_node_t *node, redis_node_t *master);
redis_node_t *node_find_slave_node(redis_node_t *node, redis_node_t *slave);
int node_add_slave_node(redis_node_t *node, redis_node_t *slave);
int node_remove_slave_node(redis_node_t *node, redis_node_t *slave);

ngx_int_t nodeset_connect_all(void);
int nodeset_connect(redis_nodeset_t *ns);
int nodeset_disconnect(redis_nodeset_t *ns);

ngx_int_t nodeset_set_status(redis_nodeset_t *nodeset, redis_nodeset_status_t status, const char *msg);

int nodeset_node_deduplicate_by_connect_params(redis_node_t *node);
int nodeset_node_deduplicate_by_run_id(redis_node_t *node);
int nodeset_node_deduplicate_by_cluster_id(redis_node_t *node);

redis_node_t *nodeset_node_find_by_connect_params(redis_nodeset_t *ns, redis_connect_params_t *rcp);
redis_node_t *nodeset_node_find_by_run_id(redis_nodeset_t *ns, ngx_str_t *run_id);
redis_node_t *nodeset_node_find_by_cluster_id(redis_nodeset_t *ns, ngx_str_t *cluster_id);
redis_node_t *nodeset_node_find_by_range(redis_nodeset_t *ns, redis_slot_range_t *range);
redis_node_t *nodeset_node_find_by_slot(redis_nodeset_t *ns, uint16_t slot);

redis_node_t *nodeset_node_create(redis_nodeset_t *ns, redis_connect_params_t *rcp);



#endif /* NCHAN_REDIS_NODESET_H */
