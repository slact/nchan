#ifndef NCHAN_REDIS_NODESET_H
#define NCHAN_REDIS_NODESET_H

#include <nchan_module.h>
#if NCHAN_HAVE_HIREDIS_WITH_SOCKADDR
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#if (NGX_OPENSSL)
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <hiredis/hiredis_ssl.h>
#endif
#else
#include <store/redis/hiredis/hiredis.h>
#include <store/redis/hiredis/async.h>
#if (NGX_OPENSSL)
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <store/redis/hiredis/hiredis_ssl.h>
#endif
#endif
#include <util/nchan_reaper.h>
#include <util/nchan_rbtree.h>
#include <util/nchan_list.h>
#include <util/nchan_slist.h>

//#include "store-private.h"

//#define REDIS_NODESET_DBG 1

#define node_log(node, lvl, fmt, args...) \
  ngx_log_error(lvl, ngx_cycle->log, 0, "nchan: Redis node %s " fmt, __node_nickname_cstr(node), ##args)
#define node_log_error(node, fmt, args...)    node_log((node), NGX_LOG_ERR, fmt, ##args)
#define node_log_warning(node, fmt, args...)  node_log((node), NGX_LOG_WARN, fmt, ##args)
#define node_log_notice(node, fmt, args...)   node_log((node), NGX_LOG_NOTICE, fmt, ##args)
#define node_log_info(node, fmt, args...)     node_log((node), NGX_LOG_INFO, fmt, ##args)
#define node_log_debug(node, fmt, args...)    node_log((node), NGX_LOG_DEBUG, fmt, ##args)
  
#define nodeset_log(nodeset, lvl, fmt, args...) \
  ngx_log_error(lvl, ngx_cycle->log, 0, "nchan: Redis %s: " fmt, (nodeset)->name, ##args)
#define nodeset_log_error(nodeset, fmt, args...)    nodeset_log((nodeset), NGX_LOG_ERR, fmt, ##args)
#define nodeset_log_warning(nodeset, fmt, args...)  nodeset_log((nodeset), NGX_LOG_WARN, fmt, ##args)
#define nodeset_log_notice(nodeset, fmt, args...)   nodeset_log((nodeset), NGX_LOG_NOTICE, fmt, ##args)
#define nodeset_log_info(nodeset, fmt, args...)     nodeset_log((nodeset), NGX_LOG_INFO, fmt, ##args)
#define nodeset_log_debug(nodeset, fmt, args...)    nodeset_log((nodeset), NGX_LOG_DEBUG, fmt, ##args)

#define NCHAN_MAX_NODESETS 128
#define REDIS_NODESET_STATUS_CHECK_TIME_MSEC 4000
#define REDIS_NODESET_MAX_CONNECTING_TIME_SEC 5
#define REDIS_NODESET_RECONNECT_WAIT_TIME_SEC 5
#define REDIS_NODESET_MAX_FAILING_TIME_SEC 2

#define REDIS_NODE_DEDUPLICATED         -100
#define REDIS_NODE_CONNECTION_TIMED_OUT   -2
#define REDIS_NODE_FAILED                 -1
#define REDIS_NODE_DISCONNECTED            0
#define REDIS_NODE_CMD_CONNECTING          1
#define REDIS_NODE_PUBSUB_CONNECTING       2
#define REDIS_NODE_CMD_CHECKING_CONNECTION 3
#define REDIS_NODE_CMD_CHECKED_CONNECTION  4
#define REDIS_NODE_PUBSUB_CHECKING_CONNECTION 5
#define REDIS_NODE_PUBSUB_CHECKED_CONNECTION 6
#define REDIS_NODE_CONNECTED               7
#define REDIS_NODE_CMD_AUTHENTICATING      8
#define REDIS_NODE_PUBSUB_AUTHENTICATING   9
#define REDIS_NODE_SELECT_DB              10
#define REDIS_NODE_CMD_SELECTING_DB       11
#define REDIS_NODE_PUBSUB_SELECTING_DB    12
#define REDIS_NODE_SCRIPTS_LOAD           13
#define REDIS_NODE_SCRIPTS_LOADING        14
#define REDIS_NODE_GET_INFO               15
#define REDIS_NODE_GETTING_INFO           16
#define REDIS_NODE_PUBSUB_GET_INFO        17
#define REDIS_NODE_PUBSUB_GETTING_INFO    18
#define REDIS_NODE_SUBSCRIBE_WORKER       19
#define REDIS_NODE_SUBSCRIBING_WORKER     20
#define REDIS_NODE_GET_CLUSTERINFO        21
#define REDIS_NODE_GETTING_CLUSTERINFO    22
#define REDIS_NODE_GET_CLUSTER_NODES      23
#define REDIS_NODE_GETTING_CLUSTER_NODES  24
#define REDIS_NODE_READY                  100
  
typedef struct redis_nodeset_s redis_nodeset_t;
typedef struct redis_node_s redis_node_t;

typedef struct { //redis_nodeset_cluster_t
  unsigned                    enabled:1;
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
  REDIS_NODESET_FAILED = -4,
  REDIS_NODESET_CLUSTER_FAILING = -3,
  REDIS_NODESET_FAILING = -2,
  REDIS_NODESET_INVALID = -1,
  REDIS_NODESET_DISCONNECTED = 0,
  REDIS_NODESET_CONNECTING,
  REDIS_NODESET_READY
} redis_nodeset_status_t;

#if REDIS_NODESET_DBG
typedef struct {
  int  n;
  redis_node_t *node[128];
} redis_node_dbg_list_t;
typedef struct {
  redis_slot_range_t range;
  redis_node_t       *node;
} redis_node_range_dbg_t;
typedef struct {
  int  n;
  redis_node_range_dbg_t node[128];
} redis_nodeset_dbg_range_tree_t;
#endif



struct redis_nodeset_s {
  //a set of redis nodes
  //  maybe just 1 master
  //  maybe a master and its slaves
  //  maybe a cluster of masters and their slaves
  //slaves of slaves not included
  char                       *name;
  redis_nodeset_status_t      status;
  ngx_event_t                 status_check_ev;
  const char                 *status_msg;
  time_t                      current_status_start;
  ngx_int_t                   current_status_times_checked;
  ngx_int_t                   generation;
  nchan_list_t                urls;
  nchan_loc_conf_t           *first_loc_conf;
  ngx_http_upstream_srv_conf_t *upstream;
  nchan_list_t                nodes;
  redis_nodeset_cluster_t     cluster;
  struct {                    //settings
    nchan_redis_storage_mode_t  storage_mode;
    ngx_int_t                   nostore_fastpublish;
    struct {                    //pubsub_subscribe_weight
      ngx_int_t                   master;
      ngx_int_t                   slave;
    }                           node_weight;
    time_t                      ping_interval;
    time_t                      cluster_check_interval;
    ngx_str_t                  *namespace;
    nchan_redis_optimize_t      optimize_target;
    ngx_msec_t                  connect_timeout;
    struct {
      int                         count;
      nchan_redis_ip_range_t     *list;
    }                           blacklist;
    nchan_redis_tls_settings_t  tls;
    ngx_str_t                   username;
    ngx_str_t                   password;
  }                           settings;
  
  #if (NGX_OPENSSL)
  SSL_CTX                    *ssl_context;
  #endif
  
  struct {
    nchan_slist_t               all;
    nchan_slist_t               disconnected_cmd;
    nchan_slist_t               disconnected_pubsub;
  }                           channels;
  
  nchan_reaper_t              chanhead_reaper;
  time_t                      reconnect_delay_sec;
  nchan_list_t                onready_callbacks;
#if REDIS_NODESET_DBG
  struct {
    redis_node_dbg_list_t       nodes;
    redis_nodeset_dbg_range_tree_t ranges;
    int                         keyspace_complete;
  }                           dbg;
#endif
  
}; //redis_nodeset_t

struct redis_node_s {
  int8_t                    state;
  unsigned                  discovered:1;
  redis_node_role_t         role;
  redis_connect_params_t    connect_params;
  void                     *connect_timeout;
  redis_nodeset_t          *nodeset;
  ngx_str_t                 run_id;
  ngx_str_t                 version;
  int                       scripts_loaded;
  int                       generation;
  ngx_event_t               ping_timer;
  struct {
    unsigned                  enabled:1;
    unsigned                  ok:1;
    ngx_str_t                 id;
    ngx_event_t               check_timer;
    time_t                    last_successful_check;
    int                       current_epoch; //as reported on this node
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
  int                       pending_commands;
  struct {
  nchan_slist_t               cmd;
  nchan_slist_t               pubsub;
  }                         channels;
}; //redis_node_t

typedef struct {
  redis_slot_range_t      range;
  redis_node_t           *node;
} redis_nodeset_slot_range_node_t;



redis_nodeset_t *nodeset_create(nchan_loc_conf_t *lcf);
ngx_int_t nodeset_initialize(char *worker_id, redisCallbackFn *subscribe_handler);
redis_nodeset_t *nodeset_find(nchan_redis_conf_t *rcf);
ngx_int_t nodeset_examine(redis_nodeset_t *nodeset);

ngx_int_t nodeset_node_destroy(redis_node_t *node);


int node_disconnect(redis_node_t *node, int disconnected_state);
int node_connect(redis_node_t *node);
redisContext *node_connect_sync_context(redis_node_t *node);
void node_set_role(redis_node_t *node, redis_node_role_t role);
int node_set_master_node(redis_node_t *node, redis_node_t *master);
redis_node_t *node_find_slave_node(redis_node_t *node, redis_node_t *slave);
int node_add_slave_node(redis_node_t *node, redis_node_t *slave);
int node_remove_slave_node(redis_node_t *node, redis_node_t *slave);

ngx_int_t nodeset_connect_all(void);
int nodeset_connect(redis_nodeset_t *ns);
int nodeset_node_keyslot_changed(redis_node_t *node);
int nodeset_disconnect(redis_nodeset_t *ns);
ngx_int_t nodeset_destroy_all(void);
ngx_int_t nodeset_each(void (*)(redis_nodeset_t *, void *), void *privdata);
ngx_int_t nodeset_each_node(redis_nodeset_t *, void (*)(redis_node_t *, void *), void *privdata);
ngx_int_t nodeset_callback_on_ready(redis_nodeset_t *ns, ngx_msec_t max_wait, ngx_int_t (*cb)(redis_nodeset_t *, void *), void *pd);
ngx_int_t nodeset_abort_on_ready_callbacks(redis_nodeset_t *ns);
ngx_int_t nodeset_run_on_ready_callbacks(redis_nodeset_t *ns);

ngx_int_t nodeset_set_status(redis_nodeset_t *nodeset, redis_nodeset_status_t status, const char *msg);

int nodeset_node_deduplicate_by_connect_params(redis_node_t *node);
int nodeset_node_deduplicate_by_run_id(redis_node_t *node);
int nodeset_node_deduplicate_by_cluster_id(redis_node_t *node);

redis_node_t *nodeset_node_find_by_connect_params(redis_nodeset_t *ns, redis_connect_params_t *rcp);
redis_node_t *nodeset_node_find_by_run_id(redis_nodeset_t *ns, ngx_str_t *run_id);
redis_node_t *nodeset_node_find_by_cluster_id(redis_nodeset_t *ns, ngx_str_t *cluster_id);
redis_node_t *nodeset_node_find_by_range(redis_nodeset_t *ns, redis_slot_range_t *range);
redis_node_t *nodeset_node_find_by_slot(redis_nodeset_t *ns, uint16_t slot);
redis_node_t *nodeset_node_find_by_channel_id(redis_nodeset_t *ns, ngx_str_t *channel_id);
redis_node_t *nodeset_node_find_by_key(redis_nodeset_t *ns, ngx_str_t *key);
redis_node_t *nodeset_node_find_any_ready_master(redis_nodeset_t *ns);
int nodeset_node_reply_keyslot_ok(redis_node_t *node, redisReply *r);
int nodeset_ready(redis_nodeset_t *nodeset);

//chanheads are (void *) here to avoid circular typedef dependency with store-private.h
//it's terrible, and dirty -- but quick
ngx_int_t nodeset_associate_chanhead(redis_nodeset_t *, void *chanhead);
ngx_int_t nodeset_dissociate_chanhead(void *chanhead);

ngx_int_t nodeset_node_associate_chanhead(redis_node_t *, void *chanhead);
ngx_int_t nodeset_node_associate_pubsub_chanhead(redis_node_t *, void *chanhead);
ngx_int_t nodeset_node_dissociate_chanhead(void *chanhead);
ngx_int_t nodeset_node_dissociate_pubsub_chanhead(void *chanhead);

redis_node_t *nodeset_node_find_by_chanhead(void *chanhead);
redis_node_t *nodeset_node_pubsub_find_by_chanhead(void *chanhead);



redis_node_t *nodeset_node_create(redis_nodeset_t *ns, redis_connect_params_t *rcp);

uint16_t redis_crc16(uint16_t crc, const char *buf, int len);


const char *__node_nickname_cstr(redis_node_t *node);
  
#endif /* NCHAN_REDIS_NODESET_H */
