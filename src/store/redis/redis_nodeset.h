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
#include <util/nchan_accumulator.h>
#include <util/nchan_timequeue.h>

#include "redis_lua_commands.h"
//#include "store-private.h"

//#define REDIS_NODESET_DBG 1

#define node_log(node, lvl, fmt, args...) \
  ngx_log_error(lvl, ngx_cycle->log, 0, "nchan: Redis %snode %s " fmt, (node->role == REDIS_NODE_ROLE_MASTER ? "master " : (node->role == REDIS_NODE_ROLE_SLAVE ? "slave " : "")), node_nickname_cstr(node), ##args)
#define node_log_error(node, fmt, args...)    node_log((node), NGX_LOG_ERR, fmt, ##args)
#define node_log_warning(node, fmt, args...)  node_log((node), NGX_LOG_WARN, fmt, ##args)
#define node_log_notice(node, fmt, args...)   node_log((node), NGX_LOG_NOTICE, fmt, ##args)
#define node_log_info(node, fmt, args...)     node_log((node), NGX_LOG_INFO, fmt, ##args)
#define node_log_debug(node, fmt, args...)    node_log((node), NGX_LOG_DEBUG, fmt, ##args)
  
#define nodeset_log(nodeset, lvl, fmt, args...) \
  ngx_log_error(lvl, ngx_cycle->log, 0, "nchan: Redis %s %s: " fmt, (nodeset)->name_type, (nodeset)->name, ##args)
#define nodeset_log_error(nodeset, fmt, args...)    nodeset_log((nodeset), NGX_LOG_ERR, fmt, ##args)
#define nodeset_log_warning(nodeset, fmt, args...)  nodeset_log((nodeset), NGX_LOG_WARN, fmt, ##args)
#define nodeset_log_notice(nodeset, fmt, args...)   nodeset_log((nodeset), NGX_LOG_NOTICE, fmt, ##args)
#define nodeset_log_info(nodeset, fmt, args...)     nodeset_log((nodeset), NGX_LOG_INFO, fmt, ##args)
#define nodeset_log_debug(nodeset, fmt, args...)    nodeset_log((nodeset), NGX_LOG_DEBUG, fmt, ##args)

#define NCHAN_MAX_NODESETS 128
#define REDIS_NODESET_STATUS_CHECK_TIME_MSEC 4000

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
#define REDIS_NODE_SCRIPTS_LOADED_CHECK   14  
#define REDIS_NODE_SCRIPTS_LOADING        15
#define REDIS_NODE_GET_INFO               16
#define REDIS_NODE_GETTING_INFO           17
#define REDIS_NODE_PUBSUB_GET_INFO        18
#define REDIS_NODE_PUBSUB_GETTING_INFO    19
#define REDIS_NODE_SUBSCRIBE_WORKER       20
#define REDIS_NODE_SUBSCRIBING_WORKER     21
#define REDIS_NODE_GET_CLUSTERINFO        22
#define REDIS_NODE_GETTING_CLUSTERINFO    23
#define REDIS_NODE_GET_SHARDED_PUBSUB_SUPPORT 24
#define REDIS_NODE_GETTING_SHARDED_PUBSUB_SUPPORT 25
#define REDIS_NODE_GET_CLUSTER_NODES      26
#define REDIS_NODE_GETTING_CLUSTER_NODES  27
#define REDIS_NODE_READY                  100
  
typedef struct redis_nodeset_s redis_nodeset_t;
typedef struct redis_node_s redis_node_t;

typedef struct { //redis_nodeset_cluster_t
  unsigned                    enabled:1;
  rbtree_seed_t               keyslots; //cluster rbtree seed
  redis_node_t               *recovering_on_node;
  int                         current_epoch;
  ngx_msec_t                  current_check_interval;
  ngx_event_t                 check_ev;
} redis_nodeset_cluster_t;

typedef struct {
  unsigned         min:16;
  unsigned         max:16;
} redis_slot_range_t;

typedef enum {
  REDIS_NODE_ROLE_ANY=-1,
  REDIS_NODE_ROLE_UNKNOWN = 0,
  REDIS_NODE_ROLE_MASTER,
  REDIS_NODE_ROLE_SLAVE
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

typedef enum {
  //command codes whose totals are tracked must start at 0
  
  //ANY is a catch-all group and not a particular command, so it can be <0
  NCHAN_REDIS_CMD_ANY                             = -1,
  
  //trackable command codes begin
  NCHAN_REDIS_CMD_CONNECT                         = 0,
  NCHAN_REDIS_CMD_PUBSUB_SUBSCRIBE,
  NCHAN_REDIS_CMD_PUBSUB_UNSUBSCRIBE,
  NCHAN_REDIS_CMD_CHANNEL_CHANGE_SUBSCRIBER_COUNT,
  NCHAN_REDIS_CMD_CHANNEL_DELETE,
  NCHAN_REDIS_CMD_CHANNEL_FIND,
  NCHAN_REDIS_CMD_CHANNEL_GET_MESSAGE,
  NCHAN_REDIS_CMD_CHANNEL_GET_LARGE_MESSAGE,
  NCHAN_REDIS_CMD_CHANNEL_PUBLISH,
  NCHAN_REDIS_CMD_CHANNEL_REQUEST_SUBSCRIBER_INFO,
  NCHAN_REDIS_CMD_CHANNEL_GET_SUBSCRIBER_INFO_ID,
  NCHAN_REDIS_CMD_CHANNEL_SUBSCRIBE,
  NCHAN_REDIS_CMD_CHANNEL_UNSUBSCRIBE,
  NCHAN_REDIS_CMD_CHANNEL_KEEPALIVE,
  NCHAN_REDIS_CMD_CLUSTER_CHECK,
  NCHAN_REDIS_CMD_CLUSTER_RECOVER,
  NCHAN_REDIS_CMD_OTHER,
  NCHAN_REDIS_CMD_ENUM_LAST
  //NCHAN_REDIS_CMD_ENUM_LAST must be last in this enum. It's the number of commands to track
} redis_node_cmd_tag_t;

#define NODE_STATS_MAX_NAME_STR_LENGTH 128
#define NODE_STATS_MAX_ID_STR_LENGTH 65

typedef struct {
  char                         name[NODE_STATS_MAX_NAME_STR_LENGTH];
  char                         id[NODE_STATS_MAX_ID_STR_LENGTH];
  unsigned                     attached:1;
  time_t                       detached_time; //when did you first stop using?
  nchan_accumulator_t          timings[NCHAN_REDIS_CMD_ENUM_LAST];
} redis_node_command_stats_t;

#define NODESET_MAX_STATUS_MSG_LENGTH 512
struct redis_nodeset_s {
  //a set of redis nodes
  //  maybe just 1 master
  //  maybe a master and its slaves
  //  maybe a cluster of masters and their slaves
  //slaves of slaves not included
  char                       *name;
  char                       *name_type;
  redis_nodeset_status_t      status;
  ngx_event_t                 status_check_ev;
  ngx_time_t                  current_status_start;
  ngx_int_t                   current_status_times_checked;
  ngx_msec_t                  current_reconnect_delay;
  ngx_time_t                  last_cluster_recovery_check_time;
  ngx_msec_t                  current_cluster_recovery_delay;
  ngx_int_t                   generation;
  nchan_list_t                urls;
  nchan_loc_conf_t           *first_loc_conf;
  ngx_http_upstream_srv_conf_t *upstream;
  nchan_list_t                nodes;
  redis_nodeset_cluster_t     cluster;
  struct {
    unsigned                    active:1;
    ngx_event_t                 cleanup_timer;
    nchan_list_t                list;
  }                           node_stats;
  unsigned                    use_spublish:1;
  struct {                    //settings
    nchan_redis_storage_mode_t  storage_mode;
    ngx_int_t                   nostore_fastpublish;
    ngx_int_t                   accurate_subscriber_count;
    struct {                    //pubsub_subscribe_weight
      ngx_int_t                   master;
      ngx_int_t                   slave;
    }                           node_weight;
    unsigned                    retry_commands:1;
    ngx_msec_t                  retry_commands_max_wait;
    time_t                      ping_interval;
    ngx_str_t                  *namespace;
    ngx_msec_t                  node_connect_timeout;
    ngx_msec_t                  cluster_connect_timeout;
    ngx_msec_t                  command_timeout;
    
    nchan_backoff_settings_t    reconnect_delay;
    nchan_backoff_settings_t    cluster_recovery_delay;
    nchan_backoff_settings_t    cluster_check_interval;
    
    nchan_backoff_settings_t    idle_channel_ttl;
    ngx_msec_t                  idle_channel_ttl_safety_margin;
    
    ngx_msec_t                  cluster_max_failing_msec;
    ngx_int_t                   load_scripts_unconditionally;
    struct {
      time_t                      max_detached_time_sec;
      ngx_int_t                   enabled;
    }                           node_stats;
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
  nchan_list_t                onready_callbacks;
  char                        status_msg[NODESET_MAX_STATUS_MSG_LENGTH];
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
  unsigned                  connecting:1;
  unsigned                  recovering:1;
  unsigned                  discovered:1;
  unsigned                  have_spublish:1;
  redis_node_role_t         role;
  redis_connect_params_t    connect_params;
  void                     *connect_timeout;
  redis_nodeset_t          *nodeset;
  ngx_str_t                 run_id;
  ngx_str_t                 version;
  int                       generation;
  ngx_event_t               ping_timer;
  struct {
    unsigned                  enabled:1;
    unsigned                  ok:1;
    ngx_str_t                 id;
    ngx_str_t                 master_id;
    int                       current_epoch; //as reported on this node
    struct {
      redis_slot_range_t         *range;
      size_t                      n;
      unsigned                    indexed:1;
    }                         slot_range; 
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
  struct {
    redis_node_command_stats_t *data;
    struct {         
      nchan_timequeue_t       cmd;
      nchan_timequeue_t       pubsub;
    }                       timequeue;
  }                         stats;
  int                       pending_commands;
  struct {
    long long                 sent;
    long long                 received;
    long long                 prev_sent;
    ngx_event_t               ev;
  }                         timeout;
  struct {
  nchan_slist_t               cmd;
  nchan_slist_t               pubsub;
  }                         channels;
  struct {
    uint8_t                   loaded[REDIS_LUA_SCRIPTS_COUNT];
    uint8_t                   current;
    unsigned                  loading:1;
  }                         scripts_load_state;
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
redis_node_t *nodeset_random_node(redis_nodeset_t *ns, int min_state, redis_node_role_t role);

int node_disconnect(redis_node_t *node, int disconnected_state);
int node_connect(redis_node_t *node);
redisContext *node_connect_sync_context(redis_node_t *node);
void node_set_role(redis_node_t *node, redis_node_role_t role);
int node_set_master_node(redis_node_t *node, redis_node_t *master);
redis_node_t *node_find_slave_node(redis_node_t *node, redis_node_t *slave);
int node_add_slave_node(redis_node_t *node, redis_node_t *slave);
int node_remove_slave_node(redis_node_t *node, redis_node_t *slave);

void node_command_sent(redis_node_t *node);
void node_command_received(redis_node_t *node);

void node_command_time_start(redis_node_t *node, redis_node_cmd_tag_t cmdtag);
void node_command_time_finish(redis_node_t *node, redis_node_cmd_tag_t cmdtag);

ngx_int_t nodeset_connect_all(void);
int nodeset_connect(redis_nodeset_t *ns);
int nodeset_node_keyslot_changed(redis_node_t *node, const char *reason);
int nodeset_disconnect(redis_nodeset_t *ns);
ngx_int_t nodeset_destroy_all(void);
ngx_int_t nodeset_each(void (*)(redis_nodeset_t *, void *), void *privdata);
ngx_int_t nodeset_each_node(redis_nodeset_t *, void (*)(redis_node_t *, void *), void *privdata);
ngx_int_t nodeset_callback_on_ready(redis_nodeset_t *ns,  ngx_int_t (*cb)(redis_nodeset_t *, void *), void *pd);
ngx_int_t nodeset_abort_on_ready_callbacks(redis_nodeset_t *ns);
ngx_int_t nodeset_run_on_ready_callbacks(redis_nodeset_t *ns);

ngx_int_t nodeset_set_status(redis_nodeset_t *nodeset, redis_nodeset_status_t status, const char *msg);


char *node_dbg_sprint(redis_node_t *node, char *buf, size_t maxlen);
void nodeset_dbg_log_nodes(redis_nodeset_t *ns, unsigned loglevel);
void nodeset_dbg_log_nodes_and_clusternodes_lines(redis_nodeset_t *ns, unsigned loglevel, void *lines, size_t line_count);

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
int nodeset_node_can_retry_commands(redis_node_t *node);
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
int node_channel_in_keyspace(redis_node_t *node, void *chanhead);

typedef enum {
  REDIS_NODE_CTX_COMMAND,
  REDIS_NODE_CTX_PUBSUB
} redis_node_ctx_type_t;

#define REDIS_NODE_BATCH_COMMAND_MAX_ARGS 256
typedef struct {
  redis_node_t    *node;
  redis_node_ctx_type_t ctxtype;
  redisCallbackFn *callback;
  void            *privdata;
  size_t           cmdc;
  size_t           argc;
  const char      *argv[REDIS_NODE_BATCH_COMMAND_MAX_ARGS];
  size_t           argvlen[REDIS_NODE_BATCH_COMMAND_MAX_ARGS];
  unsigned         commands_sent;
} node_batch_command_t;

void node_batch_command_init(node_batch_command_t *batch, redis_node_t *node, redis_node_ctx_type_t ctxtype, redisCallbackFn *fn, void *privdata, unsigned cmd_count, ...);
void node_batch_command_send(node_batch_command_t *batch);
int node_batch_command_add_ngx_str(node_batch_command_t *batch, const ngx_str_t *arg);
int node_batch_command_add(node_batch_command_t *batch, const char *arg, size_t arglen);
unsigned node_batch_command_times_sent(node_batch_command_t *batch);



redis_node_t *nodeset_node_create(redis_nodeset_t *ns, redis_connect_params_t *rcp);

uint16_t redis_keyslot_from_channel_id(ngx_str_t *chid);

uint16_t redis_crc16(uint16_t crc, const char *buf, int len);

const char *node_nickname_cstr(redis_node_t *node);


#define REDIS_NODE_CMD_TIMEQUEUE_LENGTH 64
#define REDIS_NODE_PUBSUB_TIMEQUEUE_LENGTH 64

typedef struct {
  char                        *error;
  char                        *name;
  size_t                       count;
  redis_node_command_stats_t  *stats;
} redis_nodeset_command_stats_t;

redis_node_command_stats_t *redis_nodeset_worker_command_stats_alloc(redis_nodeset_t *ns, size_t *node_stats_count);
ngx_int_t redis_nodeset_global_command_stats_palloc_async(ngx_str_t *nodeset_name, ngx_pool_t *pool, callback_pt cb, void *pd);


int redis_nodeset_stats_init(redis_nodeset_t *ns);
void redis_nodeset_stats_destroy(redis_nodeset_t *ns);
void redis_node_stats_init(redis_node_t *node);
redis_node_command_stats_t *redis_node_stats_attach(redis_node_t *node);
void redis_node_stats_detach(redis_node_t *node);
void redis_node_stats_destroy(redis_node_t *node);
redis_node_command_stats_t *redis_node_get_stats(redis_node_t *node);


void node_command_time_start(redis_node_t *node, redis_node_cmd_tag_t cmdtag);
void node_command_time_finish(redis_node_t *node, redis_node_cmd_tag_t cmdtag);
void node_command_time_finish_relaxed(redis_node_t *node, redis_node_cmd_tag_t cmdtag);

void node_pubsub_time_start(redis_node_t *node, redis_node_cmd_tag_t cmdtag);
void node_pubsub_time_finish(redis_node_t *node, redis_node_cmd_tag_t cmdtag);
void node_pubsub_time_finish_relaxed(redis_node_t *node, redis_node_cmd_tag_t cmdtag);

void node_time_record(redis_node_t *node, redis_node_cmd_tag_t cmdtag, ngx_msec_t t);

ngx_chain_t *redis_nodeset_stats_response_body_chain_palloc(redis_nodeset_command_stats_t *nstats , ngx_pool_t *pool);
  
#endif /* NCHAN_REDIS_NODESET_H */
