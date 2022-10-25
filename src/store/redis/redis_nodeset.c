#include "redis_nodeset.h"

#include <assert.h>
#include "store-private.h"
#include <store/store_common.h>
#include "store.h"
#include "redis_nginx_adapter.h"
#include "redis_nodeset_parser.h"
#include "redis_lua_commands.h"

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "REDIS NODESET: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS NODESET: " fmt, ##args)

const nchan_backoff_settings_t NCHAN_REDIS_DEFAULT_CLUSTER_CHECK_INTERVAL = {
  .min = NCHAN_REDIS_DEFAULT_CLUSTER_CHECK_INTERVAL_MIN_MSEC,
  .backoff_multiplier = NCHAN_REDIS_DEFAULT_CLUSTER_CHECK_INTERVAL_BACKOFF_MULTIPLIER,
  .jitter_multiplier = NCHAN_REDIS_DEFAULT_CLUSTER_CHECK_INTERVAL_JITTER_MULTIPLIER,
  .max = NCHAN_REDIS_DEFAULT_CLUSTER_CHECK_INTERVAL_MAX_MSEC
};

const nchan_backoff_settings_t NCHAN_REDIS_DEFAULT_RECONNECT_DELAY = {
  .min = NCHAN_REDIS_DEFAULT_RECONNECT_DELAY_MIN_MSEC,
  .backoff_multiplier = NCHAN_REDIS_DEFAULT_RECONNECT_DELAY_BACKOFF_MULTIPLIER,
  .jitter_multiplier = NCHAN_REDIS_DEFAULT_RECONNECT_DELAY_JITTER_MULTIPLIER,
  .max = NCHAN_REDIS_DEFAULT_RECONNECT_DELAY_MAX_MSEC
};

const nchan_backoff_settings_t NCHAN_REDIS_DEFAULT_CLUSTER_RECOVERY_DELAY = {
  .min = NCHAN_REDIS_DEFAULT_CLUSTER_RECOVERY_DELAY_MIN_MSEC,
  .backoff_multiplier = NCHAN_REDIS_DEFAULT_CLUSTER_RECOVERY_DELAY_BACKOFF_MULTIPLIER,
  .jitter_multiplier = NCHAN_REDIS_DEFAULT_CLUSTER_RECOVERY_DELAY_JITTER_MULTIPLIER,
  .max = NCHAN_REDIS_DEFAULT_CLUSTER_RECOVERY_DELAY_MAX_MSEC
};


redis_nodeset_t  redis_nodeset[NCHAN_MAX_NODESETS];
int              redis_nodeset_count = 0;
char            *redis_worker_id = NULL;
char            *nchan_redis_blankname = "";
static redisCallbackFn *redis_subscribe_callback = NULL;

typedef struct {
  ngx_event_t      ev;
  ngx_int_t      (*cb)(redis_nodeset_t *, void *);
  void            *pd;
  redis_nodeset_t *ns;
} nodeset_onready_callback_t;


static ngx_str_t       default_redis_url = ngx_string(NCHAN_REDIS_DEFAULT_URL);

static void node_connector_callback(redisAsyncContext *ac, void *rep, void *privdata);
static int nodeset_cluster_keyslot_space_complete(redis_nodeset_t *ns, int min_node_state);
static nchan_redis_ip_range_t *node_ip_blacklisted(redis_nodeset_t *ns, redis_connect_params_t *rcp);
static int nodeset_recover_cluster(redis_nodeset_t *ns);
static int nodeset_reset_cluster_node_info(redis_nodeset_t *ns);
static void nodeset_cluster_check_event(ngx_event_t *ev);
static void nodeset_check_status_event(ngx_event_t *ev);
static void nodeset_check_spublish_availability(redis_nodeset_t *ns);
static int nodeset_set_name_alloc(redis_nodeset_t *nodeset);
static int nodeset_link_cluster_node_roles(redis_nodeset_t *nodeset);
static int nodeset_cluster_node_is_outdated(redis_nodeset_t *ns, cluster_nodes_line_t *l);
static int node_discover_cluster_peer(redis_node_t *node, cluster_nodes_line_t *l, redis_node_t **known_node);
static int node_skip_cluster_peer(redis_node_t *node, cluster_nodes_line_t *l, int log_action, int skip_self);
static int node_set_cluster_slots(redis_node_t *node, cluster_nodes_line_t *l, char *errbuf, size_t max_err_len);
static int node_unset_cluster_slots(redis_node_t *node);
static void node_make_ready(redis_node_t *node);

static char *node_role_cstr(redis_node_role_t role) {
  switch(role) {
    case REDIS_NODE_ROLE_ANY:
      return "any_role";
    case REDIS_NODE_ROLE_MASTER:
      return "master";
    case REDIS_NODE_ROLE_SLAVE:
      return "slave";
    case REDIS_NODE_ROLE_UNKNOWN:
      return "unknown_role";
  }
  return "???";
}
static char *node_state_cstr(int state) {
  switch(state) {
    case REDIS_NODE_DEDUPLICATED:
      return "DEDUPLICATED";
    case REDIS_NODE_CONNECTION_TIMED_OUT:
      return "CONNECTION_TIMED_OUT";
    case REDIS_NODE_FAILED:
      return "FAILED";
    case REDIS_NODE_DISCONNECTED:
      return "DISCONNECTED";
    case REDIS_NODE_CMD_CONNECTING:
      return "CMD_CONNECTING";
    case REDIS_NODE_PUBSUB_CONNECTING:
      return "PUBSUB_CONNECTING";
    case REDIS_NODE_CMD_CHECKING_CONNECTION:
      return "CMD_CHECKING_CONNECTION";
    case REDIS_NODE_CMD_CHECKED_CONNECTION:
      return "CMD_CHECKED_CONNECTION";
    case REDIS_NODE_PUBSUB_CHECKING_CONNECTION:
      return "PUBSUB_CHECKING_CONNECTION";
    case REDIS_NODE_PUBSUB_CHECKED_CONNECTION:
      return "PUBSUB_CHECKED_CONNECTION";
    case REDIS_NODE_CONNECTED:
      return "CONNECTED";
    case REDIS_NODE_CMD_AUTHENTICATING:
      return "CMD_AUTHENTICATING";
    case REDIS_NODE_PUBSUB_AUTHENTICATING:
      return "PUBSUB_AUTHENTICATING";
    case REDIS_NODE_SELECT_DB:
      return "SELECT_DB";
    case REDIS_NODE_CMD_SELECTING_DB:
      return "CMD_SELECTING_DB";
    case REDIS_NODE_PUBSUB_SELECTING_DB:
      return "PUBSUB_SELECTING_DB";
    case REDIS_NODE_SCRIPTS_LOAD:
      return "SCRIPTS_LOAD";
    case REDIS_NODE_SCRIPTS_LOADED_CHECK:
      return "SCRIPTS_LOADED_CHECK";
    case REDIS_NODE_SCRIPTS_LOADING:
      return "SCRIPTS_LOADING";
    case REDIS_NODE_GET_INFO:
      return "GET_INFO";
    case REDIS_NODE_GETTING_INFO:
      return "GETTING_INFO";
    case REDIS_NODE_PUBSUB_GET_INFO:
      return "PUBSUB_GET_INFO";
    case REDIS_NODE_PUBSUB_GETTING_INFO:
      return "PUBSUB_GETTING_INFO";
    case REDIS_NODE_SUBSCRIBE_WORKER:
      return "SUBSCRIBE_WORKER";
    case REDIS_NODE_SUBSCRIBING_WORKER:
      return "SUBSCRIBING_WORKER";
    case REDIS_NODE_GET_CLUSTERINFO:
      return "GET_CLUSTERINFO";
    case REDIS_NODE_GETTING_CLUSTERINFO:
      return "GETTING_CLUSTERINFO";
    case REDIS_NODE_GET_SHARDED_PUBSUB_SUPPORT:
      return "GET_SHARDED_PUBSUB_SUPPORT";
    case REDIS_NODE_GETTING_SHARDED_PUBSUB_SUPPORT:
      return "GETTING_SHARDED_PUBSUB_SUPPORT";
    case REDIS_NODE_GET_CLUSTER_NODES:
      return "GET_CLUSTER_NODES";
    case REDIS_NODE_GETTING_CLUSTER_NODES:
      return "GETTING_CLUSTER_NODES";
    case REDIS_NODE_READY:
      return "READY";
  }
  return "(?)";
}

static void *rbtree_cluster_keyslots_node_id(void *data) {
  return &((redis_nodeset_slot_range_node_t *)data)->range;
}
static uint32_t rbtree_cluster_keyslots_bucketer(void *vid) {
  return 1; //no buckets
}
static ngx_int_t rbtree_cluster_keyslots_compare(void *v1, void *v2) {
  redis_slot_range_t   *r1 = v1;
  redis_slot_range_t   *r2 = v2;
  
  if(r1->max < r2->min) //r1 is strictly left of r2
    return -1;
  else if(r1->min > r2->max) //r1 is strictly right of r2
    return 1;
  else //there's an overlap
    return 0;
}

static int reply_str_ok(redisReply *reply) {
  return (reply != NULL && reply->type != REDIS_REPLY_ERROR && reply->type == REDIS_REPLY_STRING);
}
static int reply_array_ok(redisReply *reply) {
  return (reply != NULL && reply->type == REDIS_REPLY_ARRAY);
}
static int reply_integer_ok(redisReply *reply) {
  return (reply != NULL && reply->type == REDIS_REPLY_INTEGER);
}
static int reply_status_ok(redisReply *reply) {
  return (
    reply != NULL 
    && reply->type != REDIS_REPLY_ERROR
    && reply->type == REDIS_REPLY_STATUS
    && reply->str
    && strcmp(reply->str, "OK") == 0
  );
}

static int nodeset_cluster_node_index_keyslot_ranges(redis_node_t *node) {
  unsigned                         i, j;
  ngx_rbtree_node_t               *rbtree_node;
  redis_nodeset_slot_range_node_t *keyslot_tree_node;
  rbtree_seed_t                   *tree = &node->nodeset->cluster.keyslots;
  if(node->cluster.slot_range.indexed) {
    node_log_error(node, "cluster keyslot range already indexed");
    return 0;
  }
  
  for(i=0; i<node->cluster.slot_range.n; i++) {
    if(nodeset_node_find_by_range(node->nodeset, &node->cluster.slot_range.range[i])) { //overlap!
      return 0;
    }
  }
  
  for(i=0; i<node->cluster.slot_range.n; i++) {
    rbtree_node = rbtree_create_node(tree, sizeof(*keyslot_tree_node));
    keyslot_tree_node = rbtree_data_from_node(rbtree_node);
    keyslot_tree_node->range = node->cluster.slot_range.range[i];
    keyslot_tree_node->node = node;
    if(rbtree_insert_node(tree, rbtree_node) != NGX_OK) {
      for(j=0; j<i; j++) {
        //undo insertion so far
        rbtree_node = rbtree_find_node(tree, &node->cluster.slot_range.range[i]);
        if(rbtree_node) {
          rbtree_remove_node(tree, rbtree_node);
          rbtree_destroy_node(tree, rbtree_node);
        }
      }
      node_log_error(node, "couldn't insert keyslot node range %d-%d", keyslot_tree_node->range.min, keyslot_tree_node->range.max);
      rbtree_destroy_node(tree, rbtree_node);
      return 0;
    }
    else {
      node_log_debug(node, "inserted keyslot node range %d-%d", keyslot_tree_node->range.min, keyslot_tree_node->range.max);
    }
  }
  node->cluster.slot_range.indexed = 1;
  return 1;
}

char *node_dbg_sprint(redis_node_t *node, char *buf, size_t maxlen) {
  char           slotsline[256];
  char          *slots;
  slots = (char *)ngx_sprintf((u_char *)slotsline, "(%d)", node->cluster.slot_range.n);
  if(node->cluster.slot_range.n) {
    unsigned i;
    for(i=0; i<node->cluster.slot_range.n; i++) {
      slots+= sprintf(slots, "%d-%d,", node->cluster.slot_range.range[i].min, node->cluster.slot_range.range[i].max);
    }
    slots--;
    
    sprintf(slots, " idx:%d", node->cluster.slot_range.indexed);
  }
  else {
    sprintf(slots, "-");
  }
  
  char masterstr[256];
  ngx_sprintf((u_char *)masterstr, "%s%Z", node->peers.master ? node_nickname_cstr(node->peers.master) : "-");
  
  if(node->cluster.enabled) {
    ngx_snprintf((u_char *)buf, maxlen, "%p %s <%s> (cluster) r:%s id:%V m:[%V]%s s:[%s]%Z", node, node_nickname_cstr(node), node_state_cstr(node->state), node_role_cstr(node->role), &node->cluster.id, &node->cluster.master_id, masterstr, slotsline);
  }
  else {
    ngx_snprintf((u_char *)buf, maxlen, "%p %s <%s> %s %V%Z", node, node_nickname_cstr(node), node_state_cstr(node->state), node_role_cstr(node->role), &node->run_id);
  }
  return buf;
}

void nodeset_dbg_log_nodes(redis_nodeset_t *ns, unsigned loglevel) {
  int             n = 0;
  redis_node_t   *cur;
  char            nodestr[1024];
  
  for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    n++;
  }
  
  nodeset_log(ns, loglevel, "Redis upstream%s nodes (%d):", ns->cluster.enabled ? " cluster" : "", n); 
  
  for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    ngx_log_error(loglevel, ngx_cycle->log, 0, "    %s", node_dbg_sprint(cur, nodestr, sizeof(nodestr)));
  }
}

void nodeset_dbg_log_nodes_and_clusternodes_lines(redis_nodeset_t *ns, unsigned loglevel, void *l, size_t line_count) {
  cluster_nodes_line_t *lines = l;
  nodeset_dbg_log_nodes(ns, NGX_LOG_NOTICE);
  if(lines && line_count > 0) {
    unsigned i;
    ngx_log_error(loglevel, ngx_cycle->log, 0, "CLUSTER NODES reply:");
    for(i=0; i<line_count; i++) {
      ngx_log_error(NGX_LOG_NOTICE, ngx_cycle->log, 0,   "%V", &lines[i].line);
    }
  }
}

/*
static ngx_int_t print_slot_range_node(rbtree_seed_t *tree, void *node_data, void *privdata) {
  redis_nodeset_slot_range_node_t        *rangenode = node_data;
  node_log_notice(rangenode->node, "slots [%d - %d]", rangenode->range.min, rangenode->range.max);
  return NGX_OK;
}
*/
static int nodeset_cluster_node_unindex_keyslot_ranges(redis_node_t *node) {
  ngx_rbtree_node_t               *rbtree_node;
  redis_slot_range_t              *range;
  rbtree_seed_t                   *tree = &node->nodeset->cluster.keyslots;
  unsigned                         i;
  if(!node->cluster.slot_range.indexed) {
    //node_log_notice(node, "already unindexed");
    return 1;
  }
  //node_log_notice(node, "unindex keyslot ranges");
  
  //rbtree_walk_incr(tree, print_slot_range_node, NULL);
  
  for(i=0; i<node->cluster.slot_range.n; i++) {
    range = &node->cluster.slot_range.range[i];
    //node_log_notice(node, "unindexing range [%d - %d]", range->min, range->max);
    if((rbtree_node = rbtree_find_node(tree, range)) != NULL) {
      rbtree_remove_node(tree, rbtree_node);
      rbtree_destroy_node(tree, rbtree_node);
    }
    else {
      node_log_error(node, "unable to unindex keyslot range %d-%d: range not found in tree", range->min, range->max);
      raise(SIGABRT);
    }
  }
  node->cluster.slot_range.indexed = 0;
  return 1;
}

#if REDIS_NODESET_DBG


static ngx_int_t nodeset_debug_rangetree_collector(rbtree_seed_t *tree, void *node_data, void *privdata) {
  redis_nodeset_slot_range_node_t     *rangenode = node_data;
  redis_nodeset_dbg_range_tree_t      *dbg = privdata;
  dbg->node[dbg->n].range = rangenode->range;
  dbg->node[dbg->n].node = rangenode->node;
  dbg->n++;
  return NGX_OK;
}

static redis_node_dbg_list_t *nodeset_update_debuginfo(redis_nodeset_t *nodeset) {
  rbtree_seed_t         *tree = &nodeset->cluster.keyslots;
  redis_node_dbg_list_t *node_dbg = &nodeset->dbg.nodes;
  redis_node_t  *cur;
  ngx_memzero(&nodeset->dbg, sizeof(nodeset->dbg));
  for(cur = nchan_list_first(&nodeset->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    node_dbg->node[node_dbg->n++]=cur;
  }
  nodeset->dbg.keyspace_complete = nodeset_cluster_keyslot_space_complete(nodeset, REDIS_NODE_READY);
  rbtree_walk_incr(tree, nodeset_debug_rangetree_collector, &nodeset->dbg.ranges);
  return NGX_OK;
}
#endif

static const char *rcp_cstr(redis_connect_params_t *rcp) {
  static char    buf[512];
  ngx_snprintf((u_char *)buf, 512, "%V:%d%Z", rcp->peername.len > 0 ? &rcp->peername : &rcp->hostname, rcp->port);
  return buf;
}
static const char *node_cstr(redis_node_t *node) {
  return rcp_cstr(&node->connect_params);
}

#define MAX_RUN_ID_LENGTH 64
#define MAX_CLUSTER_ID_LENGTH 64
#define MAX_VERSION_LENGTH 16
typedef struct {
  redis_node_t    node;
  u_char          peername[INET6_ADDRSTRLEN + 2];
  u_char          run_id[MAX_RUN_ID_LENGTH];
  u_char          cluster_id[MAX_CLUSTER_ID_LENGTH];
  u_char          cluster_master_id[MAX_CLUSTER_ID_LENGTH];
  u_char          version[MAX_VERSION_LENGTH];
} node_blob_t;

int nodeset_ready(redis_nodeset_t *nodeset) {
  return nodeset && nodeset->status == REDIS_NODESET_READY;
}

ngx_int_t nodeset_initialize(char *worker_id, redisCallbackFn *subscribe_handler) {
  redis_worker_id = worker_id;
  redis_subscribe_callback = subscribe_handler;
  return NGX_OK;
}


#if (NGX_OPENSSL)
static ngx_int_t nodeset_create_ssl_ctx(redis_nodeset_t *ns, char **err) {
  char        *ca_cert =  ns->settings.tls.trusted_certificate.len > 0 ? 
                            (char *)ns->settings.tls.trusted_certificate.data
                            : NULL;
  char        *ca_path = ns->settings.tls.trusted_certificate_path.len > 0 ?
                            (char *)ns->settings.tls.trusted_certificate_path.data
                            : NULL;
  char        *client_cert_filename = ns->settings.tls.client_certificate.len > 0 ?
                            (char *)ns->settings.tls.client_certificate.data
                            : NULL;
  char        *client_cert_key_filename = ns->settings.tls.client_certificate_key.len > 0 ?
                            (char *)ns->settings.tls.client_certificate_key.data
                            : NULL;
  char        *ciphers = ns->settings.tls.ciphers.len > 0 ?
                            (char *)ns->settings.tls.ciphers.data
                            : NULL;
  int          verify_cert = ns->settings.tls.verify_certificate;
  
  SSL_CTX *ssl_ctx = SSL_CTX_new(SSLv23_client_method());
  if(!ssl_ctx) {
    *err = "failed to create SSL_CTX";
    return 0;
  }
  SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);
  SSL_CTX_set_verify(ssl_ctx, verify_cert ? SSL_VERIFY_NONE : SSL_VERIFY_PEER, NULL);
  
  if(ca_cert || ca_path) {
    if (!SSL_CTX_load_verify_locations(ssl_ctx, ca_cert, ca_path)) {
      *err = "Invalid CA Certificate File/Directory";
      SSL_CTX_free(ssl_ctx);
      return 0;
    }
  }
  else {
    if (!SSL_CTX_set_default_verify_paths(ssl_ctx)) {
      *err = "Failed to use default CA paths";
      SSL_CTX_free(ssl_ctx);
      return 0;
    }
  }
  
  if (client_cert_filename && !SSL_CTX_use_certificate_chain_file(ssl_ctx, client_cert_filename)) {
    *err = "Invalid client certificate";
    SSL_CTX_free(ssl_ctx);
    return 0;
  }
  
  if (client_cert_key_filename && !SSL_CTX_use_PrivateKey_file(ssl_ctx, client_cert_key_filename, SSL_FILETYPE_PEM)) {
    *err = "Invalid private key";
    SSL_CTX_free(ssl_ctx);
    return 0;
  }
#ifdef TLS1_3_VERSION
  if (ciphers && !SSL_CTX_set_cipher_list(ssl_ctx, ciphers)) {
    *err = "Error while configuring ciphers";
    SSL_CTX_free(ssl_ctx);
    return 0;
  }
#endif

  ns->ssl_context = ssl_ctx;
  return 1;
}
#else
static ngx_int_t nodeset_create_ssl_ctx(redis_nodeset_t *ns, char **err) {
  *err = "Can't create Redis SSL context: Nginx was built without SSL support";
  return 0;
}
#endif

redis_nodeset_t *nodeset_create(nchan_loc_conf_t *lcf) {
  nchan_redis_conf_t  *rcf = &lcf->redis;
  redis_nodeset_t     *ns = &redis_nodeset[redis_nodeset_count]; //incremented once everything is ok
  assert(rcf->enabled);
  assert(!rcf->nodeset);
  
  ns->first_loc_conf = lcf;
  
  if(redis_nodeset_count >= NCHAN_MAX_NODESETS) {
    nchan_log_error("Cannot create more than %d Redis nodesets", NCHAN_MAX_NODESETS);
    return NULL;
  }
  
  assert(!nodeset_find(rcf)); //must be unique
  
  nchan_list_init(&ns->urls, sizeof(ngx_str_t *), "redis urls");
  nchan_list_init(&ns->nodes, sizeof(node_blob_t), "redis nodes");
  nchan_list_init(&ns->onready_callbacks, sizeof(nodeset_onready_callback_t), "nodeset onReady callbacks");
  
  nchan_slist_init(&ns->channels.all, rdstore_channel_head_t, redis.slist.nodeset.prev, redis.slist.nodeset.next);
  nchan_slist_init(&ns->channels.disconnected_cmd, rdstore_channel_head_t, redis.slist.node_cmd.prev, redis.slist.node_cmd.next);
  nchan_slist_init(&ns->channels.disconnected_pubsub, rdstore_channel_head_t, redis.slist.node_pubsub.prev, redis.slist.node_pubsub.next);
  
  ns->current_status_times_checked = 0;
  ns->current_status_start.sec = 0;
  ns->current_status_start.msec = 0;
  ns->current_status_start.gmtoff = 0;
  ns->last_cluster_recovery_check_time.sec=0;
  ns->last_cluster_recovery_check_time.msec=0;
  ns->last_cluster_recovery_check_time.gmtoff=0;
  ns->generation = 0;
  ns->settings.namespace = &rcf->namespace;
  ns->settings.storage_mode = rcf->storage_mode;
  ns->settings.nostore_fastpublish = rcf->nostore_fastpublish;
  
  ns->settings.ping_interval = rcf->ping_interval;
  
  ns->status = REDIS_NODESET_DISCONNECTED;
  ngx_memzero(&ns->status_check_ev, sizeof(ns->status_check_ev));
  ns->status_msg[0] = '\0';
  nchan_init_timer(&ns->status_check_ev, nodeset_check_status_event, ns);
  
  //init cluster stuff
  ns->cluster.enabled = 0;
  ns->cluster.recovering_on_node = NULL;
  ns->cluster.current_check_interval = 0;
  ngx_memzero(&ns->cluster.check_ev, sizeof(ns->cluster.check_ev));
  nchan_init_timer(&ns->cluster.check_ev, nodeset_cluster_check_event, ns);
  ns->cluster.current_epoch = 0;
  rbtree_init(&ns->cluster.keyslots, "redis cluster node (by keyslot) data", rbtree_cluster_keyslots_node_id, rbtree_cluster_keyslots_bucketer, rbtree_cluster_keyslots_compare);
  
  ns->use_spublish = 0;
  
  //various upstream settings
  if(rcf->upstream) {
    nchan_srv_conf_t           *scf = NULL;
    scf = ngx_http_conf_upstream_srv_conf(rcf->upstream, ngx_nchan_module);
    
    ngx_uint_t                   i;
    ngx_array_t                 *servers = rcf->upstream->servers;
    ngx_http_upstream_server_t  *usrv = servers->elts;
    ngx_str_t                   *upstream_url, **urlref;
    ns->upstream = rcf->upstream;
    
    ns->settings.retry_commands = scf->redis.retry_commands == NGX_CONF_UNSET ? NCHAN_DEFAULT_REDIS_CAN_RETRY_COMMANDS : scf->redis.retry_commands;
    
    ns->settings.retry_commands_max_wait = scf->redis.retry_commands_max_wait == NGX_CONF_UNSET_MSEC ? NCHAN_DEFAULT_REDIS_RETRY_COMMANDS_MAX_WAIT_MSEC : scf->redis.retry_commands_max_wait;
    
    ns->settings.command_timeout = scf->redis.command_timeout == NGX_CONF_UNSET_MSEC ? NCHAN_DEFAULT_REDIS_COMMAND_TIMEOUT_MSEC : scf->redis.command_timeout;
    ns->settings.node_connect_timeout = scf->redis.node_connect_timeout == NGX_CONF_UNSET_MSEC ? NCHAN_DEFAULT_REDIS_NODE_CONNECT_TIMEOUT_MSEC : scf->redis.node_connect_timeout;
    ns->settings.cluster_connect_timeout = scf->redis.cluster_connect_timeout == NGX_CONF_UNSET_MSEC ? NCHAN_DEFAULT_REDIS_CLUSTER_CONNECT_TIMEOUT_MSEC : scf->redis.cluster_connect_timeout;
    ns->settings.cluster_max_failing_msec = scf->redis.cluster_max_failing_msec == NGX_CONF_UNSET_MSEC ? NCHAN_DEFAULT_REDIS_CLUSTER_MAX_FAILING_TIME_MSEC : scf->redis.cluster_max_failing_msec;
    ns->settings.load_scripts_unconditionally = scf->redis.load_scripts_unconditionally == NGX_CONF_UNSET ? 0 : scf->redis.load_scripts_unconditionally;
    
    ns->settings.reconnect_delay = NCHAN_CONF_UNSEC_BACKOFF;
    nchan_conf_merge_backoff_value(&ns->settings.reconnect_delay, &scf->redis.reconnect_delay, &NCHAN_REDIS_DEFAULT_RECONNECT_DELAY);
    
    ns->settings.cluster_recovery_delay = NCHAN_CONF_UNSEC_BACKOFF;
    nchan_conf_merge_backoff_value(&ns->settings.cluster_recovery_delay, &scf->redis.cluster_recovery_delay, &NCHAN_REDIS_DEFAULT_CLUSTER_RECOVERY_DELAY);
    
    ns->settings.cluster_check_interval = NCHAN_CONF_UNSEC_BACKOFF;
    nchan_conf_merge_backoff_value(&ns->settings.cluster_check_interval, &scf->redis.cluster_check_interval, &NCHAN_REDIS_DEFAULT_CLUSTER_CHECK_INTERVAL);
    
    ns->settings.idle_channel_ttl = NCHAN_CONF_UNSEC_BACKOFF;
    nchan_conf_merge_backoff_value(&ns->settings.idle_channel_ttl, &scf->redis.idle_channel_ttl, &NCHAN_REDIS_DEFAULT_IDLE_CHANNEL_TTL);
  
    ns->settings.idle_channel_ttl_safety_margin = scf->redis.idle_channel_ttl_safety_margin == NGX_CONF_UNSET_MSEC ? NCHAN_REDIS_IDLE_CHANNEL_TTL_SAFETY_MARGIN_MSEC : scf->redis.idle_channel_ttl_safety_margin;
    
    ns->settings.node_weight.master = scf->redis.master_weight == NGX_CONF_UNSET ? 1 : scf->redis.master_weight;
    ns->settings.node_weight.slave = scf->redis.slave_weight == NGX_CONF_UNSET ? 1 : scf->redis.slave_weight;
    
    ns->settings.accurate_subscriber_count = scf->redis.accurate_subscriber_count == NGX_CONF_UNSET ? 0 : scf->redis.accurate_subscriber_count;
    
    ns->settings.node_stats.enabled = scf->redis.stats.enabled == NGX_CONF_UNSET ? nchan_redis_stats_enabled : scf->redis.stats.enabled;
    
    ns->settings.node_stats.max_detached_time_sec = scf->redis.stats.max_detached_time_sec == NGX_CONF_UNSET ? NCHAN_REDIS_DEFAULT_STATS_MAX_DETACHED_TIME_SEC : scf->redis.stats.max_detached_time_sec;
    
    ns->settings.blacklist.count = scf->redis.blacklist_count;
    ns->settings.blacklist.list = scf->redis.blacklist;
    
    ns->settings.username = scf->redis.username;
    ns->settings.password = scf->redis.password;
    
    ns->settings.tls = scf->redis.tls;
    //clean up unset values
    if(ns->settings.tls.enabled == NGX_CONF_UNSET) {
      ns->settings.tls.enabled = 0;
    }
    if(ns->settings.tls.verify_certificate == NGX_CONF_UNSET) {
      ns->settings.tls.verify_certificate = 1;
    }
    
    for(i=0; i < servers->nelts; i++) {
#if nginx_version >= 1007002
      upstream_url = &usrv[i].name;
#else
      upstream_url = &usrv[i].addrs->name;
#endif
      urlref = nchan_list_append(&ns->urls);
      *urlref = upstream_url;
    }
  }
  else {
    ns->upstream = NULL;
    ns->settings.node_connect_timeout = NCHAN_DEFAULT_REDIS_NODE_CONNECT_TIMEOUT_MSEC;
    ns->settings.cluster_connect_timeout = NCHAN_DEFAULT_REDIS_CLUSTER_CONNECT_TIMEOUT_MSEC;
    ns->settings.node_weight.master = 1;
    ns->settings.node_weight.slave = 1;
    ns->settings.blacklist.count = 0;
    ns->settings.blacklist.list = NULL;
    ngx_str_t **urlref = nchan_list_append(&ns->urls);
    *urlref = rcf->url.len > 0 ? &rcf->url : &default_redis_url;
  }
  ns->current_reconnect_delay = 0;
  ns->current_cluster_recovery_delay = 0;
  ns->cluster.current_check_interval = 0;
  DBG("nodeset created");
  
  nodeset_set_name_alloc(ns);
  
  redis_nodeset_stats_init(ns);
  
  if(ns->settings.tls.enabled) {
    //if all the URLs are rediss://, then turn on SSL for the nodeset
    redis_connect_params_t    rcp;
    int                       use_tls = 1;
    ngx_str_t               **url;
    
    for(url = nchan_list_first(&ns->urls); url != NULL; url = nchan_list_next(url)) {
      parse_redis_url(*url, &rcp);
      use_tls &= rcp.use_tls;
    }
    if(use_tls) {
      ns->settings.tls.enabled = 1;
    }
    
    char *err = NULL;
    if(!nodeset_create_ssl_ctx(ns, &err)) {
      nodeset_log_error(ns, "Error creating Redis SSL context: %s", err ? err : "unknown error");
      return NULL;
    }
  }
  redis_nodeset_count++;
  rcf->nodeset = ns;

  return ns;
}

redis_nodeset_t *nodeset_find(nchan_redis_conf_t *rcf) {
  if(rcf->nodeset) {
    return rcf->nodeset;
  }
  else {
    int              i;
    redis_nodeset_t *ns;
    for(i=0; i<redis_nodeset_count; i++) {
      ns = &redis_nodeset[i];
      if(nchan_ngx_str_match(&rcf->namespace, ns->settings.namespace) && rcf->storage_mode == ns->settings.storage_mode) {
        if(rcf->upstream) {
          if(ns->upstream == rcf->upstream)
            return ns;
        }
        else {
          ngx_str_t *search_url = rcf->url.len > 0 ? &rcf->url : &default_redis_url;
          ngx_str_t **first_url = nchan_list_first(&ns->urls);
          
          if(first_url && nchan_ngx_str_match(search_url, *first_url)) {
            //cache it
            rcf->nodeset = ns;
            if(rcf->ping_interval > 0 && ns->settings.ping_interval > rcf->ping_interval) {
              //use the smallest ping interval found in the settings
              ns->settings.ping_interval = rcf->ping_interval;
            }
            return ns;
          }
        }
      }
    }
    return NULL;
  }
}

static int node_transfer_slaves(redis_node_t *src, redis_node_t *dst) {
  int transferred = 0;
  redis_node_t  **cur;
  for(cur = nchan_list_first(&src->peers.slaves); cur != NULL; cur = nchan_list_next(cur)) {
    node_set_master_node(*cur, dst);
    node_add_slave_node(dst, *cur); //won't be added if it's already there
    transferred++;
  }
  return transferred;
}

static int equal_redis_connect_params(void *d1, void *d2) {
  redis_connect_params_t *cp1 = d1;
  redis_connect_params_t *cp2 = d2;
  if(cp1->port != cp2->port || cp1->db != cp2->db) {
    return 0;  
  }
  if( nchan_ngx_str_nonzero_match(&cp1->hostname, &cp2->hostname)
   || nchan_ngx_str_nonzero_match(&cp1->peername, &cp2->peername)
   || nchan_ngx_str_nonzero_match(&cp1->peername, &cp2->hostname)
   || nchan_ngx_str_nonzero_match(&cp1->hostname, &cp2->peername)) {
    return 1;
  }
  else {
    return 0;
  }
}

static int equal_nonzero_strings(void *s1, void *s2) {
  return ((ngx_str_t *)s1)->len > 0 && ((ngx_str_t *)s2)->len > 0 && 
    nchan_ngx_str_match((ngx_str_t *)s1, (ngx_str_t *)s2);
}

typedef struct {
  char          *name;
  off_t          offset;
  int          (*match)(void *, void *);
} node_match_t;

static struct {
  node_match_t    run_id;
  node_match_t    cluster_id;
  node_match_t    connect_params;
} _node_match = {
  .run_id =          {"run_id",      offsetof(redis_node_t, run_id),          equal_nonzero_strings},
  .cluster_id =      {"cluster_id",  offsetof(redis_node_t, cluster.id),      equal_nonzero_strings},
  .connect_params =  {"url",         offsetof(redis_node_t, connect_params),  equal_redis_connect_params}
};

static int nodeset_node_deduplicate_by(redis_node_t *node, node_match_t *match) {
  redis_node_t   *cur;
  void *d1, *d2;
  d1 = &((char *)node)[match->offset];
  for(cur = nchan_list_first(&node->nodeset->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    d2 = &((char *)cur)[match->offset];
    if(cur != node && match->match(d1, d2)) {
      node_log_notice(node, "deduplicated by %s", match->name);
      node_transfer_slaves(node, cur); //node->cur
      nodeset_node_destroy(node);
      return 1;
    }
  }
  return 0;
}

int nodeset_node_deduplicate_by_connect_params(redis_node_t *node) {
  return nodeset_node_deduplicate_by(node, &_node_match.connect_params);
}
int nodeset_node_deduplicate_by_run_id(redis_node_t *node) {
  return nodeset_node_deduplicate_by(node, &_node_match.run_id);
}
int nodeset_node_deduplicate_by_cluster_id(redis_node_t *node) {
  return nodeset_node_deduplicate_by(node, &_node_match.cluster_id);
}

static redis_node_t *nodeset_node_find_by(redis_nodeset_t *ns, node_match_t *match, void *data) {
  redis_node_t *cur;
  void *d2;
  for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    d2 = &((char *)cur)[match->offset];
    if(match->match(data, d2)) {
      return cur;
    }
  }
  return NULL;
}
redis_node_t *nodeset_node_find_by_connect_params(redis_nodeset_t *ns, redis_connect_params_t *rcp) {
  return nodeset_node_find_by(ns, &_node_match.connect_params, rcp);
}
redis_node_t *nodeset_node_find_by_run_id(redis_nodeset_t *ns, ngx_str_t *run_id) {
  return nodeset_node_find_by(ns, &_node_match.run_id, run_id);
}
redis_node_t *nodeset_node_find_by_cluster_id(redis_nodeset_t *ns, ngx_str_t *cluster_id) {
  return nodeset_node_find_by(ns, &_node_match.cluster_id, cluster_id);
}

static int keyslot_ranges_overlap(redis_slot_range_t *r1, redis_slot_range_t *r2) {
  return rbtree_cluster_keyslots_compare(r1, r2) == 0;
}

redis_node_t *nodeset_node_find_by_range(redis_nodeset_t *ns, redis_slot_range_t *range) {
  ngx_rbtree_node_t                   *rbtree_node;
  redis_nodeset_slot_range_node_t     *keyslot_tree_node;
  
  if((rbtree_node = rbtree_find_node(&ns->cluster.keyslots, range)) != NULL) {
    keyslot_tree_node = rbtree_data_from_node(rbtree_node);
    assert(keyslot_ranges_overlap(range, &keyslot_tree_node->range));
    return keyslot_tree_node->node;
  }
  else {
    return NULL;
  }
}

redis_node_t *nodeset_node_find_by_slot(redis_nodeset_t *ns, uint16_t slot) {
  redis_slot_range_t range;
  range.min = slot;
  range.max = slot;
  return nodeset_node_find_by_range(ns, &range);
}
redis_node_t *nodeset_node_find_any_ready_master(redis_nodeset_t *ns) {
  redis_node_t *cur;
  for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    if(cur->state >= REDIS_NODE_READY && cur->role == REDIS_NODE_ROLE_MASTER) {
      return cur;
    }
  }
  return NULL;
}

int node_channel_in_keyspace(redis_node_t *node, void *chanhead) {
  rdstore_channel_head_t *ch = chanhead;  
  if(!node->cluster.enabled) {
    return 1;
  }
  uint16_t slot = redis_keyslot_from_channel_id(&ch->id);
  redis_slot_range_t  slot_as_range = {slot, slot};
  size_t              i;
  for(i=0; i<node->cluster.slot_range.n; i++) {
    if(keyslot_ranges_overlap(&node->cluster.slot_range.range[i], &slot_as_range)) {
      return 1;
    }
  }
  return 0;
}

uint16_t redis_keyslot_from_channel_id(ngx_str_t *chid) {
  static uint16_t    prefix_crc = 0;
  if(prefix_crc == 0) {
    prefix_crc = redis_crc16(0, "channel:", 8);
  }
  return redis_crc16(prefix_crc, (const char *)chid->data, chid->len) % 16384;
}

redis_node_t *nodeset_node_find_by_channel_id(redis_nodeset_t *ns, ngx_str_t *channel_id) {
  redis_node_t      *node;
  uint16_t           slot;
  
  if(!ns->cluster.enabled) {
    node = nodeset_node_find_any_ready_master(ns);
  }
  else {
    slot = redis_keyslot_from_channel_id(channel_id);
    node = nodeset_node_find_by_slot(ns, slot);
  }
  
#if REDIS_NODESET_DBG
  if(node == NULL) {
    nodeset_update_debuginfo(ns);
    raise(SIGABRT);
  }
#endif
  
  return node;
}

redis_node_t *nodeset_node_find_by_key(redis_nodeset_t *ns, ngx_str_t *key) {
  if(!ns->cluster.enabled) {
    return nodeset_node_find_any_ready_master(ns);
  }
  
  char        *start, *end;
  ngx_str_t    hashable;
  uint16_t     slot;
  
  if(((start = memchr(key->data, '{', key->len))) != NULL) {
    start++;
    end = memchr(start, '}', key->len - ((u_char *)start - key->data));
    if(end && end - start > 1) {
      hashable.data = (u_char *)start;
      hashable.len = (end - start);
    }
    else {
      hashable = *key;
      // not quite right -- need to ignore zero-length {} and scan to the next {
      // but it's good enough for the keys we're using
    }
  }
  else {
    hashable = *key;
  }
  slot = redis_crc16(0, (const char *)hashable.data, hashable.len) % 16384;
  
  return nodeset_node_find_by_slot(ns, slot);
}

static int nodeset_link_cluster_node_roles(redis_nodeset_t *ns) {
  redis_node_t   *cur, *master;
  for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    if(cur->role == REDIS_NODE_ROLE_SLAVE) {
      if(cur->cluster.master_id.len == 0 || cur->cluster.master_id.data == NULL) {
        nodeset_log_warning(ns, "cluster slave node %s has no master_id", node_nickname_cstr(cur));
        return 0;
      }
      
      if((master = nodeset_node_find_by_cluster_id(ns, &cur->cluster.master_id)) == NULL) {
        nodeset_log_warning(ns, "no master node with cluster_id %V found for slave node %s", &cur->cluster.master_id, node_nickname_cstr(cur));
        return 0;
      }
      
      node_set_master_node(cur, master); //this is idempotent
      node_add_slave_node(master, cur);  //so is this
    }
  }
  return 1;
}

static void ping_command_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply                 *reply = rep;
  redis_node_t               *node = privdata;
  if(!reply || reply->type == REDIS_REPLY_ERROR || !ac || ac->err) {
    node_log_error(node, "node ping failed");
    return;
  }
  node_log_debug(node, "node ping command reply ok");
}

static void node_ping_event(ngx_event_t *ev) {
  redis_node_t       *node = ev->data;
  redis_nodeset_t    *ns = node->nodeset;
  if(!ev->timedout || ngx_exiting || ngx_quit)
    return;
  
  node_log_debug(node, "node ping event");
  
  ev->timedout = 0;
  if(node->state == REDIS_NODE_READY) {
    assert(node->ctx.cmd);
    
    //we want to do this on EVERY node to anounce our presence. definitely don't use SPUBLISH here
    if(node->role == REDIS_NODE_ROLE_MASTER) {
      redisAsyncCommand(node->ctx.cmd, ping_command_callback, node, "PUBLISH %s ping", redis_worker_id);
    }
    else {
      redisAsyncCommand(node->ctx.cmd, ping_command_callback, node, "PING");
    }
    
    if(ns->settings.ping_interval > 0) {
      ngx_add_timer(ev, ns->settings.ping_interval * 1000);
    }
  }
}

static void node_command_timeout_check_event(ngx_event_t *ev) {
  redis_node_t       *node = ev->data;
  redis_nodeset_t    *ns = node->nodeset;
  if(!ev->timedout || ngx_exiting || ngx_quit)
    return;
  
  long long prev_sent = node->timeout.prev_sent;
  long long cur_received = node->timeout.received;
  node->timeout.prev_sent = node->timeout.sent;
  
  if(cur_received < prev_sent) {
    //TIMED OUT!
    node_log_warning(node, "%d command%s took longer than the timeout limit of %ds. Marking node as failed", prev_sent - cur_received, prev_sent - cur_received == 1 ? "" : "s", ns->settings.command_timeout/1000);
    node_disconnect(node, REDIS_NODE_FAILED);
    nodeset_examine(node->nodeset);
    return;
  }
  
  if(node->timeout.sent == node->timeout.received) {
    node_log_debug(node, "NO timeout. RESETTING. sent: %d, received: %d, prev_sent: %d", node->timeout.sent, cur_received, prev_sent);
    node->timeout.sent = 0;
    node->timeout.received = 0;
    node->timeout.prev_sent = 0;
  }
  else {
    node_log_debug(node, "NO timeout. sent: %d, received: %d, prev_sent: %d", node->timeout.sent, cur_received, prev_sent);
  }
  
  ngx_add_timer(ev, ns->settings.command_timeout);
}

static void nodeset_stop_cluster_check_timer(redis_nodeset_t *ns) {
  ns->cluster.current_check_interval = 0;
  if(ns->cluster.check_ev.timer_set) {
    ngx_del_timer(&ns->cluster.check_ev);
  }
}

static void nodeset_start_cluster_check_timer(redis_nodeset_t *ns) {
  if(ns->cluster.enabled) {
    if(!ns->cluster.check_ev.timer_set && ns->settings.cluster_check_interval.min > 0) {
      nchan_set_next_backoff(&ns->cluster.current_check_interval, &ns->settings.cluster_check_interval);
      ngx_add_timer(&ns->cluster.check_ev, ns->cluster.current_check_interval);
    }
  }
}

static int nodeset_node_remove_failed(redis_nodeset_t *ns, ngx_str_t *cluster_node_id) {
  redis_node_t *failed_node = nodeset_node_find_by_cluster_id(ns, cluster_node_id);
  if(!failed_node) {
    return 0;
  }
  nodeset_log_notice(ns, "removed failed node %s", node_nickname_cstr(failed_node));
  node_disconnect(failed_node, REDIS_NODE_FAILED);
  nodeset_node_destroy(failed_node);
  return 1;
}

static void nodeset_cluster_check_event_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply                 *reply = rep;
  redis_node_t               *node = privdata;
  redis_nodeset_t            *ns = node->nodeset;
  ngx_str_t                   epoch_str;
  int                         epoch;
  const char                 *err = NULL;
  
  node_command_time_finish(node, NCHAN_REDIS_CMD_CLUSTER_CHECK);
  node_command_received(node);
  
  if(reply == NULL) {
    //Node should have already disconnected
    return;
  }
  
  if(reply->type == REDIS_REPLY_ERROR) {
    err = reply->str ? reply->str : "error in reply";
    goto error;
  }
  
  if(reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
    err = "reply not an array of size 2";
    goto error;
  }
  redisReply *info = reply->element[0];
  redisReply *cluster_nodes = reply->element[1];
  
  //parse CLUSTER INFO reply
  if(!reply_str_ok(info)) {
    err = info->str ? info->str : "error in CLUSTER INFO reply";
    goto error;
  }
  if(!nchan_cstr_match_line(info->str, "cluster_state:ok")) {
    char errstr[512];
    ngx_snprintf((u_char *)errstr, 512, "cluster_state not ok on node %s.%Z", node_nickname_cstr(node));
    nodeset_set_status(ns, REDIS_NODESET_CLUSTER_FAILING, errstr);
    return;
  }
  if(!nchan_get_rest_of_line_in_cstr(info->str, "cluster_current_epoch:", &epoch_str)) {
    err = "Cluster check failed: CLUSTER info command is missing 'cluster_current_epoch'";
    goto error;
  }
  if((epoch = ngx_atoi(epoch_str.data, epoch_str.len)) == NGX_ERROR) {
    err = "failed to parse current config epoch number";
    //why would this happen? dunno, fail node just in case
    goto error;
  }
  
  int epoch_changed = 0, prev_epoch = 0;
  if(ns->cluster.current_epoch < epoch) {
    epoch_changed = 1;
    prev_epoch = node->cluster.current_epoch;
    node->cluster.current_epoch = epoch;
    ns->cluster.current_epoch = epoch;
  }
  
  //parse CLUSTER NODES reply
  if(cluster_nodes->type != REDIS_REPLY_STRING) {
    err = "CLUSTER NODES reply is not a string";
    goto error;
  }
  size_t                  i, n;
  cluster_nodes_line_t   *lines;
  cluster_nodes_line_t   *l;
  redis_node_t           *peer;
  
  if(!(lines = parse_cluster_nodes(node, cluster_nodes->str, &n))) {
    err = "parsing CLUSTER NODES reply failed";
    goto error;
  }
  
  int outdated_nodes = 0, failed_nodes = 0;
  
  for(i=0; i<n; i++) {
    l = &lines[i];
    if(l->failed && nodeset_node_remove_failed(ns, &l->id)) {
      failed_nodes++;
      continue;
    }
    if(node_skip_cluster_peer(node, l, 0, 0)) {
      continue;
    }
    
    node_discover_cluster_peer(node, l, &peer);
    
    if(nodeset_cluster_node_is_outdated(ns, l)) {
      outdated_nodes++;
    }
  }
  
  if(outdated_nodes) {
    char errbuf[512];
    ngx_snprintf((u_char *)errbuf, 512, "%d node%s role or keyslot assignment has changed.%s%Z", outdated_nodes, outdated_nodes == 1 ? "" : "s", epoch_changed ? " Also, the config epoch has changed." : "");
    nodeset_set_status(node->nodeset, REDIS_NODESET_CLUSTER_FAILING, errbuf);
    return;
  }
  
  if(epoch_changed) {
    if(nodeset_cluster_keyslot_space_complete(ns, REDIS_NODE_READY)) {
      nodeset_log_warning(ns, "config epoch has changed from %d to %d on node %s. Node roles remain unchanged and the cluster is still healthy.", prev_epoch, epoch, node_nickname_cstr(node));
    }
    else {
      nodeset_set_status(node->nodeset, REDIS_NODESET_CLUSTER_FAILING, "Config epoch has changed and the keyslot space is incomplete");
      return;
    }
  }
  
  if(failed_nodes) {
    if(nodeset_cluster_keyslot_space_complete(ns, REDIS_NODE_READY)) {
      nodeset_log_warning(ns, "%d failed node%s removed, but the keyspace is still complete. The cluster is healthy.", failed_nodes, failed_nodes == 1 ? " was" : "s were");
    }
    else {
      char errbuf[512];
      ngx_snprintf((u_char *)errbuf, 512, "%d failed node%s removed, and the keyspace is now incomplete%Z", failed_nodes, failed_nodes == 1 ? " was" : "s were");
      nodeset_set_status(node->nodeset, REDIS_NODESET_CLUSTER_FAILING, errbuf);
    }
  }
  
  nodeset_start_cluster_check_timer(ns);
  return;

error:
  nodeset_log_error(ns, "Cluster check failed on node %s: %s", node_nickname_cstr(node), err ? err : "unkown error");
  nodeset_start_cluster_check_timer(ns);
  if(node->state >= REDIS_NODE_READY) {
    node_disconnect(node, REDIS_NODE_FAILED);
    nodeset_examine(node->nodeset);
  }
}

static void nodeset_cluster_check_event(ngx_event_t *ev) {
  if(!ev->timedout || ngx_exiting || ngx_quit)
    return;
  
  redis_nodeset_t    *ns = ev->data;
  ev->timedout = 0;
  
  redis_node_t *node = nodeset_random_node(ns, REDIS_NODE_GET_CLUSTERINFO, REDIS_NODE_ROLE_ANY);
  
  if(!node) {
    nodeset_log_error(ns, "no suitable node to run cluster check. when idle, Nchan may not be aware of cluster changes!");
    nodeset_start_cluster_check_timer(ns);
    return;
  }
  
  node_command_sent(node);
  node_command_time_start(node, NCHAN_REDIS_CMD_CLUSTER_CHECK);
  
  nodeset_log_debug(ns, "cluster_check event on node %s", node_nickname_cstr(node));
  redisAsyncCommand(node->ctx.cmd, NULL, NULL, "MULTI");
  redisAsyncCommand(node->ctx.cmd, NULL, NULL, "CLUSTER INFO");
  redisAsyncCommand(node->ctx.cmd, NULL, NULL, "CLUSTER NODES");
  redisAsyncCommand(node->ctx.cmd, nodeset_cluster_check_event_callback, node, "EXEC");
}

static int nodeset_reset_cluster_node_info(redis_nodeset_t *ns) {
  redis_node_t   *node;
  for(node = nchan_list_first(&ns->nodes); node != NULL; node = nchan_list_next(node)) {
    node->recovering = 0;
    if(node->cluster.enabled) {
      node->cluster.ok = 0;
      node->cluster.master_id.len = 0;
      
      //don't clear the cluster node id though, that's not supposed to change.
      //we'll use it to find known nodes
      
      node_unset_cluster_slots(node);
      
      node_set_role(node, REDIS_NODE_ROLE_UNKNOWN);
      if(node->state > REDIS_NODE_GET_CLUSTERINFO) {
        node->state = REDIS_NODE_GET_CLUSTERINFO;
      }
    }
  }
  
  return 1;
}

static void nodeset_recover_cluster_handler(redisAsyncContext *ac, void *rep, void *privdata);
static int nodeset_recover_cluster(redis_nodeset_t *ns) {
  if(ns->cluster.recovering_on_node) {
    nodeset_log_error(ns, "already recoving cluster state");
    return 0;
  }
  ns->last_cluster_recovery_check_time = *ngx_timeofday();
  nodeset_reset_cluster_node_info(ns);
  
  //pick a random node
  redis_node_t *node = nodeset_random_node(ns, REDIS_NODE_GET_CLUSTERINFO, REDIS_NODE_ROLE_ANY);
  if(!node) {
    nodeset_log_error(ns, "cluster unrecoverable: no connected node found to recover on");
    return 0;
  };
  
  redis_node_t   *cur;
  for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    if(!node->connecting && node->state >= REDIS_NODE_DISCONNECTED) {
      cur->recovering = 1;
    }
  }
  ns->cluster.recovering_on_node = node;
  nodeset_log_notice(ns, "Recovering cluster though node %s", node_nickname_cstr(node));
  
  node_command_sent(node);
  node_command_time_start(node, NCHAN_REDIS_CMD_CLUSTER_RECOVER);
  
  redisAsyncCommand(node->ctx.cmd, NULL, NULL, "MULTI");
  redisAsyncCommand(node->ctx.cmd, NULL, NULL, "CLUSTER INFO");
  redisAsyncCommand(node->ctx.cmd, NULL, NULL, "CLUSTER NODES");
  redisAsyncCommand(node->ctx.cmd, NULL, NULL, "COMMAND INFO SPUBLISH");
  redisAsyncCommand(node->ctx.cmd, nodeset_recover_cluster_handler, node, "EXEC");

  return 1;
}

static void nodeset_recover_cluster_handler(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply                 *reply = rep;
  redis_node_t               *node = privdata;
  redis_nodeset_t            *ns = node->nodeset;
  int                         current_epoch;
  u_char                      errbuf[1024];
  
  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CLUSTER_RECOVER);
  
  ngx_snprintf(errbuf, 1024, "unknown reason%Z");
  
  if(ns->cluster.recovering_on_node != node) {
    ngx_snprintf(errbuf, 1024, "got a response from a different node than where recovery was attempted%Z");
    goto fail;
  }
  ns->cluster.recovering_on_node = NULL;
  
  if(!redisReplyOk(ac, reply)) {
    ngx_snprintf(errbuf, 1024, "reply not ok%Z");
    goto fail;
  }
  if(reply->type != REDIS_REPLY_ARRAY || reply->elements != 3) {
    ngx_snprintf(errbuf, 1024, "got something other than an array of size 2%Z");
    goto fail;
  }
  
  //parse CLUSTER INFO reply
  redisReply        *cluster_info_reply = reply->element[0];
  ngx_str_t          rest;
  if(cluster_info_reply->type != REDIS_REPLY_STRING) {
    ngx_snprintf(errbuf, 1024, "CLUSTER INFO reply is not a string%Z");
    goto fail;
  }
  
  if(!nchan_cstr_match_line(cluster_info_reply->str, "cluster_state:ok")) {
    node->cluster.ok=0;
    ngx_snprintf(errbuf, 1024, "cluster_state not ok on node %s.%Z", node_nickname_cstr(node));
    goto fail;
  }
  if(!nchan_get_rest_of_line_in_cstr(cluster_info_reply->str, "cluster_current_epoch:", &rest)) {
    ngx_snprintf(errbuf, 1024, "CLUSTER INFO failed to get current epoch%Z");
    goto fail;
  }
  if((current_epoch = ngx_atoi(rest.data, rest.len)) == NGX_ERROR) {
    ngx_snprintf(errbuf, 1024, "CLUSTER INFO command failed to parse current epoch number%Z");
    goto fail;
  }
  
  redisReply        *cluster_nodes_reply = reply->element[1];
  if(cluster_nodes_reply->type != REDIS_REPLY_STRING) {
    ngx_snprintf(errbuf, 1024, "CLUSTER NODES reply is not a string%Z");
    goto fail;
  }
  //parse CLUSTER NODES
  size_t                  i, n;
  int                     discovered = 0;
  cluster_nodes_line_t   *lines;
  cluster_nodes_line_t   *l;
  redis_node_t           *peer;
  
  if(!(lines = parse_cluster_nodes(node, cluster_nodes_reply->str, &n))) {
    ngx_snprintf(errbuf, 1024, "parsing CLUSTER NODES command failed%Z");
    goto fail;
  }
  for(i=0; i<n; i++) {
    l = &lines[i];
    if(l->failed && nodeset_node_remove_failed(ns, &l->id)) {
      continue;
    }
    
    if(node_skip_cluster_peer(node, l, 0, 0)) {
      continue;
    }
    
    if(node_discover_cluster_peer(node, l, &peer)) {
      discovered++;
      continue;
    }
    
    if(!peer) {
      ngx_snprintf(errbuf, 1024, "did not discover a peer, and didn't match existing node%Z");
      goto fail;
    }
    
    //found existing peer. update it
    
    if(l->master) {
      node_set_role(peer, REDIS_NODE_ROLE_MASTER);
      char sloterrbuf[256];
      if(l->slot_ranges_count > 0 && !node_set_cluster_slots(peer, l, sloterrbuf, sizeof(sloterrbuf))) {
        ngx_snprintf(errbuf, 1024, "couldn't set cluster slots for node %s: %s%Z", node_nickname_cstr(peer), sloterrbuf);
        nodeset_dbg_log_nodes_and_clusternodes_lines(ns, NGX_LOG_NOTICE, lines, n);
        goto fail;
      }
    }
    else {
      node_set_role(peer, REDIS_NODE_ROLE_SLAVE);
      nchan_strcpy(&peer->cluster.master_id, &l->master_id, MAX_CLUSTER_ID_LENGTH);
    }
    //we will set actual master-slave associations later.
  }
  
  //does this node have SPUBLISH support?
  node->have_spublish = reply->element[2]->type == REDIS_REPLY_ARRAY;
  
  //connect the disconnected
  redis_node_t      *cur;
  for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    if(cur->state <= REDIS_NODE_DISCONNECTED && !cur->connecting) {
      node_connect(cur);
    }
  }
  /* proceed if we have the whole keyspace -- Even though a cluster 
     might be healthy, when failing over Redis may not report some 
     assigned keyslots for some masters until consensus is reached
   */
  if(!nodeset_cluster_keyslot_space_complete(ns, REDIS_NODE_GET_CLUSTERINFO)) {
    ngx_snprintf(errbuf, 1024, "incomplete keyslot information%Z");
    goto fail;
  }
  
  //set all masters and slaves
  if(!nodeset_link_cluster_node_roles(ns)) {
    ngx_snprintf(errbuf, 1024, "failed to link cluster node masters and slaved%Z");
    goto fail;
  }
  
  //check 'em over
  for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    if(cur->connecting) {
      continue;
    }
    
    assert(cur->ctx.cmd);
    assert(cur->ctx.pubsub);
    ns->cluster.current_epoch = current_epoch;
    cur->cluster.current_epoch = current_epoch;
    cur->cluster.ok = 1;
    node_make_ready(cur);
    cur->recovering = 0;
  }
  
  //success?...
  nodeset_examine(ns);
  if(ns->status != REDIS_NODESET_READY) {
    ngx_snprintf(errbuf, 1024, "%s%Z", ns->status_msg[0] == '\0' ? "(unknown reason)" : ns->status_msg);
    goto fail;
  }
  ns->last_cluster_recovery_check_time.sec = 0;
  ns->last_cluster_recovery_check_time.msec = 0;
  
  return;
  
fail:
  nchan_set_next_backoff(&ns->current_cluster_recovery_delay, &ns->settings.cluster_recovery_delay);
  nodeset_reset_cluster_node_info(ns);
  ns->last_cluster_recovery_check_time = *ngx_timeofday();
  nodeset_log_warning(ns, "Cluster recovery failed: %s. Will retry in %.3f sec", (char *)errbuf, ns->current_cluster_recovery_delay/1000.0);
}

static redis_node_t *nodeset_node_create_with_space(redis_nodeset_t *ns, redis_connect_params_t *rcp, size_t extra_space, void **extraspace_ptr) {
  assert(!nodeset_node_find_by_connect_params(ns, rcp));
  node_blob_t      *node_blob;
  if(extra_space == 0) {
    assert(extraspace_ptr == NULL);
    node_blob = nchan_list_append(&ns->nodes);
  }
  else {
    assert(extraspace_ptr);
    node_blob = nchan_list_append_sized(&ns->nodes, sizeof(*node_blob)+extra_space);
    if(extra_space) {
      *extraspace_ptr = (void *)(&node_blob[1]);
    }
  }
  redis_node_t     *node = &node_blob->node;
  
  assert((void *)node_blob == (void *)node);
  assert(node);
  node->role = REDIS_NODE_ROLE_UNKNOWN,
  node->state = REDIS_NODE_DISCONNECTED;
  node->discovered = 0;
  node->connecting = 0;
  node->recovering = 0;
  node->have_spublish = 0;
  node->connect_timeout = NULL;
  node->connect_params = *rcp;
  node->connect_params.peername.data = node_blob->peername;
  node->connect_params.peername.len = 0;
  node->cluster.id.len = 0;
  node->cluster.id.data = node_blob->cluster_id;
  node->cluster.master_id.len = 0;
  node->cluster.master_id.data = node_blob->cluster_master_id;
  node->cluster.enabled = 0;
  node->cluster.ok = 0;
  node->cluster.slot_range.indexed = 0;
  node->cluster.slot_range.n = 0;
  node->cluster.slot_range.range = NULL;
  node->pending_commands = 0;
  node->run_id.len = 0;
  node->run_id.data = node_blob->run_id;
  node->nodeset = ns;
  node->generation = 0;
  
  if(rcp->password.len == 0 && ns->settings.password.len > 0) {
    node->connect_params.password = ns->settings.password;
  }
  if(rcp->username.len == 0 && ns->settings.username.len > 0) {
    node->connect_params.username = ns->settings.username;
  }
  
  nchan_slist_init(&node->channels.cmd, rdstore_channel_head_t, redis.slist.node_cmd.prev, redis.slist.node_cmd.next);
  nchan_slist_init(&node->channels.pubsub, rdstore_channel_head_t, redis.slist.node_pubsub.prev, redis.slist.node_pubsub.next);
  
  node->peers.master = NULL;
  nchan_list_init(&node->peers.slaves, sizeof(redis_node_t *), "node slaves");
  
  ngx_memzero(&node->ping_timer, sizeof(node->ping_timer));
  nchan_init_timer(&node->ping_timer, node_ping_event, node);
  
  node->timeout.sent = 0;
  node->timeout.received = 0;
  node->timeout.prev_sent = 0;
  ngx_memzero(&node->timeout.ev, sizeof(node->timeout.ev));
  nchan_init_timer(&node->timeout.ev, node_command_timeout_check_event, node);
  
  node->ctx.cmd = NULL;
  node->ctx.pubsub = NULL;
  node->ctx.sync = NULL;
  
  redis_node_stats_init(node);
  
  assert(nodeset_node_find_by_connect_params(ns, rcp));
  return node;
}

redis_node_t *nodeset_node_create(redis_nodeset_t *ns, redis_connect_params_t *rcp) {
  return nodeset_node_create_with_space(ns, rcp, 0, NULL);
}

static redis_node_t *nodeset_node_create_with_connect_params(redis_nodeset_t *ns, redis_connect_params_t *rcp) {
  redis_node_t  *node;
  u_char        *space;
  size_t         sz = rcp->hostname.len + rcp->password.len;
  node = nodeset_node_create_with_space(ns, rcp, sz, (void **)&space);
  assert(node);
  node->connect_params.hostname.data = space;
  node->connect_params.hostname.len = 0;
  nchan_strcpy(&node->connect_params.hostname, &rcp->hostname, 0);
  node->connect_params.password.data = &space[rcp->hostname.len];
  nchan_strcpy(&node->connect_params.password, &rcp->password, 0);
  
  return node;
}

static void node_remove_peer(redis_node_t *node, redis_node_t *peer) {
  redis_node_t  **cur;
  if(node->peers.master == peer) {
    node->peers.master = NULL;
  }
  
  for(cur = nchan_list_first(&node->peers.slaves); cur != NULL; cur = nchan_list_next(cur)) {
    if(*cur == peer) {
      nchan_list_remove(&node->peers.slaves, cur);
      return;
    }
  }
}

ngx_int_t nodeset_node_destroy(redis_node_t *node) {
  redisAsyncContext *ac;
  redisContext      *c;
  node_set_role(node, REDIS_NODE_ROLE_UNKNOWN); //removes from all peer lists, and clears own slave list
  if((ac = node->ctx.cmd) != NULL) {
    node->ctx.cmd = NULL;
    redisAsyncFree(ac);
  }
  if((ac = node->ctx.pubsub) != NULL) { 
    node->ctx.pubsub = NULL;
    redisAsyncFree(ac);
  }
  if((c = node->ctx.sync) != NULL) {
    node->ctx.sync = NULL;
    redisFree(c);
  }
  if(node->connect_timeout) {
    nchan_abort_oneshot_timer(node->connect_timeout);
    node->connect_timeout = NULL;
  }
  
  redis_node_stats_destroy(node);
  nchan_list_remove(&node->nodeset->nodes, node);
  
  return NGX_OK;
}

static void node_discover_slave(redis_node_t *master, redis_connect_params_t *rcp) {
  redis_node_t    *slave;
  if((slave = nodeset_node_find_by_connect_params(master->nodeset, rcp))!= NULL) {
    //we know about it already
    if(slave->role != REDIS_NODE_ROLE_SLAVE && slave->state > REDIS_NODE_GET_INFO) {
      node_log_notice(slave, "Node appears to have changed to slave -- need to update");
      node_set_role(slave, REDIS_NODE_ROLE_UNKNOWN);
      node_disconnect(slave, REDIS_NODE_FAILED);
      node_connect(slave);
    }
    //assert(slave->peers.master == master);
  }
  else {
    
    
    slave = nodeset_node_create_with_connect_params(master->nodeset, rcp);
    slave->discovered = 1;
    node_set_role(slave, REDIS_NODE_ROLE_SLAVE);
    node_log_notice(master, "Discovering own slave %s", rcp_cstr(rcp));
  }
  node_set_master_node(slave, master); //this is idempotent
  node_add_slave_node(master, slave);  //so is this
  //try to connect
  if(slave->state <= REDIS_NODE_DISCONNECTED) {
    node_connect(slave);
  }
}

static void node_discover_master(redis_node_t  *slave, redis_connect_params_t *rcp) {
  redis_node_t *master;
  if ((master = nodeset_node_find_by_connect_params(slave->nodeset, rcp)) != NULL) {
      if(master->role != REDIS_NODE_ROLE_MASTER && master->state > REDIS_NODE_GET_INFO) {
        node_log_notice(master, "Node appears to have changed to master -- need to update");
        node_set_role(master, REDIS_NODE_ROLE_UNKNOWN);
        node_disconnect(master, REDIS_NODE_FAILED);
        node_connect(master);
      }
    //assert(node_find_slave_node(master, slave));
    //node_log_notice(slave, "Discovering master %s... already known", rcp_cstr(rcp));
  }
  else {
    master = nodeset_node_create_with_connect_params(slave->nodeset, rcp);
    master->discovered = 1;
    node_set_role(master, REDIS_NODE_ROLE_MASTER);
    node_log_notice(slave, "Discovering own master %s", rcp_cstr(rcp));
  }
  node_set_master_node(slave, master);
  node_add_slave_node(master, slave);
  //try to connect
  if(master->state <= REDIS_NODE_DISCONNECTED) {
    node_connect(master);
  }
}

static int node_skip_cluster_peer(redis_node_t *node, cluster_nodes_line_t *l, int log_action, int skip_self) {
  redis_connect_params_t   rcp;
  char                    *description = "";
  char                    *detail = "";
  char                     detail_buf[64];
  char                    *role = NULL;
  ngx_uint_t               loglevel = NGX_LOG_NOTICE;
  nchan_redis_ip_range_t  *matched;
  rcp.hostname = l->hostname;
  rcp.port = l->port;
  rcp.peername.len = 0;
  rcp.db = node->connect_params.db;
  rcp.password = node->connect_params.password;
  
  if(l->noaddr) {
    role = "node";
    description = "no-address";
    return 1;
  }
  else if(l->handshake) {
    role = "node";
    description = "handshaking";
  }
  else if(l->hostname.len == 0) {
    role = "node";
    description = "empty hostname";
  }
  else if(l->failed) {
    description = "failed";
  }
  else if(!l->connected) {
    description = "disconnected";
  }
  else if(l->self && skip_self) {
    description = "self";
    loglevel = NGX_LOG_INFO;
  }
  else if((matched = node_ip_blacklisted(node->nodeset, &rcp)) != NULL) {
    description = "blacklisted";
    detail = detail_buf;
    ngx_snprintf((u_char *)detail_buf, 64, " (matched blacklist entry %V)%Z", &matched->str);
  }
  else {
    return 0;
  }
  if(!role) role = l->master ? "master" : "slave";
  if(log_action) {
    nodeset_log(node->nodeset, loglevel, "Skipping %s %s %s%s", description, role, rcp_cstr(&rcp), detail);
  }
  return 1;
}

static int nodeset_cluster_node_is_outdated(redis_nodeset_t *ns, cluster_nodes_line_t *l) {
  redis_node_t *node = nodeset_node_find_by_cluster_id(ns, &l->id);
  
  if(!node) {
    return 0;
  }
  
  int role = l->master ? REDIS_NODE_ROLE_MASTER : REDIS_NODE_ROLE_SLAVE;
  
  if(node->role != role) {
    nodeset_log_notice(ns, "Node %s has changed from %s to %s", node_nickname_cstr(node), node_role_cstr(node->role), node_role_cstr(role));
    return 1;
  }
  
  if(node->cluster.slot_range.n != (size_t )l->slot_ranges_count) {
    nodeset_log_notice(ns, "Node %s slot range count has changed from %d to %d.", node_nickname_cstr(node), node->cluster.slot_range.n, l->slot_ranges_count);
    return 1;
  }
  
  redis_slot_range_t *range = ngx_alloc(sizeof(redis_slot_range_t) * node->cluster.slot_range.n, ngx_cycle->log);
  if(range == NULL) {
    nodeset_log_error(ns, "Out of memory: failed to allocate slot range during cluster check");
    return 1;
  }
  if(!parse_cluster_node_slots(l, range)) {
    ngx_free(range);
    nodeset_log_error(ns, "failed parsing cluster slots range");
    return 1;
  }
  
  int i;
  for(i=0; i<l->slot_ranges_count; i++) {
    if((node->cluster.slot_range.range[i].min != range[i].min) 
    || (node->cluster.slot_range.range[i].max != range[i].max)) {
      nodeset_log_notice(ns, "Node %s slot range has changed", node_nickname_cstr(node));
      ngx_free(range);
      return 1;
    }
  }
  
  ngx_free(range);
  return 0;
}

static int node_discover_cluster_peer(redis_node_t *node, cluster_nodes_line_t *l, redis_node_t **known_node) {
  redis_connect_params_t   rcp;
  redis_node_t            *peer;
  int                      newly_discovered = 0;
  if(l->failed || !l->connected || l->noaddr || l->self) {
    if(known_node && l->self) {
      *known_node = node;
    }
    return 0;
  }
  rcp.hostname = l->hostname;
  rcp.port = l->port;
  rcp.peername.len = 0;
  rcp.db = node->connect_params.db;
  rcp.password = node->connect_params.password;
  rcp.username = node->connect_params.username;
  rcp.use_tls = node->nodeset->settings.tls.enabled;
  
  if( ((peer = nodeset_node_find_by_connect_params(node->nodeset, &rcp)) != NULL)
   || ((peer = nodeset_node_find_by_cluster_id(node->nodeset, &l->id)) != NULL)
  ) {
    //node_log_notice(node, "Discovering cluster node %s... already known", rcp_cstr(&rcp));
    if(known_node) {
      *known_node = peer;
    }
  }
  else {
    newly_discovered=1;
    nodeset_log_notice(node->nodeset, "Discovered cluster %s %s", (l->master ? "master" : "slave"), rcp_cstr(&rcp));
    peer = nodeset_node_create_with_connect_params(node->nodeset, &rcp);
    peer->discovered = 1;
  }
  
  
  peer->cluster.enabled = 1;
  if(!l->master && l->master_id.len > 0 && peer->cluster.master_id.len == 0) {
    nchan_strcpy(&peer->cluster.master_id, &l->master_id, MAX_CLUSTER_ID_LENGTH);
  }
  nchan_strcpy(&peer->cluster.id, &l->id, MAX_CLUSTER_ID_LENGTH);
  
  node_set_role(peer, l->master ? REDIS_NODE_ROLE_MASTER : REDIS_NODE_ROLE_SLAVE);
  //ignore all the other things for now
  
  if(newly_discovered) {
    node_connect(peer);
  }
  return newly_discovered;
}

static ngx_int_t set_preallocated_peername(redisAsyncContext *ctx, ngx_str_t *dst);

static void node_connector_fail(redis_node_t *node, const char *err) {
  const char  *ctxerr = NULL;
  node->connecting = 0;
  node_command_time_finish(node, NCHAN_REDIS_CMD_CONNECT);
  if(node->ctx.cmd && node->ctx.cmd->err) {
    ctxerr = node->ctx.cmd->errstr;
  }
  else if(node->ctx.pubsub && node->ctx.pubsub->err) {
    ctxerr = node->ctx.pubsub->errstr;
  }
  else if(node->ctx.sync && node->ctx.sync->err) {
    ctxerr = node->ctx.sync->errstr;
  }
  if(node->state == REDIS_NODE_CONNECTION_TIMED_OUT) {
    node_log_error(node, "connection failed: %s", err == NULL ? "timeout" : err);
  }
  else if(ctxerr) {
    if(err) {
      node_log_error(node, "connection failed: %s (%s)", err, ctxerr);
    }
    else {
      node_log_error(node, "connection failed: %s", ctxerr);
    }
  }
  else if(err) {
    node_log_error(node, "connection failed: %s", err);
  }
  else {
    node_log_error(node, "connection failed");
  }
  node_disconnect(node, REDIS_NODE_FAILED);
}

int node_connect(redis_node_t *node) {
  assert(node->state <= REDIS_NODE_DISCONNECTED);
  node->connecting = 1;
  node_connector_callback(NULL, NULL, node);
  return 1;
}

int node_disconnect(redis_node_t *node, int disconnected_state) {
  ngx_int_t prev_state = node->state;
  node->state = disconnected_state;
  node->connecting = 0;
  redisAsyncContext *ac;
  redisContext      *c;
  if(node->connect_timeout) {
    nchan_abort_oneshot_timer(node->connect_timeout);
    node->connect_timeout = NULL;
  }
  
  if((ac = node->ctx.cmd) != NULL) {
    node->ctx.cmd->onDisconnect = NULL;
    node->ctx.cmd = NULL;
    //redisAsyncSetDisconnectCallback(ac, NULL); //this only sets the callback if it's currently null...
    redisAsyncFree(ac);
    node_log_debug(node, "redisAsyncFree %p", ac);
    node_log_notice(node, "disconnected");
  }
  if((ac = node->ctx.pubsub) != NULL) {
    node->ctx.pubsub->onDisconnect = NULL;
    node->ctx.pubsub = NULL;
    //redisAsyncSetDisconnectCallback(ac, NULL);  //this only sets the callback if it's currently null...
    redisAsyncFree(ac);
    node_log_debug(node, "redisAsyncFree pubsub %p", ac);
  }
  if((c = node->ctx.sync) != NULL) {
    node->ctx.sync = NULL;
    redisFree(c);
  }

  if(prev_state >= REDIS_NODE_GET_CLUSTERINFO) {
    nchan_stats_worker_incr(redis_connected_servers, -1);
  }
  if(node->cluster.enabled) {
    node_unset_cluster_slots(node);
  }
  
  if(node->ping_timer.timer_set) {
    ngx_del_timer(&node->ping_timer);
  }
  
  if(node->timeout.ev.timer_set) {
    ngx_del_timer(&node->timeout.ev);
  }
  
  //reset timeout values.
  //this isn't stricrlt necessary for correct operation -- after redisAsyncFree, all outstanding commands will have been received (with an error, but that's beside the point)
  //this is just to stay clean and restart from 0
  node->timeout.sent = 0;
  node->timeout.received = 0;
  node->timeout.prev_sent = 0;
  
  node->pending_commands = 0;
  
  node->scripts_load_state.loading = 0;
  node->scripts_load_state.current = 0;
  int i;
  for(i=0; i<REDIS_LUA_SCRIPTS_COUNT; i++) {
    node->scripts_load_state.loaded[i]=0;
  }
  
  rdstore_channel_head_t *cur;
  nchan_slist_t *cmd = &node->channels.cmd;
  nchan_slist_t *pubsub = &node->channels.pubsub;
  nchan_slist_t *disconnected_cmd = &node->nodeset->channels.disconnected_cmd;
  
  for(cur = nchan_slist_first(cmd); cur != NULL; cur = nchan_slist_first(cmd)) {
    nodeset_node_dissociate_chanhead(cur);
    nchan_slist_append(disconnected_cmd, cur);
    cur->redis.slist.in_disconnected_cmd_list = 1;
    if(cur->status == READY) {
      cur->status = NOTREADY;
    }
  }
  for(cur = nchan_slist_first(pubsub); cur != NULL; cur = nchan_slist_first(pubsub)) {
    redis_chanhead_set_pubsub_status(cur, NULL, REDIS_PUBSUB_UNSUBSCRIBED);
  }
  
  redis_node_stats_detach(node);
  
  return 1;
}

void node_set_role(redis_node_t *node, redis_node_role_t role) {
  if(node->role == role) {
    return;
  }
  node->role = role;
  redis_node_t  **cur;
  switch(node->role) {
    case REDIS_NODE_ROLE_ANY:
      node_log_error(node, "tried setting role to REDIS_NODE_ROLE_ANY. That's not allowed");
      //invalid role should never be called.
      raise(SIGABRT);
      break;
    case REDIS_NODE_ROLE_UNKNOWN:
      if(node->peers.master) {
        node_remove_peer(node->peers.master, node);
        DBG("removed %p from peers of %p", node->peers.master, node);
        node->peers.master = NULL;
      }
      for(cur = nchan_list_first(&node->peers.slaves); cur != NULL; cur = nchan_list_next(cur)) {
        node_remove_peer(*cur, node);
      }
      nchan_list_empty(&node->peers.slaves);
      break;
    
    case REDIS_NODE_ROLE_MASTER:
      if(node->peers.master) {
        node_remove_peer(node->peers.master, node);
        node->peers.master = NULL;
      }
      break;
    
    case REDIS_NODE_ROLE_SLAVE:
      //do nothing
      break;
      
  }
}

int node_set_master_node(redis_node_t *node, redis_node_t *master) {
  if(node->peers.master && node->peers.master != master) {
    node_remove_slave_node(master, node);
  }
  node->peers.master = master;
  return 1;
}
redis_node_t *node_find_slave_node(redis_node_t *node, redis_node_t *slave) {
  redis_node_t **cur;
  for(cur = nchan_list_first(&node->peers.slaves); cur != NULL; cur = nchan_list_next(cur)) {
    if (*cur == slave) {
      return slave;
    }
  }
  return NULL;
}
int node_add_slave_node(redis_node_t *node, redis_node_t *slave) {
  if(!node_find_slave_node(node, slave)) {
    redis_node_t **slaveref;
    slaveref = nchan_list_append(&node->peers.slaves);
    *slaveref = slave;
    return 1;
  }
  return 1;
}
int node_remove_slave_node(redis_node_t *node, redis_node_t *slave) {
  if(!node_find_slave_node(node, slave)) {
    nchan_list_remove(&node->peers.slaves, slave);
  }
  return 1;
}

static int node_parseinfo_set_preallocd_str(redis_node_t *node, ngx_str_t *target, const char *info, const char *linestart, size_t maxlen) {
  ngx_str_t found;
  if(nchan_get_rest_of_line_in_cstr(info, linestart, &found)) {
    if(found.len > maxlen) {
      node_log_error(node, "\"%s\" is too long", linestart);
      return 0;
    }
    else {
      target->len = found.len;
      ngx_memcpy(target->data, found.data, found.len);
      return 1;
    }
  }
  return 0;
}

static int node_parseinfo_set_run_id(redis_node_t *node, const char *info) {
  return node_parseinfo_set_preallocd_str(node, &node->run_id, info, "run_id:", MAX_RUN_ID_LENGTH);
}

static int node_connector_loadscript_reply_ok(redis_node_t *node, redis_lua_script_t *script, redisReply *reply) {
  if (reply == NULL) {
    node_log_error(node, "missing reply after loading Redis Lua script %s", script->name);
    return 0;
  }
  switch(reply->type) {
    case REDIS_REPLY_ERROR:
      node_log_error(node, "failed loading Redis Lua script %s: %s", script->name, reply->str);
      return 0;
    
    case REDIS_REPLY_STRING:
      if(ngx_strncmp(reply->str, script->hash, REDIS_LUA_HASH_LENGTH)!=0) {
        node_log_error(node, "Lua script %s has unexpected hash %s (expected %s)", script->name, reply->str, script->hash);
        return 0;
      }
      else {
        return 1;
      }
      break;
      
    default:
      node_log_error(node, "unexpected reply type while loading Redis Lua script %s", script->name);
      return 0;
  }
}

static void redis_nginx_unexpected_disconnect_event_handler(const redisAsyncContext *ac, int status) {
  redis_node_t    *node = ac->data;
  //char            *which_ctx;
  //DBG("unexpected disconnect event handler ac %p", ac);
  if(node) {
    if(node->ctx.cmd == ac) {
      //which_ctx = "cmd";
      node->ctx.cmd = NULL;
    }
    else if(node->ctx.pubsub == ac) {
      node->ctx.pubsub = NULL;
      //which_ctx = "pubsub";
    }
    else {
      node_log_error(node, "unknown redisAsyncContext disconnected");
      //which_ctx = "unknown";
    }
    
    if(node->state >= REDIS_NODE_READY && !ngx_exiting && !ngx_quit) {
      if(ac->err) {
        node_log_error(node, "connection lost: %s.", ac->errstr);
      }
      else {
        node_log_error(node, "connection lost");
      }
    }
    node_disconnect(node, REDIS_NODE_FAILED);
    nodeset_examine(node->nodeset);
  }
}

static void redis_nginx_connect_event_handler(const redisAsyncContext *ac, int status) {
  node_connector_callback((redisAsyncContext *)ac, NULL, ac->data);
}

static redisAsyncContext *node_connect_context(redis_node_t *node) {
  redisAsyncContext       *ac = NULL;
  redis_connect_params_t  *rcp = &node->connect_params;
  u_char                   hostchr[1024] = {0};
    if(rcp->hostname.len >= 1023) {
    node_log_error(node, "redis hostname is too long");
    return NULL;
  }
  ngx_memcpy(hostchr, rcp->hostname.data, rcp->hostname.len);
  ac = redisAsyncConnect((const char *)hostchr, rcp->port);
  if (ac == NULL) {
    node_log_error(node, "count not allocate Redis context");
    return NULL;
  }
  if(ac->err) {
    node_log_error(node, "could not create Redis context: %s", ac->errstr);
    redisAsyncFree(ac);
    return NULL;
  }
  
#if (NGX_OPENSSL)
  if(node->nodeset->settings.tls.enabled) {
    SSL *ssl = SSL_new(node->nodeset->ssl_context);
    if (!ssl) {
      redisAsyncFree(ac);
      node_log_error(node, "Failed to create SSL object");
      return NULL;
    }
    
    if (node->nodeset->settings.tls.server_name.len > 0 && !SSL_set_tlsext_host_name(ssl, (char *)node->nodeset->settings.tls.server_name.data)) {
        node_log_error(node, "Failed to configure SSL server name");
        SSL_free(ssl);
        redisAsyncFree(ac);
        return NULL;
    }
    
    if(redisInitiateSSL(&ac->c, ssl) != REDIS_OK) {
      node_log_error(node, "could not initialize Redis SSL context: %s", (ac->errstr ? ac->errstr : "unknown error"));
      redisAsyncFree(ac);
      return NULL;
    }
  }
#endif
  
  if(redis_nginx_event_attach(ac) != REDIS_OK) {
    node_log_error(node, "could not attach Nginx events");
    redisAsyncFree(ac);
    return NULL;
  }
    
  ac->data = node;
  
  redisAsyncSetConnectCallback(ac, redis_nginx_connect_event_handler);
  redisAsyncSetDisconnectCallback(ac, redis_nginx_unexpected_disconnect_event_handler);
  return ac;
}

redisContext *node_connect_sync_context(redis_node_t *node) {
  redisReply              *reply;
  redisContext            *c;
  redis_connect_params_t  *rcp = &node->connect_params;
  u_char                   hostchr[1024] = {0};
    if(rcp->hostname.len >= 1023) {
    node_log_error(node, "redis hostname is too long");
    return NULL;
  }
  ngx_memcpy(hostchr, rcp->hostname.data, rcp->hostname.len);
  
  c = redisConnect((const char *)hostchr, rcp->port);
  if(c == NULL) {
    node_log_error(node, "could not connect synchronously to Redis");
    return NULL;
  }
  else if(c->err) {
    node_log_error(node, "could not connect synchronously to Redis: %s", c->errstr);
    redisFree(c);
    return NULL;
  }
  
#if (NGX_OPENSSL)
  if(node->nodeset->settings.tls.enabled) {
    
    SSL *ssl = SSL_new(node->nodeset->ssl_context);
    if (!ssl) {
      redisFree(c);
      node_log_error(node, "could not connect synchronously to Redis: Failed to create SSL object");
      return NULL;
    }
    
    if (node->nodeset->settings.tls.server_name.len > 0 && !SSL_set_tlsext_host_name(ssl, (char *)node->nodeset->settings.tls.server_name.data)) {
      node_log_error(node, "could not connect synchronously to Redis: Failed to configure SSL server name");
      SSL_free(ssl);
      redisFree(c);
      return NULL;
    }
    
    if(redisInitiateSSL(c, ssl) != REDIS_OK) {
      node_log_error(node, "could not initialize Redis SSL context: %s", c->errstr);
      SSL_free(ssl);
      redisFree(c);
      return NULL;
    }
  }
#endif

  if(rcp->password.len > 0) {
    if(rcp->username.len > 0) {
      reply = redisCommand(c, "AUTH %b %b", rcp->username.data, rcp->username.len, rcp->password.data, rcp->password.len);
    }
    else {
      reply = redisCommand(c, "AUTH %b", rcp->password.data, rcp->password.len);
    }
    if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
      node_log_error(node, "could not connect synchronously to Redis: bad password");
      redisFree(c);
      return NULL;
    }
  }
  if(rcp->db != -1) {
    reply = redisCommand(c, "SELECT %d", rcp->db);
    if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
      node_log_error(node, "could not connect synchronously to Redis: bad database number");
      redisFree(c);
      return NULL;
    }
  }
  
  return c;
}

static nchan_redis_ip_range_t *node_ip_blacklisted(redis_nodeset_t *ns, redis_connect_params_t *rcp) {
  struct addrinfo *res;
  char hostname_buf[128];
  ngx_memzero(hostname_buf, sizeof(hostname_buf));
  ngx_memcpy(hostname_buf, rcp->hostname.data, rcp->hostname.len);
  
  if(getaddrinfo(hostname_buf, NULL, NULL, &res) != 0) {
    nodeset_log_error(ns, "Failed to getaddrinfo for hostname %V while deciding if IP is blacklisted");
    return NULL;
  }
  int fam = res->ai_family;
  union {
    struct in_addr  ipv4;
#ifdef AF_INET6
    struct in6_addr ipv6;
#endif
  } addr;
  
  if(res->ai_family == AF_INET) {
    addr.ipv4 = ((struct sockaddr_in *)res->ai_addr)->sin_addr;
  }
#ifdef AF_INET6
  else if(res->ai_family == AF_INET6) {
    addr.ipv6 = ((struct sockaddr_in6 *)res->ai_addr)->sin6_addr;
  }
#endif
  freeaddrinfo(res);
  int i;
  for(i=0; i < ns->settings.blacklist.count; i++) {
    nchan_redis_ip_range_t *entry = &ns->settings.blacklist.list[i];
    if(entry->family != fam) {
      continue;
    }
    if(fam == AF_INET) {
      if(entry->addr_block.ipv4.s_addr == (addr.ipv4.s_addr & entry->mask.ipv4.s_addr)) {
        return entry;
      }
    }
#ifdef AF_INET6
    else if(fam == AF_INET6) {
      unsigned           j;
      struct in6_addr    buf = addr.ipv6;
      uint8_t           *masked_addr = entry->addr_block.ipv6.s6_addr;
      uint8_t           *mask = entry->mask.ipv6.s6_addr;
      
      for(j=0; j<sizeof(entry->addr_block.ipv6.s6_addr); j++) {
        masked_addr[j] &= mask[j];
      }
      
      if(memcmp(entry->addr_block.ipv6.s6_addr, &buf, sizeof(buf)) == 0) {
        return entry;
      }
    }
#endif
  }
  
  return NULL;
}

static int node_discover_slaves_from_info_reply(redis_node_t *node, redisReply *reply) {
  redis_connect_params_t   *rcp;
  size_t                    i, n;
  if(!(rcp = parse_info_slaves(node, reply->str, &n))) {
    return 0;
  }
  for(i=0; i<n; i++) {
    nchan_redis_ip_range_t *matched = node_ip_blacklisted(node->nodeset, &rcp[i]);
    if(matched) {
      nodeset_log_notice(node->nodeset, "Skipping slave node %V blacklisted by %V", &rcp->hostname, &matched->str);
    }
    else {
      node_discover_slave(node, &rcp[i]);
    }
  }
  return 1;
}

int nodeset_node_keyslot_changed(redis_node_t *node, const char *reason) {
  
  char errstr[512];
  if(reason) {
    ngx_snprintf((u_char *)errstr, 512, "cluster keyspace needs to be updated as reported by node %V:%d (%s)%Z", &(node)->connect_params.hostname, node->connect_params.port, reason);
  }
  else {
    ngx_snprintf((u_char *)errstr, 512, "cluster keyspace needs to be updated as reported by node %V:%d%Z", &(node)->connect_params.hostname, node->connect_params.port);
  }
  nodeset_set_status(node->nodeset, REDIS_NODESET_CLUSTER_FAILING, errstr);
  return 1;
}

int nodeset_node_reply_keyslot_ok(redis_node_t *node, redisReply *reply) {
  if(reply && reply->type == REDIS_REPLY_ERROR) {
    char    *script_nonlocal_key_error = "Lua script attempted to access a non local key in a cluster node";
    char    *script_nonlocal_key_error_redis_7 = "ERR Script attempted to access a non local key in a cluster node";
    char    *script_error_start = "ERR Error running script";
    char    *command_move_error = "MOVED ";
    char    *command_ask_error = "ASK ";
    
    if((nchan_cstr_startswith(reply->str, script_error_start) && nchan_strstrn(reply->str, script_nonlocal_key_error))
     || nchan_cstr_startswith(reply->str, script_nonlocal_key_error_redis_7)
     || nchan_cstr_startswith(reply->str, command_move_error)
     || nchan_cstr_startswith(reply->str, command_ask_error)) {
      if(!node) {
        nchan_log_error("Got a keyslot error from Redis on a NULL node");
      }
      else if(!node->cluster.enabled) {
        node_log_error(node, "got a cluster error on a non-cluster redis connection: %s", reply->str);
        node_disconnect(node, REDIS_NODE_FAILED);
        nodeset_set_status(node->nodeset, REDIS_NODESET_CLUSTER_FAILING, "Strange response from node");
      }
      else {
        nodeset_node_keyslot_changed(node, "keyslot error in response");
      }
      return 0;
    }
  }
  return 1;
}

int nodeset_node_can_retry_commands(redis_node_t *node) {
  if(!node) {
    return 0;
  }
  
  return node->nodeset->settings.retry_commands;
}

static void node_subscribe_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply                 *reply = rep;
  redis_node_t               *node = privdata;
  if(node->state == REDIS_NODE_SUBSCRIBING_WORKER) {
    node_connector_callback(ac, rep, privdata);
  }
  else if(reply && reply->type == REDIS_REPLY_ARRAY && reply->elements == 3
   && reply->element[0]->type == REDIS_REPLY_STRING 
   && reply->element[1]->type == REDIS_REPLY_STRING
   && reply->element[2]->type == REDIS_REPLY_STRING
   && strcmp(reply->element[0]->str, "message") == 0
   && strcmp(reply->element[1]->str, redis_worker_id) == 0
   && strcmp(reply->element[2]->str, "ping") == 0
  ) {
    node_log_debug(node, "received PUBSUB ping message");
  }
  else {
    redis_subscribe_callback(ac, rep, privdata);
  }
}

static int node_unset_cluster_slots(redis_node_t *node) {
  int success = 1;
  if(node->cluster.slot_range.indexed) {
    success &= nodeset_cluster_node_unindex_keyslot_ranges(node);
  }
  if(node->cluster.slot_range.range) {
    ngx_free(node->cluster.slot_range.range);
  }
  node->cluster.slot_range.range = NULL;
  node->cluster.slot_range.n = 0;
  return success;
}

static int node_set_cluster_slots(redis_node_t *node, cluster_nodes_line_t *l, char *errbuf, size_t max_err_len) {
  redis_node_t       *conflict_node;
  
  node_unset_cluster_slots(node);
  
  size_t                 j;
  if(l->slot_ranges_count == 0) {
    ngx_snprintf((u_char *)errbuf, max_err_len, "Tried to set cluster slots with 0 slots assigned for node %s%Z", node_cstr(node));
    goto fail;
  }
  
  node->cluster.slot_range.n = l->slot_ranges_count;
  node->cluster.slot_range.range = ngx_alloc(sizeof(redis_slot_range_t) * node->cluster.slot_range.n, ngx_cycle->log);
  if(!node->cluster.slot_range.range) {
    ngx_snprintf((u_char *)errbuf, max_err_len, "failed allocating cluster slots range%Z");
    goto fail;
  }
  if(!parse_cluster_node_slots(l, node->cluster.slot_range.range)) {
    ngx_snprintf((u_char *)errbuf, max_err_len, "failed parsing cluster slots range");
    goto fail;
  }
  
  for(j = 0; j<node->cluster.slot_range.n; j++) {
    redis_slot_range_t r = node->cluster.slot_range.range[j];
    if((conflict_node = nodeset_node_find_by_range(node->nodeset, &r)) != NULL) {
      if(node == conflict_node) {
        ngx_snprintf((u_char *)errbuf, max_err_len, "keyslot range conflicts with itself. This is very strange indeed.");
      }
      else {
        
        ngx_snprintf((u_char *)errbuf, max_err_len, "keyslot range [%d-%d] conflict with node %s. These nodes are probably from different clusters.%Z", r.min, r.max, node_cstr(conflict_node));
      }
      goto fail;
    }
  }
  
  if(!nodeset_cluster_node_index_keyslot_ranges(node)) {
    ngx_snprintf((u_char *)errbuf, max_err_len, "indexing keyslot ranges failed");
    goto fail;
  }
  
  return 1;
  
fail:
  if(node->cluster.slot_range.range) {
    ngx_free(node->cluster.slot_range.range);
    node->cluster.slot_range.range = NULL;
  }
  return 0;
}

static int node_redis_version_ok(char *info_reply, char *err, size_t maxerrlen) {
  ngx_str_t versionstart = ngx_string("redis_version:");
  char *cur = info_reply;
  if(!nchan_strscanstr((u_char **)&cur, &versionstart, (u_char *)&cur[strlen(cur)])) {
    ngx_snprintf((u_char *)err, maxerrlen, "INFO reply missing redis_version");
    return 0;
  }  
  return 1;
}

static void node_connector_connect_timeout(void *pd) {
  redis_node_t  *node = pd;
  node->connect_timeout = NULL;
  if(node->state == REDIS_NODE_CMD_CONNECTING || node->state == REDIS_NODE_PUBSUB_CONNECTING) {
    //onConnect won't be fired, so the connector must be called manually
    node->state = REDIS_NODE_CONNECTION_TIMED_OUT;
    node_connector_callback(NULL, NULL, node);
  }
  else {
    node_disconnect(node, REDIS_NODE_CONNECTION_TIMED_OUT);
  }
}

static void node_connector_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply                 *reply = rep;
  redis_node_t               *node = privdata;
  redis_nodeset_t            *nodeset = node->nodeset;
  char                        errstr[1024];
  redis_connect_params_t     *cp = &node->connect_params;
  node_log_debug(node, "node_connector_callback state %d", node->state);
  ngx_str_t                   rest;
  redis_lua_script_t         *scripts = (redis_lua_script_t *)&redis_lua_scripts;
  
  switch(node->state) {
    case REDIS_NODE_CONNECTION_TIMED_OUT:
      return node_connector_fail(node, "connection timed out");
      break;
    case REDIS_NODE_FAILED:
    case REDIS_NODE_DISCONNECTED:
      node_command_time_start(node, NCHAN_REDIS_CMD_CONNECT);
      assert(!node->connect_timeout);
      if((node->ctx.cmd = node_connect_context(node)) == NULL) { //always connect the cmd ctx to the hostname
        return node_connector_fail(node, "failed to open redis async context for cmd");
      }
      else if(cp->peername.len == 0) { //don't know peername yet
        set_preallocated_peername(node->ctx.cmd, &cp->peername);
      }
      node->connect_timeout = nchan_add_oneshot_timer(node_connector_connect_timeout, node, nodeset->settings.node_connect_timeout);
      node->state = REDIS_NODE_CMD_CONNECTING;
      break; //wait until the onConnect callback brings us back
      
    case REDIS_NODE_CMD_CONNECTING:
      if(ac->err || ac->c.err) {
        node->ctx.cmd = NULL; //to avoid calling redisAsyncFree on the ctx during node_disconnect()
        //(it will be called automatically when this function returns control back to hiredis
        return node_connector_fail(node, ac->errstr);
      }
      if((node->ctx.pubsub = node_connect_context(node)) == NULL) {
        return node_connector_fail(node, "failed to open redis async context for pubsub");
      }
      node->state++;
      break; //wait until the onConnect callback brings us back
    
    case REDIS_NODE_PUBSUB_CONNECTING:
      if(ac->err || ac->c.err) {
        node->ctx.pubsub = NULL; //to avoid calling redisAsyncFree on the ctx during node_disconnect()
        //(it will be called automatically when this function returns control back to hiredis
        ngx_snprintf((u_char *)errstr, 1024, "(pubsub) %s%Z", ac->errstr);
        return node_connector_fail(node, errstr);
      }
      //connection established. move on...   
      node->state++;
      /* fall through */
      
    case REDIS_NODE_CMD_CHECKING_CONNECTION:
      redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "PING");
      node->state++;
      break;
      
    case REDIS_NODE_CMD_CHECKED_CONNECTION:
      //Was the ping ok? it's alright if there was an AUTH error, as long as we have a response
      if(!reply || ac->err) {
        return node_connector_fail(node, NULL);
      }
      if(reply->type == REDIS_REPLY_ERROR && nchan_cstr_startswith(reply->str, "NOAUTH")  && cp->password.len == 0) {
        return node_connector_fail(node, "server expects a password, but none was configured");
      }
      node->state++;
      /* fall through */
      
    case REDIS_NODE_PUBSUB_CHECKING_CONNECTION:
      redisAsyncCommand(node->ctx.pubsub, node_connector_callback, node, "PING");
      node->state++;
      break;
      
    case REDIS_NODE_PUBSUB_CHECKED_CONNECTION:
      if(!reply || ac->err) {
        return node_connector_fail(node, NULL);
      }
      if(reply->type == REDIS_REPLY_ERROR && nchan_cstr_startswith(reply->str, "NOAUTH")  && cp->password.len == 0) {
        return node_connector_fail(node, "server expects a password, but none was configured");
      }
      node->state++;
      /* fall through */
      
    case REDIS_NODE_CONNECTED:
      //now we need to authenticate maybe?
      if(cp->password.len > 0) {
        if(!node->ctx.cmd) {
          return node_connector_fail(node, "cmd connection missing, can't send AUTH command");
        }
        if(cp->username.len > 0) {
          redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "AUTH %b %b", STR(&cp->username), STR(&cp->password));
        }
        else {
          redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "AUTH %b", STR(&cp->password));
        }
        node->state++;
      }
      else {
        node->state = REDIS_NODE_SELECT_DB;
        return node_connector_callback(NULL, NULL, node); //continue as if authenticated
      }
      break;
      
    case REDIS_NODE_CMD_AUTHENTICATING:
      if(!reply_status_ok(reply)) {
        return node_connector_fail(node, "AUTH command failed");
      }
      if(!node->ctx.pubsub) {
        return node_connector_fail(node, "pubsub connection missing, can't send AUTH command");
      }
      //now authenticate pubsub ctx
      redisAsyncCommand(node->ctx.pubsub, node_connector_callback, node, "AUTH %b", STR(&cp->password));
      node->state++;
      break;
    
    case REDIS_NODE_PUBSUB_AUTHENTICATING:
      if(!reply_status_ok(reply)) {
        return node_connector_fail(node, "AUTH command failed");
      }
      node->state++;
      /* fall through */
    case REDIS_NODE_SELECT_DB:
      if(cp->db > 0) {
        if(!node->ctx.cmd) {
          return node_connector_fail(node, "cmd connection missing, SELECT command");
        }
        redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "SELECT %d", cp->db);
        node->state++;
      }
      else {
        node->state = REDIS_NODE_SCRIPTS_LOAD;
        return node_connector_callback(NULL, NULL, node);
      }
      break;
    
    case REDIS_NODE_CMD_SELECTING_DB:
      if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "Redis SELECT command failed,");
      }
      if(!node->ctx.cmd) {
        return node_connector_fail(node, "pubsub connection missing, can't send SELECT command");
      }
      redisAsyncCommand(node->ctx.pubsub, node_connector_callback, node, "SELECT %d", cp->db);
      node->state++;
      break;
    
    case REDIS_NODE_PUBSUB_SELECTING_DB:
      if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "Redis SELECT command failed,");
      }
      node->state++; // fallthru
      /* fall through */
    case REDIS_NODE_SCRIPTS_LOAD:
      if(!node->ctx.cmd) {
        return node_connector_fail(node, "cmd connection missing, can't send SCRIPT EXISTS command");
      }
      redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "SCRIPT EXISTS " REDIS_LUA_SCRIPTS_ALL_HASHES);
      node->state++;
      break;
    
    case REDIS_NODE_SCRIPTS_LOADED_CHECK: {
      int   i;
      if(!reply_array_ok(reply)) {
        return node_connector_fail(node, "SCRIPT EXISTS failed,");
      }
      if(reply->elements != REDIS_LUA_SCRIPTS_COUNT) {
        return node_connector_fail(node, "SCRIPT EXISTS returned wrong number of elements");
      }
      for(i=0; i<REDIS_LUA_SCRIPTS_COUNT; i++) {
        if(!reply_integer_ok(reply->element[i])) {
          return node_connector_fail(node, "SCRIPT EXISTS returned non-integer element type");
        }
        if(nodeset->settings.load_scripts_unconditionally) {
          node->scripts_load_state.loaded[i]=0;
        }
        else {
          node->scripts_load_state.loaded[i]=reply->element[i]->integer;
        }
      }
      node->scripts_load_state.loading = 0;
      node->scripts_load_state.current = 0;
      node->state++;//fallthru
    }
    /* fall through */
    case REDIS_NODE_SCRIPTS_LOADING: {
      uint8_t             *current = &node->scripts_load_state.current;
      if(node->scripts_load_state.loading) {
        //already loading. check response from previous load
        if(!node_connector_loadscript_reply_ok(node, &scripts[*current], reply)) {
          return node_connector_fail(node, "SCRIPT LOAD failed,");
        }
        assert(node->scripts_load_state.loaded[*current] == 0);
        node->scripts_load_state.loaded[*current] = 1;
        (*current)++;
      }
      
      node->scripts_load_state.loading = 1;
      
      //skip to next non-loaded script
      while(*current < REDIS_LUA_SCRIPTS_COUNT) {
        if(!node->scripts_load_state.loaded[*current]) {
          break;
        }
        (*current)++;
      }
      
      if(*current < REDIS_LUA_SCRIPTS_COUNT) {
        if(!node->ctx.cmd) {
          return node_connector_fail(node, "cmd connection missing, can't send SCRIPT LOAD command");
        }
        redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "SCRIPT LOAD %s", scripts[*current].script);
        break;
      }
      else {
        //we're done here
        node->scripts_load_state.loading = 0;
        node_log_debug(node, "all scripts loaded");
        node->state++;
      }
    }
    /* fall through */
    case REDIS_NODE_GET_INFO:
      //getinfo time
      if(!node->ctx.cmd) {
        return node_connector_fail(node, "cmd connection missing, can't send INFO ALL command");
      }
      redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "INFO ALL");
      node->state++;
      break;
      
    case REDIS_NODE_GETTING_INFO:
      if(reply && reply->type == REDIS_REPLY_ERROR && nchan_cstr_startswith(reply->str, "NOAUTH")) {
        return node_connector_fail(node, "authentication required");
      }
      else if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "INFO command failed");
      }
      else if(!node_parseinfo_set_run_id(node, reply->str)) {
        return node_connector_fail(node, "failed to set node run_id");
      }
      else if(nodeset_node_deduplicate_by_run_id(node)) {
        // this node already exists
        // the deduplication has deleted it; we're done here.
        // commence rejoicing.
        return;
      }
      
      
      if(!node_redis_version_ok(reply->str, errstr, sizeof(errstr))) {
        return node_connector_fail(node, errstr);
      }
      
      if(nchan_cstr_match_line(reply->str, "loading:1")) {
        return node_connector_fail(node, "is busy loading data...");
      }
      
      if(nchan_cstr_match_line(reply->str, "cluster_enabled:1")) {
        node->cluster.enabled = 1;
      }
      
      if(nchan_cstr_match_line(reply->str, "role:master")) {
        node_set_role(node, REDIS_NODE_ROLE_MASTER);
        if(!node->cluster.enabled && !node_discover_slaves_from_info_reply(node, reply)) {
          return node_connector_fail(node, "failed parsing slaves from INFO");
        }
      }
      else if(nchan_cstr_match_line(reply->str, "role:slave")) {
        redis_connect_params_t   *rcp;
        node_set_role(node, REDIS_NODE_ROLE_SLAVE);
        if(!(rcp = parse_info_master(node, reply->str))) {
          return node_connector_fail(node, "failed parsing master from INFO");
        }
        if(!node->cluster.enabled) {
          node_discover_master(node, rcp);
        }
      }
      else {
        return node_connector_fail(node, "can't tell if node is master or slave");
      }
      node->state++;
      /* fall through */
    case REDIS_NODE_PUBSUB_GET_INFO:
      if(!node->ctx.pubsub) {
        return node_connector_fail(node, "cmd connection missing, can't send INFO SERVER command");
      }
      redisAsyncCommand(node->ctx.pubsub, node_connector_callback, node, "INFO SERVER");
      node->state++;
      break;
      
    case REDIS_NODE_PUBSUB_GETTING_INFO:
      if(reply && reply->type == REDIS_REPLY_ERROR && nchan_cstr_startswith(reply->str, "NOAUTH")) {
        return node_connector_fail(node, "authentication required");
      }
      else if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "INFO command failed");
      }
      else {
        u_char    idbuf[MAX_RUN_ID_LENGTH];
        ngx_str_t pubsub_run_id = {0, idbuf};
        node_parseinfo_set_preallocd_str(node, &pubsub_run_id, reply->str, "run_id:", MAX_RUN_ID_LENGTH);
        if(!nchan_ngx_str_match(&node->run_id, &pubsub_run_id)) {
          return node_connector_fail(node, "IP address connects to more than one server. Is Redis behind a proxy?");
        }
        node->state++;
      }
      /* fall through */
    case REDIS_NODE_SUBSCRIBE_WORKER:
      if(!node->ctx.pubsub) {
        return node_connector_fail(node, "pubsub connection missing, can't send worker SUBSCRIBE command");
      }
      redisAsyncCommand(node->ctx.pubsub, node_subscribe_callback, node, "SUBSCRIBE %s", redis_worker_id);
      node->state++;
      break;
    
    case REDIS_NODE_SUBSCRIBING_WORKER: 
      if(!reply) {
        return node_connector_fail(node, "disconnected while subscribing to worker PUBSUB channel");
      }
      if( reply->type != REDIS_REPLY_ARRAY || reply->elements != 3 
       || reply->element[0]->type != REDIS_REPLY_STRING || reply->element[1]->type != REDIS_REPLY_STRING
       || strcmp(reply->element[0]->str, "subscribe") != 0
       || strcmp(reply->element[1]->str, redis_worker_id) != 0
      ) {
        return node_connector_fail(node, "failed to subscribe to worker PUBSUB channel");
      }
      nchan_stats_worker_incr(redis_connected_servers, 1);
      
      node->state++;
      /* fall through */
    case REDIS_NODE_GET_CLUSTERINFO:
      if(!node->cluster.enabled) {
        node->state = REDIS_NODE_READY;
        return node_connector_callback(NULL, NULL, node);
      }
      else {
        if(!node->ctx.cmd) {
          return node_connector_fail(node, "cmd connection missing, can't send CLUSTER INFO command");
        }
        redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "CLUSTER INFO");
        node->state++;
      }
      break;
//     
    case REDIS_NODE_GETTING_CLUSTERINFO:
      if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "CLUSTER INFO command failed");
      }
      if(!nchan_cstr_match_line(reply->str, "cluster_state:ok")) {
        node->cluster.ok=0;
        return node_connector_fail(node, "cluster_state not ok");
      }
      
      if(!nchan_get_rest_of_line_in_cstr(reply->str, "cluster_current_epoch:", &rest)) {
        return node_connector_fail(node, "CLUSTER INFO command failed to get current epoch");
      }
      if((node->cluster.current_epoch = ngx_atoi(rest.data, rest.len)) == NGX_ERROR) {
        return node_connector_fail(node, "CLUSTER INFO command failed to parse current epoch number");
      }
      
      //assuming this node's epoch is the cluster's epoch. we will do this for all nodes. if it turns out the epoch is wrong,
      //Redis will mark the cluster as failing or will update the epoch for all other nodes.
      nodeset->cluster.current_epoch = node->cluster.current_epoch;
      node->cluster.ok=1;
      node->state++;
      /* fall through */
      
    case REDIS_NODE_GET_SHARDED_PUBSUB_SUPPORT:
      redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "COMMAND INFO SPUBLISH");
      node->state++;
      break;
      
    case REDIS_NODE_GETTING_SHARDED_PUBSUB_SUPPORT:
      if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "COMMAND INFO reply not ok");
      }
      node->have_spublish = reply->element[0]->type == REDIS_REPLY_ARRAY;
      node->state++;
      /* fall through */
      
    case REDIS_NODE_GET_CLUSTER_NODES:
      if(!node->ctx.cmd) {
        return node_connector_fail(node, "cmd connection missing, can't send CLUSTER NODES command");
      }
      redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "CLUSTER NODES");
      node->state++;
      break;
    
    case REDIS_NODE_GETTING_CLUSTER_NODES:
      if(!reply_str_ok(reply)) {
        return node_connector_fail(node, "CLUSTER NODES command failed");
      }
      else {
        size_t                  i, n;
        cluster_nodes_line_t   *lines, *l;
        if(!(lines = parse_cluster_nodes(node, reply->str, &n))) {
          return node_connector_fail(node, "parsing CLUSTER NODES command failed");
        }
        for(i=0, l = lines; i<n; i++, l++) {
          if(l->self) {
            nchan_strcpy(&node->cluster.id, &l->id, MAX_CLUSTER_ID_LENGTH);
            if(l->master) {
              if(l->slot_ranges_count == 0) {
                node_log_notice(node, "is a master cluster node with no keyslots");
              }
              else if(!node_set_cluster_slots(node, l, errstr, sizeof(errstr))) {
                nodeset_dbg_log_nodes_and_clusternodes_lines(nodeset, NGX_LOG_NOTICE, lines, n);
                return node_connector_fail(node, errstr);
              }
            }
            else {
              nchan_strcpy(&node->cluster.master_id, &l->master_id, MAX_CLUSTER_ID_LENGTH);
            }
          }
          else if(!node_skip_cluster_peer(node, l, 1, 1)) {
            node_discover_cluster_peer(node, l, NULL);
          }
        }
      }
      if(!nodeset_link_cluster_node_roles(nodeset)) {
        return node_connector_fail(node, "failed to link all discovered cluster masters and slaves");
      }
      node->state = REDIS_NODE_READY;
      //intentional fall-through is affirmatively consensual
      //yes, i consent to being fallen through.
      //                               Signed, 
      //                                 REDIS_NODE_GETTING_CLUSTER_NODES
      //NOTE: consent required each time a fallthrough is imposed
      /* fall through */
    case REDIS_NODE_READY:
      node_command_time_finish(node, NCHAN_REDIS_CMD_CONNECT);
      node_make_ready(node);
      node->connecting = 0;
      nodeset_examine(nodeset);
      break;
  }
}


void node_batch_command_init(node_batch_command_t *batch, redis_node_t *node, redis_node_ctx_type_t ctxtype, redisCallbackFn *fn, void *privdata, unsigned cmdc, ...) {
  batch->node = node;
  batch->callback = fn;
  batch->privdata = privdata;
  batch->cmdc = cmdc;
  batch->argc = cmdc;
  batch->ctxtype = ctxtype;
  batch->commands_sent = 0;
  
  va_list argp;
  va_start(argp, cmdc);
  
  unsigned i;
  for(i=0; i<cmdc; i++) {
    batch->argv[i]=va_arg(argp, const char *);
    batch->argvlen[i]=strlen(batch->argv[i]);
  }
  
  va_end(argp);
}

void node_batch_command_send(node_batch_command_t *batch) {
  if(batch->argc <= batch->cmdc) {
    return;
  }
  redisAsyncContext *redis = NULL;
  switch(batch->ctxtype) {
    case REDIS_NODE_CTX_COMMAND:
      redis = batch->node->ctx.cmd;
      break;
    case REDIS_NODE_CTX_PUBSUB:
      redis = batch->node->ctx.pubsub;
      break;
  }
  batch->commands_sent++;
  redisAsyncCommandArgv(redis, batch->callback, batch->privdata, batch->argc, batch->argv, batch->argvlen);
  batch->argc=batch->cmdc;
}

int node_batch_command_add_ngx_str(node_batch_command_t *batch, const ngx_str_t *arg) {
  return node_batch_command_add(batch, (char *)arg->data, arg->len);
}

int node_batch_command_add(node_batch_command_t *batch, const char *arg, size_t arglen) {
  unsigned c = batch->argc++;
  batch->argv[c]=arg;
  batch->argvlen[c]=arglen;
  
  if(c+1==REDIS_NODE_BATCH_COMMAND_MAX_ARGS) {
    node_batch_command_send(batch);
    return 1;
  }
  return 0;
}

unsigned node_batch_command_times_sent(node_batch_command_t *batch) {
  return batch->commands_sent;
}

static void node_make_ready(redis_node_t *node) {
  node->state = REDIS_NODE_READY;
  if(node->connect_timeout) {
    nchan_abort_oneshot_timer(node->connect_timeout);
    node->connect_timeout = NULL;
  }
  if(!node->ping_timer.timer_set && node->nodeset->settings.ping_interval > 0) {
    ngx_add_timer(&node->ping_timer, node->nodeset->settings.ping_interval * 1000);
  }
  
  if(!node->timeout.ev.timer_set && node->nodeset->settings.command_timeout > 0) {
    ngx_add_timer(&node->timeout.ev, node->nodeset->settings.command_timeout);
  }
  
  //do we have any channels that don't belong here?
  if(node->cluster.enabled) {
    
    rdstore_channel_head_t *cur, *next;
    nchan_slist_t *cmd = &node->channels.cmd;
    nchan_slist_t *pubsub = &node->channels.pubsub;
    
    nchan_slist_t *disconnected_cmd = &node->nodeset->channels.disconnected_cmd;
    int uncommanded_count = 0;
    for(cur = nchan_slist_first(cmd); cur != NULL; cur = next) {
      next = nchan_slist_next(cmd, cur);
      if(node_channel_in_keyspace(node, cur)) {
        //all good
        continue;
      }
      
      nodeset_node_dissociate_chanhead(cur);
      nchan_slist_append(disconnected_cmd, cur);
      cur->redis.slist.in_disconnected_cmd_list = 1;
      if(cur->status && cur->status == READY) {
        cur->status = NOTREADY;
      }
      uncommanded_count++;
    }
    
    node_batch_command_t unsub;
    /* we don't actually care about the response. if it succeeds, good. if not, it means
      that the node had already disconnected or the subscription had become
      invalid on the server for unknown reasons.
      So, the response handler is NULL
    */
    node_batch_command_init(&unsub, node, REDIS_NODE_CTX_PUBSUB, NULL, NULL, 1, node->nodeset->use_spublish ? "SUNSUBSCRIBE" : "UNSUBSCRIBE");
    
    int unsubbed_count = 0;
    for(cur = nchan_slist_first(pubsub); cur != NULL; cur = next) {
      next = nchan_slist_next(pubsub, cur);
      if(node_channel_in_keyspace(node, cur)) {
        //all good
        continue;
      }
      if(cur->pubsub_status == REDIS_PUBSUB_UNSUBSCRIBED) {
        continue;
      }
      redis_chanhead_set_pubsub_status(cur, NULL, REDIS_PUBSUB_UNSUBSCRIBED);
      unsubbed_count++;
      
      node_batch_command_add_ngx_str(&unsub, &cur->redis.pubsub_id);
    }
    if(unsubbed_count > 0) {
      node_batch_command_send(&unsub);
    }
    int times_sent = node_batch_command_times_sent(&unsub);
    int i;
    for(i=0; i< times_sent; i++) {
      node_pubsub_time_start(node, NCHAN_REDIS_CMD_PUBSUB_UNSUBSCRIBE);
    }
    
    if(unsubbed_count + uncommanded_count > 0) {
      const char *reason = "";
      if(node->role == REDIS_NODE_ROLE_SLAVE) {
        reason = " (the node is now a slave)";
      }
      else if(node->role == REDIS_NODE_ROLE_MASTER) {
        if(node->cluster.slot_range.n == 0) {
          reason = " (slotless master, probably on its way to becoming a slave)";
        }
        else {
          reason = " (no longer in this node's keyspace)";
        }
      }
      node_log_notice(node, "paused subscription on %d and publication on %d channels%s.", unsubbed_count, uncommanded_count, reason);
    }
  }
  
  if(node->recovering) {
    node_log_notice(node, "recovered");
  }
  else {
    node_log_notice(node, "%s", node->generation == 0 ? "connected" : "reconnected");
  }
  node->generation++;
  node->connecting=0;
  node->recovering=0;
}

static int nodeset_cluster_keyslot_space_complete(redis_nodeset_t *ns, int min_node_state) {
  ngx_rbtree_node_t                  *node;
  redis_slot_range_t                  range = {0, 0};
  redis_nodeset_slot_range_node_t    *rangenode;
  
  while(range.min <= 16383) {
    if((node = rbtree_find_node(&ns->cluster.keyslots, &range)) == NULL) {
      DBG("cluster slots range incomplete: can't find slot %i", range.min);
      return 0;
    }
    rangenode = rbtree_data_from_node(node);
    
    if(rangenode->node->state < min_node_state) {
      node_log_notice(rangenode->node, "cluster node for range %d - %d not connected", rangenode->range.min, rangenode->range.max);
      return 0;
    }
    if(rangenode->node->role != REDIS_NODE_ROLE_MASTER) {
      node_log_notice(rangenode->node, "cluster node for range %d - %d is not a master. That's weird.", rangenode->range.min, rangenode->range.max);
      return 0;
    }
    
    range.min = rangenode->range.max + 1;
    range.max = range.min;
  }
  DBG("cluster range complete");
  //print_cluster_slots(cluster);
  return 1;
}

static int nodeset_status_timer_interval(redis_nodeset_status_t status) {
  switch(status) {
    case REDIS_NODESET_FAILED:
    case REDIS_NODESET_INVALID:
    case REDIS_NODESET_DISCONNECTED:
      return 500;
    case REDIS_NODESET_FAILING:
    case REDIS_NODESET_CLUSTER_FAILING:
      return 300;
    case REDIS_NODESET_CONNECTING:
      return 1000;
    case REDIS_NODESET_READY:
      return 10000;
  }
  return 500; //default?
}

static void nodeset_onready_expire_event(ngx_event_t *ev) {
  nodeset_onready_callback_t *rcb = ev->data;
  rcb->cb(rcb->ns, rcb->pd);
  nchan_list_remove(&rcb->ns->onready_callbacks, rcb);
}

ngx_int_t nodeset_callback_on_ready(redis_nodeset_t *ns, ngx_int_t (*cb)(redis_nodeset_t *, void *), void *pd) {
  nodeset_onready_callback_t *ncb;
  
  ngx_msec_t max_wait = ns->settings.retry_commands_max_wait;
  
  if(ns->status == REDIS_NODESET_READY) {
    cb(ns, pd);
    return NGX_OK;
  }
  
  ncb = nchan_list_append(&ns->onready_callbacks);
  if(ncb == NULL) {
    ERR("failed to add to onready_callback list");
    return NGX_ERROR;
  }
  
  ncb->cb = cb;
  ncb->pd = pd;
  ncb->ns = ns;
  ngx_memzero(&ncb->ev, sizeof(ncb->ev));
  if(max_wait > 0) {
    nchan_init_timer(&ncb->ev, nodeset_onready_expire_event, ncb);
    ngx_add_timer(&ncb->ev, max_wait);
  }
  
  return NGX_OK;
}

ngx_int_t nodeset_run_on_ready_callbacks(redis_nodeset_t *ns) {
  nodeset_onready_callback_t *rcb;
  for(rcb = nchan_list_first(&ns->onready_callbacks); rcb != NULL; rcb = nchan_list_next(rcb)) {
    if(rcb->ev.timer_set) {
      ngx_del_timer(&rcb->ev);
    }
    rcb->cb(ns, rcb->pd);
  }
  nchan_list_empty(&ns->onready_callbacks);
  return NGX_OK;
}

ngx_int_t nodeset_abort_on_ready_callbacks(redis_nodeset_t *ns) {
  nodeset_onready_callback_t *rcb;
  for(rcb = nchan_list_first(&ns->onready_callbacks); rcb != NULL; rcb = nchan_list_next(rcb)) {
    if(rcb->ev.timer_set) {
      ngx_del_timer(&rcb->ev);
    }
    rcb->cb(ns, rcb->pd);
  }
  nchan_list_empty(&ns->onready_callbacks);
  return NGX_OK;
}

static ngx_int_t update_chanhead_status_on_reconnect(rdstore_channel_head_t *ch) {
  if(ch->redis.node.cmd && ch->redis.node.pubsub && ch->pubsub_status == REDIS_PUBSUB_SUBSCRIBED && ch->status == NOTREADY) {
    ch->status = READY;
  }
  return NGX_OK;
}

static ngx_int_t nodeset_reconnect_disconnected_channels(redis_nodeset_t *ns) {
  rdstore_channel_head_t *cur;
  nchan_slist_t *disconnected_cmd = &ns->channels.disconnected_cmd;
  nchan_slist_t *disconnected_pubsub = &ns->channels.disconnected_pubsub;
  assert(nodeset_ready(ns));
  int cmd_count=0, pubsub_count=0;
  while((cur = nchan_slist_pop(disconnected_cmd)) != NULL) {
    cmd_count++;
    assert(cur->redis.node.cmd == NULL);
    cur->redis.slist.in_disconnected_cmd_list = 0;
    assert(nodeset_node_find_by_chanhead(cur)); // this reuses the linked-list fields
    update_chanhead_status_on_reconnect(cur);
  }
  
  while((cur = nchan_slist_pop(disconnected_pubsub)) != NULL) {
    pubsub_count++;
    assert(cur->redis.node.pubsub == NULL);
    cur->redis.slist.in_disconnected_pubsub_list = 0;
    assert(nodeset_node_pubsub_find_by_chanhead(cur)); // this reuses the linked-list fields
    redis_chanhead_catch_up_after_reconnect(cur);
    ensure_chanhead_pubsub_subscribed_if_needed(cur);
    update_chanhead_status_on_reconnect(cur);
  }
  
  if(cmd_count + pubsub_count > 0) {
    nodeset_log_notice(ns, "resume subscription on %d and publication on %d channels", pubsub_count, cmd_count);
  }
  return NGX_OK;
}

static int nodeset_set_name_alloc(redis_nodeset_t *nodeset) {
  ngx_str_t  *name = NULL;
  if(nodeset->upstream) {
    nodeset->name_type = "upstream";
    name = &nodeset->upstream->host;
  }
  else {
    nodeset->name_type = "host";
    ngx_str_t **url = nchan_list_first(&nodeset->urls);
    if(url && *url) {
      name = *url;
    }
  }
  
  if(name == NULL || name->len == 0) {
    nodeset->name = nchan_redis_blankname;
    return 1;
  }
  
  char *namebuf = ngx_alloc(name->len+1, ngx_cycle->log);
  if(!namebuf) {
    return 0;
  }
  
  ngx_snprintf((u_char *)namebuf, name->len+1, "%V%Z", name);
  nodeset->name = namebuf;
  
  return 1;
}

const char *node_nickname_cstr(redis_node_t *node) {
  if(node) {
    return node_cstr(node);
  }
  else {
    return "???";
  }
}

ngx_int_t nodeset_set_status(redis_nodeset_t *nodeset, redis_nodeset_status_t status, const char *msg) {
  ngx_snprintf((u_char *)nodeset->status_msg, NODESET_MAX_STATUS_MSG_LENGTH, "%s%Z", msg ? msg : "");
  if(nodeset->status != status) {
    if(msg) {
      ngx_uint_t  lvl;
      if(status == REDIS_NODESET_INVALID) {
        lvl = NGX_LOG_ERR;
      }
      else if(status == REDIS_NODESET_DISCONNECTED
        ||    status == REDIS_NODESET_CLUSTER_FAILING
        ||    status == REDIS_NODESET_FAILED
      ) {
        lvl = NGX_LOG_WARN;
      }
      else {
        lvl = NGX_LOG_NOTICE;
      }
      nodeset_log(nodeset, lvl, "%s", msg);
    }
    
    if(status == REDIS_NODESET_READY) {
      nchan_stats_worker_incr(redis_unhealthy_upstreams, -1);
    }
    else if(nodeset->status == REDIS_NODESET_READY) {
      nchan_stats_worker_incr(redis_unhealthy_upstreams, 1);
    }
    
    nodeset->current_status_start = *ngx_timeofday();
    nodeset->current_status_times_checked = 0;
    nodeset->status = status;
    
    if(nodeset->status_check_ev.timer_set) {
      ngx_del_timer(&nodeset->status_check_ev);
    }
    
    switch(status) {
      case REDIS_NODESET_FAILED:
      case REDIS_NODESET_DISCONNECTED:
      case REDIS_NODESET_INVALID:
        nodeset_stop_cluster_check_timer(nodeset);
        nodeset_disconnect(nodeset);
        break;
      case REDIS_NODESET_CLUSTER_FAILING:
        nodeset_stop_cluster_check_timer(nodeset);
        nodeset_reset_cluster_node_info(nodeset);
        //but don't disconnect! we don't want to start a connection storm just because a single node got replaced
        if(!nodeset_recover_cluster(nodeset)) {
          nodeset_set_status(nodeset, REDIS_NODESET_FAILED, "failed to recover cluster");
        }
        break;
      case REDIS_NODESET_FAILING:
        nodeset_stop_cluster_check_timer(nodeset);
        nodeset_connect(nodeset);
        break;
      case REDIS_NODESET_CONNECTING:
        //no special actions
        break;
      case REDIS_NODESET_READY:
        if(nodeset->cluster.enabled) {
          nodeset_start_cluster_check_timer(nodeset);
        }
        nodeset_check_spublish_availability(nodeset);
        nodeset->current_reconnect_delay = 0;
        nodeset_reconnect_disconnected_channels(nodeset);
        nodeset_run_on_ready_callbacks(nodeset);
        break;
    }
  }
  
  if(!nodeset->status_check_ev.timer_set) {
    ngx_add_timer(&nodeset->status_check_ev, nodeset_status_timer_interval(status));
  }
  return NGX_OK;
}

static void nodeset_check_spublish_availability(redis_nodeset_t *ns) {
  if(!ns->cluster.enabled) {
    //no cluster? we don't care.
    return;
  }
  
  redis_node_t *cur;
  
  int no_spublish = 0;
  for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    if(cur->state == REDIS_NODE_READY && !cur->have_spublish) {
      no_spublish ++;
    }
  }
  ns->use_spublish = no_spublish == 0;
  
  if(no_spublish) {
    nodeset_log_warning(ns, "This cluster has nodes running Redis version < 7. Nchan is forced to use non-sharded pubsub commands that scale inversely to the cluster size. Upgrade to Redis >= 7 for much better scalability.");
  }
  
}

static void node_find_slaves_callback(redisAsyncContext *ac, void *rep, void *pd) {
  redis_node_t   *node = pd;
  redisReply     *reply = rep;
  if(!reply) {
    node_log_debug(node, "INFO REPLICATION aborted reply");
    return;
  }
  node_discover_slaves_from_info_reply(node, reply);
}

ngx_int_t nodeset_examine(redis_nodeset_t *nodeset) {
  redis_node_t *cur, *next;
  int cluster = 0, masters = 0, slaves = 0, total = 0, connecting = 0, ready = 0, disconnected = 0;
  int discovered = 0, failed_masters=0, failed_slaves = 0, failed_unknowns = 0;
  int ready_cluster = 0, ready_non_cluster = 0, connecting_masters = 0;
  redis_nodeset_status_t current_status = nodeset->status;
  //ERR("check nodeset %p", nodeset);
  
  for(cur = nchan_list_first(&nodeset->nodes); cur != NULL; cur = next) {
    next = nchan_list_next(cur);
    total++;
    if(cur->cluster.enabled == 1) {
      cluster++;
    }
    if(cur->discovered)
      discovered++;
    if(cur->role == REDIS_NODE_ROLE_MASTER) {
      masters++;
      if(cur->state > REDIS_NODE_DISCONNECTED && cur->state < REDIS_NODE_READY) {
        connecting_masters++;
      }
    }
    if(cur->role == REDIS_NODE_ROLE_SLAVE)
      slaves++;
    if(cur->state <= REDIS_NODE_DISCONNECTED)
      disconnected++;
    if(cur->state > REDIS_NODE_DISCONNECTED && cur->state < REDIS_NODE_READY)
      connecting++;
    if(cur->state == REDIS_NODE_READY) {
      ready++;
      if(cur->cluster.enabled == 1)
        ready_cluster++;
      else
        ready_non_cluster++;
    }
    if(cur->state == REDIS_NODE_FAILED) {
      if(cur->role == REDIS_NODE_ROLE_MASTER) {
        failed_masters++;
      }
      else if(cur->role == REDIS_NODE_ROLE_SLAVE) {
        failed_slaves++;
        if(cur->peers.master && cur->peers.master->state >= REDIS_NODE_READY && cur->nodeset->status == REDIS_NODESET_READY) {
          //rediscover slaves
          redisAsyncCommand(cur->peers.master->ctx.cmd, node_find_slaves_callback, cur->peers.master, "INFO REPLICATION");
        }
        //remove failed slave
        node_log_notice(cur, "removed failed slave node");
        node_disconnect(cur, REDIS_NODE_FAILED);
        nodeset_node_destroy(cur);
        total--;
      }
      else {
        failed_unknowns++;
      }
    }
  }
  
  nodeset->cluster.enabled = cluster > 0;
  
  if(current_status == REDIS_NODESET_CONNECTING && connecting > 0) {
    //still connecting, with a few nodws yet to try to connect
    return NGX_OK;
  }
  if(ready == 0 && total == 0) {
    nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "no reachable servers");
  }
  else if(cluster == 0 && masters > 1) {
    nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "invalid config, more than one master in non-cluster");
  }
  else if(ready_cluster > 0 && ready_non_cluster > 0) {
    nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "invalid config, cluster and non-cluster servers present");
  }
  else if(connecting > 0) {
    if(current_status != REDIS_NODESET_CLUSTER_FAILING) {
      nodeset_set_status(nodeset, REDIS_NODESET_CONNECTING, NULL);
    }
  }
  else if(failed_masters > 0) {
    if(current_status == REDIS_NODESET_READY) {
      if(nodeset->cluster.enabled) {
        nodeset_set_status(nodeset, REDIS_NODESET_CLUSTER_FAILING, "a master node has disconnected");
      }
      else {
        nodeset_set_status(nodeset, REDIS_NODESET_FAILING, NULL);
      }
    }
    else {
      nodeset_set_status(nodeset, REDIS_NODESET_FAILED, NULL);
    }
  }
  else if (masters == 0) {
    //this prevents slave-of-slave-of-master lookups
    nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "no reachable masters");
  }
  else if(cluster > 0 && !nodeset_cluster_keyslot_space_complete(nodeset, REDIS_NODE_READY)) {
      nodeset_set_status(nodeset, current_status, "keyslot space incomplete");
  }
  else if(current_status == REDIS_NODESET_READY && (ready == 0 || ready < total)) {
    nodeset_set_status(nodeset, REDIS_NODESET_FAILING, NULL);
  }
  else if(ready == 0) {
    nodeset_set_status(nodeset, REDIS_NODESET_DISCONNECTED, "no connected servers");
  }
  else {
    nodeset_set_status(nodeset, REDIS_NODESET_READY, "ready");
  } 
  
  return NGX_OK;
}

static int nodeset_time_exceeded(ngx_time_t *t, ngx_msec_t msec_time) {
  if(t->sec == 0) {
    return 1;
  }
  ngx_time_t *now = ngx_timeofday();
  return (now->sec - t->sec) * 1000 + (now->msec - t->msec) > msec_time;
}

static void nodeset_check_status_event(ngx_event_t *ev) {
  redis_nodeset_t *ns = ev->data;
  
  if(!ev->timedout) {
    return;
  }
  DBG("nodeset %p status check event", ns);
  ev->timedout = 0;
  
  switch(ns->status) {
    case REDIS_NODESET_FAILED:
      if(nodeset_time_exceeded(&ns->current_status_start, ns->current_reconnect_delay)) {
        nchan_set_next_backoff(&ns->current_reconnect_delay, &ns->settings.reconnect_delay);
        nodeset_log_notice(ns, "reconnecting...");
        nodeset_connect(ns);
      }
      break;
    
    case REDIS_NODESET_INVALID:
    case REDIS_NODESET_DISCONNECTED:
      if(nodeset_time_exceeded(&ns->current_status_start, ns->current_reconnect_delay)) {
        nodeset_log_notice(ns, "connecting...");
        //connect whatever needs to be connected
        nodeset_connect(ns);
      }
      break;
    
    case REDIS_NODESET_CONNECTING:
      //wait it out
      if(nodeset_time_exceeded(&ns->current_status_start, ns->settings.cluster_connect_timeout)) {
        nodeset_set_status(ns, REDIS_NODESET_DISCONNECTED, "Redis node set took too long to connect");
      }
      else {
        nodeset_examine(ns); // full status check
        if(ns->status == REDIS_NODESET_FAILED || ns->status == REDIS_NODESET_DISCONNECTED || ns->status == REDIS_NODESET_INVALID) {
          nchan_set_next_backoff(&ns->current_reconnect_delay, &ns->settings.reconnect_delay);
          nodeset_log_notice(ns, "will reconnect in %.3f sec", (ns->current_reconnect_delay/1000.0));
        }
      }
      break;
    
    case REDIS_NODESET_CLUSTER_FAILING:
      if(!ns->cluster.enabled) {
        nodeset_log_error(ns, "this is not a cluster, but it's been marked as a failing cluster. that's really weird");
      }
      else if(nodeset_time_exceeded(&ns->current_status_start, ns->settings.cluster_max_failing_msec)) {
        nodeset_set_status(ns, REDIS_NODESET_FAILED, "Cluster could not be recovered in time. Disconnecting.");
      }
      else if(!ns->cluster.recovering_on_node) {
        if(nodeset_time_exceeded(&ns->last_cluster_recovery_check_time, ns->current_cluster_recovery_delay)) {
          if(!nodeset_recover_cluster(ns)) {
            nodeset_set_status(ns, REDIS_NODESET_CLUSTER_FAILING, "failed to recover cluster");
          }
        }
      }
      break;
    
    case REDIS_NODESET_FAILING:
      if(nodeset_time_exceeded(&ns->current_status_start, ns->settings.cluster_max_failing_msec)) {
        nodeset_set_status(ns, REDIS_NODESET_FAILED, "Redis node set has failed");
      }
      break;
    
    case REDIS_NODESET_READY:
      nodeset_reconnect_disconnected_channels(ns); //in case there are any waiting around
      nodeset_run_on_ready_callbacks(ns); //in case there are any that got left behind
      break;
  }
  
  //check again soon!
  if(!ev->timer_set) {
    ngx_add_timer(ev, nodeset_status_timer_interval(ns->status));
  }
}

int nodeset_connect(redis_nodeset_t *ns) {
  redis_node_t             *node;
  ngx_str_t               **url;
  redis_connect_params_t    rcp;
  
  for(url = nchan_list_first(&ns->urls); url != NULL; url = nchan_list_next(url)) {
    parse_redis_url(*url, &rcp);
    if((node = nodeset_node_find_by_connect_params(ns, &rcp)) == NULL) {
      node = nodeset_node_create(ns, &rcp);
      node_log_debug(node, "created");
    }
    assert(node);
  }
  for(node = nchan_list_first(&ns->nodes); node != NULL; node = nchan_list_next(node)) {
    if(node->state <= REDIS_NODE_DISCONNECTED) {
      node_log_debug(node, "start connecting");
      node_connect(node);
    }
  }
  nodeset_set_status(ns, REDIS_NODESET_CONNECTING, NULL);
  return 1;
}

int nodeset_disconnect(redis_nodeset_t *ns) {
  nodeset_stop_cluster_check_timer(ns);
  
  redis_node_t *node;
  for(node = nchan_list_first(&ns->nodes); node != NULL; node = nchan_list_first(&ns->nodes)) {
    node_log_debug(node, "destroy %p", node);
    if(node->state > REDIS_NODE_DISCONNECTED) {
      node_log_debug(node, "intiating disconnect");
      node_disconnect(node, REDIS_NODE_DISCONNECTED);
    }
    nodeset_node_destroy(node);
  }
  
  return 1;
}


ngx_int_t nodeset_connect_all(void) {
  int                      i;
  redis_nodeset_t         *ns;
  DBG("connect all");
  nchan_stats_worker_incr(redis_unhealthy_upstreams, redis_nodeset_count);
  for(i=0; i<redis_nodeset_count; i++) {
    ns = &redis_nodeset[i];
    nodeset_connect(ns);
  }
  return NGX_OK;
}

ngx_int_t nodeset_destroy_all(void) {
  int                      i;
  redis_nodeset_t         *ns;
  DBG("nodeset destroy all");
  for(i=0; i<redis_nodeset_count; i++) {
    ns = &redis_nodeset[i];
    nodeset_disconnect(ns);
    redis_nodeset_stats_destroy(ns);
    if(ns->name && ns->name != nchan_redis_blankname) {
      ngx_free(ns->name);
    }
#if (NGX_OPENSSL)
    if(ns->ssl_context) {
      SSL_CTX_free(ns->ssl_context);
      ns->ssl_context = NULL;
    }
#endif
    nchan_list_empty(&ns->urls);
  }
  redis_nodeset_count = 0;
  return NGX_OK;
}

ngx_int_t nodeset_each(void (*cb)(redis_nodeset_t *, void *), void *privdata) {
  int                      i;
  redis_nodeset_t         *ns;
  for(i=0; i<redis_nodeset_count; i++) {
    ns = &redis_nodeset[i];
    cb(ns, privdata);
  }
  return NGX_OK;
}
ngx_int_t nodeset_each_node(redis_nodeset_t *ns, void (*cb)(redis_node_t *, void *), void *privdata) {
  redis_node_t             *node, *next;
  for(node = nchan_list_first(&ns->nodes); node != NULL; node = next) {
    next = nchan_list_next(node);
    cb(node, privdata);
  }
  return NGX_OK;
}

int redis_node_role_match(redis_node_t *node, redis_node_role_t role) {
  if(role == REDIS_NODE_ROLE_ANY) {
    return 1;
  }
  return node->role == role;
}

redis_node_t *nodeset_random_node(redis_nodeset_t *ns, int min_state, redis_node_role_t role) {
  int n = 0, chosen;
  redis_node_t *cur;
  for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    if(cur->state >= min_state && redis_node_role_match(cur, role)) {
      n++;
    }
  }
  if(n > 0) {
    chosen = ngx_random() % n;
    n = 0;
    for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
      if(cur->state >= min_state && redis_node_role_match(cur, role)) {
        if(n == chosen) {
          return cur;
        }
        n++;
      }
    }
  }
  return NULL;
}

static ngx_int_t set_preallocated_peername(redisAsyncContext *ctx, ngx_str_t *dst) {
  char                  *ipstr = (char *)dst->data;
  struct sockaddr_in    *s4;
  struct sockaddr_in6   *s6;
  // deal with both IPv4 and IPv6:
  switch(ctx->c.sockaddr.sa_family) {
    case AF_INET:
      s4 = (struct sockaddr_in *)&ctx->c.sockaddr;
      inet_ntop(AF_INET, &s4->sin_addr, ipstr, INET6_ADDRSTRLEN);
      break;
#ifdef AF_INET6
    case AF_INET6:
      s6 = (struct sockaddr_in6 *)&ctx->c.sockaddr;
      inet_ntop(AF_INET6, &s6->sin6_addr, ipstr, INET6_ADDRSTRLEN);
      break;
#endif
    case AF_UNSPEC:
    default:
      DBG("couldn't get sockaddr");
      return NGX_ERROR;
  }
  dst->len = strlen(ipstr);
  return NGX_OK;
}


//sneaky channel stuff
ngx_int_t nodeset_associate_chanhead(redis_nodeset_t *ns, void *chan) {
  rdstore_channel_head_t *ch = chan;
  if(ch->redis.nodeset && ch->redis.nodeset != ns) {
    nodeset_dissociate_chanhead(ch);
  }
  ngx_memzero(&ch->redis.slist, sizeof(ch->redis.slist));
  ch->redis.nodeset = ns;
  nchan_slist_append(&ns->channels.all, ch);
  return NGX_OK;
}
ngx_int_t nodeset_dissociate_chanhead(void *chan) {
  rdstore_channel_head_t *ch = chan;
  redis_nodeset_t *ns = ch->redis.nodeset;
  
  if(ns) {
    if(ch->redis.node.cmd) {
      assert(!ch->redis.slist.in_disconnected_cmd_list);
      nodeset_node_dissociate_chanhead(ch);
    }
    else if(ch->redis.slist.in_disconnected_cmd_list) {
      ch->redis.slist.in_disconnected_cmd_list = 0;
      nchan_slist_remove(&ns->channels.disconnected_cmd, ch);
    }
    
    if(ch->redis.node.pubsub) {
      assert(!ch->redis.slist.in_disconnected_pubsub_list);
      nodeset_node_dissociate_pubsub_chanhead(ch);
    }
    else if(ch->redis.slist.in_disconnected_pubsub_list) {
      ch->redis.slist.in_disconnected_pubsub_list = 0;
      nchan_slist_remove(&ns->channels.disconnected_pubsub, ch);
    }
    
    ch->redis.nodeset = NULL;
    nchan_slist_remove(&ns->channels.all, ch);
  }
  return NGX_OK;
}
ngx_int_t nodeset_node_associate_chanhead(redis_node_t *node, void *chan) {
  rdstore_channel_head_t *ch = chan;
  assert(ch->redis.node.cmd == NULL);
  assert(node->nodeset == ch->redis.nodeset);
  assert(ch->redis.slist.in_disconnected_cmd_list == 0);
  if(ch->redis.node.cmd != node) { //dat idempotence tho
    nchan_slist_append(&node->channels.cmd, ch);
  }
  ch->redis.node.cmd = node;
  return NGX_OK;
}
ngx_int_t nodeset_node_associate_pubsub_chanhead(redis_node_t *node, void *chan) {
  rdstore_channel_head_t *ch = chan;
  
  if(ch->redis.node.pubsub == node) {
    //already associated
    return NGX_OK;
  }
  assert(ch->redis.node.pubsub == NULL);
  assert(node->nodeset == ch->redis.nodeset);
  assert(ch->redis.slist.in_disconnected_pubsub_list == 0);
  if(ch->redis.node.pubsub != node) { //dat idempotence tho
    nchan_slist_append(&node->channels.pubsub, ch);
  }
  ch->redis.node.pubsub = node;
  return NGX_OK;
}
ngx_int_t nodeset_node_dissociate_chanhead(void *chan) {
  rdstore_channel_head_t *ch = chan;
  if(ch->redis.node.cmd) {
    nchan_slist_remove(&ch->redis.node.cmd->channels.cmd, ch);
  }
  ch->redis.node.cmd = NULL;
  return NGX_OK;
}
ngx_int_t nodeset_node_dissociate_pubsub_chanhead(void *chan) {
  rdstore_channel_head_t *ch = chan;
  
  if(ch->redis.node.pubsub == NULL) {
    //already dissociated
    return NGX_OK;
  }
  nchan_slist_remove(&ch->redis.node.pubsub->channels.pubsub, ch);
  ch->redis.node.pubsub = NULL; 
  return NGX_OK;
}

void node_command_sent(redis_node_t *node) {
  if(node) {
    node->timeout.sent++;
    node->pending_commands++;
  }
  nchan_stats_worker_incr(redis_pending_commands, 1); 
  nchan_stats_global_incr(total_redis_commands_sent, 1); 
}
void node_command_received(redis_node_t *node) {
  if(node) {
    node->timeout.received++;
    node->pending_commands--;
  }
  nchan_stats_worker_incr(redis_pending_commands, -1); 
}

static redis_node_t *nodeset_node_random_master_or_slave(redis_node_t *master) {
  redis_nodeset_t *ns = master->nodeset;
  int master_total = ns->settings.node_weight.master;
  int slave_total = master->peers.slaves.n * ns->settings.node_weight.slave;
  int n;
  
  assert(master->role == REDIS_NODE_ROLE_MASTER);
  
  if(master_total + slave_total == 0) {
    return master;
  }
  
  n = ngx_random() % (slave_total + master_total);
  if(n < master_total) {
    return master;
  }
  else {
    int           i = 0;
    n = ngx_random() % master->peers.slaves.n; //random slave
    redis_node_t **nodeptr;
    for(nodeptr = nchan_list_first(&master->peers.slaves); nodeptr != NULL && i < n; nodeptr = nchan_list_next(nodeptr)) {
      i++;
    }
    if(nodeptr == NULL || (*nodeptr)->state < REDIS_NODE_READY) {
      //not ready? play it safe.
      //node_log_error(*nodeptr, "got slave, but it's not ready. return master instead");
      return master;
    }
    else {
      //node_log_error(*nodeptr, "got slave");
      return *nodeptr;
    }
  }
}

redis_node_t *nodeset_node_find_by_chanhead(void *chan) {
  rdstore_channel_head_t *ch = chan;
  redis_node_t           *node;
  if(ch->redis.node.cmd) {
    return ch->redis.node.cmd;
  }
  node = nodeset_node_find_by_channel_id(ch->redis.nodeset, &ch->id);
  nodeset_node_associate_chanhead(node, ch);
  return node;
}
redis_node_t *nodeset_node_pubsub_find_by_chanhead(void *chan) {
  rdstore_channel_head_t *ch = chan;
  redis_node_t           *node;
  if(ch->redis.node.pubsub) {
    return ch->redis.node.pubsub;
  }
  node = nodeset_node_find_by_channel_id(ch->redis.nodeset, &ch->id);
  node = nodeset_node_random_master_or_slave(node);
  nodeset_node_associate_pubsub_chanhead(node, ch);
  return ch->redis.node.pubsub;
}



/*
 * Copyright 2001-2010 Georges Menie (www.menie.org)
 * Copyright 2010 Salvatore Sanfilippo (adapted to Redis coding style)
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the University of California, Berkeley nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/* CRC16 implementation according to CCITT standards.
 *
 * Note by @antirez: this is actually the XMODEM CRC 16 algorithm, using the
 * following parameters:
 *
 * Name                       : "XMODEM", also known as "ZMODEM", "CRC-16/ACORN"
 * Width                      : 16 bit
 * Poly                       : 1021 (That is actually x^16 + x^12 + x^5 + 1)
 * Initialization             : 0000
 * Reflect Input byte         : False
 * Reflect Output CRC         : False
 * Xor constant to output CRC : 0000
 * Output for "123456789"     : 31C3
 */

static const uint16_t crc16tab[256]= {
    0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
    0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
    0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
    0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
    0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
    0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
    0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
    0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
    0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
    0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
    0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
    0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
    0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
    0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
    0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
    0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
    0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
    0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
    0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
    0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
    0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
    0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
    0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
    0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
    0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
    0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
    0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
    0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
    0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
    0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
    0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
    0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

uint16_t redis_crc16(uint16_t crc, const char *buf, int len) {
    int counter;
    for (counter = 0; counter < len; counter++)
            crc = (crc<<8) ^ crc16tab[((crc>>8) ^ *buf++)&0x00FF];
    return crc;
}

