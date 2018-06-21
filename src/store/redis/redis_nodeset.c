#include "redis_nodeset.h"

#include <assert.h>
#include "store-private.h"
#include <store/store_common.h>
#include "store.h"
#include "redis_nginx_adapter.h"
#include "redis_nodeset_parser.h"
#include "redis_lua_commands.h"

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "REDIS NODESET: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS NODESET: " fmt, ##args)

static redis_nodeset_t  redis_nodeset[NCHAN_MAX_NODESETS];
static int              redis_nodeset_count = 0;
static char            *redis_worker_id = NULL;
static redisCallbackFn *redis_subscribe_callback = NULL;

typedef struct {
  ngx_event_t      ev;
  ngx_int_t      (*cb)(redis_nodeset_t *, void *);
  void            *pd;
  redis_nodeset_t *ns;
} nodeset_onready_callback_t;


static ngx_str_t       default_redis_url = ngx_string(NCHAN_REDIS_DEFAULT_URL);

static void node_connector_callback(redisAsyncContext *ac, void *rep, void *privdata);

static void *rbtree_cluster_keyslots_node_id(void *data) {
  return &((redis_nodeset_slot_range_node_t *)data)->range;
}
static uint32_t rbtree_cluster_keyslots_bucketer(void *vid) {
  return 1; //no buckets
}
static ngx_int_t rbtree_cluster_keyslots_compare(void *v1, void *v2) {
  redis_slot_range_t   *r1 = v1;
  redis_slot_range_t   *r2 = v2;
  
  if(r2->max < r1->min) //r2 is strictly left of r1
    return -1;
  else if(r2->min > r1->max) //r2 is strictly right of r1
    return 1;
  else //there's an overlap
    return 0;
}


static int nodeset_cluster_node_index_keyslot_ranges(redis_node_t *node) {
  unsigned                         i;
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
      node_log_error(node, "couldn't insert keyslot node range %d-%d", keyslot_tree_node->range.min, keyslot_tree_node->range.max);
      rbtree_destroy_node(tree, rbtree_node);
      return 0;
    }
    else {
      node_log_info(node, "inserted keyslot node range %d-%d", keyslot_tree_node->range.min, keyslot_tree_node->range.max);
    }
  }
  node->cluster.slot_range.indexed = 1;
  return 1;
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
    node_log_notice(node, "already unindexed");
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

static char *rcp_cstr(redis_connect_params_t *rcp) {
  static char    buf[512];
  ngx_snprintf((u_char *)buf, 512, "%V:%d%Z", &rcp->hostname, rcp->port, &rcp->peername);
    return buf;
}
static char *node_cstr(redis_node_t *node) {
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
  u_char          version[MAX_VERSION_LENGTH];
} node_blob_t;

static void nodeset_check_status_event(ngx_event_t *ev);

int nodeset_ready(redis_nodeset_t *nodeset) {
  return nodeset->status == REDIS_NODESET_READY;
}

ngx_int_t nodeset_initialize(char *worker_id, redisCallbackFn *subscribe_handler) {
  redis_worker_id = worker_id;
  redis_subscribe_callback = subscribe_handler;
  return NGX_OK;
}

redis_nodeset_t *nodeset_create(nchan_loc_conf_t *lcf) {
  nchan_redis_conf_t  *rcf = &lcf->redis;
  redis_nodeset_t     *ns = &redis_nodeset[redis_nodeset_count]; //incremented once everything is ok
  assert(rcf->enabled);
  assert(!rcf->nodeset);
  
  ns->first_loc_conf = lcf;
  
  if(redis_nodeset_count >= NCHAN_MAX_NODESETS) {
    nchan_log_error("Cannot create more than %i Redis nodesets", NCHAN_MAX_NODESETS);
    return NULL;
  }
  
  assert(!nodeset_find(rcf)); //must be unique
  
  nchan_list_init(&ns->urls, sizeof(ngx_str_t *), "redis urls");
  nchan_list_init(&ns->nodes, sizeof(node_blob_t), "redis nodes");
  nchan_list_init(&ns->onready_callbacks, sizeof(nodeset_onready_callback_t), "nodeset onReady callbacks");
  
  nchan_slist_init(&ns->channels, rdstore_channel_head_t, redis.slist.nodeset.prev, redis.slist.nodeset.next);
  
  ns->reconnect_delay_sec = 5;
  ns->current_status_times_checked = 0;
  ns->current_status_start = 0;
  ns->generation = 0;
  ns->settings.namespace = &rcf->namespace;
  ns->settings.storage_mode = rcf->storage_mode;
  
  ns->settings.ping_interval = rcf->ping_interval;
  
  ns->status = REDIS_NODESET_DISCONNECTED;
  ngx_memzero(&ns->status_check_ev, sizeof(ns->status_check_ev));
  ns->status_msg = NULL;
  nchan_init_timer(&ns->status_check_ev, nodeset_check_status_event, ns);
  
  //init cluster stuff
  ns->cluster.enabled = 0;
  ns->cluster.ready = 0;
  rbtree_init(&ns->cluster.keyslots, "redis cluster node (by keyslot) data", rbtree_cluster_keyslots_node_id, rbtree_cluster_keyslots_bucketer, rbtree_cluster_keyslots_compare);
  
  //urls
  if(rcf->upstream) {
    ngx_uint_t                   i;
    ngx_array_t                 *servers = rcf->upstream->servers;
    ngx_http_upstream_server_t  *usrv = servers->elts;
    ngx_str_t                   *upstream_url, **urlref;
    ns->upstream = rcf->upstream;
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
    ngx_str_t **urlref = nchan_list_append(&ns->urls);
    *urlref = rcf->url.len > 0 ? &rcf->url : &default_redis_url;
  }
  DBG("nodeset created");
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
redis_node_t *nodeset_node_find_any_master(redis_nodeset_t *ns) {
  redis_node_t *cur;
  for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    if(cur->role == REDIS_NODE_ROLE_MASTER) {
      return cur;
    }
  }
  return NULL;
}

redis_node_t *nodeset_node_find_by_channel_id(redis_nodeset_t *ns, ngx_str_t *channel_id) {
  if(!ns->cluster.enabled) {
    return nodeset_node_find_any_master(ns);
  }
  
  static uint16_t  prefix_crc = 0;
  if(prefix_crc == 0) {
    prefix_crc = redis_crc16(0, "channel:", 8);
  }
  uint16_t   slot = redis_crc16(prefix_crc, (const char *)channel_id->data, channel_id->len) % 16384;
  //DBG("channel id %V (key {channel:%V}) slot %i", str, str, slot);
  
  return nodeset_node_find_by_slot(ns, slot);
}

redis_node_t *nodeset_node_find_by_key(redis_nodeset_t *ns, ngx_str_t *key) {
  if(!ns->cluster.enabled) {
    return nodeset_node_find_any_master(ns);
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
  
  node_log_error(node, "node ping event");
  
  ev->timedout = 0;
  if(node->state == REDIS_NODE_READY) {
    assert(node->ctx.cmd);
    
    //we used to PUBLISH to the correct keyslot-mapped cluster node
    //but Redis clusters don't shard the PUBSUB keyspace, so this discrimination isn't necessary
    //just publish the damn thing if this is a master node, and just a PING for slaves
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

redis_node_t *nodeset_node_create_with_space(redis_nodeset_t *ns, redis_connect_params_t *rcp, size_t extra_space, void **extraspace_ptr) {
  assert(!nodeset_node_find_by_connect_params(ns, rcp));
  node_blob_t      *node_blob;
  if(!extra_space) {
    node_blob = nchan_list_append(&ns->nodes);
  }
  else {
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
  node->connect_params = *rcp;
  node->connect_params.peername.data = node_blob->peername;
  node->connect_params.peername.len = 0;
  node->cluster.id.len = 0;
  node->cluster.id.data = node_blob->cluster_id;
  node->cluster.enabled = 0;
  node->cluster.ok = 0;
  node->cluster.slot_range.indexed = 0;
  node->cluster.slot_range.n = 0;
  node->cluster.slot_range.range = NULL;
  node->pending_commands = 0;
  node->run_id.len = 0;
  node->run_id.data = node_blob->run_id;
  node->nodeset = ns;
  
  nchan_slist_init(&node->channels, rdstore_channel_head_t, redis.slist.nodeset.prev, redis.slist.nodeset.next);
  
  node->peers.master = NULL;
  nchan_list_init(&node->peers.slaves, sizeof(redis_node_t *), "node slaves");
  
  ngx_memzero(&node->ping_timer, sizeof(node->ping_timer));
  nchan_init_timer(&node->ping_timer, node_ping_event, node);
  
  node->ctx.cmd = NULL;
  node->ctx.pubsub = NULL;
  node->ctx.sync = NULL;
  
  assert(nodeset_node_find_by_connect_params(ns, rcp));
  return node;
}

redis_node_t *nodeset_node_create(redis_nodeset_t *ns, redis_connect_params_t *rcp) {
  return nodeset_node_create_with_space(ns, rcp, 0, NULL);
}

redis_node_t *nodeset_node_create_with_connect_params(redis_nodeset_t *ns, redis_connect_params_t *rcp) {
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
  node_set_role(node, REDIS_NODE_ROLE_UNKNOWN); //removes from all peer lists, and clears own slave list
  if(node->ctx.cmd)
    redisAsyncFree(node->ctx.cmd);
  if(node->ctx.pubsub)
    redisAsyncFree(node->ctx.pubsub);
  if(node->ctx.sync)
    redisFree(node->ctx.sync);
  nchan_list_remove(&node->nodeset->nodes, node);
  return NGX_OK;
}

static void node_discover_slave(redis_node_t *master, redis_connect_params_t *rcp) {
  redis_node_t    *slave;
  if((slave = nodeset_node_find_by_connect_params(master->nodeset, rcp))!= NULL) {
    //we know about it already
    assert(slave->role != REDIS_NODE_ROLE_MASTER);
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
    assert(master->role != REDIS_NODE_ROLE_SLAVE);
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

static void node_discover_cluster_peer(redis_node_t *node, cluster_nodes_line_t *l) {
  redis_connect_params_t   rcp;
  redis_node_t            *peer;
  assert(!l->self);
  if(l->failed) {
    return;
  }
  rcp.hostname = l->hostname;
  rcp.port = l->port;
  rcp.peername.len = 0;
  rcp.db = node->connect_params.db;
  rcp.password = node->connect_params.password;
  
  if( ((peer = nodeset_node_find_by_connect_params(node->nodeset, &rcp)) != NULL)
   || ((peer = nodeset_node_find_by_cluster_id(node->nodeset, &l->id)) != NULL)
  ) {
    //node_log_notice(node, "Discovering cluster node %s... already known", rcp_cstr(&rcp));
    return; //we already know this one.
  }
  node_log_notice(node, "Discovering cluster %s %s", (l->master ? "master" : "slave"), rcp_cstr(&rcp));
  peer = nodeset_node_create_with_connect_params(node->nodeset, &rcp);
  peer->discovered = 1;
  nchan_strcpy(&peer->cluster.id, &l->id, MAX_CLUSTER_ID_LENGTH);
  node_set_role(peer, l->master ? REDIS_NODE_ROLE_MASTER : REDIS_NODE_ROLE_SLAVE);
  //ignore all the other things for now
  node_connect(peer);
}

static ngx_int_t set_preallocated_peername(redisAsyncContext *ctx, ngx_str_t *dst);

static void node_connector_fail(redis_node_t *node, const char *err) {
  const char  *ctxerr = NULL;
  if(node->ctx.cmd && node->ctx.cmd->err) {
    ctxerr = node->ctx.cmd->errstr;
  }
  else if(node->ctx.pubsub && node->ctx.pubsub->err) {
    ctxerr = node->ctx.pubsub->errstr;
  }
  else if(node->ctx.sync && node->ctx.sync->err) {
    ctxerr = node->ctx.sync->errstr;
  }
  if(ctxerr) {
    node_log_error(node, "connection failed: %s (%s)", err, ctxerr);
  }
  else {
    node_log_error(node, "connection failed: %s", err);
  }
  node_disconnect(node);
  node->state = REDIS_NODE_FAILED;
}

int node_connect(redis_node_t *node) {
  assert(node->state <= REDIS_NODE_DISCONNECTED);
  node_connector_callback(NULL, NULL, node);
  return 1;
}

int node_disconnect(redis_node_t *node) {
  ngx_int_t prev_state = node->state;
  node_log_debug(node, "disconnect");
  if(node->ctx.cmd) {
    node->ctx.cmd->onDisconnect = NULL;
    //redisAsyncSetDisconnectCallback(node->ctx.cmd, NULL); //this only sets the callback if it's currently null...
    redisAsyncFree(node->ctx.cmd);
    node_log_debug(node, "redisAsyncFree %p", node->ctx.cmd);
  }
  if(node->ctx.pubsub) {
    node->ctx.pubsub->onDisconnect = NULL;
    //redisAsyncSetDisconnectCallback(node->ctx.pubsub, NULL);  //this only sets the callback if it's currently null...
    redisAsyncFree(node->ctx.pubsub);
    node_log_debug(node, "redisAsyncFree pubsub %p", node->ctx.pubsub);
  }
  if(node->ctx.sync) {
    redisFree(node->ctx.sync);
  }
  node->ctx.cmd = NULL;
  node->ctx.pubsub = NULL;
  node->ctx.sync = NULL;
  node->state = REDIS_NODE_DISCONNECTED;
  if(prev_state >= REDIS_NODE_READY) {
    nchan_update_stub_status(redis_connected_servers, -1);
  }
  if(node->cluster.enabled) {
    nodeset_cluster_node_unindex_keyslot_ranges(node);
  }
  if(node->cluster.slot_range.range) {
    ngx_free(node->cluster.slot_range.range);
    node->cluster.slot_range.n=0;
    node->cluster.slot_range.range = NULL;
  }
  if(node->ping_timer.timer_set) {
    ngx_del_timer(&node->ping_timer);
  }
  return 1;
}

void node_set_role(redis_node_t *node, redis_node_role_t role) {
  if(node->role == role) {
    return;
  }
  node->role = role;
  redis_node_t  **cur;
  switch(node->role) {
    case REDIS_NODE_ROLE_UNKNOWN:
      if(node->peers.master) {
        node_remove_peer(node->peers.master, node);
        ERR("removed %p from peers of %p", node->peers.master, node);
        node->peers.master = NULL;
      }
      for(cur = nchan_list_first(&node->peers.slaves); cur != NULL; cur = nchan_list_next(cur)) {
        ERR("*cur=%p (*cur)->peers.master=%p", *cur, (*cur)->peers.master);
        assert((*cur)->peers.master == node);
        ERR("remove peer *cur=%p node=%p", *cur, node);
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

static int node_connector_reply_str_ok(redisReply *reply) {
  return (reply != NULL && reply->type != REDIS_REPLY_ERROR && reply->type == REDIS_REPLY_STRING);
}
static int node_connector_reply_status_ok(redisReply *reply) {
  return (
    reply != NULL 
    && reply->type != REDIS_REPLY_ERROR
    && reply->type == REDIS_REPLY_STATUS
    && reply->str
    && strcmp(reply->str, "OK") == 0
  );
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
  char            *which_ctx;
  //DBG("unexpected disconnect event handler ac %p", ac);
  if(node) {
    if(node->ctx.cmd == ac) {
      which_ctx = "cmd";
      node->ctx.cmd = NULL;
    }
    else if(node->ctx.pubsub == ac) {
      node->ctx.pubsub = NULL;
      which_ctx = "pubsub";
    }
    else {
      node_log_error(node, "unknown redisAsyncContext disconnected");
      which_ctx = "unknown";
    }
    
    if(node->state >= REDIS_NODE_READY && !ngx_exiting && !ngx_quit) {
      if(ac->err) {
        node_log_error(node, "%s connection lost: %s.", which_ctx, ac->errstr);
      }
      else {
        node_log_error(node, "%s connection lost", which_ctx);
      }
    }
    node_disconnect(node);
    node->state = REDIS_NODE_FAILED;
    nchan_add_oneshot_timer((void (*)(void *))nodeset_check_status, node->nodeset, 10);
  }
}

static void redis_nginx_connect_event_handler(const redisAsyncContext *ac, int status) {
  node_connector_callback((redisAsyncContext *)ac, NULL, ac->data);
}

static redisAsyncContext *node_connect_context(redis_node_t *node, ngx_str_t *host, ngx_int_t port) {
  redisAsyncContext *ctx = redis_nginx_open_context(host, port, node);
  if(ctx) {
    redisAsyncSetConnectCallback(ctx, redis_nginx_connect_event_handler);
    redisAsyncSetDisconnectCallback(ctx, redis_nginx_unexpected_disconnect_event_handler);
  }
  return ctx;
}

static int node_discover_slaves_from_info_reply(redis_node_t *node, redisReply *reply) {
  redis_connect_params_t   *rcp;
  size_t                    i, n;
  if(!(rcp = parse_info_slaves(node, reply->str, &n))) {
    return 0;
  }
  for(i=0; i<n; i++) {
    node_discover_slave(node, &rcp[i]);
  }
  return 1;
}

int nodeset_node_reply_keyslot_ok(redis_node_t *node, redisReply *reply) {
  if(reply && reply->type == REDIS_REPLY_ERROR) {
    char    *script_nonlocal_key_error = "Lua script attempted to access a non local key in a cluster node";
    char    *script_error_start = "ERR Error running script";
    char    *command_move_error = "MOVED ";
    char    *command_ask_error = "ASK ";
    
    if((nchan_cstr_startswith(reply->str, script_error_start) && nchan_strstrn(reply->str, script_nonlocal_key_error))
     || nchan_cstr_startswith(reply->str, command_move_error)
     || nchan_cstr_startswith(reply->str, command_ask_error)) {
      if(!node->cluster.enabled) {
        node_log_error(node, "got a cluster error on a non-cluster redis connection: %s", reply->str);
      }
      else {
        nodeset_set_status(node->nodeset, REDIS_NODESET_CLUSTER_FAILING, "cluster keyspace needs to be updated");
      }
      return 0;
    }
    else
      return 1;
  }
  return 1;
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


static void node_connector_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply                 *reply = rep;
  redis_node_t               *node = privdata;
  redis_nodeset_t            *nodeset = node->nodeset;
  char                        errstr[1024];
  redis_connect_params_t     *cp = &node->connect_params;
  redis_lua_script_t         *next_script = (redis_lua_script_t *)&redis_lua_scripts;
  //node_log_notice(node, "node_connector_callback state %d", node->state);
  
  
  switch(node->state) {
    case REDIS_NODE_FAILED:
    case REDIS_NODE_DISCONNECTED:
      if((node->ctx.cmd = node_connect_context(node, &cp->hostname, cp->port)) == NULL) { //always connect the cmd ctx to the hostname
        return node_connector_fail(node, "failed to open redis async context for cmd");
      }
      else if(cp->peername.len == 0) { //don't know peername yet
        set_preallocated_peername(node->ctx.cmd, &cp->peername);
      }
      node->state = REDIS_NODE_CMD_CONNECTING;
      break; //wait until the onConnect callback brings us back
      
    case REDIS_NODE_CMD_CONNECTING:
      if(ac->err || ac->c.err) {
        node->ctx.cmd = NULL; //to avoid calling redisAsyncFree on the ctx during node_disconnect()
        //(it will be called automatically when this function returns control back to hiredis
        return node_connector_fail(node, ac->errstr);
      }
      if((node->ctx.pubsub = node_connect_context(node, &cp->peername, cp->port)) == NULL) {
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
      // intentional fallthrough IS INTENTIONAL!
    
    case REDIS_NODE_CONNECTED:
      //now we need to authenticate maybe?
      if(cp->password.len > 0) {
        redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "AUTH %b", STR(&cp->password));
        node->state++;
      }
      else {
        node->state = REDIS_NODE_SELECT_DB;
        return node_connector_callback(NULL, NULL, node); //continue as if authenticated
      }
      break;
    
    case REDIS_NODE_CMD_AUTHENTICATING:
      if(!node_connector_reply_status_ok(reply)) {
        return node_connector_fail(node, "AUTH command failed");
      }
      //now authenticate pubsub ctx
      redisAsyncCommand(node->ctx.pubsub, node_connector_callback, node, "AUTH %b", STR(&cp->password));
      node->state++;
      break;
    
    case REDIS_NODE_PUBSUB_AUTHENTICATING:
      if(!node_connector_reply_status_ok(reply)) {
        return node_connector_fail(node, "AUTH command failed");
      }
      node->state++;
      //intentional, i tell you
    
    case REDIS_NODE_SELECT_DB:
      if(cp->db > 0) {
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
      redisAsyncCommand(node->ctx.pubsub, node_connector_callback, node, "SELECT %d", cp->db);
      node->state++;
      break;
    
    case REDIS_NODE_PUBSUB_SELECTING_DB:
      if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "Redis SELECT command failed,");
      }
      node->state++;
      //falling throooooouuuughhhhh
    
    case REDIS_NODE_SCRIPTS_LOAD:
      node->scripts_loaded = 0;
      redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "SCRIPT LOAD %s", next_script->script);
      node->state++;
      break;
    
    case REDIS_NODE_SCRIPTS_LOADING:
      next_script = &next_script[node->scripts_loaded];
      
      if(!node_connector_loadscript_reply_ok(node, next_script, reply)) {
        return node_connector_fail(node, "SCRIPT LOAD failed,");
      }
      else {
        //node_log_debug(node, "loaded script %s", next_script->name);
        node->scripts_loaded++;
        next_script++;
      }
      if(node->scripts_loaded < redis_lua_scripts_count) {
        //load next script
        redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "SCRIPT LOAD %s", next_script->script);
        return;
      }
      node_log_debug(node, "all scripts loaded");
      node->state++;
      //fallthrough is falley-throughey
    
    case REDIS_NODE_SUBSCRIBE_WORKER:
      //TODO
      redisAsyncCommand(node->ctx.pubsub, node_subscribe_callback, node, "SUBSCRIBE %s", redis_worker_id);
      node->state++;
      break;
    
    case REDIS_NODE_SUBSCRIBING_WORKER:  
      if( reply->type != REDIS_REPLY_ARRAY || reply->elements != 3 
       || reply->element[0]->type != REDIS_REPLY_STRING || reply->element[1]->type != REDIS_REPLY_STRING
       || strcmp(reply->element[0]->str, "subscribe") != 0
       || strcmp(reply->element[1]->str, redis_worker_id) != 0
      ) {
        return node_connector_fail(node, "failed to subscribe to worker PUBSUB channel");
      }
      nchan_update_stub_status(redis_connected_servers, 1);
      
      node->state++;
      //fallthrough quite intentionally
      
    case REDIS_NODE_GET_INFO:
      //getinfo time
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
      
      if(nchan_cstr_match_line(reply->str, "loading:1")) {
        return node_connector_fail(node, "is busy loading data...");
        //TODO: retry later
      }
      
      if(nchan_cstr_match_line(reply->str, "role:master")) {
        node_set_role(node, REDIS_NODE_ROLE_MASTER);
        if(!node_discover_slaves_from_info_reply(node, reply)) {
          return node_connector_fail(node, "failed parsing slaves from INFO");
        }
      }
      else if(nchan_cstr_match_line(reply->str, "role:slave")) {
        redis_connect_params_t   *rcp;
        node_set_role(node, REDIS_NODE_ROLE_SLAVE);
        if(!(rcp = parse_info_master(node, reply->str))) {
          return node_connector_fail(node, "failed parsing master from INFO");
        }
        node_discover_master(node, rcp);
      }
      else {
        return node_connector_fail(node, "can't tell if node is master or slave");
      }
      
      //what's next?
      if(nchan_cstr_match_line(reply->str, "cluster_enabled:1")) {
        node->cluster.enabled = 1;
      }
      node->state++;
      //vert intentionally falling through
    
    
    case REDIS_NODE_GET_CLUSTERINFO:
      if(!node->cluster.enabled) {
        node->state = REDIS_NODE_READY;
        return node_connector_callback(NULL, NULL, node);
      }
      else {
        redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "CLUSTER INFO");
        node->state++;
      }
      break;
    
    case REDIS_NODE_GETTING_CLUSTERINFO:
      if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "CLUSTER INFO command failed,");
      }
      if(!nchan_cstr_match_line(reply->str, "cluster_state:ok")) {
        node->cluster.ok=0;
        return node_connector_fail(node, "cluster_state not ok");
      }
      node->cluster.ok=1;
      node->state++;
      //Oi! 'Ave ya got a loicence for that fallthrough?
    
    case REDIS_NODE_GET_CLUSTER_NODES:
      redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "CLUSTER NODES");
      node->state++;
      break;
    
    case REDIS_NODE_GETTING_CLUSTER_NODES:
      if(!node_connector_reply_str_ok(reply)) {
        return node_connector_fail(node, "CLUSTER NODES command failed");
      }
      else {
        size_t                  i, n;
        cluster_nodes_line_t   *l;
        if(!(l = parse_cluster_nodes(node, reply->str, &n))) {
          return node_connector_fail(node, "parsing CLUSTER NODES command failed");
        }
        for(i=0; i<n; i++) {
          if(l[i].self) {
            nchan_strcpy(&node->cluster.id, &l[i].id, MAX_CLUSTER_ID_LENGTH);
            if(l[i].slot_ranges_count == 0 && l[i].master) {
              node_log_notice(node, "is a master cluster node with no keyslots");
            }
            else {
              redis_node_t       *conflict_node;
              node->cluster.slot_range.n = l[i].slot_ranges_count;
              size_t                 j;
              if(node->cluster.slot_range.range) {
                ngx_free(node->cluster.slot_range.range);
              }
              node->cluster.slot_range.range = ngx_alloc(sizeof(redis_slot_range_t) * node->cluster.slot_range.n, ngx_cycle->log);
              if(!node->cluster.slot_range.range) {
                return node_connector_fail(node, "failed allocating cluster slots range");
              }
              if(!parse_cluster_node_slots(&l[i], node->cluster.slot_range.range)) {
                return node_connector_fail(node, "failed parsing cluster slots range");
              }
              for(j = 0; j<node->cluster.slot_range.n; j++) {
                if((conflict_node = nodeset_node_find_by_range(nodeset, &node->cluster.slot_range.range[j]))!=NULL) {
                  u_char buf[1024];
                  ngx_snprintf(buf, 1024, "keyslot range conflict with node %s. These nodes are probably from different clusters.%Z", node_cstr(conflict_node));
                  return node_connector_fail(node, (char *)buf);
                }
              }
              
              if(!nodeset_cluster_node_index_keyslot_ranges(node)) {
                return node_connector_fail(node, "indexing keyslot ranges failed");
              }
            }
          }
          else if(!l[i].failed) {
            node_discover_cluster_peer(node, &l[i]);
          }
        }
      }
      node->state = REDIS_NODE_READY;
      //inteonional fall-through is affirmatively consensual
      //yes, i consent to being fallen through.
      //                               Signed, 
      //                                 Redis_Node_script_Load
      //NOTE: consent required each time a fallthrough is imposed
    
    case REDIS_NODE_READY:
      if(!node->ping_timer.timer_set && nodeset->settings.ping_interval > 0) {
        ngx_add_timer(&node->ping_timer, nodeset->settings.ping_interval * 1000);
      }
      nodeset_check_status(nodeset);
      break;
      
  }
}

static int nodeset_cluster_keyslot_space_complete(redis_nodeset_t *ns) {
  ngx_rbtree_node_t                  *node;
  redis_slot_range_t                  range = {0, 0};
  redis_nodeset_slot_range_node_t    *rangenode;
  
  while(range.min <= 16383) {
    if((node = rbtree_find_node(&ns->cluster.keyslots, &range)) == NULL) {
      DBG("cluster slots range incomplete: can't find slot %i", range.min);
      return 0;
    }
    rangenode = rbtree_data_from_node(node);
    
    if(rangenode->node->state < REDIS_NODE_READY) {
      node_log_notice(rangenode->node, "cluster node for range %d - %d not connected", rangenode->range.min, rangenode->range.max);
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
      return 2000;
    case REDIS_NODESET_FAILING:
    case REDIS_NODESET_CLUSTER_FAILING:
      return 300;
    case REDIS_NODESET_CONNECTING:
      return 1000;
    case REDIS_NODESET_READY:
      return 10000;
  }
}

void nodeset_onready_expire_event(ngx_event_t *ev) {
  nodeset_onready_callback_t *rcb = ev->data;
  rcb->cb(rcb->ns, rcb->pd);
  nchan_list_remove(&rcb->ns->onready_callbacks, rcb);
}

ngx_int_t nodeset_callback_on_ready(redis_nodeset_t *ns, ngx_msec_t max_wait, ngx_int_t (*cb)(redis_nodeset_t *, void *), void *pd) {
  nodeset_onready_callback_t *ncb;
  
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

ngx_int_t nodeset_set_status(redis_nodeset_t *nodeset, redis_nodeset_status_t status, const char *msg) {
  nodeset->status_msg = msg;
  if(nodeset->status != status) {
    if(msg) {
      ngx_uint_t  lvl;
      if(status == REDIS_NODESET_INVALID)
        lvl = NGX_LOG_ERR;
      else if(status == REDIS_NODESET_DISCONNECTED)
        lvl = NGX_LOG_WARN;
      else
        lvl = NGX_LOG_NOTICE;
      nchan_log(lvl, ngx_cycle->log, 0, "%s", msg);
    }
    nodeset->current_status_start = ngx_time();
    nodeset->current_status_times_checked = 0;
    nodeset->status = status;
    
    if(nodeset->status_check_ev.timer_set) {
      ngx_del_timer(&nodeset->status_check_ev);
    }
    
    nodeset_onready_callback_t *rcb;
    
    switch(status) {
      case REDIS_NODESET_FAILED:
      case REDIS_NODESET_DISCONNECTED:
      case REDIS_NODESET_INVALID:
        nodeset_disconnect(nodeset);
        break;
      case REDIS_NODESET_CLUSTER_FAILING:
        
        //very intentional fallthrough
      case REDIS_NODESET_FAILING:
        nodeset_connect(nodeset);
        break;
      case REDIS_NODESET_CONNECTING:
        //no special actions
        break;
      case REDIS_NODESET_READY:
        for(rcb = nchan_list_first(&nodeset->onready_callbacks); rcb != NULL; rcb = nchan_list_next(rcb)) {
          if(rcb->ev.timer_set) {
            ngx_del_timer(&rcb->ev);
          }
          rcb->cb(nodeset, rcb->pd);
        }
        nchan_list_empty(&nodeset->onready_callbacks);
        break;
    }
  }
  
  if(!nodeset->status_check_ev.timer_set) {
    ngx_add_timer(&nodeset->status_check_ev, nodeset_status_timer_interval(status));
  }
  return NGX_OK;
}

static void node_find_slaves_callback(redisAsyncContext *ac, void *rep, void *pd) {
  redis_node_t   *node = pd;
  redisReply     *reply = rep;
  node_discover_slaves_from_info_reply(node, reply);
}

ngx_int_t nodeset_check_status(redis_nodeset_t *nodeset) {
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
        if(cur->peers.master && cur->peers.master->state >= REDIS_NODE_READY) {
          //rediscover slaves
          redisAsyncCommand(cur->peers.master->ctx.cmd, node_find_slaves_callback, cur->peers.master, "INFO REPLICATION");
        }
        //remove failed slave
        node_log_notice(cur, "removed failed slave node");
        node_disconnect(cur);
        nodeset_node_destroy(cur);
        total--;
      }
      else {
        failed_unknowns++;
      }
    }
  }

  if(current_status == REDIS_NODESET_CONNECTING && disconnected > 0) {
    //still connecting, with a few nodws yet to try to connect
    return NGX_OK;
  }
  if(ready == 0 && total == 0) {
    nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "no reachable Redis servers");
  }
  else if(cluster == 0 && masters > 1) {
    nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "more than one master Redis servers in non-cluster set");
  }
  else if(ready_cluster > 0 && ready_non_cluster > 0) {
    nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "cluster and non-cluster Redis servers in set");
  }
  else if(connecting > 0) {
    nodeset_set_status(nodeset, REDIS_NODESET_CONNECTING, NULL);
  }
  else if (masters == 0) {
    //this prevents slave-of-slave-of-master lookups
    nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "no reachable master Redis servers in set");
  }
  else if(cluster > 0 && !nodeset_cluster_keyslot_space_complete(nodeset)) {
    nodeset_set_status(nodeset, REDIS_NODESET_CONNECTING, "keyslot space incomplete");
  }
  else if(current_status == REDIS_NODESET_READY && (ready == 0 || ready < total)) {
    nodeset_set_status(nodeset, REDIS_NODESET_FAILING, NULL);
  }
  else if(ready == 0) {
    nodeset_set_status(nodeset, REDIS_NODESET_DISCONNECTED, "no connected Redis servers");
  }
  else {
    nodeset_set_status(nodeset, REDIS_NODESET_READY, cluster > 0 ? "Redis cluster ready" : "Redis server ready");
  } 
  
  return NGX_OK;
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
      //fall-through rather intentionally
      if(ngx_time() - ns->current_status_start > REDIS_NODESET_RECONNECT_WAIT_TIME_SEC) {
        nchan_log_notice("Reconnecting node set...");
        nodeset_connect(ns);
      }
      break;
    case REDIS_NODESET_INVALID:
    case REDIS_NODESET_DISCONNECTED:
      //connect whatever needs to be connected
      nodeset_connect(ns);
      break;
    
    case REDIS_NODESET_CONNECTING:
      //wait it out
      if(ngx_time() - ns->current_status_start > REDIS_NODESET_MAX_CONNECTING_TIME_SEC) {
        nodeset_set_status(ns, REDIS_NODESET_DISCONNECTED, "Redis node set took too long to connect");
      }
      else {
        nodeset_check_status(ns); // full status check
      }
      break;
    case REDIS_NODESET_CLUSTER_FAILING:
    case REDIS_NODESET_FAILING:
      if(ngx_time() - ns->current_status_start > REDIS_NODESET_MAX_FAILING_TIME_SEC) {
        nodeset_set_status(ns, REDIS_NODESET_FAILED, "Redis node set has failed");
      }
      break;
    
    case REDIS_NODESET_READY:
      //all is well.
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
  redis_node_t *node;
  for(node = nchan_list_first(&ns->nodes); node != NULL; node = nchan_list_first(&ns->nodes)) {
    if(node->state > REDIS_NODE_DISCONNECTED) {
      node_log_debug(node, "intiating disconnect");
      node_disconnect(node);
    }
    ERR("destroy node %p", node);
    nodeset_node_destroy(node);
  }
  
  return 1;
}


ngx_int_t nodeset_connect_all(void) {
  int                      i;
  redis_nodeset_t         *ns;
  DBG("connect all");
  for(i=0; i<redis_nodeset_count; i++) {
    ns = &redis_nodeset[i];
    nodeset_connect(ns);
  }
  return NGX_OK;
}

ngx_int_t nodeset_destroy_all(void) {
  int                      i;
  redis_nodeset_t         *ns;
  ERR ("nodeset destroy all");
  for(i=0; i<redis_nodeset_count; i++) {
    ns = &redis_nodeset[i];
    nodeset_disconnect(ns);
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
    case AF_INET6:
      s6 = (struct sockaddr_in6 *)&ctx->c.sockaddr;
      inet_ntop(AF_INET6, &s6->sin6_addr, ipstr, INET6_ADDRSTRLEN);
      break;
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
  ch->redis.nodeset = ns;
  nchan_slist_append(&ns->channels, ch);
  return NGX_OK;
}
ngx_int_t nodeset_dissociate_chanhead(void *chan) {
  rdstore_channel_head_t *ch = chan;
  redis_nodeset_t *ns = ch->redis.nodeset;
  if(ns) {
    ch->redis.nodeset = NULL;
    nchan_slist_remove(&ns->channels, ch);
  }
  return NGX_OK;
}
ngx_int_t nodeset_node_associate_chanhead(redis_node_t *node, void *chan) {
  rdstore_channel_head_t *ch = chan;
  assert(ch->redis.node.cmd == NULL);
  assert(node->nodeset == ch->redis.nodeset);
  ch->redis.node.cmd = node;
  if(ch->redis.node.pubsub != node) {
    //not associated with this node for pubsub reasons
    nchan_slist_append(&node->channels, ch);
  }
  return NGX_OK;
}
ngx_int_t nodeset_node_associate_pubsub_chanhead(redis_node_t *node, void *chan) {
  rdstore_channel_head_t *ch = chan;
  assert(ch->redis.node.pubsub == NULL);
  assert(node->nodeset == ch->redis.nodeset);
  ch->redis.node.pubsub = node;
  if(ch->redis.node.cmd != node) {
    //not associated with this node for pubsub reasons
    nchan_slist_append(&node->channels, ch);
  }
  return NGX_OK;
}
ngx_int_t nodeset_node_dissociate_chanhead(void *chan) {
  rdstore_channel_head_t *ch = chan;
  if(ch->redis.node.cmd && ch->redis.node.cmd != ch->redis.node.pubsub) {
    nchan_slist_remove(&ch->redis.node.cmd->channels, ch);
  }
  ch->redis.node.cmd = NULL;
  return NGX_OK;
}
ngx_int_t nodeset_node_dissociate_pubsub_chanhead(void *chan) {
  rdstore_channel_head_t *ch = chan;
  if(ch->redis.node.pubsub && ch->redis.node.pubsub != ch->redis.node.cmd) {
    nchan_slist_remove(&ch->redis.node.pubsub->channels, ch);
  }
  ch->redis.node.pubsub = NULL; 
  return NGX_OK;
}

redis_node_t *nodeset_node_find_by_chanhead(void *chan) {
  rdstore_channel_head_t *ch = chan;
  if(ch->redis.node.cmd) {
    return ch->redis.node.cmd;
  }
  ch->redis.node.cmd = nodeset_node_find_by_channel_id(ch->redis.nodeset, &ch->id);
  return ch->redis.node.cmd;
}
redis_node_t *nodeset_node_pubsub_find_by_chanhead(void *chan) {
  rdstore_channel_head_t *ch = chan;
  if(ch->redis.node.pubsub) {
    return ch->redis.node.pubsub;
  }
  ch->redis.node.pubsub = nodeset_node_find_by_channel_id(ch->redis.nodeset, &ch->id);
  //TODO: maybe subscribe to a slave?
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

