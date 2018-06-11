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

static redis_nodeset_t redis_nodeset[NCHAN_MAX_NODESETS];
static int             redis_nodeset_count = 0;

static ngx_str_t       default_redis_url = ngx_string(NCHAN_REDIS_DEFAULT_URL);

static void node_connector_callback(redisAsyncContext *ac, void *rep, void *privdata);

static void *rbtree_cluster_keyslots_node_id(void *data) {
  return &((redis_cluster_keyslot_range_node_t *)data)->range;
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
  for(i=0; i<node->cluster.slot_range.n; i++) {
    if(nodeset_node_find_by_range(node->nodeset, &node->cluster.slot_range.range[i])) { //overlap!
      return 0;
    }
  }
  
  for(i=0; i<node->cluster.slot_range.n; i++) {
    rbtree_node = rbtree_create_node(&node->nodeset->cluster.keyslots, sizeof(*keyslot_tree_node));
    keyslot_tree_node = rbtree_data_from_node(rbtree_node);
    keyslot_tree_node->range = node->cluster.slot_range.range[i];
    keyslot_tree_node->node = node;
  }
  return 1;
}

static char *rcp_cstr(redis_connect_params_t *rcp) {
  static char    buf[512];
  ngx_snprintf((u_char *)buf, 512, "%s:%d (%s) %Z", &rcp->hostname, rcp->port, rcp->peername);
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

redis_nodeset_t *nodeset_create(nchan_redis_conf_t *rcf) {
  redis_nodeset_t *ns = &redis_nodeset[redis_nodeset_count]; //incremented once everything is ok

  if(redis_nodeset_count >= NCHAN_MAX_NODESETS) {
    nchan_log_error("Cannot create more than %i Redis nodesets", NCHAN_MAX_NODESETS);
    return NULL;
  }
  
  assert(!nodeset_find(rcf)); //must be unique
  
  nchan_list_init(&ns->urls, sizeof(ngx_str_t *), "redis urls");
  ns->ready = 0;
  nchan_list_init(&ns->nodes, sizeof(node_blob_t), "redis nodes");
  
  ns->status = REDIS_NODESET_DISCONNECTED;
  
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
  return ns;
}

redis_nodeset_t *nodeset_find(nchan_redis_conf_t *rcf) {
  int              i;
  redis_nodeset_t *ns;
  for(i=0; i<redis_nodeset_count; i++) {
    ns = &redis_nodeset[i];
    if(rcf->upstream) {
      if(ns->upstream == rcf->upstream)
        return ns;
    }
    else {
      ngx_str_t *search_url = rcf->url.len > 0 ? &rcf->url : &default_redis_url;
      ngx_str_t **first_url = nchan_list_first(&ns->urls);
      
      if(first_url && nchan_ngx_str_match(search_url, *first_url)) {
        return ns;
      }
    }
  }
  return NULL;
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
  if(cp1->port != cp2->port
   || cp1->db != cp2->db
   || cp1->hostname.len != cp2->hostname.len) {
    return 0;  
  }
  return ngx_strncmp(cp1->hostname.data, cp2->hostname.data, cp1->hostname.len) == 0;
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



redis_node_t *nodeset_node_create_with_space(redis_nodeset_t *ns, redis_connect_params_t *rcp, size_t extra_space, void **extraspace_ptr) {
  assert(!nodeset_node_find_by_connect_params(ns, rcp));
  node_blob_t      *node_blob;
  if(extra_space) {
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
  node->cluster.slot_range.n = 0;
  node->cluster.slot_range.range = NULL;
  node->run_id.len = 0;
  node->run_id.data = node_blob->run_id;
  node->nodeset = ns;
  
  node->peers.master = NULL;
  nchan_list_init(&node->peers.slaves, sizeof(redis_node_t *), "node slaves");
  
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
    if(*cur == node) {
      nchan_list_remove(&node->peers.slaves, cur);
      return;
    }
  }
}

ngx_int_t nodeset_node_destroy(redis_node_t *node) {
  node_set_role(node, REDIS_NODE_ROLE_UNKNOWN); //removes from all peer lists
  nchan_list_empty(&node->peers.slaves);
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
  node_log_notice(master, "Discovering slave %s", rcp_cstr(rcp));
  if((slave = nodeset_node_find_by_connect_params(master->nodeset, rcp))!= NULL) {
    //we know about it already
    assert(slave->role == REDIS_NODE_ROLE_SLAVE);
    assert(slave->peers.master == master);
  }
  else {
    slave = nodeset_node_create_with_connect_params(master->nodeset, rcp);
    slave->discovered = 1;
    node_set_role(slave, REDIS_NODE_ROLE_SLAVE);
  }
  node_set_master_node(slave, master); //this is idempotent
  node_add_slave_node(master, slave);  //so is this
  //try to connect
  if(slave->state <= REDIS_NODE_DISCONNECTED) {
    node_connector_callback(NULL, NULL, slave);
  }
}

static void node_discover_master(redis_node_t  *slave, redis_connect_params_t *rcp) {
  redis_node_t *master;
  if ((master = nodeset_node_find_by_connect_params(slave->nodeset, rcp)) != NULL) {
    assert(master->role == REDIS_NODE_ROLE_MASTER);
    assert(node_find_slave_node(master, slave));
    node_log_notice(master, "Discovering master %s... already known", rcp_cstr(rcp));
  }
  else {
    master = nodeset_node_create_with_connect_params(slave->nodeset, rcp);
    master->discovered = 1;
    node_set_role(master, REDIS_NODE_ROLE_MASTER);
    node_log_notice(master, "Discovering master %s", rcp_cstr(rcp));
  }
  node_set_master_node(slave, master);
  node_add_slave_node(master, slave);
  //try to connect
  if(master->state <= REDIS_NODE_DISCONNECTED) {
    node_connector_callback(NULL, NULL, master);
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
    node_log_notice(node, "Discovering cluster node %s... already known", rcp_cstr(&rcp));
    return; //we already know this one.
  }
  node_log_notice(node, "Discovering cluster node %s", rcp_cstr(&rcp));
  peer = nodeset_node_create_with_connect_params(node->nodeset, &rcp);
  peer->discovered = 1;
  nchan_strcpy(&peer->cluster.id, &l->id, MAX_CLUSTER_ID_LENGTH);
  //ignore all the other things for now
  node_connector_callback(NULL, NULL, peer);
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
  node->state = REDIS_NODE_FAILED;
  if(node->ctx.cmd) {
    redisAsyncFree(node->ctx.cmd);
  }
  if(node->ctx.pubsub) {
    redisAsyncFree(node->ctx.pubsub);
  }
  if(node->ctx.sync) {
    redisFree(node->ctx.sync);
  }
  node->ctx.cmd = NULL;
  node->ctx.pubsub = NULL;
  node->ctx.sync = NULL;
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
        node->peers.master = NULL;
      }
      for(cur = nchan_list_first(&node->peers.slaves); cur != NULL; cur = nchan_list_next(cur)) {
        assert((*cur)->peers.master == node);
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



static void node_connector_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply                 *reply = rep;
  redis_node_t               *node = privdata;
  
  redis_connect_params_t     *cp = &node->connect_params;
  redis_lua_script_t         *next_script = (redis_lua_script_t *)&redis_lua_scripts;

  switch(node->state) {
    case REDIS_NODE_FAILED:
    case REDIS_NODE_DISCONNECTED:
      node->ctx.cmd = redis_nginx_open_context(&cp->hostname, cp->port, node); //always connect the cmd ctx to the hostname
      if(node->ctx.cmd == NULL) {
        return node_connector_fail(node, "failed to open redis async context for cmd");
      }
      else if(cp->peername.len == 0) { //don't know peername yet
        set_preallocated_peername(node->ctx.cmd, &cp->peername);
      }
      node->ctx.pubsub = redis_nginx_open_context(cp->peername.len > 0 ? &cp->peername : &cp->hostname, cp->port, node); //try to connect to peername if possible
      if(node->ctx.pubsub == NULL) {
        return node_connector_fail(node, "failed to open redis async context for pubsub");
      }
      //connection established. move on...   
      node->state = REDIS_NODE_CONNECTED;
      // intentional fallthrough IS INTENTIONAL!
    
    case REDIS_NODE_CONNECTED:
      //now we need to authenticate maybe?
      if(cp->password.len > 0) {
        redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "AUTH %b", STR(&cp->password));
        node->state++;
      }
      else {
        node->state = REDIS_NODE_AUTHENTICATED;
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
    
    case REDIS_NODE_AUTHENTICATED:
      if(cp->db > 0) {
        redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "SELECT %d", cp->db);
        node->state++;
      }
      else {
        node->state = REDIS_NODE_DB_SELECTED;
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
    
    case REDIS_NODE_DB_SELECTED:
      //getinfo time
      redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "INFO all");
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
        size_t                    i, n;
        redis_connect_params_t   *rcp;
        node_set_role(node, REDIS_NODE_ROLE_MASTER);
        if(!(rcp = parse_info_slaves(node, reply->str, &n))) {
          return node_connector_fail(node, "failed parsing slaves from INFO");
        }
        for(i=0; i<n; i++) {
          node_discover_slave(node, &rcp[i]);
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
        node->state = REDIS_NODE_GET_CLUSTERINFO;
        return node_connector_callback(NULL, NULL, node);
      }
      else {
        //we're done!
        node->state = REDIS_NODE_SCRIPTS_LOAD;
        return node_connector_callback(NULL, NULL, node);
      }
      break;
    
    case REDIS_NODE_GET_CLUSTERINFO:
      redisAsyncCommand(node->ctx.cmd, node_connector_callback, NULL, "CLUSTER INFO");
      node->state++;
      break;
    
    case REDIS_NODE_GETTING_CLUSTERINFO:
      if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "CLUSTER INFO command failed,");
      }
      if(!nchan_cstr_match_line(reply->str, "cluster_state:ok")) {
        node->cluster.ok=0;
        return node_connector_fail(node, "reports cluster_state not ok");
      }
      node->cluster.ok=1;
      node->state++;
      //Oi! 'Ave ya got a loicence for that fallthrough?
    
    case REDIS_NODE_GET_CLUSTER_NODES:
      redisAsyncCommand(node->ctx.cmd, node_connector_callback, NULL, "CLUSTER NODES");
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
              node->cluster.slot_range.range = ngx_alloc(sizeof(redis_slot_range_t) * node->cluster.slot_range.n, ngx_cycle->log);
              if(!node->cluster.slot_range.range) {
                return node_connector_fail(node, "failed allocating cluster slots range");
              }
              if(!parse_cluster_node_slots(&l[i], node->cluster.slot_range.range)) {
                return node_connector_fail(node, "failed parsing cluster slots range");
              }
              for(j = 0; j<node->cluster.slot_range.n; j++) {
                if((conflict_node = nodeset_node_find_by_range(node->nodeset, &node->cluster.slot_range.range[j]))!=NULL) {
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
      node->state = REDIS_NODE_SCRIPTS_LOAD;
      //inteonional fall-through is affirmatively consensual
      //yes, i consent to being fallen through.
      //                               Signed, 
      //                                 Redis_Node_script_Load
      //NOTE: consent required each time a fallthrough is imposed
    
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
        node_log_debug(node, "loaded script %s", next_script->name);
        node->scripts_loaded++;
        next_script++;
      }
      if(node->scripts_loaded < redis_lua_scripts_count) {
        //load next script
        redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "SCRIPT LOAD %s", next_script->script);
        return;
      }
      node_log_notice(node, "all scripts loaded!");
  }
}

ngx_int_t nodeset_set_status(redis_nodeset_t *nodeset, redis_nodeset_status_t status, const char *msg) {
  //TODO
  if(msg) {
    ngx_uint_t  lvl;
    if(status == REDIS_NODESET_INVALID)
      lvl = NGX_LOG_ERR;
    else if(status == REDIS_NODE_DISCONNECTED)
      lvl = NGX_LOG_WARN;
    else
      lvl = NGX_LOG_NOTICE;
    nchan_log(lvl, ngx_cycle->log, 0, "%s", msg);
  }
  //TODO: actual status-setting-stuff
  return NGX_OK;
}

ngx_int_t nodeset_check_status(redis_nodeset_t *nodeset) {
  redis_node_t *cur;
  int cluster = 0, masters = 0, slaves = 0, total = 0, connecting = 0, ready = 0, disconnected = 0;
  int discovered = 0;
  for(cur = nchan_list_first(&nodeset->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    total++;
    if(cur->cluster.enabled == 1) {
      cluster++;
    }
    if(cur->discovered)
      discovered++;
    if(cur->role == REDIS_NODE_ROLE_MASTER)
      masters++;
    if(cur->role == REDIS_NODE_ROLE_SLAVE)
      slaves++;
    if(cur->state <= REDIS_NODE_DISCONNECTED)
      disconnected++;
    if(cur->state > REDIS_NODE_DISCONNECTED && cur->state < REDIS_NODE_READY)
      connecting++;
    if(cur->state == REDIS_NODE_READY)
      ready++;
  }
  if(ready == total) {
    if(ready == 0) {
      nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "no reachable servers");
    }
    else if(cluster && cluster < total) {
      nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "cluster and non-cluster servers in set");
    }
    else if (cluster == 0 && masters > 1) {
      nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "more than one master servers in non-cluster set");
    }
    else if (cluster == 0 && masters == 0) {
      nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "no reachable master servers in set");
    }
    else {
      nodeset_set_status(nodeset, REDIS_NODESET_READY, NULL);
    } 
  }
  else if(ready == 0) {
    nodeset_set_status(nodeset, REDIS_NODESET_DISCONNECTED, NULL);
  }
  else if(ready < total) {
    nodeset_set_status(nodeset, REDIS_NODESET_CONNECTING, NULL);
  }
  return NGX_OK;
}

ngx_int_t nodeset_connect_all(void) {
  int                      i;
  ngx_str_t              **url;
  redis_connect_params_t   rcp;
  redis_nodeset_t         *ns;
  redis_node_t            *node;
  DBG("connect all");
  for(i=0; i<redis_nodeset_count; i++) {
    ns = &redis_nodeset[i];
    for(url = nchan_list_first(&ns->urls); url != NULL; url = nchan_list_next(url)) {
      parse_redis_url(*url, &rcp);
      if((node = nodeset_node_find_by_connect_params(ns, &rcp)) == NULL) {
        node = nodeset_node_create(ns, &rcp);
        node_log_debug(node, "created");
      }
      assert(node);
      if(node->state <= REDIS_NODE_DISCONNECTED) {
        node_log_debug(node, "start connecting");
        node_connector_callback(NULL, NULL, node);
      }
    }
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
