#include "redis_nodeset.h"

#include <assert.h>
#include "store-private.h"
#include <store/store_common.h>
#include "store.h"
#include "redis_nginx_adapter.h"
#include "redis_nodeset_parser.h"

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "REDIS NODESET: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS NODESET: " fmt, ##args)

static redis_nodeset_t redis_nodeset[NCHAN_MAX_NODESETS];
static int             redis_nodeset_count = 0;

static ngx_str_t       default_redis_url = ngx_string(NCHAN_REDIS_DEFAULT_URL);

static void node_connector_callback(redisAsyncContext *ac, void *rep, void *privdata);

static void *rbtree_cluster_hashslots_id(void *data) {
  return &((redis_cluster_keyslot_range_node_t *)data)->range;
}
static uint32_t rbtree_cluster_hashslots_bucketer(void *vid) {
  return 1; //no buckets
}
static ngx_int_t rbtree_cluster_hashslots_compare(void *v1, void *v2) {
  redis_cluster_slot_range_t   *r1 = v1;
  redis_cluster_slot_range_t   *r2 = v2;
  
  if(r2->max < r1->min) //r2 is strictly left of r1
    return -1;
  else if(r2->min > r1->max) //r2 is strictly right of r1
    return 1;
  else //there's an overlap
    return 0;
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
  rbtree_init(&ns->cluster.hashslots, "redis cluster node (by keyslot) data", rbtree_cluster_hashslots_id, rbtree_cluster_hashslots_bucketer, rbtree_cluster_hashslots_compare);
  
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
  unsigned       byref;
  int          (*match)(void *, void *);
} node_match_t;

static struct {
  node_match_t    run_id;
  node_match_t    cluster_id;
  node_match_t    connect_params;
} _node_match = {
  .run_id =          {"run_id",      offsetof(redis_node_t, run_id),          1, equal_nonzero_strings},
  .cluster_id =      {"cluster_id",  offsetof(redis_node_t, cluster.id),      1, equal_nonzero_strings},
  .connect_params =  {"url",         offsetof(redis_node_t, connect_params),  1, equal_redis_connect_params}
};

static int nodeset_node_deduplicate_by(redis_node_t *node, node_match_t *match) {
  redis_node_t   *cur;
  void *d1, *d2;
  d1 = &((char *)node)[match->offset];
  for(cur = nchan_list_first(&node->nodeset->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    d2 = &((char *)cur)[match->offset];
    if(cur != node && match->match(d1, d2)) {
      node_log_info(node, "deduplicated by %s", match->name);
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
  
  node->state = REDIS_NODE_DISCONNECTED;
  node->discovered = 0;
  node->connect_params = *rcp;
  node->connect_params.peername.data = node_blob->peername;
  node->connect_params.peername.len = 0;
  node->cluster.id.len = 0;
  node->cluster.id.data = node_blob->cluster_id;
  node->cluster.enabled = 0;
  node->cluster.ok = 0;
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
  node_set_role(node, REDIS_NODE_UNKNOWN); //removes from all peer lists
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

void node_discover_slave(redis_node_t *master, redis_connect_params_t *rcp) {
  redis_node_t    *slave;
  if((slave = nodeset_node_find_by_connect_params(master->nodeset, rcp))!= NULL) {
    //we know about it already
    assert(slave->role == REDIS_NODE_SLAVE);
    assert(slave->peers.master == master);
  }
  else {
    slave = nodeset_node_create_with_connect_params(master->nodeset, rcp);
    slave->discovered = 1;
    node_set_role(slave, REDIS_NODE_SLAVE);
  }
  node_set_master_node(slave, master); //this is idempotent
  node_add_slave_node(master, slave);  //so is this
  //try to connect
  if(slave->state <= REDIS_NODE_DISCONNECTED) {
    node_connector_callback(NULL, NULL, slave);
  }
}

void node_discover_master(redis_node_t  *slave, redis_connect_params_t *rcp) {
  redis_node_t *master;
  if ((master = nodeset_node_find_by_connect_params(slave->nodeset, rcp)) != NULL) {
    assert(master->role == REDIS_NODE_MASTER);
    assert(node_find_slave_node(master, slave));
  }
  else {
    master = nodeset_node_create_with_connect_params(slave->nodeset, rcp);
    master->discovered = 1;
    node_set_role(master, REDIS_NODE_MASTER);
  }
  node_set_master_node(slave, master);
  node_add_slave_node(master, slave);
  //try to connect
  if(master->state <= REDIS_NODE_DISCONNECTED) {
    node_connector_callback(NULL, NULL, master);
  }
}

static ngx_int_t set_preallocated_peername(redisAsyncContext *ctx, ngx_str_t *dst);

static void node_connector_fail(redis_node_t *node, const char *err) {
  assert(err);
  node_log_error(node, "connection failed: %s", err);
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
    case REDIS_NODE_UNKNOWN:
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
    
    case REDIS_NODE_MASTER:
      if(node->peers.master) {
        node_remove_peer(node->peers.master, node);
        node->peers.master = NULL;
      }
      break;
    
    case REDIS_NODE_SLAVE:
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
  return (reply != NULL && reply->type == REDIS_REPLY_ERROR && reply->type == REDIS_REPLY_STRING);
}
static int node_parseinfo_set_run_id(redis_node_t *node, const char *info) {
  return node_parseinfo_set_preallocd_str(node, &node->run_id, info, "run_id:", MAX_RUN_ID_LENGTH);
}

static void node_connector_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply                 *reply = rep;
  redis_node_t               *node = privdata;
  
  redis_connect_params_t     *cp = &node->connect_params;

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
      raise(SIGSTOP);
      if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "AUTH command failed, probably because the password is incorrect");
      }
      //now authenticate pubsub ctx
      redisAsyncCommand(node->ctx.pubsub, node_connector_callback, node, "AUTH %b", STR(&cp->password));
      node->state++;
      break;
    
    case REDIS_NODE_PUBSUB_AUTHENTICATING:
      if(node_connector_reply_str_ok(reply)) {
        return node_connector_fail(node, "AUTH command failed, probably because the password is incorrect");
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
        node_set_role(node, REDIS_NODE_MASTER);
        parse_info_discover_slaves(node, reply->str);
      }
      else if(nchan_cstr_match_line(reply->str, "role:slave")) {
        node_set_role(node, REDIS_NODE_SLAVE);
        parse_info_discover_master(node, reply->str);
      }
      else {
        return node_connector_fail(node, "can't tell if node is master or slave");
      }
      
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
        return node_connector_fail(node, "CLUSTER NODES command failed,");
      }
      if(!parse_cluster_nodes_discover_peers(node, reply->str)) {
        return node_connector_fail(node, "failed parsing CLUSTER NODES response");
      }
      node->state = REDIS_NODE_SCRIPTS_LOAD;
      //inteonional fall-through is affirmatively consensual
      //yes, i consent to being fallen through.
      //                               Signed, 
      //                                 Redis_Node_script_Load
      //NOTE: consent required each time a fallthrough is imposed
    
    case REDIS_NODE_SCRIPTS_LOAD:
      
      break;
    

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
    if(cur->role == REDIS_NODE_MASTER)
      masters++;
    if(cur->role == REDIS_NODE_SLAVE)
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
