#include "redis_nodeset.h"

#include <assert.h>
#include "store-private.h"
#include <store/store_common.h>
#include "store.h"
#include "redis_nginx_adapter.h"

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "REDIS NODESET: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS NODESET: " fmt, ##args)

#define node_log(node, lvl, fmt, args...) \
  ngx_log_error(lvl, ngx_cycle->log, 0, "nchan: Redis node %V:%d " fmt, &node->connect_params.hostname, node->connect_params.port, ##args)
#define node_log_error(node, fmt, args...)    node_log(node, NGX_LOG_ERR, fmt, ##args)
#define node_log_warning(node, fmt, args...)  node_log(node, NGX_LOG_WARN, fmt, ##args)
#define node_log_notice(node, fmt, args...)   node_log(node, NGX_LOG_NOTICE, fmt, ##args)
#define node_log_info(node, fmt, args...)     node_log(node, NGX_LOG_INFO, fmt, ##args)
#define node_log_debug(node, fmt, args...)    node_log(node, NGX_LOG_DEBUG, fmt, ##args)

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

#define MAX_RUN_ID_LENGTH 63
#define MAX_CLUSTER_ID_LENGTH 63
typedef struct {
  redis_node_t    node;
  u_char          peername[INET6_ADDRSTRLEN + 2];
  u_char          run_id[MAX_RUN_ID_LENGTH+1];
  u_char          cluster_id[MAX_CLUSTER_ID_LENGTH+1];
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


static int redis_connect_params_equal(redis_connect_params_t *id1, redis_connect_params_t *id2) {
  if(id1->port != id2->port
   || id1->db != id2->db
   || id1->hostname.len != id2->hostname.len) {
    return 0;  
  }
  return ngx_strncmp(id1->hostname.data, id2->hostname.data, id1->hostname.len) == 0;
}

redis_node_t *nodeset_node_find(redis_nodeset_t *ns, redis_connect_params_t *rcp) {
  redis_node_t *cur;
  for(cur = nchan_list_first(&ns->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    if(redis_connect_params_equal(rcp, &cur->connect_params)) {
      return cur;
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

static int nodeset_node_deduplicate_by_run_id(redis_node_t *node) {
  redis_node_t   *cur;
  for(cur = nchan_list_first(&node->nodeset->nodes); cur != NULL; cur = nchan_list_next(cur)) {
    if(cur != node && nchan_ngx_str_match(&node->run_id, &cur->run_id)) {
      node_log_info(node, "deduplicated");
      node_transfer_slaves(node, cur); //node->cur
      nodeset_node_destroy(node);
      return 1;
    }
  }
  return 0;
}


redis_node_t *nodeset_node_create_with_space(redis_nodeset_t *ns, redis_connect_params_t *rcp, size_t extra_space, void **extraspace_ptr) {
  assert(!nodeset_node_find(ns, rcp));
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
  node->run_id.len = 0;
  node->run_id.data = node_blob->run_id;
  node->nodeset = ns;
  
  node->peers.master = NULL;
  nchan_list_init(&node->peers.slaves, sizeof(redis_node_t *), "node slaves");
  
  node->ctx.cmd = NULL;
  node->ctx.pubsub = NULL;
  node->ctx.sync = NULL;
  
  assert(nodeset_node_find(ns, rcp));
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

static void node_discover_slave(redis_node_t *master, redis_connect_params_t *rcp) {
  redis_node_t    *slave;
  if((slave = nodeset_node_find(master->nodeset, rcp))!= NULL) {
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

static void node_parseinfo_discover_slaves(redis_node_t *node, const char *info) {
  char                   slavebuf[20]="slave0:";
  int                    i = 0;
  redis_connect_params_t rcp;
  ngx_str_t              line;
  while(nchan_get_rest_of_line_in_cstr(info, slavebuf, &line)) {
    //ip=localhost,port=8537,state=online,offset=420,lag=1
    ngx_str_t hostname, port;
    nchan_scan_until_chr_on_line(&line, NULL,      '='); //ip=
    nchan_scan_until_chr_on_line(&line, &hostname, ','); //ip=([^,]*),
    nchan_scan_until_chr_on_line(&line, NULL,      '='); //port=
    nchan_scan_until_chr_on_line(&line, &port,     ','); //port=([^,]*),
    //don't care about the rest
    rcp.hostname = hostname;
    rcp.port = ngx_atoi(port.data, port.len);
    rcp.password = node->connect_params.password;
    rcp.peername.len = 0;
    rcp.db = node->connect_params.db;
    node_discover_slave(node, &rcp);
    //next slave
    i++;
    ngx_sprintf((u_char *)slavebuf, "slave%d:", i);    
  }
  
}

static void node_discover_master(redis_node_t  *slave, redis_connect_params_t *rcp) {
  redis_node_t *master;
  if ((master = nodeset_node_find(slave->nodeset, rcp)) != NULL) {
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

static void node_parseinfo_discover_master(redis_node_t *node, const char *info) {
  redis_connect_params_t    rcp;
  ngx_str_t                 port;
  if(!nchan_get_rest_of_line_in_cstr(info, "master_host:", &rcp.hostname)) {
    node_log_error(node, "failed to find master_host while discovering master");
    return;
  }
  
  if(!nchan_get_rest_of_line_in_cstr(info, "master_port:", &port)) {
    node_log_error(node, "failed to find master_port while discovering master");
    return;
  }
  rcp.port = ngx_atoi(port.data, port.len);
  if(rcp.port == NGX_ERROR) {
    node_log_error(node, "failed to parse master_port while discovering master");
    return;
  }
  rcp.db = node->connect_params.db;
  rcp.password = node->connect_params.password;

  rcp.peername.data = NULL;
  rcp.peername.len = 0;
  
  node_discover_master(node, &rcp);
}

static ngx_int_t set_preallocated_peername(redisAsyncContext *ctx, ngx_str_t *dst);

static void node_connector_fail(redis_node_t *node, const char *err) {
  node_log_error(node, "%s", err ? err : "failed to connect");
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
      if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "AUTH command failed, probably because the password is incorrect");
      }
      //now authenticate pubsub ctx
      redisAsyncCommand(node->ctx.pubsub, node_connector_callback, node, "AUTH %b", STR(&cp->password));
      node->state++;
      break;
    
    case REDIS_NODE_PUBSUB_AUTHENTICATING:
      if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
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
      if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "Redis INFO command failed,");
      }
      if(!node_parseinfo_set_run_id(node, reply->str)) {
        return node_connector_fail(node, "failed to set node run_id");
      }
      raise(SIGSTOP);
      if(nodeset_node_deduplicate_by_run_id(node)) {
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
        node_parseinfo_discover_slaves(node, reply->str);
      }
      else if(nchan_cstr_match_line(reply->str, "role:slave")) {
        node_set_role(node, REDIS_NODE_SLAVE);
        node_parseinfo_discover_master(node, reply->str);
      }
      else {
        return node_connector_fail(node, "can't tell if node is master or slave");
      }
      
      if(nchan_cstrmatch(reply->str, 1, "cluster_enabled:1")) {
        redisAsyncCommand(node->ctx.cmd, node_connector_callback, NULL, "CLUSTER INFO");
        node->state++;
      }
      else {
        //we're done!
        node->state = REDIS_NODE_READY;
        nodeset_check_status(node->nodeset);
      }

  }
}

ngx_int_t nodeset_set_status(redis_nodeset_t *nodeset, redis_nodeset_status_t status, const char *msg) {
  //TODO
  assert(0);
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
      nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "no reachable nodes");
    }
    else if(cluster && cluster < total) {
      nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "cluster and non-cluster nodes in set");
    }
    else if (cluster == 0 && masters > 1) {
      nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "more than one master node in non-cluster set");
    }
    else if (cluster == 0 && masters == 0) {
      nodeset_set_status(nodeset, REDIS_NODESET_INVALID, "no reachable master nodes in set");
    }
    else {
      nodeset_set_status(nodeset, REDIS_NODESET_READY, "no reachable master nodes in set");
    } 
  }
  else if(ready == 0) {
    nodeset_set_status(nodeset, REDIS_NODESET_DISCONNECTED, NULL);
  }
  else if(ready < total) {
    nodeset_set_status(nodeset, REDIS_NODESET_CONNECTING, NULL);
  }
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
      if((node = nodeset_node_find(ns, &rcp)) == NULL) {
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



