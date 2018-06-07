#include "redis_nodeset.h"

#include <assert.h>
#include "store-private.h"
#include "store.h"
#include "redis_nginx_adapter.h"

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "REDIS NODESET: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS NODESET: " fmt, ##args)

#define node_log(node, lvl, fmt, args...) \
  ngx_log_error(lvl, ngx_cycle->log, 0, "nchan: Redis node %V:%d " fmt, &node->connect_params.hostname, node->connect_params.port, ##args)
#define node_log_error(node, fmt, args...) \
  ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan: Redis node %V:%d " fmt, &node->connect_params.hostname, node->connect_params.port, ##args)
#define node_log_warning(node, fmt, args...)\
  ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "nchan: Redis node %V:%d " fmt, &node->connect_params.hostname, node->connect_params.port, ##args)
#define node_log_notice(node, fmt, args...) \
  ngx_log_error(NGX_LOG_NOTICE, ngx_cycle->log, 0, "nchan: Redis node %V:%d " fmt, &node->connect_params.hostname, node->connect_params.port, ##args)
#define node_log_info(node, fmt, args...) \
  ngx_log_error(NGX_LOG_INFO, ngx_cycle->log, 0, "nchan: Redis node %V:%d " fmt, &node->connect_params.hostname, node->connect_params.port, ##args)
  
}
static ngx_int_t node_log_warning(redis_node_t *node) {
  
}

static redis_nodeset_t redis_nodeset[NCHAN_MAX_NODESETS];
static int             redis_nodeset_count = 0;

static ngx_str_t       default_redis_url = ngx_string(NCHAN_REDIS_DEFAULT_URL);


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

typedef struct {
  redis_node_t    node;
  u_char          peername[INET6_ADDRSTRLEN + 2];
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

redis_node_t *nodeset_node_create(redis_nodeset_t *ns, redis_connect_params_t *rcp) {
  assert(!nodeset_node_find(ns, rcp));
  
  node_blob_t      *node_blob = nchan_list_append(&ns->nodes);
  redis_node_t     *node = &node_blob->node;
  
  assert((void *)node_blob == (void *)node);
  assert(node);
  assert(nodeset_node_find(ns, rcp));
  
  node->connect_params = *rcp;
  node->connect_params.peername.data = node_blob->peername;
  node->connect_params.peername.len = 0;
  node->nodeset = ns;
  ngx_memzero(&node->status, sizeof(node->status));
  
  node->peers.master = NULL;
  nchan_list_init(&node->peers.slaves, sizeof(redis_node_t *), "node slaves");
  
  node->ctx.cmd = NULL;
  node->ctx.pubsub = NULL;
  node->ctx.sync = NULL;
  return node;
}


static ngx_int_t set_preallocated_peername(redisAsyncContext *ctx, ngx_str_t *dst);

static void node_connector_fail(redis_node_t *node) {
  DBG("fail");
  node->state = REDIS_NODE_FAILED;
  if(node->ctx.cmd) {
    redisAsyncFree(node->ctx.cmd);
  }
  if(node->ctx.pubsub) {
    redisAsyncFree(node->ctx.pubsub);
  }
  if(node->ctx.sync) {
    redisFree(node->ctx.syncl
  }
  node->ctx.cmd = NULL;
  node->ctx.pubsub = NULL;
  node->ctx.sync = NULL;
}

static void node_connector_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply =               *reply = rep;
  redis_node_t               *node = *privdata;
  
  redis_connect_params_t     *cp = node->connect_params;
  redisAsyncContext          *ctx;

  switch(node->state) {
    case REDIS_NODE_FAILED:
    case REDIS_NODE_DISCONNECTED:
      node->ctx.cmd = redis_nginx_open_context(&cp->hostname, cp->port, node); //always connect the cmd ctx to the hostname
      if(node->ctx.cmd == NULL) {
        return node_connector_fail(node);
      }
      else if(cp->peername.len == 0) { //don't know peername yet
        set_preallocated_peername(node->ctx.cmd, &cp->peername);
      }
      node->ctx.pubsub = redis_nginx_open_context(cp->peername.len > 0 ? &cp->peername : &cp->hostname, cp->port, node); //try to connect to peername if possible
      if(node->ctx.pubsub == NULL) {
        return node_connector_fail(node);
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
      redisAsyncCommand(node->ctx.cmd, node_connector_callback, node, "INFO");
      node->state++;
    
    case REDIS_NODE_GETTING_INFO:
      if(reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return node_connector_fail(node, "Redis INFO command failed,");
      }
      if(ngx_strstrn((u_char *)reply->str, "loading:1", 8)) {
        return node_connector_fail("is busy loading data...");
        //TODO: retry later
      }
      if(/*cluster*/) {
        //do cluster stuff
      }
      else {
        if(/*master*/) {
          //get slaves
        }
        else {
          //get master
        }
      }
      
      
  }
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



