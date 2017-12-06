#include <nchan_module.h>
#include <assert.h>

#include "redis_nginx_adapter.h"

#include <util/nchan_msg.h>
#include <util/nchan_rbtree.h>
#include <store/store_common.h>

#include "cluster.h"

//#define DBG(fmt, args...) ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "REDISTORE(CLUSTER): " fmt, ##args)
#define DBG(args...) //

#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDISTORE(CLUSTER): " fmt, ##args)


#define CLUSTER_NOTREADY_RETRY_TIME 1000

static rbtree_seed_t              redis_cluster_node_id_tree;
static nchan_list_t               redis_cluster_list;

typedef struct {
  ngx_str_t      line;
  ngx_str_t      id;         //node id
  ngx_str_t      address;    //address as known by redis
  ngx_str_t      flags;
  
  ngx_str_t      master_id;  //if slave
  ngx_str_t      ping_sent;
  ngx_str_t      pong_recv;
  ngx_str_t      config_epoch;
  ngx_str_t      link_state; //connected or disconnected
  
  ngx_str_t      slots;
  
  unsigned       connected:1;
  unsigned       master:1;
  unsigned       failed:1;
  unsigned       self:1;
} cluster_nodes_line_t;

static ngx_int_t index_rdata_by_cluster_node_id(rdstore_data_t *rdata, cluster_nodes_line_t *line);
static ngx_int_t unindex_rdata_by_cluster_node_id(rdstore_data_t *rdata);

static void redis_cluster_node_drop_keyslots(rdstore_data_t *rdata);
static void rdata_set_cluster_node_flags(rdstore_data_t *rdata, cluster_nodes_line_t *l);

static ngx_int_t associate_rdata_with_cluster(rdstore_data_t *rdata, redis_cluster_t *cluster);
static ngx_int_t dissociate_rdata_from_cluster(rdstore_data_t *rdata);

static void *redis_data_rbtree_node_cluster_id(void *data) {
  return &(*(rdstore_data_t **)data)->node.id;
}
void redis_cluster_init_postconfig(ngx_conf_t *cf) {
  rbtree_init(&redis_cluster_node_id_tree, "redis cluster node (by id) data", redis_data_rbtree_node_cluster_id, NULL, NULL);
  nchan_list_init(&redis_cluster_list, sizeof(redis_cluster_t), "clusters");
}

void redis_cluster_exit_worker(ngx_cycle_t *cycle) {
  nchan_list_el_t  *cur;
  redis_cluster_t  *cluster;
  for(cur = redis_cluster_list.head; cur != NULL; cur = cur->next) {
    cluster = nchan_list_data_from_el(cur);
    nchan_list_empty(&cluster->nodes.master);
    nchan_list_empty(&cluster->nodes.slave);
    nchan_list_empty(&cluster->nodes.disconnected);
    
    nchan_list_empty(&cluster->retry_commands);
    
    rbtree_empty(&cluster->hashslots, NULL, NULL);
    
    cluster->status = CLUSTER_NOTREADY;
    nchan_reaper_stop(&cluster->chanhead_reaper);
    if(cluster->still_notready_timer.timer_set) {
      ngx_del_timer(&cluster->still_notready_timer);
    }
    if(cluster->pool) {
      ngx_destroy_pool(cluster->pool);
    }
  }
  nchan_list_empty(&redis_cluster_list);
  rbtree_empty(&redis_cluster_node_id_tree, NULL, NULL);
}

static ngx_int_t cluster_set_status(redis_cluster_t *cluster, redis_cluster_status_t status);

static void redis_cluster_info_callback(redisAsyncContext *ac, void *rep, void *privdata);
static void redis_get_cluster_nodes_callback(redisAsyncContext *ac, void *rep, void *privdata);
static uint16_t redis_crc16(uint16_t crc, const char *buf, int len);

static void redis_check_if_cluster_ready_handler(ngx_event_t *ev) {
  rdstore_data_t   *rdata = ev->data;
  if(rdata->status != DISCONNECTED && rdata->ctx) {
    redis_get_cluster_info(rdata);
  }
  
  ngx_free(ev);
}

static void nchan_list_rdata_init(nchan_list_t *list, char *name) {
  nchan_list_init(list, sizeof(rdstore_data_t *), name);
}

static ngx_int_t nchan_cluster_nodes_list_rdata_remove(rdstore_data_t *rdata) {
  if(rdata->node.in_node_list) {
    assert(rdata->node.node_list_el_data && *rdata->node.node_list_el_data == rdata);
    nchan_list_remove(rdata->node.in_node_list, rdata->node.node_list_el_data);
    rdata->node.in_node_list = NULL;
    rdata->node.node_list_el_data = NULL;
    return NGX_OK;
  }
  return NGX_DECLINED;
}

#define nchan_list_rdata_from_el(cur) *(rdstore_data_t **)nchan_list_data_from_el(cur)


void redis_get_cluster_info(rdstore_data_t *rdata) {
  redisAsyncCommand(rdata->ctx, redis_cluster_info_callback, NULL, "CLUSTER INFO");
}

static void redis_cluster_info_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply        *reply = rep;
  rdstore_data_t    *rdata = ac->data;
  //uintptr_t          cluster_size = 0;
  //u_char            *cluster_size_start, *cluster_size_end;
  if(!redisReplyOk(ac, reply) || reply->type != REDIS_REPLY_STRING) {
    return;
  }
  
  //what's the cluster size (# of master nodes)
  /*
  if((cluster_size_start = ngx_strstrn((u_char *)reply->str, "cluster_size:", 12)) != 0) {
    cluster_size_start += 13;
    cluster_size_end = (u_char *)ngx_strchr(cluster_size_start, '\r');
    cluster_size = ngx_atoi(cluster_size_start, cluster_size_end - cluster_size_start);
  }
  */
  
  if(ngx_strstrn((u_char *)reply->str, "cluster_state:ok", 15)) {
    redis_get_cluster_nodes(rdata);
  }
  else {
    nchan_log_warning("Redis cluster not ready, says node %V", rdata->connect_url);
    ngx_event_t      *evt = ngx_calloc(sizeof(*evt), ngx_cycle->log);
    nchan_init_timer(evt, redis_check_if_cluster_ready_handler, rdata);
    //rdt_set_status(rdata, WAITING_FOR_CLUSTER_READY, ac);
    ngx_add_timer(evt, 1000);
  }
}


static void *rbtree_cluster_hashslots_id(void *data) {
  return &((redis_cluster_keyslot_range_node_t *)data)->range;
}
static uint32_t rbtree_cluster_hashslots_bucketer(void *vid) {
  return 1; //no buckets
}
static ngx_int_t rbtree_cluster_hashslots_compare(void *v1, void *v2) {
  redis_cluster_slot_range_t   *r1 = v1;
  redis_cluster_slot_range_t   *r2 = v2;
  
  if(r2->max < r1->min) {
    //r2 is strictly left of r1
    return -1;
  }
  else if(r2->min > r1->max) {
    //r2 is structly right of r1
    return 1;
  }
  else {
    //there's an overlap
    return 0;
  }
}


void redis_get_cluster_nodes(rdstore_data_t *rdata) {
  redisAsyncCommand(rdata->ctx, redis_get_cluster_nodes_callback, NULL, "CLUSTER NODES");
}

#define nchan_scan_str(str_src, cur, chr, str)\
  (str)->data = (u_char *)memchr(cur, chr, (str_src)->len - (cur - (str_src)->data));\
  if(!(str)->data)                            \
    (str)->data = (str_src)->data + (str_src)->len;\
  if((str)->data) {                           \
    (str)->len = (str)->data - cur;           \
    (str)->data = cur;                        \
    cur+=(str)->len+1;                        \
  }                                           \
  else                                        \
    goto fail

static rdstore_data_t *find_rdata_by_node_id(ngx_str_t *id) {
  ngx_rbtree_node_t   *rbtree_node;
  if((rbtree_node = rbtree_find_node(&redis_cluster_node_id_tree, id)) != NULL) {
    //do any other nodes already have the cluster set? if so, use that cluster struct.
    return *(rdstore_data_t **)rbtree_data_from_node(rbtree_node);
  }
  return NULL;
}

static char *redis_scan_cluster_nodes_line(char *line, cluster_nodes_line_t *l) {
  u_char     *cur = (u_char *)line;
  u_char     *max = cur;
  u_char     *tmp;
  ngx_str_t   rest_line;
  
  if(cur[0]=='\0')
    return NULL;
  
  nchan_scan_split_by_chr(&max, strlen((char *)max), &rest_line, '\n');
  l->line = rest_line;
  
  nchan_scan_until_chr_on_line(&rest_line, &l->id,           ' ');
  nchan_scan_until_chr_on_line(&rest_line, &l->address,      ' ');
  nchan_scan_until_chr_on_line(&rest_line, &l->flags,        ' ');
  
  nchan_scan_until_chr_on_line(&rest_line, &l->master_id,    ' ');
  nchan_scan_until_chr_on_line(&rest_line, &l->ping_sent,    ' ');
  nchan_scan_until_chr_on_line(&rest_line, &l->pong_recv,    ' ');
  nchan_scan_until_chr_on_line(&rest_line, &l->config_epoch, ' ');
  nchan_scan_until_chr_on_line(&rest_line, &l->link_state,   ' ');
  
  if(nchan_ngx_str_substr((&l->flags), "master")) {
    l->slots = rest_line;
    l->master = 1;
  }
  else {
    l->slots.data = NULL;
    l->slots.len = 0;
    l->master = 0;
  }
  l->failed = nchan_ngx_str_substr((&l->flags), "fail");
  l->self = nchan_ngx_str_substr((&l->flags), "myself") ? 1 : 0;
  
  l->connected = l->link_state.data[0]=='c' ? 1 : 0; //[c]onnected
  
  //redis >= 4.0 CLUSTER NODES format compatibility
  if((tmp = memrchr(l->address.data, '@', l->address.len)) != NULL) {
    l->address.len = tmp - l->address.data;
  }
  
  cur = max;
  if(&cur[-1] > (u_char *)line && cur[-1] == '\0')
    cur--;
  return (char *)cur;
}

static u_char *redis_scan_cluster_nodes_slots_string(ngx_str_t *str, u_char *cur, redis_cluster_slot_range_t *r) {
  ngx_str_t       slot_min_str, slot_max_str, slot;
  ngx_int_t       slot_min,     slot_max;
  u_char         *dash;
  
  if(cur == NULL) {
    cur = str->data;
  }
  else if(cur >= str->data + str->len) {
    return NULL;
  }
  if(str->len == 0) {
    return NULL;
  }
  
  nchan_scan_str(str, cur, ' ', &slot);
  if(slot.data[0] == '[') {
    //transitional special slot. ignore it.
    return redis_scan_cluster_nodes_slots_string(str, cur, r);
  }
  
  dash = (u_char *)memchr(slot.data, '-', slot.len);
  if(dash) {
    slot_min_str.data = slot.data;
    slot_min_str.len = dash - slot.data;
    
    slot_max_str.data = dash + 1;
    slot_max_str.len = slot.len - (slot_max_str.data - slot.data);
  }
  else {
    slot_min_str = slot;
    slot_max_str = slot;
  }
  
  slot_min = ngx_atoi(slot_min_str.data, slot_min_str.len);
  slot_max = ngx_atoi(slot_max_str.data, slot_max_str.len);
  
  DBG("slots: %i - %i", slot_min, slot_max);
  
  r->min = slot_min;
  r->max = slot_max;
  
  return cur;
  
fail:
  return NULL;
}

static void redis_cluster_discover_and_connect_to_missing_nodes(redisReply *reply, nchan_loc_conf_t *cf, redis_cluster_t *cluster) {
  char                  *line;
  redis_connect_params_t rcp;
  rdstore_data_t        *rdata;
  cluster_nodes_line_t   l;
  ngx_str_t             *url;
  
  DBG("discover new nodes");
  line = reply->str;
  while((line = redis_scan_cluster_nodes_line(line, &l)) != NULL) {
    if(l.master && !l.failed) {
      if((rdata = find_rdata_by_node_id(&l.id)) == NULL) {
        DBG("found a new node %V %V", &l.id, &l.address);
        url = ngx_palloc(ngx_cycle->pool, sizeof(*url) + l.address.len + 1); //TODO: pallocate from a more fitting pool
        url->data = (u_char *)&url[1];
        nchan_strcpy(url, &l.address, 0);
        url->data[url->len] = '\0';
        parse_redis_url(url, &rcp);
        rdata = redis_create_rdata(url, &rcp, &cf->redis, cf);
        index_rdata_by_cluster_node_id(rdata, &l);
      }
      assert(rdata);
      
      if(rdata->node.cluster && rdata->node.cluster != cluster) {
        dissociate_rdata_from_cluster(rdata);
      }
      
      if(!nchan_ngx_str_match(&rdata->node.slots, &l.slots)) {
        if(rdata->node.cluster) {
          redis_cluster_node_drop_keyslots(rdata);
        }
        
        unindex_rdata_by_cluster_node_id(rdata);
        index_rdata_by_cluster_node_id(rdata, &l);
      }
      
      rdata_set_cluster_node_flags(rdata, &l);
      
      associate_rdata_with_cluster(rdata, cluster);
      
      if(!rdata->node.failed && rdata->status != CONNECTED) {
        cluster->node_connections_pending++;
        redis_ensure_connected(rdata);
      }
    }
    else if(!l.master && !l.failed && (rdata = find_rdata_by_node_id(&l.id)) != NULL) {
      if(rdata->node.master) {
        //master got downgraded to slave
        rdata_set_cluster_node_flags(rdata, &l);
      }
    }
  }
}

static rdstore_data_t *get_any_connected_cluster_node(redis_cluster_t *cluster) {
  rdstore_data_t     *rdata;
  nchan_list_el_t    *cur;
  nchan_list_el_t   *node_head[] = {cluster->nodes.master.head, cluster->nodes.slave.head};
  int                 i;
  
  for(i = 0; i < 2; i++) {
    for(cur = node_head[i]; cur != NULL; cur = cur->next) {
      rdata = nchan_list_rdata_from_el(cur);
      if(rdata->status == CONNECTED) {
        return rdata;
      }
    }
  }
  
  return NULL;
}

static void cluster_not_ready_timer_handler(ngx_event_t *ev) {
  redis_cluster_t               *cluster = ev->data;
  rdstore_data_t                *rdata;
  
  if(ngx_exiting || ngx_quit) {
    return;
  }
  
  if(ev->timedout) {
    ev->timedout=0;
    if((rdata = get_any_connected_cluster_node(cluster)) != NULL) {
      cluster->node_connections_pending = 0; //enough waiting. assume no more pending connections
      redis_get_cluster_nodes(rdata);
    }
    else {
      nchan_log_warning("No connected Redis cluster nodes. Wait until a connection can be established to at least one...");
    }
  }
  
  ngx_add_timer(ev, CLUSTER_NOTREADY_RETRY_TIME);
}

static ngx_int_t index_rdata_by_cluster_node_id(rdstore_data_t *rdata, cluster_nodes_line_t *line) {
  struct {
    rdstore_data_t   *rdata;
    u_char            chr;
  } *rdata_ptr_and_buf;
  
  ngx_rbtree_node_t      *rbtree_node;
  
  if(rdata->node.indexed)
    return NGX_OK;
  
  if((rbtree_node = rbtree_create_node(&redis_cluster_node_id_tree, sizeof(*rdata) + line->id.len + line->address.len + line->slots.len)) == NULL) {
    ERR("can't create rbtree node for redis connection");
    return NGX_ERROR;
  }
  
  rdata_ptr_and_buf = rbtree_data_from_node(rbtree_node);
  rdata_ptr_and_buf->rdata = rdata;
  
  rdata->node.id.data = &rdata_ptr_and_buf->chr;
  nchan_strcpy(&rdata->node.id, &line->id, 0);
  
  rdata->node.address.data = &rdata_ptr_and_buf->chr + line->id.len;
  nchan_strcpy(&rdata->node.address, &line->address, 0);
  
  rdata->node.slots.data = &rdata_ptr_and_buf->chr + line->id.len + line->address.len;
  nchan_strcpy(&rdata->node.slots, &line->slots, 0);
  
  if(rbtree_insert_node(&redis_cluster_node_id_tree, rbtree_node) != NGX_OK) {
    ERR("couldn't insert redis cluster node ");
    rbtree_destroy_node(&redis_cluster_node_id_tree, rbtree_node);
    return NGX_ERROR;
  }
  
  rdata->node.indexed = 1;
  
  return NGX_OK;
}

static ngx_int_t unindex_rdata_by_cluster_node_id(rdstore_data_t *rdata) {
  ngx_rbtree_node_t    *node = rbtree_find_node(&redis_cluster_node_id_tree, &rdata->node.id);
  if(node) {
    rbtree_remove_node(&redis_cluster_node_id_tree, node);
    rbtree_destroy_node(&redis_cluster_node_id_tree, node);
    ngx_memzero(&rdata->node.id, sizeof(rdata->node.id));
    ngx_memzero(&rdata->node.address, sizeof(rdata->node.address));
    ngx_memzero(&rdata->node.slots, sizeof(rdata->node.slots));
    rdata->node.indexed = 0;
  }
  return NGX_OK;
}


#define script_nonlocal_key_error "Lua script attempted to access a non local key in a cluster node"

int clusterKeySlotOk(redisAsyncContext *c, void *r) {
  redisReply *reply = (redisReply *)r;
  if(reply && reply->type == REDIS_REPLY_ERROR) {
    char    *script_error_start = "ERR Error running script";
    char    *command_move_error = "MOVED ";
    char    *command_ask_error = "ASK ";
    
    if((nchan_cstr_startswith(reply->str, script_error_start) && nchan_strstrn(reply->str, script_nonlocal_key_error))
     || nchan_cstr_startswith(reply->str, command_move_error)
     || nchan_cstr_startswith(reply->str, command_ask_error)) {
      rdstore_data_t  *rdata = c->data;
      
      if(rdata->node.cluster == NULL) {
        ERR("got a cluster error on a non-cluster redis connection: %s", reply->str);
      }
      else {
        rbtree_empty(&rdata->node.cluster->hashslots, NULL, NULL);
        cluster_set_status(rdata->node.cluster, CLUSTER_NOTREADY);
      }
      return 0;
    }
    else
      return 1;
  }
  return 1;
}

void *cluster_retry_palloc(redis_cluster_t *cluster, size_t sz) {
  return ngx_palloc(nchan_list_get_pool(&cluster->retry_commands), sz);
}
void  cluster_retry_pfree(redis_cluster_t *cluster, void *ptr) {
  ngx_pfree(cluster->retry_commands.pool, ptr);
}

static redis_cluster_retry_t *cluster_create_retry_command(redis_cluster_t *cluster, void (*handler)(rdstore_data_t *, void *), void *pd) {
  redis_cluster_retry_t *retry;
  if((retry = nchan_list_append(&cluster->retry_commands)) == NULL)
    return NULL;
  retry->retry = handler;
  retry->data = pd;
  return retry;
}

ngx_int_t cluster_add_retry_command_with_chanhead(rdstore_channel_head_t *chanhead, void (*handler)(rdstore_data_t *, void *), void *pd) {
  redis_cluster_retry_t         *retry;
  if((retry = cluster_create_retry_command(chanhead->rdt->node.cluster, handler, pd)) == NULL)
    return NGX_ERROR;
  
  retry->type = CLUSTER_RETRY_BY_CHANHEAD;
  retry->chanhead = chanhead;
  chanhead->reserved++;
  
  return NGX_OK;
}

ngx_int_t cluster_add_retry_command_with_key(redis_cluster_t *cluster, ngx_str_t *key, void (*handler)(rdstore_data_t *, void *pd), void *pd) {
  redis_cluster_retry_t         *retry;
  if((retry = cluster_create_retry_command(cluster, handler, pd)) == NULL)
    return NGX_ERROR;
  
  retry->type = CLUSTER_RETRY_BY_KEY;
  retry->str.data = cluster_retry_palloc(cluster, key->len);
  nchan_strcpy(&retry->str, key, 0);
  
  return NGX_OK;
}

ngx_int_t cluster_add_retry_command_with_channel_id(redis_cluster_t *cluster, ngx_str_t *chid, void (*handler)(rdstore_data_t *, void *pd), void *pd) {
  redis_cluster_retry_t         *retry;
  if((retry = cluster_create_retry_command(cluster, handler, pd)) == NULL)
    return NGX_ERROR;
  
  retry->type = CLUSTER_RETRY_BY_CHANNEL_ID;
  retry->str.data = cluster_retry_palloc(cluster, chid->len);
  nchan_strcpy(&retry->str, chid, 0);
  
  return NGX_OK;
}

ngx_int_t cluster_add_retry_command_with_cstr(redis_cluster_t *cluster, u_char *cstr, void (*handler)(rdstore_data_t *, void *pd), void *pd) {
  redis_cluster_retry_t         *retry;
  size_t                         str_sz = strlen((char *)cstr);
  
  if((retry = cluster_create_retry_command(cluster, handler, pd)) == NULL)
    return NGX_ERROR;
  
  retry->type = CLUSTER_RETRY_BY_CSTR;
  retry->cstr = cluster_retry_palloc(cluster, str_sz + 1);
  strcpy((char *)retry->cstr, (char *)cstr);
  
  return NGX_OK;
}

static void retry_commands_traverse_callback(void *data, void *pd) {
  redis_cluster_t         *cluster = pd;
  redis_cluster_retry_t   *retry = data;
  rdstore_data_t          *rdata = NULL;
  rdstore_data_t          *any_rdata = get_any_connected_cluster_node(cluster);
  switch(retry->type) {
    case CLUSTER_RETRY_BY_CHANHEAD:
      retry->chanhead->reserved--;
      rdata = redis_cluster_rdata_from_channel(retry->chanhead);
      break;
    
    case CLUSTER_RETRY_BY_CHANNEL_ID:
      rdata = redis_cluster_rdata_from_channel_id(any_rdata, &retry->str);
      break;
      
    case CLUSTER_RETRY_BY_KEY:
      rdata = redis_cluster_rdata_from_key(any_rdata, &retry->str);
      break;
      
    case CLUSTER_RETRY_BY_CSTR:
      rdata = redis_cluster_rdata_from_cstr(any_rdata, retry->cstr);
      break;
    
  }
  retry->retry(rdata, retry->data);
}

static ngx_int_t cluster_run_retry_commands(redis_cluster_t *cluster) {
  nchan_list_traverse_and_empty(&cluster->retry_commands, retry_commands_traverse_callback, cluster);
  return NGX_OK;
}

static void retry_commands_traverse_abort_callback(void *data, void *pd) {
  redis_cluster_retry_t   *retry = data;
  if(retry->type == CLUSTER_RETRY_BY_CHANHEAD) {
    retry->chanhead->reserved--;
  }
  retry->retry(NULL, retry->data);
}

static ngx_int_t cluster_abort_retry_commands(redis_cluster_t *cluster) {
  nchan_list_traverse_and_empty(&cluster->retry_commands, retry_commands_traverse_abort_callback, cluster);
  return NGX_OK;
}


static redis_cluster_t *create_cluster_data(rdstore_data_t *node_rdata, int num_master_nodes, uint32_t homebrew_cluster_id, int configured_unverified_nodes) {
  redis_cluster_t               *cluster = NULL;
  size_t                         reaper_name_len = strlen("redis channel (cluster orphans) ()   ") + 60; //whatever
  char                          *reaper_name;
  
  if((cluster = nchan_list_append_sized(&redis_cluster_list, sizeof(*cluster) + reaper_name_len )) == NULL) { //TODO: don't allocate from heap, use a pool or something
    ERR("can't allocate cluster data");
    return NULL;
  }
  ngx_memzero(cluster, sizeof(*cluster));
  
  rbtree_init(&cluster->hashslots, "redis cluster node (by id) data", rbtree_cluster_hashslots_id, rbtree_cluster_hashslots_bucketer, rbtree_cluster_hashslots_compare);
  
  cluster->size = num_master_nodes;
  cluster->uscf = node_rdata->lcf->redis.upstream;
  cluster->pool = NULL;
  cluster->homebrew_id = homebrew_cluster_id;
  nchan_list_rdata_init(&cluster->nodes.master, "connected master nodes");
  nchan_list_rdata_init(&cluster->nodes.slave, "connected slave nodes");
  nchan_list_rdata_init(&cluster->nodes.disconnected, "disconnected nodes");
  //nchan_list_rdata_init(&cluster->failed_nodes, "failed nodes");
  
  nchan_list_pool_init(&cluster->retry_commands, sizeof(redis_cluster_retry_t), NGX_DEFAULT_POOL_SIZE, "retry commands");
  
  nchan_init_timer(&cluster->still_notready_timer, cluster_not_ready_timer_handler, cluster);
  ngx_add_timer(&cluster->still_notready_timer, CLUSTER_NOTREADY_RETRY_TIME);
  
  cluster->node_connections_pending = configured_unverified_nodes;
  
  reaper_name = (char *)&cluster[1];
  ngx_sprintf((u_char *)reaper_name, "redis channel (cluster orphans) (%p hid=%i)%Z", cluster, cluster->homebrew_id);
  
  rdstore_initialize_chanhead_reaper(&cluster->chanhead_reaper, reaper_name);
  return cluster;
}
/*
static void print_cluster_slots(redis_cluster_t *cluster) {
  ngx_rbtree_node_t                   *node;
  redis_cluster_slot_range_t           range = {0, 0};
  redis_cluster_keyslot_range_node_t  *keyslot_tree_node;
  
  while(range.min <= 16383) {
    if((node = rbtree_find_node(&cluster->hashslots, &range)) == NULL) {
      DBG("cluster slots range incomplete: can't find slot %i", range.min);
      return;
    }
    keyslot_tree_node = rbtree_data_from_node(node);
    
    if(keyslot_tree_node->rdata->status != CONNECTED) {
      DBG("cluster node for range %i - %i not connected", keyslot_tree_node->range.min, keyslot_tree_node->range.max);
      return;
    }
    
    DBG("%p %V : range %i - %i", keyslot_tree_node->rdata, keyslot_tree_node->rdata->connect_url, keyslot_tree_node->range.min, keyslot_tree_node->range.max);
    range.min = keyslot_tree_node->range.max + 1;
    range.max = range.min;
  }
  DBG("cluster range complete");
}
*/
static int check_cluster_slots_range_ok(redis_cluster_t *cluster) {
  ngx_rbtree_node_t                   *node;
  redis_cluster_slot_range_t           range = {0, 0};
  redis_cluster_keyslot_range_node_t  *keyslot_tree_node;
  
  while(range.min <= 16383) {
    if((node = rbtree_find_node(&cluster->hashslots, &range)) == NULL) {
      DBG("cluster slots range incomplete: can't find slot %i", range.min);
      return 0;
    }
    keyslot_tree_node = rbtree_data_from_node(node);
    
    if(keyslot_tree_node->rdata->status != CONNECTED) {
      DBG("cluster node for range %i - %i not connected", keyslot_tree_node->range.min, keyslot_tree_node->range.max);
      return 0;
    }
    
    range.min = keyslot_tree_node->range.max + 1;
    range.max = range.min;
  }
  DBG("cluster range complete");
  //print_cluster_slots(cluster);
  return 1;
}

static rdstore_data_t  **nchan_list_rdata_append(nchan_list_t *list, rdstore_data_t *rdata) {
  rdstore_data_t   **ptr_rdata;
  assert(rdata);
  ptr_rdata = nchan_list_append(list);
  *ptr_rdata = rdata;
  return ptr_rdata;
}

static ngx_int_t update_rdata_cluster_node_lists(rdstore_data_t *rdata) {
  nchan_list_t      *list;
  rdstore_data_t   **ptr_rdata;
  redis_cluster_t   *cluster = rdata->node.cluster;
  
  if(!cluster) {
    nchan_cluster_nodes_list_rdata_remove(rdata);
    return NGX_DECLINED;
  }
  
  if(rdata->status != CONNECTED)
    list = &cluster->nodes.disconnected;
  else if(rdata->node.master)
    list = &cluster->nodes.master;
  else
    list = &cluster->nodes.slave;
  
  if(rdata->node.in_node_list != list) {
    nchan_cluster_nodes_list_rdata_remove(rdata);
    
    assert(rdata->node.in_node_list == NULL);
    assert(rdata->node.node_list_el_data == NULL);
    
    ptr_rdata = nchan_list_rdata_append(list, rdata);
    
    rdata->node.in_node_list = list;
    rdata->node.node_list_el_data = ptr_rdata;
  }
  return NGX_OK;
}


static ngx_int_t associate_rdata_with_cluster(rdstore_data_t *rdata, redis_cluster_t *cluster) {
  u_char                              *cur;
  redis_cluster_slot_range_t           range;
  ngx_rbtree_node_t                   *rbtree_node;
  redis_cluster_keyslot_range_node_t  *keyslot_tree_node;
  
  if(!rdata->node.cluster) {
    rdata->node.cluster = cluster;
  }
  
  update_rdata_cluster_node_lists(rdata);
  
  if(rdata->node.master) {
    //hash slots
    cur = NULL;
    while((cur = redis_scan_cluster_nodes_slots_string(&rdata->node.slots, cur, &range)) != NULL) {
      
      rbtree_node = rbtree_find_node(&cluster->hashslots, &range);
      if(rbtree_node) {
        keyslot_tree_node = rbtree_data_from_node(rbtree_node);
        if(keyslot_tree_node->rdata == rdata && keyslot_tree_node->range.min == range.min && keyslot_tree_node->range.max == range.max) {
          DBG("cluster node keyslot range already exists. no problem here.");
        }
        else {
          DBG("this slot range is a little different. get rid of the old one");
          rbtree_remove_node(&cluster->hashslots, rbtree_node);
          rbtree_destroy_node(&cluster->hashslots, rbtree_node);
          rbtree_node = NULL;
        }
      }
      
      if(rbtree_node == NULL) {
        rbtree_node = rbtree_create_node(&cluster->hashslots, sizeof(*keyslot_tree_node));
        keyslot_tree_node = rbtree_data_from_node(rbtree_node);
        keyslot_tree_node->range = range;
        keyslot_tree_node->rdata = rdata;
        
        if(rbtree_insert_node(&cluster->hashslots, rbtree_node) != NGX_OK) {
          ERR("couldn't insert redis cluster hashslots node ");
          rbtree_destroy_node(&cluster->hashslots, rbtree_node);
          assert(0);
        }
      }
    }
  }
  
  if(check_cluster_slots_range_ok(cluster)) {
    cluster_set_status(cluster, CLUSTER_READY);
  }
  return NGX_OK;
}

static void rdata_set_cluster_node_flags(rdstore_data_t *rdata, cluster_nodes_line_t *l) {
  if(l->failed != rdata->node.failed) {
    rdata->node.failed = l->failed;
  }
  
  if(l->master != rdata->node.master) {
    if(rdata->node.cluster && rdata->node.master)
      redis_cluster_node_drop_keyslots(rdata);
    
    rdata->node.master = l->master;
  }
  
  update_rdata_cluster_node_lists(rdata);
}

static void redis_get_cluster_nodes_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply                    *reply = rep;
  rdstore_data_t                *rdata = ac->data;
  rdstore_data_t                *found_rdata = NULL, *unassoc_rdata, *my_rdata = NULL; //cluster tree node
  redis_cluster_t               *cluster = NULL;
  ngx_uint_t                     num_master_nodes = 0;
  uint32_t                       homebrew_cluster_id = 0;
  int                            configured_unverified_nodes = 0;
  
  nchan_list_t                   unassociated_nodes;
  nchan_list_el_t               *cur;
  nchan_list_rdata_init(&unassociated_nodes, "unassociated nodes");
  
  
  nchan_loc_conf_t              *cf = rdata->lcf;
  
  DBG("redis_get_cluster_nodes_callback for %p", rdata);
  
  if(cf->redis.upstream) {
    configured_unverified_nodes = cf->redis.upstream->servers->nelts;
  }
  else {
    assert(0);
  }
  
  if(!redisReplyOk(ac, reply) || reply->type != REDIS_REPLY_STRING) {
    return;
  }
  
  char                 *line;
  cluster_nodes_line_t  l;
  
  line = reply->str;
  DBG("\n%s", reply->str);
  while((line = redis_scan_cluster_nodes_line(line, &l)) != NULL) {
    found_rdata = NULL;
    
    if(l.master && !l.failed) {
      num_master_nodes++;
      homebrew_cluster_id += redis_crc16(0, (const char*)l.id.data, l.id.len);
    }
      
    if((found_rdata = find_rdata_by_node_id(&l.id)) == NULL) {
      redis_connect_params_t rcp;
      parse_redis_url(&l.address, &rcp);
      found_rdata = find_rdata_by_connect_params(&rcp);
    }
    
    if(found_rdata) {
      rdata_set_cluster_node_flags(found_rdata, &l);
      index_rdata_by_cluster_node_id(found_rdata, &l);
      if(found_rdata->node.cluster) {
        if(cluster)
          assert(cluster == found_rdata->node.cluster);
        else
          cluster = found_rdata->node.cluster;
      }
      else {
        nchan_list_rdata_append(&unassociated_nodes, found_rdata);
      }
      if(cluster)
        associate_rdata_with_cluster(found_rdata, cluster);
    }
    
    if(l.self) {
      //myself
      rdata_set_cluster_node_flags(rdata, &l);
      index_rdata_by_cluster_node_id(rdata, &l);
      if(found_rdata) {
        assert(found_rdata == rdata);
        DBG("%p %V %V already added to redis_cluster_node_id_tree... weird... how?...", rdata, &rdata->node.id, &rdata->node.id);
      }
      
      my_rdata = rdata;
    }
  }
  
  if(my_rdata) {
    if(!cluster) {
      cluster = create_cluster_data(my_rdata, num_master_nodes, homebrew_cluster_id, configured_unverified_nodes);
    }
    if(cluster->homebrew_id != homebrew_cluster_id && cluster->status != CLUSTER_READY) {
      DBG("Cluster homebrew_id changed maybe");
      cluster->homebrew_id = homebrew_cluster_id;
    }
    
    associate_rdata_with_cluster(my_rdata, cluster);
    
    if(cluster->node_connections_pending > 0) {
      cluster->node_connections_pending --;
    }
    if(cluster->node_connections_pending == 0 && cluster->nodes.master.n < cluster->size) {
      redis_cluster_discover_and_connect_to_missing_nodes(reply, cf, cluster);
    }
    rdata_set_status_flag(rdata, cluster_checked, 1);
  }
  else {
    DBG("my_rdata was blank... eh?...");
  }
  
  if(cluster) {
    for(cur = unassociated_nodes.head; cur != NULL; cur = cur->next) {
      unassoc_rdata = nchan_list_rdata_from_el(cur);
      associate_rdata_with_cluster(unassoc_rdata, cluster);
    }
  }
  nchan_list_empty(&unassociated_nodes);
  
}

static ngx_int_t cluster_set_status(redis_cluster_t *cluster, redis_cluster_status_t status) {
  redis_cluster_status_t     prev_status = cluster->status;
  rdstore_channel_head_t    *ch_cur;
  
  
  if(status == CLUSTER_READY && prev_status != CLUSTER_READY) {
    assert(check_cluster_slots_range_ok(cluster));
    while((ch_cur = cluster->orphan_channels_head) != NULL) {
      redis_chanhead_gc_withdraw(ch_cur);
      
      ensure_chanhead_pubsub_subscribed_if_needed(ch_cur);
      
      cluster->orphan_channels_head = ch_cur->rd_next;
      if(cluster->orphan_channels_head) {
        cluster->orphan_channels_head->rd_prev = NULL;
      }
      
      ch_cur->rd_prev = NULL;
      ch_cur->rd_next = NULL;
      assert(redis_cluster_associate_chanhead_with_rdata(ch_cur) == NGX_OK);
      redis_chanhead_catch_up_after_reconnect(ch_cur);
    }
    
    //stop any still running rdata reconnect timers
    rdstore_data_t    *rcur;
    nchan_list_el_t   *cur;
    nchan_list_t     **list;
    nchan_list_t      *lists[] = {&cluster->nodes.master, &cluster->nodes.slave, &cluster->nodes.disconnected, NULL};
    for(list = lists; *list != NULL; list++) {
      for(cur = (*list)->head; cur != NULL; cur = cur->next) {
        rcur = nchan_list_rdata_from_el(cur);
        if(rcur->reconnect_timer.timer_set) {
          ngx_del_timer(&rcur->reconnect_timer);
        }
      }
    }
    
    if(cluster->still_notready_timer.timer_set) {
      ngx_del_timer(&cluster->still_notready_timer);
    }
    
    cluster_run_retry_commands(cluster);
    
    nchan_log_notice("Redis cluster ready.");
    
  }
  else if(status != CLUSTER_READY && prev_status == CLUSTER_READY) {
    for(ch_cur = cluster->orphan_channels_head; ch_cur != NULL; ch_cur = ch_cur->rd_next) {
      if(ch_cur->in_gc_reaper) {
        redis_chanhead_gc_withdraw(ch_cur);
        redis_chanhead_gc_add_to_reaper(&cluster->chanhead_reaper, ch_cur, NCHAN_CHANHEAD_CLUSTER_ORPHAN_EXPIRE_SEC, "redis connection to cluster node gone"); //automatically added to cluster's gc
      }
    }
    
    if(!cluster->still_notready_timer.timer_set) {
      ngx_add_timer(&cluster->still_notready_timer, CLUSTER_NOTREADY_RETRY_TIME);
    }
    
  }
  
  else if(status == CLUSTER_FAILED && prev_status != CLUSTER_FAILED) {
    cluster_abort_retry_commands(cluster);
  }
  
  cluster->status = status;
  
  return NGX_OK;
}

static ngx_int_t rdata_make_chanheads_cluster_orphans(rdstore_data_t *rdata) {
  redis_cluster_t            *cluster = rdata->node.cluster;
  if(rdata->channels_head) {
    rdstore_channel_head_t  *cur, *last = NULL;
    for(cur = rdata->channels_head; cur != NULL; cur = cur->rd_next){
      last = cur;
      redis_chanhead_gc_withdraw(cur);
      cur->status = NOTREADY;
      cur->pubsub_status = UNSUBBED;
      cur->cluster.node_rdt = NULL;
    }
    if(last) {
      last->rd_next = cluster->orphan_channels_head;
      if(cluster->orphan_channels_head)
        cluster->orphan_channels_head->rd_prev = last;
      cluster->orphan_channels_head = last;
    }
    rdata->channels_head = NULL;
  }
  return NGX_OK;
}

ngx_int_t redis_cluster_node_change_status(rdstore_data_t *rdata, redis_connection_status_t status) {
  redis_connection_status_t   prev_status = rdata->status;
  redis_cluster_t            *cluster = rdata->node.cluster;
  
  if(status == CONNECTED && prev_status != CONNECTED) {
    cluster->nodes_connected++;
  }
  else if(status != CONNECTED && prev_status == CONNECTED) {
    
    cluster->nodes_connected--;
    //add to orphan chanheads list
    
    //wait to reconnect maybe?
    rdata_make_chanheads_cluster_orphans(rdata);
    
    cluster_set_status(cluster, CLUSTER_NOTREADY);
  }

  return NGX_OK; 
}

typedef struct {
  rdstore_data_t      *rdata;
  ngx_rbtree_node_t   *found;
} rdata_node_finder_data_t;

rbtree_walk_direction_t rdata_node_finder(rbtree_seed_t *seed, void *data, void *privdata) {
  redis_cluster_keyslot_range_node_t *d = data;
  rdata_node_finder_data_t           *pd = privdata;
  
  if(d->rdata == pd->rdata) {
    pd->found = rbtree_node_from_data(d);
    return RBTREE_WALK_STOP;
  }
  else {
    return RBTREE_WALK_LEFT_RIGHT;
  }
}

static void redis_cluster_node_drop_keyslots(rdstore_data_t *rdata) {
  rdata_node_finder_data_t   finder_data;
  redis_cluster_t           *cluster = rdata->node.cluster;
  
  //remove from hashslots. this is a little tricky, we walk the hashslots tree 
  // until we can's find this rdata
  
  finder_data.rdata = rdata;
  while(1) {
    finder_data.found = NULL;
    rbtree_conditional_walk(&cluster->hashslots, rdata_node_finder, &finder_data);
    if(finder_data.found != NULL) {
      DBG("destroyed node %p", finder_data.found);
      rbtree_remove_node(&cluster->hashslots, finder_data.found);
      rbtree_destroy_node(&cluster->hashslots, finder_data.found);
    }
    else {
      break;
    }
  }
  
  rdata_make_chanheads_cluster_orphans(rdata);
}


static ngx_int_t dissociate_rdata_from_cluster(rdstore_data_t *rdata) {
  assert(rdata->node.cluster != NULL);
  if((rdata->node.failed || !rdata->node.master) && rdata->reconnect_timer.timer_set) {
    ngx_del_timer(&rdata->reconnect_timer);
  }
  
  rdata_make_chanheads_cluster_orphans(rdata);
  
  redis_cluster_node_drop_keyslots(rdata);
  
  rdata->node.cluster = NULL;
  update_rdata_cluster_node_lists(rdata);
  return NGX_OK;
}



static uint16_t redis_crc16(uint16_t crc, const char *buf, int len);
static rdstore_data_t *redis_cluster_rdata_from_keyslot(rdstore_data_t *rdata, uint16_t slot);


ngx_int_t redis_cluster_associate_chanhead_with_rdata(rdstore_channel_head_t *ch) {
  if(redis_cluster_rdata_from_channel(ch)) {
    return NGX_OK;
  }
  else {
    return NGX_ERROR;
  }
}

rdstore_data_t *redis_cluster_rdata_from_channel(rdstore_channel_head_t *ch) {
  rdstore_data_t  *rdata;
  if(!ch->cluster.enabled) {
    return ch->rdt;
  }
  
  if(ch->cluster.node_rdt) {
    return ch->cluster.node_rdt;
  }
  
  rdata = redis_cluster_rdata_from_channel_id(ch->rdt, &ch->id);
  
  assert(ch->rd_prev == NULL);
  assert(ch->rd_next == NULL);
  
  if(rdata) {
    redis_associate_chanhead_with_rdata(ch, rdata);
  }
  else {
    redis_cluster_t   *cluster = ch->rdt->node.cluster;
    ch->rd_prev = NULL;
    
    if(cluster->orphan_channels_head) {
      cluster->orphan_channels_head = ch;
    }
    cluster->orphan_channels_head = ch;
  }
  
  ch->cluster.node_rdt = rdata;
  return rdata;
  
}

rdstore_data_t *redis_cluster_rdata_from_channel_id(rdstore_data_t *rdata, ngx_str_t *str){
  if(!rdata->node.cluster)
    return rdata;
  
  static uint16_t  prefix_crc = 0;
  if(prefix_crc == 0) {
    prefix_crc = redis_crc16(0, "channel:", 8);
  }
  uint16_t   slot = redis_crc16(prefix_crc, (const char *)str->data, str->len) % 16384;
  //DBG("channel id %V (key {channel:%V}) slot %i", str, str, slot);
  
  return redis_cluster_rdata_from_keyslot(rdata, slot);
}

rdstore_data_t *redis_cluster_rdata_from_key(rdstore_data_t *rdata, ngx_str_t *key) {
  char        *start, *end;
  ngx_str_t    hashable;
  
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
    return redis_cluster_rdata(rdata, &hashable);
  }
  return redis_cluster_rdata(rdata, key);
}

rdstore_data_t *redis_cluster_rdata(rdstore_data_t *rdata, ngx_str_t *str) {
  if(!rdata->node.cluster)
    return rdata;
  
  uint16_t   slot = redis_crc16(0, (const char *)str->data, str->len) % 16384;
  DBG("str %V slot %i", str, slot);
  
  return redis_cluster_rdata_from_keyslot(rdata, slot);
}

rdstore_data_t *redis_cluster_rdata_from_cstr(rdstore_data_t *rdata, u_char *str) {
  if(!rdata->node.cluster)
    return rdata;
  
  uint16_t   slot = redis_crc16(0, (const char *)str, strlen((char *)str)) % 16384;
  DBG("cstr %s slot %i", str, slot);
  
  return redis_cluster_rdata_from_keyslot(rdata, slot);
}

static rdstore_data_t *redis_cluster_rdata_from_keyslot(rdstore_data_t *rdata, uint16_t slot) {
  

  redis_cluster_slot_range_t           range = {slot, slot};
  ngx_rbtree_node_t                   *rbtree_node;
  redis_cluster_keyslot_range_node_t  *keyslot_tree_node;
  
  if((rbtree_node = rbtree_find_node(&rdata->node.cluster->hashslots, &range)) == NULL) {
    DBG("hashslot not found. what do?!");
    return NULL;
  }
  
  keyslot_tree_node = rbtree_data_from_node(rbtree_node);
  assert(keyslot_tree_node->range.min <= slot && keyslot_tree_node->range.max >= slot);
  return keyslot_tree_node->rdata;
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

static uint16_t redis_crc16(uint16_t crc, const char *buf, int len) {
    int counter;
    for (counter = 0; counter < len; counter++)
            crc = (crc<<8) ^ crc16tab[((crc>>8) ^ *buf++)&0x00FF];
    return crc;
}
