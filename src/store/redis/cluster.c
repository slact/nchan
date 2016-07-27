#include <nchan_module.h>
#include <assert.h>

#include "redis_nginx_adapter.h"

#include <util/nchan_msgid.h>
#include <util/nchan_rbtree.h>
#include <store/store_common.h>

#include "cluster.h"

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "REDISTORE(CLUSTER): " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDISTORE(CLUSTER): " fmt, ##args)


static rbtree_seed_t              redis_cluster_node_id_tree;

static void *redis_data_rbtree_node_cluster_id(void *data) {
  return &(*(rdstore_data_t **)data)->node.id;
}
void redis_cluster_init_postconfig(ngx_conf_t *cf) {
  rbtree_init(&redis_cluster_node_id_tree, "redis cluster node (by id) data", redis_data_rbtree_node_cluster_id, NULL, NULL);
}

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



void redis_get_cluster_info(rdstore_data_t *rdata) {
  redisAsyncCommand(rdata->ctx, redis_cluster_info_callback, NULL, "CLUSTER INFO");
}

static void redis_cluster_info_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply        *reply = rep;
  rdstore_data_t    *rdata = ac->data;
  uintptr_t          cluster_size = 0;
  u_char            *cluster_size_start, *cluster_size_end;
    if(ac->err || !reply || reply->type != REDIS_REPLY_STRING) {
    redisCheckErrorCallback(ac, reply, privdata);
    return;
  }
  
  //what's the cluster size (# of master nodes)
  if((cluster_size_start = ngx_strstrn((u_char *)reply->str, "cluster_size:", 12)) != 0) {
    cluster_size_start += 13;
    cluster_size_end = (u_char *)ngx_strchr(cluster_size_start, '\r');
    cluster_size = ngx_atoi(cluster_size_start, cluster_size_end - cluster_size_start);
  }
  
  
  if(ngx_strstrn((u_char *)reply->str, "cluster_state:ok", 15)) {
    redis_get_cluster_nodes(rdata, cluster_size);
  }
  else {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "Nchan: Redis cluster not ready");
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





void redis_get_cluster_nodes(rdstore_data_t *rdata, uintptr_t cluster_size) {
  redisAsyncCommand(rdata->ctx, redis_get_cluster_nodes_callback, (void *)cluster_size, "CLUSTER NODES");
}


static void nchan_scan_nearest_chr(u_char **cur, ngx_str_t *str, ngx_int_t n, ...) {
  u_char    chr;
  va_list   args;
  u_char   *shortest = NULL;
  
  u_char *tmp_cur;
  
  ngx_int_t i;
  
  for(tmp_cur = *cur; shortest == NULL && (tmp_cur == *cur || tmp_cur[-1] != '\0'); tmp_cur++) {
    va_start(args, n);
    for(i=0; shortest == NULL && i<n; i++) {
      chr = (u_char )va_arg(args, int);
      if(*tmp_cur == chr) {
        shortest = tmp_cur;
      }
    }
    va_end(args);
  }
  if(shortest) {
    str->data = (u_char *)*cur;
    str->len = shortest - *cur;
    *cur = shortest + 1;
  }
  else {
    str->data = NULL;
    str->len = 0;
  }
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
  unsigned       self:1;
} cluster_nodes_line_t;
    
static char *redis_scan_cluster_nodes_line(char *line, cluster_nodes_line_t *l) {
  u_char     *cur = (u_char *)line;
  
  if(cur[0]=='\0')
    return NULL;
  
  nchan_scan_nearest_chr(&cur, &l->id,           1, ' ');
  nchan_scan_nearest_chr(&cur, &l->address,      1, ' ');
  nchan_scan_nearest_chr(&cur, &l->flags,        1, ' ');
  
  nchan_scan_nearest_chr(&cur, &l->master_id,    1, ' ');
  nchan_scan_nearest_chr(&cur, &l->ping_sent,    1, ' ');
  nchan_scan_nearest_chr(&cur, &l->pong_recv,    1, ' ');
  nchan_scan_nearest_chr(&cur, &l->config_epoch, 1, ' ');
  nchan_scan_nearest_chr(&cur, &l->link_state,   3, ' ', '\n', '\0');
  
  if(nchan_ngx_str_substr((&l->flags), "master")) {
    nchan_scan_nearest_chr(&cur, &l->slots, 2, '\n', '\0');
    l->master = 1;
  }
  else {
    l->slots.data = NULL;
    l->slots.len = 0;
    l->master = 0;
  }
  
  l->self = nchan_ngx_str_substr((&l->flags), "myself") ? 1 : 0;
  
  l->connected = l->link_state.data[0]=='c' ? 1 : 0; //[c]onnected
  
  l->line.data = (u_char *)line;
  l->line.len = cur - l->line.data;
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
    if(l.master && (rbtree_find_node(&redis_cluster_node_id_tree, &l.id) == NULL)) {
      DBG("found a node %V %V", &l.id, &l.address);
      url = ngx_palloc(ngx_cycle->pool, sizeof(*url) + l.address.len + 1); //TODO: pallocate in a more fitting pool
      url->data = (u_char *)&url[1];
      url->len = l.address.len;
      memcpy(url->data, l.address.data, l.address.len);
      url->data[url->len] = '\0';
      parse_redis_url(url, &rcp);
      rdata = redis_create_rdata(url, &rcp, &cf->redis, cf);
      cluster->node_connections_pending++;
      redis_ensure_connected(rdata);
    }
  }
  
}

static void redis_get_cluster_nodes_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply                    *reply = rep;
  rdstore_data_t                *rdata = ac->data;
  ngx_rbtree_node_t             *rbtree_node = NULL;
  rdstore_data_t                *ctnode = NULL, *my_ctnode = NULL; //cluster tree node
  redis_cluster_t               *cluster = NULL;
  ngx_uint_t                     num_master_nodes = 0;
  uintptr_t                      cluster_size = (uintptr_t )privdata;
  uint32_t                       homebrew_cluster_id = 0;
  int                            configured_unverified_nodes;
  
  nchan_loc_conf_t              *cf = rdata->lcf;
  
  if(cf->redis.upstream) {
    configured_unverified_nodes = cf->redis.upstream->servers->nelts;
  }
  else {
    assert(0);
  }
  
  if(ac->err || !reply || reply->type != REDIS_REPLY_STRING) {
    redisCheckErrorCallback(ac, reply, privdata);
    return;
  }
  
  char                 *line;
  cluster_nodes_line_t  l;
  
  line = reply->str;
  //DBG("\n%s", reply->str);
  while((line = redis_scan_cluster_nodes_line(line, &l)) != NULL) {
    
    if(l.master) {
      num_master_nodes++;
      homebrew_cluster_id += redis_crc16(0, (const char*)l.id.data, l.id.len);
    }
    
    if(!l.self) {
      if((rbtree_node = rbtree_find_node(&redis_cluster_node_id_tree, &l.id)) != NULL) {
        //do any other nodes already have the cluster set? if so, use that cluster struct.
        ctnode = *(rdstore_data_t **)rbtree_data_from_node(rbtree_node);
        if(ctnode->node.cluster) {
          if(cluster)
            assert(cluster == ctnode->node.cluster);
          else
            cluster = ctnode->node.cluster;
        }
      }
    }
    else if(l.master) {
      //myself and master!
      
      struct {
        rdstore_data_t   *rdata;
        u_char            chr;
      } *rdata_ptr_and_buf;
      
      if((rbtree_node = rbtree_find_node(&redis_cluster_node_id_tree, &l.id)) != NULL) {
        //node already known. what do?...
        rdstore_data_t *my_rdata;
        my_rdata = *(rdstore_data_t **)rbtree_data_from_node(rbtree_node);
        
        assert(my_rdata == rdata);
        my_ctnode = my_rdata;
        ERR("%p %V %V already added to redis_cluster_node_id_tree... weird... how?...", rdata, &rdata->node.id, &my_rdata->node.id);
        assert(0);
      }
      else {
        if((rbtree_node = rbtree_create_node(&redis_cluster_node_id_tree, sizeof(*rdata) + l.id.len + l.address.len + l.slots.len)) == NULL) {
          ERR("can't create rbtree node for redis connection");
          return;
        }
        
        rdata_ptr_and_buf = rbtree_data_from_node(rbtree_node);
        rdata_ptr_and_buf->rdata = rdata;
        
        rdata->node.id.data = &rdata_ptr_and_buf->chr;
        nchan_strcpy(&rdata->node.id, &l.id, 0);
        
        rdata->node.address.data = &rdata_ptr_and_buf->chr + l.id.len;
        nchan_strcpy(&rdata->node.address, &l.address, 0);
        
        rdata->node.slots.data = &rdata_ptr_and_buf->chr + l.id.len + l.address.len;
        nchan_strcpy(&rdata->node.slots, &l.slots, 0);
        
        if(rbtree_insert_node(&redis_cluster_node_id_tree, rbtree_node) != NGX_OK) {
          ERR("couldn't insert redis cluster node ");
          rbtree_destroy_node(&redis_cluster_node_id_tree, rbtree_node);
          assert(0);
        }
        
        my_ctnode = *(rdstore_data_t **)rbtree_data_from_node(rbtree_node);
      }
    }
    else {
      //don't care about slaves. disconnect!
      //TODO
      assert(0);
    }
  }
  
  if(my_ctnode) {
    rdstore_data_t                     **ptr_rdata;
    redis_cluster_slot_range_t           range;
    redis_cluster_keyslot_range_node_t  *keyslot_tree_node;
    u_char                              *cur;
    
    if(!cluster) {
      //cluster struct not made by any node yet. make it so!
      cluster = ngx_calloc(sizeof(*cluster), ngx_cycle->log); //TODO: don't allocate from heap, use a pool or something
      
      rbtree_init(&cluster->hashslots, "redis cluster node (by id) data", rbtree_cluster_hashslots_id, rbtree_cluster_hashslots_bucketer, rbtree_cluster_hashslots_compare);
      
      cluster->size = num_master_nodes;
      assert(num_master_nodes == cluster_size);
      cluster->uscf = rdata->lcf->redis.upstream;
      cluster->pool = NULL;
      cluster->homebrew_id = homebrew_cluster_id;
      nchan_list_init(&cluster->nodes, sizeof(rdstore_data_t *));
      
      cluster->node_connections_pending = configured_unverified_nodes;
      
      rdstore_initialize_chanhead_reaper(&cluster->chanhead_reaper, "redis channels (cluster orphans)");
      
    }
    rdata->node.cluster = cluster;
    ptr_rdata = nchan_list_append(&cluster->nodes);
    *ptr_rdata = rdata;
    
    //hash slots
    cur = NULL;
    while((cur = redis_scan_cluster_nodes_slots_string(&l.slots, cur, &range)) != NULL) {
      
      if((rbtree_node = rbtree_find_node(&cluster->hashslots, &range)) == NULL) {
        if((rbtree_node = rbtree_create_node(&cluster->hashslots, sizeof(*keyslot_tree_node))) == NULL) {
          assert(0);
        }
        keyslot_tree_node = rbtree_data_from_node(rbtree_node);
        keyslot_tree_node->range = range;
        keyslot_tree_node->rdata = rdata;
        
        
        if(rbtree_insert_node(&cluster->hashslots, rbtree_node) != NGX_OK) {
          ERR("couldn't insert redis cluster node ");
          rbtree_destroy_node(&cluster->hashslots, rbtree_node);
          assert(0);
        }
      }
      else {
        //overlapping range found! uh oh!
        assert(0);
      }
      
    }
    
    cluster->node_connections_pending --;
    if(cluster->node_connections_pending == 0 && cluster->nodes.n < cluster->size) {
      redis_cluster_discover_and_connect_to_missing_nodes(reply, cf, cluster);
    }
    
  }
  
}

static ngx_int_t cluster_change_status(redis_cluster_t *cluster, redis_cluster_status_t status) {
  redis_cluster_status_t     prev_status = cluster->status;
  rdstore_data_t            *node_rdata;
  rdstore_channel_head_t    *cur;
  
  if(status == CLUSTER_READY && prev_status != CLUSTER_READY) {
    while((cur = cluster->orphan_channels_head) != NULL) {
      node_rdata = redis_cluster_rdata_from_channel(cur);
      assert(node_rdata);
      redis_chanhead_gc_withdraw_from_reaper(&cluster->chanhead_reaper,  cur);
      
      cluster->orphan_channels_head = cur->rd_next;
      if(cluster->orphan_channels_head) {
        cluster->orphan_channels_head->rd_prev = NULL;
      }
      
      cur->rd_prev = NULL;
      cur->rd_next = NULL;
      redis_cluster_associate_chanhead_with_rdata(cur);
    }
  }
  else if(status != CLUSTER_READY && prev_status == CLUSTER_READY) {
    for(cur = cluster->orphan_channels_head; cur != NULL; cur = cur->rd_next) {
      if(!cur->in_gc_queue) {
        redis_chanhead_gc_add_to_reaper(&cluster->chanhead_reaper, cur, NCHAN_CHANHEAD_CLUSTER_ORPHAN_EXPIRE_SEC, "redis connection to cluster node gone"); //automatically added to cluster's gc
      }
    }
  }
  return NGX_OK;
}

ngx_int_t redis_cluster_node_change_status(rdstore_data_t *rdata, redis_connection_status_t status) {
  redis_connection_status_t   prev_status = rdata->status;
  redis_cluster_t            *cluster = rdata->node.cluster;
  rdstore_channel_head_t     *cur, *last = NULL;
  
  if(status == CONNECTED && prev_status != CONNECTED) {
    cluster->nodes_connected++;
    
    if(cluster->size == cluster->nodes_connected) {
      cluster_change_status(cluster, CLUSTER_READY);
    }
  }
  else if(status != CONNECTED && prev_status == CONNECTED) {    
    
    //add to orphan chanheads list
    
    //wait to reconnect maybe?
    for(cur = rdata->channels_head; cur != NULL; cur = cur->rd_next) {
      redis_chanhead_gc_withdraw(cur);
      last = cur;
    }
    
    if(rdata->node.cluster->orphan_channels_head) {
      rdata->node.cluster->orphan_channels_head->rd_prev = last;
    }
    if(last) {
      last->rd_next = rdata->node.cluster->orphan_channels_head;
    }
    rdata->node.cluster->orphan_channels_head = rdata->channels_head;
    
    rdata->channels_head = NULL;
    
    cluster_change_status(cluster, CLUSTER_NOTREADY);
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

void redis_cluster_drop_node(rdstore_data_t *rdata) {
  redis_cluster_t           *cluster = rdata->node.cluster;
  rdstore_data_t           **rdata_pptr;
  int                        found = 0;
  ngx_rbtree_node_t         *rbtree_node;
  nchan_list_el_t           *cur;
  rdata_node_finder_data_t   finder_data;
  
  if(!cluster) {
    ERR("not a cluster node");
    return;
  }
  
  ERR("drop cluster node for rdata %p", rdata);
  
  //remove from hashslots. this is a little tricky, we walk the hashslots tree 
  // until we can's find this rdata
  finder_data.rdata = rdata;
  while(1) {
    finder_data.found = NULL;
    rbtree_conditional_walk(&cluster->hashslots, rdata_node_finder, &finder_data);
    if(finder_data.found != NULL) {
      ERR("destroyed node %p", finder_data.found);
      rbtree_remove_node(&cluster->hashslots, finder_data.found);
      rbtree_destroy_node(&cluster->hashslots, finder_data.found);
    }
    else {
      break;
    }
  }
  
  assert(cluster->nodes.n > 0);
  
  for(cur = cluster->nodes.head; cur != NULL; cur = cur->next) {
    rdata_pptr = nchan_list_data_from_el(cur);
    if(*rdata_pptr == rdata) {
      found = 1;
      break;
    }
  }
  
  assert(found);
  nchan_list_remove(&cluster->nodes, rdata_pptr);
  
  if(cluster->nodes.n == 0) {
    ngx_free(cluster);
  }
  
  rbtree_node = rbtree_find_node(&redis_cluster_node_id_tree, &rdata->node.id);
  assert(rbtree_node);
  rbtree_remove_node(&redis_cluster_node_id_tree, rbtree_node);
  
  rbtree_destroy_node(&redis_cluster_node_id_tree, rbtree_node);
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
    ch->rd_prev = NULL;
    ch->rd_next = rdata->channels_head;
    if(rdata->channels_head) {
      rdata->channels_head->rd_prev = ch;
    }
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
  DBG("channel id %V (key {channel:%V}) slot %i", str, str, slot);
  
  return redis_cluster_rdata_from_keyslot(rdata, slot);
}

rdstore_data_t *redis_cluster_rdata(rdstore_data_t *rdata, ngx_str_t *str) {
  if(!rdata->node.cluster)
    return rdata;
  
  uint16_t   slot = redis_crc16(0, (const char *)str->data, str->len) % 16384;
  DBG("str %V slot %i", str, str, slot);
  
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
    ERR("hashslot not found. what do?!");
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
