#ifndef NCHAN_REDIS_STORE_PRIVATE_H
#define NCHAN_REDIS_STORE_PRIVATE_H

#include <nchan_module.h>
#include "uthash.h"
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include <util/nchan_reaper.h>
#include <util/nchan_rbtree.h>
#include <util/nchan_list.h>
#include <store/spool.h>

typedef struct rdstore_data_s rdstore_data_t;
typedef struct rdstore_channel_head_s rdstore_channel_head_t;

typedef struct {
  rdstore_data_t          *node_rdt;
  unsigned                 enabled:1;
  //rdstore_channel_head_t  *next;
  //rdstore_channel_head_t  *prev;
} rdstore_channel_head_cluster_data_t;


struct rdstore_channel_head_s {
  ngx_str_t                    id; //channel id
  channel_spooler_t            spooler;
  ngx_uint_t                   generation; //subscriber pool generation.
  chanhead_pubsub_status_t     status;
  ngx_uint_t                   sub_count;
  ngx_int_t                    fetching_message_count;
  ngx_uint_t                   internal_sub_count;
  ngx_event_t                  keepalive_timer;
  nchan_msg_id_t               last_msgid;
  
  void                        *redis_subscriber_privdata;
  rdstore_data_t              *rdt;
  rdstore_channel_head_cluster_data_t cluster;
  
  rdstore_channel_head_t      *gc_prev;
  rdstore_channel_head_t      *gc_next;
  
  time_t                       gc_time;
  unsigned                     in_gc_queue:1;
  
  unsigned                     meta:1;
  unsigned                     shutting_down:1;
  UT_hash_handle               hh;
};

typedef struct {
  ngx_str_t     host;
  ngx_int_t     port;
  ngx_str_t     password;
  ngx_int_t     db;
} redis_connect_params_t;

typedef struct callback_chain_s callback_chain_t;
struct callback_chain_s {
  callback_pt                  cb;
  void                        *pd;
  callback_chain_t            *next;
};

typedef enum {DISCONNECTED, CONNECTING, AUTHENTICATING, LOADING, LOADING_SCRIPTS, CONNECTED} redis_connection_status_t;

typedef struct {
  rbtree_seed_t                    hashslots; //cluster rbtree seed
  nchan_list_t                     nodes; //cluster rbtree seed
  ngx_uint_t                       size; //number of master nodes
  ngx_int_t                        node_connections_pending; //number of master nodes
  uint32_t                         homebrew_id;
  ngx_http_upstream_srv_conf_t    *uscf;
  ngx_pool_t                      *pool;
} redis_cluster_t;

typedef struct {
  ngx_str_t         id;
  ngx_str_t         address;
  ngx_str_t         slots;
  redis_cluster_t  *cluster;
} redis_cluster_node_t;

typedef struct {
  unsigned         min:16;
  unsigned         max:16;
} redis_cluster_slot_range_t;

typedef struct {
  redis_cluster_slot_range_t   range;
  rdstore_data_t              *rdata;
} redis_cluster_keyslot_range_node_t;

struct rdstore_data_s {
  ngx_str_t                       *connect_url;
  redis_connect_params_t           connect_params;
  
  redisAsyncContext               *ctx;
  redisAsyncContext               *sub_ctx;
  redisContext                    *sync_ctx;
  
  nchan_reaper_t                   chanhead_reaper;
  
  redis_connection_status_t        status;
  int                              scripts_loaded_count;
  int                              generation;
  ngx_event_t                      reconnect_timer;
  ngx_event_t                      ping_timer;
  time_t                           ping_interval;
  callback_chain_t                *on_connected;
  nchan_loc_conf_t                *lcf;
  
  //cluster stuff
  redis_cluster_node_t             node;
  
  unsigned                         shutting_down:1;
}; // rdstore_data_t

redis_connection_status_t redis_connection_status(nchan_loc_conf_t *cf);
void redisCheckErrorCallback(redisAsyncContext *c, void *r, void *privdata);

#endif //NCHAN_REDIS_STORE_PRIVATE_H
