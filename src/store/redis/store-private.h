#ifndef NCHAN_REDIS_STORE_PRIVATE_H
#define NCHAN_REDIS_STORE_PRIVATE_H

#define NCHAN_CHANHEAD_EXPIRE_SEC 1
#define NCHAN_CHANHEAD_CLUSTER_ORPHAN_EXPIRE_SEC 15
#define NCHAN_NOTICE_REDIS_CHANNEL_MESSAGE_BUFFER_SIZE_CHANGE 0xB00F

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
} rdstore_channel_head_cluster_data_t;


typedef enum {SUBBING, SUBBED, UNSUBBING, UNSUBBED} redis_pubsub_status_t;

struct rdstore_channel_head_s {
  ngx_str_t                    id; //channel id
  channel_spooler_t            spooler;
  ngx_uint_t                   generation; //subscriber pool generation.
  chanhead_pubsub_status_t     status;
  ngx_uint_t                   sub_count;
  ngx_int_t                    fetching_message_count;
  ngx_uint_t                   internal_sub_count;
  ngx_event_t                  keepalive_timer;
  ngx_uint_t                   keepalive_times_sent;
  nchan_msg_id_t               last_msgid;
  
  void                        *redis_subscriber_privdata;
  rdstore_data_t              *rdt;
  rdstore_channel_head_cluster_data_t cluster;
  
  ngx_int_t                    reserved;
  
  rdstore_channel_head_t      *gc_prev;
  rdstore_channel_head_t      *gc_next;
  
  rdstore_channel_head_t      *rd_next;
  rdstore_channel_head_t      *rd_prev;
  
  time_t                       gc_time;
  nchan_reaper_t              *in_gc_reaper;
  
  redis_pubsub_status_t        pubsub_status;
  unsigned                     meta:1;
  unsigned                     shutting_down:1;
  UT_hash_handle               hh;
};

typedef struct {
  ngx_str_t     hostname;
  ngx_str_t     peername; // resolved hostname (ip address)
  ngx_int_t     port;
  ngx_str_t     password;
  ngx_int_t     db;
} redis_connect_params_t;

typedef struct callback_chain_s callback_chain_t;
struct callback_chain_s {
  callback_pt                  cb;
  void                        *pd;
  ngx_event_t                  timeout_ev;
  rdstore_data_t              *rdata;
  callback_chain_t            *prev;
  callback_chain_t            *next;
};

typedef enum {DISCONNECTED, CONNECTING, AUTHENTICATING, LOADING, LOADING_SCRIPTS, CONNECTED} redis_connection_status_t;

typedef enum {CLUSTER_DISCONNECTED, CLUSTER_CONNECTING, CLUSTER_NOTREADY, CLUSTER_READY, CLUSTER_FAILED} redis_cluster_status_t;


typedef enum {CLUSTER_RETRY_BY_CHANHEAD, CLUSTER_RETRY_BY_CHANNEL_ID, CLUSTER_RETRY_BY_KEY, CLUSTER_RETRY_BY_CSTR} redis_cluster_retry_hashslot_t;
typedef struct redis_cluster_retry_s redis_cluster_retry_t;
struct redis_cluster_retry_s {
  redis_cluster_retry_hashslot_t     type;
  union {
    rdstore_channel_head_t          *chanhead;
    ngx_str_t                        str;
    u_char                          *cstr;
  };
  void                              (*retry)(rdstore_data_t*, void *);
  void                               *data;
}; //retry_redis_script_t

typedef struct {
  redis_cluster_status_t           status;
  
  rbtree_seed_t                    hashslots; //cluster rbtree seed
  struct {
    nchan_list_t                     master;
    nchan_list_t                     slave;
    nchan_list_t                     disconnected;
  } nodes;
  ngx_uint_t                       size; //number of master nodes
  ngx_uint_t                       nodes_connected; //number of connected nodes
  ngx_int_t                        node_connections_pending; //number of master nodes
  uint32_t                         homebrew_id;
  ngx_http_upstream_srv_conf_t    *uscf;
  ngx_pool_t                      *pool;
  
  ngx_event_t                      still_notready_timer;
  
  nchan_reaper_t                   chanhead_reaper;
  rdstore_channel_head_t          *orphan_channels_head;
  
  nchan_list_t                     retry_commands;
} redis_cluster_t;

typedef struct {
  ngx_str_t         id;
  ngx_str_t         address;
  ngx_str_t         slots;
  redis_cluster_t  *cluster;
  
  nchan_list_t     *in_node_list;
  rdstore_data_t  **node_list_el_data;
  
  unsigned          master:1;
  unsigned          failed:1;
  unsigned          inactive:1;
  unsigned          indexed:1;
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
  ngx_str_t                        namespace;
  nchan_redis_storage_mode_t       storage_mode;
  
  redisAsyncContext               *ctx;
  redisAsyncContext               *sub_ctx;
  redisContext                    *sync_ctx;
  
  nchan_reaper_t                   chanhead_reaper;
  
  redis_connection_status_t        status;
  struct {
    unsigned                         connection_established:1;
    unsigned                         authenticated:1;
    unsigned                         not_loading_data:1;
    unsigned                         loaded_scripts:1;
    unsigned                         cluster_checked:1;
  }                                detailed_status;
  time_t                           time_connected;
  
  int                              scripts_loaded_count;
  int                              generation;
  ngx_event_t                      reconnect_timer;
  ngx_event_t                      ping_timer;
  time_t                           ping_interval;
  struct {
    callback_chain_t                *head;
    callback_chain_t                *tail;
  }                                on_connected;
  nchan_loc_conf_t                *lcf;
  
  //cluster stuff
  redis_cluster_node_t             node;
  rdstore_channel_head_t          *channels_head;
  rdstore_channel_head_t          *almost_deleted_channels_head;
  
  ngx_int_t                        pending_commands;
  ngx_event_t                      stall_timer;
  
  unsigned                         shutting_down:1;
}; // rdstore_data_t


redis_connection_status_t redis_connection_status(nchan_loc_conf_t *cf);
void redisCheckErrorCallback(redisAsyncContext *c, void *r, void *privdata);
int redisReplyOk(redisAsyncContext *c, void *r);
ngx_int_t redis_add_connection_data(nchan_redis_conf_t *rcf, nchan_loc_conf_t *lcf, ngx_str_t *override_url);
rdstore_data_t *redis_create_rdata(ngx_str_t *url, redis_connect_params_t *rcp, nchan_redis_conf_t *rcf, nchan_loc_conf_t *lcf);
ngx_int_t redis_ensure_connected(rdstore_data_t *rdata);
ngx_int_t parse_redis_url(ngx_str_t *url, redis_connect_params_t *rcp);
ngx_int_t rdstore_initialize_chanhead_reaper(nchan_reaper_t *reaper, char *name);

ngx_int_t redis_chanhead_gc_add(rdstore_channel_head_t *head, ngx_int_t expire, const char *reason);
ngx_int_t redis_chanhead_gc_add_to_reaper(nchan_reaper_t *, rdstore_channel_head_t *head, ngx_int_t expire, const char *reason);
ngx_int_t redis_chanhead_gc_withdraw(rdstore_channel_head_t *head);
ngx_int_t redis_chanhead_gc_withdraw_from_reaper(nchan_reaper_t *, rdstore_channel_head_t *head);
ngx_int_t redis_chanhead_catch_up_after_reconnect(rdstore_channel_head_t *ch);


void redis_associate_chanhead_with_rdata(rdstore_channel_head_t *head, rdstore_data_t *rdata);
nchan_reaper_t *rdstore_get_chanhead_reaper(rdstore_channel_head_t *ch);
ngx_int_t ensure_chanhead_pubsub_subscribed_if_needed(rdstore_channel_head_t *ch);

void __rdt_process_detailed_status(rdstore_data_t *rdata);
#define rdata_set_status_flag(rdata, flag, val) \
  rdata->detailed_status.flag=val; \
  __rdt_process_detailed_status(rdata)

rdstore_data_t *find_rdata_by_connect_params(redis_connect_params_t *rcp);
rdstore_data_t *find_rdata_by_url(ngx_str_t *url);
#endif //NCHAN_REDIS_STORE_PRIVATE_H
