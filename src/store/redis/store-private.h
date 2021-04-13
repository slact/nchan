#ifndef NCHAN_REDIS_STORE_PRIVATE_H
#define NCHAN_REDIS_STORE_PRIVATE_H

#define NCHAN_CHANHEAD_EXPIRE_SEC 1
#define NCHAN_CHANHEAD_CLUSTER_ORPHAN_EXPIRE_SEC 15
#define NCHAN_NOTICE_REDIS_CHANNEL_MESSAGE_BUFFER_SIZE_CHANGE 0xB00F

#define NCHAN_REDIS_UNIQUE_REQUEST_ID_KEY "nchan:unique_request_id"

#include <nchan_module.h>
#include "uthash.h"
#if NCHAN_HAVE_HIREDIS_WITH_SOCKADDR
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#else
#include <store/redis/hiredis/hiredis.h>
#include <store/redis/hiredis/async.h>
#endif
#include <util/nchan_reaper.h>
#include <util/nchan_rbtree.h>
#include <util/nchan_list.h>
#include <store/spool.h>

#include "redis_nodeset.h"
#define REDIS_LUA_HASH_LENGTH 40
#define REDIS_NODESET_NOT_READY_MAX_RETRIES 2

//OBSOLETE
typedef struct {
  unsigned         min:16;
  unsigned         max:16;
} redis_cluster_slot_range_t;


typedef struct rdstore_channel_head_s rdstore_channel_head_t;

typedef enum {REDIS_PUBSUB_SUBSCRIBING, REDIS_PUBSUB_SUBSCRIBED, REDIS_PUBSUB_UNSUBSCRIBED} redis_pubsub_status_t;

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
  //rdstore_channel_head_cluster_data_t cluster;
  
  ngx_int_t                    reserved;
  
  struct {                   //redis
    int                          generation;
    redis_nodeset_t             *nodeset;
    struct {                   //node
      redis_node_t                *cmd;
      redis_node_t                *pubsub;
    }                            node;
    
    struct {                  //linked list links
      struct {
        rdstore_channel_head_t      *prev;
        rdstore_channel_head_t      *next;
      }                            nodeset;
      struct {
        rdstore_channel_head_t      *prev;
        rdstore_channel_head_t      *next;
      }                            node_cmd;
      struct {
        rdstore_channel_head_t      *prev;
        rdstore_channel_head_t      *next;
      }                            node_pubsub;
      unsigned                     in_disconnected_cmd_list:1;
      unsigned                     in_disconnected_pubsub_list:1;
    }                            slist;
    
  }                            redis;
  
  struct {                   //gc
    rdstore_channel_head_t      *prev;
    rdstore_channel_head_t      *next;
    time_t                       time;
    unsigned                     in_reaper:1;
  }                            gc;
  
  
  
  redis_pubsub_status_t        pubsub_status;
  unsigned                     meta:1;
  unsigned                     shutting_down:1;
  UT_hash_handle               hh;
};


void redisCheckErrorCallback(redisAsyncContext *c, void *r, void *privdata);
int redisReplyOk(redisAsyncContext *c, void *r);
ngx_int_t parse_redis_url(ngx_str_t *url, redis_connect_params_t *rcp);
ngx_int_t rdstore_initialize_chanhead_reaper(nchan_reaper_t *reaper, char *name);

ngx_int_t redis_chanhead_gc_add(rdstore_channel_head_t *head, ngx_int_t expire, const char *reason);
ngx_int_t redis_chanhead_gc_withdraw(rdstore_channel_head_t *head);
ngx_int_t redis_chanhead_catch_up_after_reconnect(rdstore_channel_head_t *ch);

ngx_int_t ensure_chanhead_pubsub_subscribed_if_needed(rdstore_channel_head_t *ch);


#endif //NCHAN_REDIS_STORE_PRIVATE_H
