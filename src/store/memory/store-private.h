 #ifndef MEMSTORE_PRIVATE_HEADER
#define MEMSTORE_PRIVATE_HEADER
#include <util/shmem.h>
//#define MESTORE_CHANHEAD_RESERVE_DEBUG 0
#include "uthash.h"
typedef struct memstore_channel_head_s memstore_channel_head_t;
typedef struct store_message_s store_message_t;

struct store_message_s {
  nchan_msg_t               *msg;
  store_message_t           *prev;
  store_message_t           *next;
}; //store_message_t

#include "../spool.h"

typedef struct {
  ngx_atomic_t                sub_count;
  ngx_atomic_t                internal_sub_count;
  ngx_atomic_t                total_message_count;
  ngx_atomic_t                stored_message_count;
  ngx_atomic_t                last_seen;
} store_channel_head_shm_t;

#define MSG_REFCOUNT_INVALID -9000

typedef struct {
  ngx_str_t            id;
  subscriber_t        *sub;
} memstore_multi_t;

struct memstore_channel_head_s {
  ngx_str_t                       id; //channel id
  ngx_int_t                       owner;
  ngx_int_t                       slot;
  nchan_channel_t                 channel;
  channel_spooler_t               spooler;
  chanhead_pubsub_status_t        status;
  ngx_atomic_int_t                total_sub_count;
  ngx_int_t                       internal_sub_count;
  time_t                          last_subscribed_local;

#if MESTORE_CHANHEAD_RESERVE_DEBUG
  nchan_list_t                    reserved;
#else
  uint16_t                        reserved;
#endif
  
  uint8_t                         multi_waiting;
  uint8_t                         multi_count;
  memstore_multi_t               *multi;
  
  ngx_int_t                       gc_queued_times; // useful for debugging
  store_channel_head_shm_t       *shared;
  
  ngx_uint_t                      max_messages;
  store_message_t                *msg_first;
  store_message_t                *msg_last;
  nchan_msg_id_t                  latest_msgid;
  nchan_msg_id_t                  oldest_msgid;
  subscriber_t                   *foreign_owner_ipc_sub; //points to NULL or inaacceessible memory.
  time_t                          redis_idle_cache_ttl;
  unsigned                        stub:1;
  unsigned                        shutting_down:1;
  unsigned                        meta:1;
  
  nchan_loc_conf_t               *cf;

  subscriber_t                   *redis_sub;
  ngx_int_t                       delta_fakesubs;
  ngx_event_t                     delta_fakesubs_timer_ev;
  
  memstore_channel_head_t        *gc_prev;
  memstore_channel_head_t        *gc_next;
  time_t                          gc_start_time;
  unsigned                        in_gc_queue:1;
  
  memstore_channel_head_t        *churn_prev;
  memstore_channel_head_t        *churn_next;
  time_t                          churn_start_time;
  unsigned                        in_churn_queue:1;
  
  UT_hash_handle                  hh;
};

typedef struct nchan_reloading_channel_s nchan_reloading_channel_t;

struct nchan_reloading_channel_s {
  ngx_str_t                          id;
  ngx_uint_t                         max_messages;
  unsigned                           use_redis:1;
  struct nchan_reloading_channel_s  *prev;
  struct nchan_reloading_channel_s  *next;
  nchan_msg_t                       *msgs;
};

#define NCHAN_INVALID_SLOT           -1

typedef struct {
  nchan_reloading_channel_t         *rlch;
  ngx_atomic_int_t                   procslot[NGX_MAX_PROCESSES];
  ngx_atomic_int_t                   max_workers;
  ngx_atomic_int_t                   old_max_workers;
  ngx_atomic_int_t                   total_active_workers;
  ngx_atomic_int_t                   current_active_workers;
  ngx_atomic_int_t                   reloading;
  ngx_atomic_uint_t                  generation;
  
  nchan_loc_conf_shared_data_t      *conf_data;
  
  nchan_stub_status_t                stats;
  
  ngx_atomic_uint_t                  shmem_pages_used;

#if NCHAN_MSG_LEAK_DEBUG
  nchan_msg_t                       *msgdebug_head;
#endif
} shm_data_t;

memstore_channel_head_t *nchan_memstore_find_chanhead(ngx_str_t *channel_id);
memstore_channel_head_t *nchan_memstore_get_chanhead(ngx_str_t *channel_id, nchan_loc_conf_t *cf);
memstore_channel_head_t *nchan_memstore_get_chanhead_no_ipc_sub(ngx_str_t *channel_id, nchan_loc_conf_t *cf);
store_message_t *chanhead_find_next_message(memstore_channel_head_t *ch, nchan_msg_id_t *msgid, nchan_msg_status_t *status);
shmem_t *nchan_memstore_get_shm(void);
ipc_t *nchan_memstore_get_ipc(void);
ngx_int_t nchan_memstore_handle_get_message_reply(nchan_msg_t *msg, nchan_msg_status_t findmsg_status, void *d);

ngx_int_t nchan_store_publish_message_generic(ngx_str_t *channel_id, nchan_msg_t *msg, ngx_int_t msg_in_shm, nchan_loc_conf_t *cf, callback_pt callback, void *privdata);
ngx_int_t nchan_memstore_publish_generic(memstore_channel_head_t *head, nchan_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line);
ngx_int_t nchan_store_chanhead_publish_message_generic(memstore_channel_head_t *chead, nchan_msg_t *msg, ngx_int_t msg_in_shm, nchan_loc_conf_t *cf, callback_pt callback, void *privdata);
ngx_int_t nchan_memstore_force_delete_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata);
ngx_int_t memstore_ensure_chanhead_is_ready(memstore_channel_head_t *head, uint8_t ipc_subscribe_if_needed);
ngx_int_t memstore_ready_chanhead_unless_stub(memstore_channel_head_t *head);
void memstore_fakesub_add(memstore_channel_head_t *head, ngx_int_t n);
ngx_int_t memstore_chanhead_messages_gc(memstore_channel_head_t *ch);

#if FAKESHARD
void memstore_fakeprocess_push(ngx_int_t slot);
void memstore_fakeprocess_push_random(void);
ngx_int_t memstore_fakeprocess_pop(void);
#endif

ngx_int_t memstore_slot(void);
int memstore_ready(void);
ngx_int_t chanhead_gc_add(memstore_channel_head_t *head, const char *);
ngx_int_t chanhead_gc_withdraw(memstore_channel_head_t *chanhead, const char *);

uint16_t  memstore_worker_generation; //times nginx has been restarted + 1

#endif /*MEMSTORE_PRIVATE_HEADER*/
