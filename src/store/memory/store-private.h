#ifndef MEMSTORE_PRIVATE_HEADER
#define MEMSTORE_PRIVATE_HEADER

#define FAKESHARD 1

#include "uthash.h"
typedef struct nhpm_channel_head_s nhpm_channel_head_t;
typedef struct nhpm_channel_head_cleanup_s nhpm_channel_head_cleanup_t;
typedef struct nhpm_message_s nhpm_message_t;

typedef enum {INACTIVE, NOTREADY, WAITING, READY} chanhead_pubsub_status_t;

struct nhpm_message_s {
  ngx_http_push_msg_t      *msg;
  nhpm_message_t           *prev;
  nhpm_message_t           *next;
}; //nhpm_message_t

#include "../spool.h"

typedef struct {
  ngx_atomic_t                sub_count;
  ngx_atomic_t                internal_sub_count;
  ngx_atomic_t                total_message_count;
  ngx_atomic_t                stored_message_count;
  ngx_atomic_t                last_seen;
} nhpm_channel_head_shm_t;

struct nhpm_channel_head_s {
  ngx_str_t                       id; //channel id
  ngx_uint_t                      owner;
  ngx_uint_t                      slot;
  ngx_http_push_channel_t         channel;
  channel_spooler_t               spooler;
  ngx_atomic_t                    generation; //subscriber pool generation.
  chanhead_pubsub_status_t        status;
  ngx_atomic_t                    sub_count;
  time_t                          last_subscribed;
  nhpm_channel_head_shm_t        *shared;
  ngx_int_t                       internal_sub_count;
  ngx_uint_t                      min_messages;
  ngx_uint_t                      max_messages;
  nhpm_message_t                 *msg_first;
  nhpm_message_t                 *msg_last;
  ngx_http_push_msg_id_t          last_msgid;
  subscriber_t                   *ipc_sub; //points to NULL or inaacceessible memory.
  nhpm_llist_timed_t              cleanlink;
  UT_hash_handle                  hh;
};

typedef struct {
  
} shm_data_t;

nhpm_channel_head_t *ngx_http_push_memstore_find_chanhead(ngx_str_t *channel_id);
nhpm_channel_head_t *ngx_http_push_memstore_get_chanhead(ngx_str_t *channel_id);
nhpm_message_t *chanhead_find_next_message(nhpm_channel_head_t *ch, ngx_http_push_msg_id_t *msgid, ngx_int_t *status);
shmem_t *ngx_http_push_memstore_get_shm(void);
ipc_t *ngx_http_push_memstore_get_ipc(void);
ngx_int_t ngx_http_push_memstore_handle_get_message_reply(ngx_http_push_msg_t *msg, ngx_int_t findmsg_status, void *d);
ngx_int_t memstore_channel_owner(ngx_str_t *id);
ngx_int_t ngx_http_push_store_publish_message_generic(ngx_str_t *channel_id, ngx_http_push_msg_t *msg, ngx_int_t msg_in_shm, ngx_int_t msg_timeout, ngx_int_t max_msg,  ngx_int_t min_msg, callback_pt callback, void *privdata);
ngx_int_t ngx_http_push_memstore_publish_generic(nhpm_channel_head_t *head, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line);
ngx_int_t nhpm_memstore_subscriber_register(nhpm_channel_head_t *chanhead, subscriber_t *sub);
ngx_int_t nhpm_memstore_subscriber_unregister(nhpm_channel_head_t *chanhead, subscriber_t *sub);
ngx_int_t ngx_http_push_memstore_force_delete_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata);
ngx_int_t nhpm_memstore_subscriber_create(nhpm_channel_head_t *chanhead, subscriber_t *sub);


typedef struct {
  ngx_event_t             gc_timer;
  nhpm_llist_timed_t     *gc_head;
  nhpm_llist_timed_t     *gc_tail;
  nhpm_channel_head_t    *hash;
  ngx_int_t               fake_slot;
} memstore_data_t;
memstore_data_t *mpt;
void memstore_fakeprocess_push(ngx_int_t slot);
void memstore_fakeprocess_push_random(void);
void memstore_fakeprocess_pop();
ngx_int_t memstore_slot();

nhpm_channel_head_t * chanhead_memstore_find(ngx_str_t *channel_id);
ngx_int_t chanhead_gc_add(nhpm_channel_head_t *head, const char *);
ngx_int_t chanhead_gc_withdraw(nhpm_channel_head_t *chanhead, const char *);

#endif