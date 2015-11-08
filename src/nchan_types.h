#ifndef NCHAN_TYPES_H
#define NCHAN_TYPES_H

typedef ngx_int_t (*callback_pt)(ngx_int_t, void *, void *);

typedef enum {MSG_CHANNEL_NOTREADY, MSG_INVALID, MSG_PENDING, MSG_NOTFOUND, MSG_FOUND, MSG_EXPECTED, MSG_EXPIRED} nchan_msg_status_t;
typedef enum {INACTIVE, NOTREADY, WAITING, READY} chanhead_pubsub_status_t;

//on with the declarations
typedef struct {
  size_t                          shm_size;
  ngx_str_t                       redis_url;
} nchan_main_conf_t;

typedef struct {
  ngx_atomic_int_t  lock;
  ngx_atomic_t      mutex;
  ngx_int_t         write_pid;
} ngx_rwlock_t;


typedef struct {
  time_t                          time; //tag message by time
  ngx_int_t                       tag;  //used in conjunction with message_time if more than one message have the same time.
} nchan_msg_id_t;

typedef struct {
  ngx_chain_t     chain;
  ngx_buf_t       buf;
} nchan_buf_and_chain_t;



//message queue

#if NCHAN_MSG_LEAK_DEBUG
typedef struct msg_rsv_dbg_s msg_rsv_dbg_t;
struct msg_rsv_dbg_s {
  char                  *lbl;
  struct msg_rsv_dbg_s  *prev;
  struct msg_rsv_dbg_s  *next;
}; //msg_rsv_dbg_s
#endif

typedef struct nchan_msg_s nchan_msg_t;
struct nchan_msg_s {
  nchan_msg_id_t                  id;
  nchan_msg_id_t                  prev_id;
  ngx_str_t                       content_type;
  //  ngx_str_t                   charset;
  ngx_buf_t                      *buf;
  time_t                          expires;
  ngx_uint_t                      delete_oldest_received_min_messages; //NGX_MAX_UINT32_VALUE for 'never'
  ngx_atomic_t                    refcount;
  unsigned                        shared:1; //for debugging
#if NCHAN_MSG_LEAK_DEBUG
  ngx_str_t                       lbl;
  struct msg_rsv_dbg_s           *rsv;
  struct nchan_msg_s             *dbg_prev;
  struct nchan_msg_s             *dbg_next;
#endif
}; // nchan_msg_t


typedef struct {
  ngx_str_t                       id;
  ngx_int_t                       messages;
  ngx_int_t                       subscribers;
  time_t                          last_seen;
  time_t                          expires;
} nchan_channel_t;


//garbage collecting goodness
typedef struct {
  ngx_queue_t                     queue;
  nchan_channel_t                *channel;
} nchan_channel_queue_t;


typedef struct {
  ngx_queue_t                    queue;
  ngx_rwlock_t                   lock;
} nchan_worker_msg_sentinel_t;

//shared memory
typedef struct {
  ngx_rbtree_t                  tree;
  ngx_uint_t                    channels; //# of channels being used
  ngx_uint_t                    messages; //# of channels being used
  nchan_worker_msg_sentinel_t  *ipc; //interprocess stuff
} nchan_shm_data_t;

typedef struct subscriber_s subscriber_t;
typedef struct nchan_loc_conf_s nchan_loc_conf_t;

typedef struct{
  //init
  ngx_int_t (*init_module)(ngx_cycle_t *cycle);
  ngx_int_t (*init_worker)(ngx_cycle_t *cycle);
  ngx_int_t (*init_postconfig)(ngx_conf_t *cf);
  void      (*create_main_conf)(ngx_conf_t *cf, nchan_main_conf_t *mcf);
  
  //quit
  void      (*exit_worker)(ngx_cycle_t *cycle);
  void      (*exit_master)(ngx_cycle_t *cycle);
  
  //async-friendly functions with callbacks
  ngx_int_t (*get_message) (ngx_str_t *, nchan_msg_id_t *, callback_pt, void *);
  ngx_int_t (*subscribe)   (ngx_str_t *, nchan_msg_id_t *, struct subscriber_s *, callback_pt, void *);
  ngx_int_t (*publish)     (ngx_str_t *, nchan_msg_t *, struct nchan_loc_conf_s *, callback_pt, void *);
  
  ngx_int_t (*delete_channel)(ngx_str_t *, callback_pt, void *);
  
  //channel actions
  ngx_int_t (*find_channel)(ngx_str_t *, callback_pt, void*);
  
  
  
  //message actions and properties
  ngx_str_t * (*message_etag)(nchan_msg_t *msg, ngx_pool_t *pool);
  ngx_str_t * (*message_content_type)(nchan_msg_t *msg, ngx_pool_t *pool);

} nchan_store_t;

typedef struct {
  unsigned                        http:1;
  unsigned                        websocket:1;
} nchan_conf_publisher_types_t;

typedef struct {
  unsigned                        poll:1; //bleugh
  unsigned                        longpoll:1;
  unsigned                        eventsource:1;
  unsigned                        websocket:1;
} nchan_conf_subscriber_types_t;


struct nchan_loc_conf_s {
  time_t                          buffer_timeout;
  ngx_int_t                       min_messages;
  ngx_int_t                       max_messages;
  
  ngx_http_complex_value_t       *pub_channel_id;
  nchan_conf_publisher_types_t    pub;
  
  ngx_http_complex_value_t       *sub_channel_id;
  nchan_conf_subscriber_types_t   sub; 
  
  ngx_http_complex_value_t       *pubsub_channel_id;
  
  ngx_int_t                       subscriber_concurrency;
  time_t                          subscriber_timeout;
  
  ngx_int_t                       authorize_channel;
  ngx_int_t                       use_redis;
  ngx_int_t                       delete_oldest_received_message;
  ngx_str_t                       channel_group;
  ngx_int_t                       max_channel_id_length;
  ngx_int_t                       max_channel_subscribers;
  ngx_int_t                       ignore_queue_on_no_cache;
  time_t                          channel_timeout;
  nchan_store_t                  *storage_engine;
};// nchan_loc_conf_t;

typedef struct {
  char              *subtype;
  size_t             len;
  const ngx_str_t   *format;
} nchan_content_subtype_t;

typedef struct nchan_llist_timed_s {
  struct nchan_llist_timed_s     *prev;
  void                           *data;
  time_t                          time;
  struct nchan_llist_timed_s     *next;
} nchan_llist_timed_t;

typedef enum {PUB, SUB} pub_or_sub_t;
typedef enum {LONGPOLL, EVENTSOURCE, WEBSOCKET, INTERNAL, SUBSCRIBER_TYPES} subscriber_type_t;
typedef void (*subscriber_callback_pt)(subscriber_t *, void *);

typedef struct {
  ngx_int_t              (*enqueue)(struct subscriber_s *);
  ngx_int_t              (*dequeue)(struct subscriber_s *);
  ngx_int_t              (*respond_message)(struct subscriber_s *, nchan_msg_t *);
  ngx_int_t              (*respond_status)(struct subscriber_s *, ngx_int_t, const ngx_str_t *);
  ngx_int_t              (*set_timeout_callback)(subscriber_t *self, subscriber_callback_pt cb, void *privdata);
  ngx_int_t              (*set_dequeue_callback)(subscriber_t *self, subscriber_callback_pt cb, void *privdata);
  ngx_int_t              (*reserve)(struct subscriber_s *);
  ngx_int_t              (*release)(struct subscriber_s *, uint8_t nodestroy);
  ngx_int_t              (*notify)(struct subscriber_s *, ngx_int_t code, void *data);
} subscriber_fn_t;

struct subscriber_s {
  const char             *name;
  subscriber_type_t       type;
  const subscriber_fn_t  *fn;
  nchan_msg_id_t          last_msg_id;
  nchan_loc_conf_t       *cf;
  ngx_uint_t              reserved;
  unsigned                dequeue_after_response:1;
  unsigned                destroy_after_dequeue:1;
  unsigned                enqueued:1;
#if NCHAN_SUBSCRIBER_LEAK_DEBUG
  u_char                 *lbl;
  struct subscriber_s    *dbg_prev;
  struct subscriber_s    *dbg_next;
#endif
}; //subscriber_t

#endif  /* NCHAN_TYPES_H */