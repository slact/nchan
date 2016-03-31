#ifndef NCHAN_TYPES_H
#define NCHAN_TYPES_H
#include <util/nchan_reuse_queue.h>
#include <util/nchan_bufchainpool.h>

typedef ngx_int_t (*callback_pt)(ngx_int_t, void *, void *);

typedef enum {MSG_CHANNEL_NOTREADY, MSG_INVALID, MSG_PENDING, MSG_NOTFOUND, MSG_FOUND, MSG_EXPECTED, MSG_EXPIRED} nchan_msg_status_t;
typedef enum {INACTIVE, NOTREADY, WAITING, STUBBED, READY} chanhead_pubsub_status_t;

typedef enum {
  SUB_ENQUEUE, SUB_DEQUEUE, SUB_RECEIVE_MESSAGE, SUB_RECEIVE_STATUS, 
  CHAN_PUBLISH, CHAN_DELETE  
} channel_event_type_t;
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

#define NCHAN_ZERO_MSGID {0, {{0}}, 0, 0}
#define NCHAN_OLDEST_MSGID {0, {{0}}, 1, 0}
#define NCHAN_NEWEST_MSGID {-1, {{0}}, 1, 0}

#define NCHAN_MULTITAG_MAX 255
#define NCHAN_FIXED_MULTITAG_MAX 4
union nchan_msg_multitag {
  int16_t         fixed[NCHAN_FIXED_MULTITAG_MAX];
  int16_t        *allocd;
};

typedef struct {
  time_t                          time; //tag message by time
  union nchan_msg_multitag        tag;
  unsigned                        tagactive:16;
  unsigned                        tagcount:16;
} nchan_msg_id_t;

//message queue

#if NCHAN_MSG_RESERVE_DEBUG
typedef struct msg_rsv_dbg_s msg_rsv_dbg_t;
struct msg_rsv_dbg_s {
  char                  *lbl;
  msg_rsv_dbg_t         *prev;
  msg_rsv_dbg_t         *next;
}; //msg_rsv_dbg_s
#endif

typedef struct nchan_loc_conf_s nchan_loc_conf_t;
typedef struct nchan_msg_s nchan_msg_t;

struct nchan_msg_s {
  nchan_msg_id_t                  id;
  nchan_msg_id_t                  prev_id;
  ngx_str_t                       content_type;
  ngx_str_t                       eventsource_event;
  //  ngx_str_t                   charset;
  ngx_buf_t                      *buf;
  time_t                          expires;
  ngx_atomic_int_t                refcount;
  
  struct nchan_msg_s             *reload_next;
  
  unsigned                        shared:1; //for debugging
  unsigned                        temp_allocd:1;
#if NCHAN_MSG_RESERVE_DEBUG
  struct msg_rsv_dbg_s           *rsv;
#endif
#if NCHAN_MSG_LEAK_DEBUG
  ngx_str_t                       lbl;
  struct nchan_msg_s             *dbg_prev;
  struct nchan_msg_s             *dbg_next;
#endif
#if NCHAN_BENCHMARK
  struct timeval                  start_tv;
#endif
}; // nchan_msg_t

typedef struct {
  nchan_msg_t       copy;
  nchan_msg_t      *original;
} nchan_msg_copy_t;

typedef struct {
  ngx_str_t                       id;
  ngx_int_t                       messages;
  ngx_int_t                       subscribers;
  time_t                          last_seen;
  time_t                          expires;
  nchan_msg_id_t                  last_published_msg_id;
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
  ngx_int_t (*subscribe)   (ngx_str_t *, subscriber_t *);
  ngx_int_t (*publish)     (ngx_str_t *, nchan_msg_t *, struct nchan_loc_conf_s *, callback_pt, void *);
  
  ngx_int_t (*delete_channel)(ngx_str_t *, callback_pt, void *);
  
  //channel actions
  ngx_int_t (*find_channel)(ngx_str_t *, callback_pt, void*);
  
  
  
  //message actions and properties
  ngx_str_t * (*message_etag)(nchan_msg_t *msg, ngx_pool_t *pool);
  ngx_str_t * (*message_content_type)(nchan_msg_t *msg, ngx_pool_t *pool);

} nchan_store_t;

#define NCHAN_MULTI_SEP_CHR '\0'

typedef struct {
  unsigned                        http:1;
  unsigned                        websocket:1;
} nchan_conf_publisher_types_t;

typedef struct {
  unsigned                        poll:1; //bleugh
  unsigned                        longpoll:1;
  unsigned                        http_chunked:1;
  unsigned                        http_multipart:1;
  unsigned                        eventsource:1;
  unsigned                        websocket:1;
} nchan_conf_subscriber_types_t;

#define NCHAN_COMPLEX_VALUE_ARRAY_MAX 8
typedef struct {
  ngx_http_complex_value_t       *cv[NCHAN_COMPLEX_VALUE_ARRAY_MAX];
  ngx_int_t                       n;
} nchan_complex_value_arr_t;

struct nchan_loc_conf_s { //nchan_loc_conf_t
  
  time_t                          buffer_timeout;
  ngx_int_t                       max_messages;
  
  ngx_http_complex_value_t       *authorize_request_url;
  ngx_http_complex_value_t       *publisher_upstream_request_url;
  
  nchan_complex_value_arr_t       pub_chid;
  nchan_complex_value_arr_t       sub_chid;
  nchan_complex_value_arr_t       pubsub_chid;
  ngx_str_t                       channel_group;
  ngx_str_t                       channel_id_split_delimiter;

  ngx_str_t                       allow_origin;
  
  nchan_complex_value_arr_t       last_message_id;
  ngx_str_t                       custom_msgtag_header;
  ngx_int_t                       msg_in_etag_only;
  
  nchan_conf_publisher_types_t    pub;
  nchan_conf_subscriber_types_t   sub; 
  
  time_t                          subscriber_timeout;
  
  ngx_int_t                       longpoll_multimsg;
  ngx_str_t                       eventsource_event;
  
  time_t                          websocket_ping_interval;
  
  ngx_int_t                       subscriber_start_at_oldest_message;
  
  ngx_http_complex_value_t       *channel_events_channel_id;
  ngx_http_complex_value_t       *channel_event_string;
  
  ngx_int_t                       subscribe_only_existing_channel;
  ngx_int_t                       use_redis;
  ngx_int_t                       max_channel_id_length;
  ngx_int_t                       max_channel_subscribers;
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
typedef enum {LONGPOLL, HTTP_CHUNKED, HTTP_MULTIPART, INTERVALPOLL, EVENTSOURCE, WEBSOCKET, INTERNAL, SUBSCRIBER_TYPES} subscriber_type_t;
typedef void (*subscriber_callback_pt)(subscriber_t *, void *);

typedef struct {
  ngx_int_t              (*enqueue)(struct subscriber_s *);
  ngx_int_t              (*dequeue)(struct subscriber_s *);
  ngx_int_t              (*respond_message)(struct subscriber_s *, nchan_msg_t *);
  ngx_int_t              (*respond_status)(struct subscriber_s *, ngx_int_t, const ngx_str_t *);
  ngx_int_t              (*set_dequeue_callback)(subscriber_t *self, subscriber_callback_pt cb, void *privdata);
  ngx_int_t              (*reserve)(struct subscriber_s *);
  ngx_int_t              (*release)(struct subscriber_s *, uint8_t nodestroy);
  ngx_int_t              (*notify)(struct subscriber_s *, ngx_int_t code, void *data);
  ngx_int_t              (*subscribe)(subscriber_t *, ngx_str_t *);
  
} subscriber_fn_t;

typedef enum {ALIVE, DEAD, UNKNOWN, PININGFORTHEFJORDS} nchan_subscriber_status_t;

struct subscriber_s {
  ngx_str_t                 *name;
  subscriber_type_t          type;
  const subscriber_fn_t     *fn;
  nchan_subscriber_status_t  status;
  nchan_msg_id_t             last_msgid;
  nchan_loc_conf_t          *cf;
  ngx_http_request_t        *request;
  ngx_uint_t                 reserved;
  unsigned                   dequeue_after_response:1;
  unsigned                   destroy_after_dequeue:1;
  unsigned                   enqueued:1;
#if FAKESHARD
  ngx_int_t                  owner;
#endif
#if NCHAN_SUBSCRIBER_LEAK_DEBUG
  u_char                    *lbl;
  subscriber_t              *dbg_prev;
  subscriber_t              *dbg_next;
#endif
}; //subscriber_t

#define NCHAN_MULTITAG_REQUEST_CTX_MAX 4
typedef struct {
  subscriber_t                  *sub;
  nchan_reuse_queue_t           *output_str_queue;
  nchan_reuse_queue_t           *reserved_msg_queue;
  nchan_bufchain_pool_t         *bcp; //bufchainpool maybe?
  
  ngx_str_t                     *subscriber_type;
  nchan_msg_id_t                 msg_id;
  nchan_msg_id_t                 prev_msg_id;
  ngx_str_t                     *publisher_type;
  ngx_str_t                     *multipart_boundary;
  ngx_str_t                     *channel_event_name;
  ngx_str_t                      channel_id[NCHAN_MULTITAG_REQUEST_CTX_MAX];
  int                            channel_id_count;
  ngx_str_t                      request_origin_header;
#if NCHAN_BENCHMARK
  struct timeval                 start_tv;
#endif
  
} nchan_request_ctx_t;


#endif  /* NCHAN_TYPES_H */
