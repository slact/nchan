#ifndef NCHAN_TYPES_H
#define NCHAN_TYPES_H
#include <util/nchan_reuse_queue.h>
#include <util/nchan_bufchainpool.h>

typedef ngx_int_t (*callback_pt)(ngx_int_t, void *, void *);

typedef enum {MSG_CHANNEL_NOTREADY, MSG_NORESPONSE, MSG_INVALID, MSG_PENDING, MSG_NOTFOUND, MSG_FOUND, MSG_EXPECTED, MSG_EXPIRED} nchan_msg_status_t;
typedef enum {INACTIVE, NOTREADY, WAITING, STUBBED, READY, DELETED} chanhead_pubsub_status_t;

typedef enum {NCHAN_CONTENT_TYPE_PLAIN, NCHAN_CONTENT_TYPE_JSON, NCHAN_CONTENT_TYPE_XML, NCHAN_CONTENT_TYPE_YAML, NCHAN_CONTENT_TYPE_HTML} nchan_content_type_t;

typedef enum {REDIS_MODE_CONF_UNSET = NGX_CONF_UNSET, REDIS_MODE_BACKUP = 1, REDIS_MODE_DISTRIBUTED = 2} nchan_redis_storage_mode_t;

typedef enum {
  SUB_ENQUEUE, SUB_DEQUEUE, SUB_RECEIVE_MESSAGE, SUB_RECEIVE_STATUS, 
  CHAN_PUBLISH, CHAN_DELETE  
} channel_event_type_t;
//on with the declarations
typedef struct {
  size_t                          shm_size;
  ngx_msec_t                      redis_fakesub_timer_interval;
  size_t                          redis_publish_message_msgkey_size;
#if (NGX_ZLIB)
  struct {
                                    int level;
                                    int windowBits;
                                    int memLevel;
                                    int strategy;
  }                               zlib_params;
#endif
  ngx_path_t                     *message_temp_path;
} nchan_main_conf_t;


typedef struct {
  ngx_str_t                     url;
  ngx_flag_t                    url_enabled;
  time_t                        ping_interval;
  ngx_str_t                     namespace;
  nchan_redis_storage_mode_t    storage_mode;
  ngx_str_t                     upstream_url;
  ngx_http_upstream_srv_conf_t *upstream;
  ngx_flag_t                    upstream_inheritable;
  unsigned                      enabled:1;
  time_t                        after_connect_wait_time;
  void                         *privdata;
} nchan_redis_conf_t;

typedef struct {
  ngx_atomic_int_t  lock;
  ngx_atomic_t      mutex;
  ngx_int_t         write_pid;
} ngx_rwlock_t;


#define NCHAN_OLDEST_MSGID_TIME 0
#define NCHAN_NEWEST_MSGID_TIME -1
#define NCHAN_NTH_MSGID_TIME -2

#define NCHAN_ZERO_MSGID {0, {{0}}, 0, 0}
#define NCHAN_OLDEST_MSGID {NCHAN_OLDEST_MSGID_TIME, {{0}}, 0, 1}
#define NCHAN_NEWEST_MSGID {NCHAN_NEWEST_MSGID_TIME, {{0}}, 0, 1}
#define NCHAN_NTH_MSGID {NCHAN_NTH_MSGID_TIME, {{0}}, 0, 1}

#define NCHAN_MULTITAG_MAX 255
#define NCHAN_FIXED_MULTITAG_MAX 4
union nchan_msg_multitag {
  int16_t         fixed[NCHAN_FIXED_MULTITAG_MAX];
  int16_t        *allocd;
};

typedef struct {
  time_t                          time; //tag message by time
  union nchan_msg_multitag        tag;
  int16_t                         tagactive;
  int16_t                         tagcount;
} nchan_msg_id_t;

typedef struct {
  time_t                          time;
  int16_t                         tag;
} nchan_msg_tiny_id_t;

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

typedef enum {NCHAN_MSG_SHARED, NCHAN_MSG_HEAP, NCHAN_MSG_POOL, NCHAN_MSG_STACK} nchan_msg_storage_t;

typedef enum {
  NCHAN_MSG_COMPRESSION_INVALID = -1,
  NCHAN_MSG_NO_COMPRESSION = 0, 
  NCHAN_MSG_COMPRESSION_WEBSOCKET_PERMESSAGE_DEFLATE
} nchan_msg_compression_type_t;
  
typedef struct {
  ngx_buf_t                       buf;
  nchan_msg_compression_type_t    compression;
} nchan_compressed_msg_t;

struct nchan_msg_s {
  nchan_msg_id_t                  id;
  nchan_msg_id_t                  prev_id;
  ngx_str_t                      *content_type;
  ngx_str_t                      *eventsource_event;
  //  ngx_str_t                   charset;
  ngx_buf_t                       buf;
  time_t                          expires;
  
  ngx_atomic_int_t                refcount;
  nchan_msg_t                    *parent;
  nchan_compressed_msg_t         *compressed;
  //struct nchan_msg_s             *reload_next;
  
  nchan_msg_storage_t             storage;
  
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

typedef struct {
  ngx_atomic_uint_t      channels;
  ngx_atomic_uint_t      subscribers;
  ngx_atomic_uint_t      total_published_messages;
  ngx_atomic_uint_t      messages;
  ngx_atomic_uint_t      redis_pending_commands;
  ngx_atomic_uint_t      redis_connected_servers;
  ngx_atomic_uint_t      ipc_total_alerts_sent;
  ngx_atomic_uint_t      ipc_total_alerts_received;
  ngx_atomic_uint_t      ipc_queue_size;
  ngx_atomic_uint_t      ipc_total_send_delay;
  ngx_atomic_uint_t      ipc_total_receive_delay;
} nchan_stub_status_t;

typedef struct subscriber_s subscriber_t;

typedef struct {
  //must be made entirely of ngx_atomic_int_t
  ngx_atomic_int_t                channels;
  ngx_atomic_int_t                subscribers;
  ngx_atomic_int_t                messages;
  ngx_atomic_int_t                messages_shmem_bytes;
  ngx_atomic_int_t                messages_file_bytes;
} nchan_group_limits_t;

typedef struct {
  ngx_atomic_int_t               channels;
  ngx_atomic_int_t               multiplexed_channels;
  ngx_atomic_int_t               subscribers;
  ngx_atomic_int_t               messages;
  ngx_atomic_int_t               messages_shmem_bytes;
  ngx_atomic_int_t               messages_file_bytes;
  nchan_group_limits_t           limit;
  ngx_str_t                      name;
} nchan_group_t;

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
  ngx_int_t (*get_message) (ngx_str_t *, nchan_msg_id_t *, nchan_loc_conf_t *cf, callback_pt, void *);
  ngx_int_t (*subscribe)   (ngx_str_t *, subscriber_t *);
  ngx_int_t (*publish)     (ngx_str_t *, nchan_msg_t *, nchan_loc_conf_t *, callback_pt, void *);
  
    //channel actions
  ngx_int_t (*delete_channel)(ngx_str_t *, nchan_loc_conf_t *, callback_pt, void *);
  ngx_int_t (*find_channel)(ngx_str_t *, nchan_loc_conf_t *, callback_pt, void*);
  
  //group actions
  ngx_int_t (*get_group)(ngx_str_t *name, nchan_loc_conf_t *, callback_pt, void *);
  ngx_int_t (*set_group_limits)(ngx_str_t *name, nchan_loc_conf_t *, nchan_group_limits_t *limits, callback_pt, void *);
  ngx_int_t (*delete_group)(ngx_str_t *name, nchan_loc_conf_t *, callback_pt, void *);

} nchan_store_t;

#define NCHAN_MULTI_SEP_CHR '\0'

typedef struct {
  unsigned                        http:1;
  unsigned                        websocket:1;
} nchan_conf_publisher_types_t;

typedef struct {
  unsigned                        poll:1; //bleugh
  unsigned                        http_raw_stream:1; //ugleh
  unsigned                        longpoll:1;
  unsigned                        http_chunked:1;
  unsigned                        http_multipart:1;
  unsigned                        eventsource:1;
  unsigned                        websocket:1;
} nchan_conf_subscriber_types_t;

typedef struct {
  unsigned                        get:1;
  unsigned                        set:1;
  unsigned                        delete:1;
  
  ngx_int_t                       enable_accounting;
  
  ngx_http_complex_value_t       *max_channels;
  ngx_http_complex_value_t       *max_subscribers;
  ngx_http_complex_value_t       *max_messages;
  ngx_http_complex_value_t       *max_messages_shm_bytes;
  ngx_http_complex_value_t       *max_messages_file_bytes;
} nchan_conf_group_t;

#define NCHAN_COMPLEX_VALUE_ARRAY_MAX 8
typedef struct {
  ngx_http_complex_value_t       *cv[NCHAN_COMPLEX_VALUE_ARRAY_MAX];
  ngx_int_t                       n;
} nchan_complex_value_arr_t;

typedef struct {
 ngx_atomic_uint_t               message_timeout;
 ngx_atomic_uint_t               max_messages;
} nchan_loc_conf_shared_data_t;

struct nchan_loc_conf_s { //nchan_loc_conf_t
  
  ngx_int_t                       shared_data_index;
  
  time_t                          message_timeout;
  ngx_int_t                       max_messages;
  
  ngx_http_complex_value_t       *complex_message_timeout;
  ngx_http_complex_value_t       *complex_max_messages;
  
  ngx_http_complex_value_t       *authorize_request_url;
  ngx_http_complex_value_t       *publisher_upstream_request_url;
  
  ngx_http_complex_value_t       *unsubscribe_request_url;
  ngx_http_complex_value_t       *subscribe_request_url;
  
  nchan_complex_value_arr_t       pub_chid;
  nchan_complex_value_arr_t       sub_chid;
  nchan_complex_value_arr_t       pubsub_chid;
  ngx_http_complex_value_t       *channel_group;
  
  ngx_str_t                       channel_id_split_delimiter;
  
  ngx_str_t                       subscriber_http_raw_stream_separator;

  ngx_http_complex_value_t       *allow_origin;
  
  nchan_complex_value_arr_t       last_message_id;
  ngx_str_t                       custom_msgtag_header;
  ngx_int_t                       msg_in_etag_only;
  
  nchan_conf_publisher_types_t    pub;
  nchan_conf_subscriber_types_t   sub; 
  nchan_conf_group_t              group;
  time_t                          subscriber_timeout;
  
  ngx_int_t                       longpoll_multimsg;
  ngx_int_t                       longpoll_multimsg_use_raw_stream_separator;
  
  ngx_str_t                       eventsource_event;
  
  time_t                          websocket_ping_interval;
  
  struct {
    ngx_int_t   enabled;
    ngx_str_t  *in;
    ngx_str_t  *out;
  }                               websocket_heartbeat;
  
  nchan_msg_compression_type_t    message_compression;
  
  ngx_int_t                       subscriber_first_message;
  
  ngx_http_complex_value_t       *channel_events_channel_id;
  ngx_http_complex_value_t       *channel_event_string;
  
  ngx_int_t                       subscribe_only_existing_channel;
  
  nchan_redis_conf_t              redis;
  time_t                          redis_idle_channel_cache_timeout;
  
  ngx_int_t                       max_channel_id_length;
  ngx_int_t                       max_channel_subscribers;
  time_t                          channel_timeout;
  nchan_store_t                  *storage_engine;
  
  ngx_int_t                     (*request_handler)(ngx_http_request_t *r);
};// nchan_loc_conf_t;

typedef struct nchan_llist_timed_s {
  struct nchan_llist_timed_s     *prev;
  void                           *data;
  time_t                          time;
  struct nchan_llist_timed_s     *next;
} nchan_llist_timed_t;

typedef enum {PUB, SUB} pub_or_sub_t;
typedef enum {LONGPOLL, HTTP_CHUNKED, HTTP_MULTIPART, HTTP_RAW_STREAM, INTERVALPOLL, EVENTSOURCE, WEBSOCKET, INTERNAL, SUBSCRIBER_TYPES} subscriber_type_t;
typedef void (*subscriber_callback_pt)(subscriber_t *, void *);

typedef struct {
  ngx_int_t              (*enqueue)(struct subscriber_s *);
  ngx_int_t              (*dequeue)(struct subscriber_s *);
  ngx_int_t              (*respond_message)(struct subscriber_s *, nchan_msg_t *);
  ngx_int_t              (*respond_status)(struct subscriber_s *, ngx_int_t, const ngx_str_t *, ngx_chain_t *);
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
  unsigned                   enable_sub_unsub_callbacks;
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
  ngx_str_t                     *channel_group_name;
  
  ngx_str_t                     *request_origin_header;
  ngx_str_t                     *allow_origin;
  
  ngx_int_t                      unsubscribe_request_callback_finalize_code;
  unsigned                       sent_unsubscribe_request:1;
  unsigned                       request_ran_content_handler:1;
#if NCHAN_BENCHMARK
  struct timeval                 start_tv;
#endif
  
} nchan_request_ctx_t;


#endif  /* NCHAN_TYPES_H */
