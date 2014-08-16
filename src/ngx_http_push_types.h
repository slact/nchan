//on with the declarations
typedef struct {
  size_t                          shm_size;
} ngx_http_push_main_conf_t;

typedef struct {
  ngx_atomic_int_t  lock;
  ngx_atomic_t      mutex;
  ngx_int_t         write_pid;
} ngx_rwlock_t;


typedef struct {
  time_t                          time; //tag message by time
  ngx_int_t                       tag;  //used in conjunction with message_time if more than one message have the same time.
} ngx_http_push_msg_id_t;

//message queue
typedef struct {
  ngx_queue_t                     queue; //this MUST be first.
  ngx_str_t                       content_type;
  //  ngx_str_t                       charset;
  ngx_buf_t                      *buf;
  time_t                          expires;
  ngx_uint_t                      delete_oldest_received_min_messages; //NGX_MAX_UINT32_VALUE for 'never'
  time_t                          message_time; //tag message by time
  ngx_int_t                       message_tag;  //used in conjunction with message_time if more than one message have the same time.
  ngx_int_t                       refcount;
} ngx_http_push_msg_t;

typedef struct ngx_http_push_subscriber_cleanup_s ngx_http_push_subscriber_cleanup_t;

//subscriber request queue
typedef struct {
  ngx_queue_t                     queue; //this MUST be first.
  ngx_http_request_t             *request;
  ngx_http_push_subscriber_cleanup_t *clndata; 
  ngx_event_t                     event;
} ngx_http_push_subscriber_t;

typedef struct {
  ngx_queue_t                     queue;
  pid_t                           pid;
  ngx_int_t                       slot;
  ngx_http_push_subscriber_t     *subscriber_sentinel;
} ngx_http_push_pid_queue_t; 

//our typecast-friendly rbtree node (channel)
typedef struct {
  ngx_rbtree_node_t               node; //this MUST be first.
  ngx_str_t                       id;
  ngx_http_push_msg_t            *message_queue;
  ngx_uint_t                      messages;
  ngx_http_push_pid_queue_t      *workers_with_subscribers;
  ngx_uint_t                      subscribers;
  time_t                          last_seen;
  time_t                          expires;
} ngx_http_push_channel_t; 

//cleaning supplies
struct ngx_http_push_subscriber_cleanup_s {
  ngx_http_push_subscriber_t    *subscriber;
  ngx_http_push_channel_t       *channel;
  ngx_int_t                     *buf_use_count;
  ngx_buf_t                     *buf;
  ngx_chain_t                   *rchain;
  ngx_pool_t                    *rpool;
};

//garbage collecting goodness
typedef struct {
  ngx_queue_t                     queue;
  ngx_http_push_channel_t        *channel;
} ngx_http_push_channel_queue_t;

//messages to worker processes
typedef struct {
  ngx_queue_t                     queue;
  ngx_http_push_msg_t            *msg; //->shared memory
  ngx_int_t                       status_code;
  ngx_pid_t                       pid; 
  ngx_http_push_channel_t        *channel; //->shared memory
  ngx_http_push_subscriber_t     *subscriber_sentinel; //->a worker's local pool
} ngx_http_push_worker_msg_t;

typedef struct {
  ngx_queue_t                    queue;
  ngx_rwlock_t                   lock;
} ngx_http_push_worker_msg_sentinel_t;

//shared memory
typedef struct {
  ngx_rbtree_t                          tree;
  ngx_uint_t                            channels; //# of channels being used
  ngx_uint_t                            messages; //# of channels being used
  ngx_http_push_worker_msg_sentinel_t  *ipc; //interprocess stuff
} ngx_http_push_shm_data_t;

typedef struct {
  ngx_int_t                       index;
  time_t                          buffer_timeout;
  ngx_int_t                       min_messages;
  ngx_int_t                       max_messages;
  ngx_int_t                       subscriber_concurrency;
  ngx_int_t                       subscriber_poll_mechanism;
  time_t                          subscriber_timeout;
  ngx_int_t                       authorize_channel;
  ngx_int_t                       delete_oldest_received_message;
  ngx_str_t                       channel_group;
  ngx_int_t                       max_channel_id_length;
  ngx_int_t                       max_channel_subscribers;
  ngx_int_t                       ignore_queue_on_no_cache;
  time_t                          channel_timeout;
} ngx_http_push_loc_conf_t;

typedef struct {
  char *subtype;
  size_t len;
  const ngx_str_t *format;
} ngx_http_push_content_subtype_t;
