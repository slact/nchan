typedef ngx_int_t (*callback_pt)(ngx_int_t, void *, void *);

//on with the declarations
typedef struct {
  size_t                          shm_size;
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

//message queue
typedef struct {
//  ngx_queue_t                     queue; //this MUST be first.
  ngx_str_t                       content_type;
  //  ngx_str_t                   charset;
  ngx_buf_t                      *buf;
  time_t                          expires;
  ngx_uint_t                      delete_oldest_received_min_messages; //NGX_MAX_UINT32_VALUE for 'never'
  time_t                          message_time; //tag message by time
  ngx_int_t                       message_tag;  //used in conjunction with message_time if more than one message have the same time.
  unsigned                        shared:1; //for debugging
  ngx_atomic_t                    refcount;
} nchan_msg_t;


typedef struct {
  ngx_rbtree_node_t               node; //this MUST be first.
  ngx_str_t                       id;
  nchan_msg_t            *message_queue;
  ngx_atomic_t                    messages;
  ngx_atomic_t                    subscribers;
  time_t                          last_seen;
  time_t                          expires;
} nchan_channel_t;


//garbage collecting goodness
typedef struct {
  ngx_queue_t                     queue;
  nchan_channel_t        *channel;
} nchan_channel_queue_t;


typedef struct {
  ngx_queue_t                    queue;
  ngx_rwlock_t                   lock;
} nchan_worker_msg_sentinel_t;

//shared memory
typedef struct {
  ngx_rbtree_t                          tree;
  ngx_uint_t                            channels; //# of channels being used
  ngx_uint_t                            messages; //# of channels being used
  nchan_worker_msg_sentinel_t  *ipc; //interprocess stuff
} nchan_shm_data_t;

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

typedef struct {
  ngx_int_t                       index;
  time_t                          buffer_timeout;
  ngx_int_t                       min_messages;
  ngx_int_t                       max_messages;
  
  nchan_conf_publisher_types_t    pub;
  nchan_conf_subscriber_types_t   sub; 
  
  ngx_int_t                       subscriber_concurrency;
  time_t                          subscriber_timeout;
  
  ngx_int_t                       authorize_channel;
  ngx_int_t                       delete_oldest_received_message;
  ngx_str_t                       channel_group;
  ngx_int_t                       max_channel_id_length;
  ngx_int_t                       max_channel_subscribers;
  ngx_int_t                       ignore_queue_on_no_cache;
  time_t                          channel_timeout;
  ngx_str_t                       storage_engine;
  ngx_str_t                       storage_engine_name;
} nchan_loc_conf_t;

typedef struct {
  char *subtype;
  size_t len;
  const ngx_str_t *format;
} nchan_content_subtype_t;

typedef struct nchan_llist_timed_s {
  struct nchan_llist_timed_s     *prev;
  void                          *data;
  time_t                         time;
  struct nchan_llist_timed_s     *next;
} nchan_llist_timed_t;

typedef struct subscriber_s subscriber_t;
typedef enum {LONGPOLL, EVENTSOURCE, WEBSOCKET, INTERNAL} subscriber_type_t;
typedef void (*subscriber_callback_pt)(subscriber_t *, void *);
struct subscriber_s {
  ngx_int_t            (*enqueue)(struct subscriber_s *);
  ngx_int_t            (*dequeue)(struct subscriber_s *);
  ngx_int_t            (*respond_message)(struct subscriber_s *, nchan_msg_t *);
  ngx_int_t            (*respond_status)(struct subscriber_s *, ngx_int_t, const ngx_str_t *);
  ngx_int_t            (*set_timeout_callback)(subscriber_t *self, subscriber_callback_pt cb, void *privdata);
  ngx_int_t            (*set_dequeue_callback)(subscriber_t *self, subscriber_callback_pt cb, void *privdata);
  ngx_int_t            (*reserve)(struct subscriber_s *);
  ngx_int_t            (*release)(struct subscriber_s *);
  const char          *name;
  subscriber_type_t    type;
  unsigned             dequeue_after_response:1;
  unsigned             destroy_after_dequeue:1;
  nchan_loc_conf_t *cf;
  void                *data;
}; //subscriber_t