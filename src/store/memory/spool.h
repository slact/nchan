#ifndef SPOOL_HEADER
#define SPOOL_HEADER

typedef enum {SHORTLIVED, PERSISTENT} spool_type_t;

typedef struct spooled_subscriber_s spooled_subscriber_t;
typedef struct subscriber_pool_s subscriber_pool_t;

typedef struct {
  spooled_subscriber_t  *ssub;
  subscriber_pool_t     *spool;
} spooled_subscriber_cleanup_t;
  
struct spooled_subscriber_s {
  ngx_uint_t                    id; //could be useful later
  subscriber_t                 *sub;
  spooled_subscriber_cleanup_t  dequeue_callback_data;
  spooled_subscriber_t         *next;
  spooled_subscriber_t         *prev;
}; //spooled_subscriber_t


struct subscriber_pool_s{
  spool_type_t          type;
  ngx_uint_t            responded_count;
  spooled_subscriber_t *first;
  ngx_pool_t           *pool;
  ngx_uint_t            sub_count;
  nhpm_llist_timed_t    cleanlink; //unused for now

  ngx_int_t             (*add)(subscriber_pool_t *self, subscriber_t *sub);
  ngx_int_t             (*respond_message)(subscriber_pool_t *self, ngx_http_push_msg_t *msg);
  ngx_int_t             (*respond_status)(subscriber_pool_t *self, ngx_int_t status_code, const ngx_str_t *status_line);
  ngx_int_t             (*set_dequeue_handler)(subscriber_pool_t *, void (*cb)(subscriber_pool_t *, ngx_int_t, void*), void*);
  void                  (*dequeue_handler)(subscriber_pool_t *, ngx_int_t, void *); //called after dequeueing 1 or many subs
  void                 *dequeue_handler_privdata;
}; // subscriber_pool_t

typedef struct channel_spooler_s channel_spooler_t; //holds many different spools

struct channel_spooler_s {
  subscriber_pool_t     *shortlived;
  subscriber_pool_t     *persistent;
  ngx_uint_t             responded_count;
  ngx_atomic_t          *shared_sub_count;
  ngx_int_t              (*add)(channel_spooler_t *self, subscriber_t *sub);
  ngx_int_t              (*respond_message)(channel_spooler_t *self, ngx_http_push_msg_t *msg);
  ngx_int_t              (*respond_status)(channel_spooler_t *self, ngx_int_t status_code, const ngx_str_t *status_line);

  ngx_int_t              (*set_dequeue_handler)(channel_spooler_t *, void (*cb)(channel_spooler_t *, ngx_int_t, void*), void*);
  void                   (*dequeue_handler)(channel_spooler_t *, ngx_int_t, void *); //called after dequeueing 1 or many subs
  void                  *dequeue_handler_privdata;
  
}; //nhpm_channel_head_spooler_t


channel_spooler_t *start_spooler(channel_spooler_t *spl);
ngx_int_t stop_spooler(channel_spooler_t *spl);


#endif