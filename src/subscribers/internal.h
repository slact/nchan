#define NCHAN_DEFAULT_INTERNAL_SUBSCRIBER_POOL_SIZE 1024

typedef struct {
  subscriber_t            sub;
  callback_pt             enqueue;
  callback_pt             dequeue;
  callback_pt             respond_message;
  callback_pt             respond_status;
  ngx_event_t             timeout_ev;
  subscriber_callback_pt  timeout_handler;
  void                   *timeout_handler_data;
  subscriber_callback_pt  dequeue_handler;
  void                   *dequeue_handler_data;
  void                   *privdata;
  ngx_int_t               owner;
  unsigned                already_dequeued:1;
  unsigned                awaiting_destruction:1;
} internal_subscriber_t;

extern subscriber_t *internal_subscriber_create(const char*, void *privdata);
extern ngx_int_t internal_subscriber_destroy(subscriber_t *sub);

ngx_int_t internal_subscriber_set_name(subscriber_t *sub, const char *name);
ngx_int_t internal_subscriber_set_enqueue_handler(subscriber_t *sub, callback_pt handler);
ngx_int_t internal_subscriber_set_dequeue_handler(subscriber_t *sub, callback_pt handler);
ngx_int_t internal_subscriber_set_respond_message_handler(subscriber_t *sub, callback_pt handler);
ngx_int_t internal_subscriber_set_respond_status_handler(subscriber_t *sub, callback_pt handler);
void *internal_subscriber_get_privdata(subscriber_t *sub);