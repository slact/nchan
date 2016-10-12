#define NCHAN_DEFAULT_INTERNAL_SUBSCRIBER_POOL_SIZE 1024

typedef struct {
  subscriber_t            sub;
  callback_pt             enqueue;
  callback_pt             dequeue;
  callback_pt             respond_message;
  callback_pt             respond_status;
  callback_pt             notify;
  callback_pt             destroy;
  ngx_event_t             timeout_ev;
  subscriber_callback_pt  dequeue_handler;
  void                   *dequeue_handler_data;
  void                   *privdata;
  unsigned                already_dequeued:1;
  unsigned                awaiting_destruction:1;
} internal_subscriber_t;

extern subscriber_t *internal_subscriber_create(ngx_str_t *, nchan_loc_conf_t *cf, size_t pd_sz, void **pd);
extern ngx_int_t internal_subscriber_destroy(subscriber_t *sub);

subscriber_t *internal_subscriber_create_init(ngx_str_t *sub_name, nchan_loc_conf_t *cf, size_t pd_sz, void **pd, callback_pt enqueue, callback_pt dequeue, callback_pt respond_message, callback_pt respond_status, callback_pt notify_handler, callback_pt destroy_handler);
ngx_int_t internal_subscriber_set_name(subscriber_t *sub, ngx_str_t *name);
ngx_int_t internal_subscriber_set_enqueue_handler(subscriber_t *sub, callback_pt handler);
ngx_int_t internal_subscriber_set_dequeue_handler(subscriber_t *sub, callback_pt handler);
ngx_int_t internal_subscriber_set_notify_handler(subscriber_t *sub, callback_pt handler);
ngx_int_t internal_subscriber_set_respond_message_handler(subscriber_t *sub, callback_pt handler);
ngx_int_t internal_subscriber_set_respond_status_handler(subscriber_t *sub, callback_pt handler);
ngx_int_t internal_subscriber_set_destroy_handler(subscriber_t *sub, callback_pt handler);
void *internal_subscriber_get_privdata(subscriber_t *sub);
