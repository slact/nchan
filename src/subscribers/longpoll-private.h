typedef struct {
  ngx_http_cleanup_t      *cln;
  subscriber_callback_pt  dequeue_handler;
  void                   *dequeue_handler_data;
  ngx_event_t             timeout_ev;
  subscriber_callback_pt  timeout_handler;
  void                   *timeout_handler_data;
  ngx_int_t               owner;
  unsigned                holding:1;
  unsigned                finalize_request:1;
  unsigned                already_responded:1;
  unsigned                awaiting_destruction:1;
  unsigned                shook_hands:1;
} subscriber_data_t;

typedef struct {
  subscriber_t       sub;
  subscriber_data_t  data;
} full_subscriber_t;

ngx_int_t longpoll_enqueue(subscriber_t *self);