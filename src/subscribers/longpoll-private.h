typedef struct nchan_longpoll_multimsg_s nchan_longpoll_multimsg_t;
struct nchan_longpoll_multimsg_s {
  nchan_msg_t                   *msg;
  nchan_longpoll_multimsg_t     *next;
};

typedef struct {
  ngx_http_cleanup_t      *cln;
  subscriber_callback_pt  dequeue_handler;
  void                   *dequeue_handler_data;
  ngx_event_t             timeout_ev;
  ngx_event_t             keep_alive_ev;
  
  nchan_longpoll_multimsg_t *multimsg_first;
  nchan_longpoll_multimsg_t *multimsg_last;
  
  unsigned                act_as_intervalpoll:1;
  unsigned                holding:1;
  unsigned                finalize_request:1;
  unsigned                already_responded:1;
  unsigned                awaiting_destruction:1;
  unsigned                shook_hands:1;
} subscriber_data_t;

typedef struct {
  subscriber_t       sub;
  subscriber_data_t  data;
  void              *privdata;
} full_subscriber_t;

ngx_int_t longpoll_enqueue(subscriber_t *self);

void subscriber_maybe_dequeue_after_status_response(full_subscriber_t *fsub, ngx_int_t status_code);

ngx_int_t subscriber_respond_unqueued_status(full_subscriber_t *fsub, ngx_int_t status_code, const ngx_str_t *status_line);
