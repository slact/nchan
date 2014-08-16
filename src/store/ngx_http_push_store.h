typedef struct {
  //init
  ngx_int_t (*init_module)(ngx_cycle_t *cycle);
  ngx_int_t (*init_worker)(ngx_cycle_t *cycle);
  ngx_int_t (*init_postconfig)(ngx_conf_t *cf);
  void      (*create_main_conf)(ngx_conf_t *cf, ngx_http_push_main_conf_t *mcf);
  
  //quit
  void      (*exit_worker)(ngx_cycle_t *cycle);
  void      (*exit_master)(ngx_cycle_t *cycle);
  
  
  //async-friendly functions with callbacks
  ngx_http_push_msg_t * (*get_message) (ngx_str_t *channel_id, ngx_http_push_msg_id_t *msg_id, ngx_int_t *msg_search_outcome, ngx_http_request_t *r, ngx_int_t (*callback)(ngx_http_push_msg_t *msg, ngx_int_t msg_search_outcome, ngx_http_request_t *r));
  ngx_int_t             (*subscribe)   (ngx_str_t *channel_id, ngx_http_push_msg_id_t *msg_id, ngx_http_request_t *r, ngx_int_t (*callback)(ngx_int_t status, ngx_http_request_t *r));
  ngx_int_t             (*publish)     (ngx_str_t *channel_id, ngx_http_request_t *r, ngx_int_t (*callback)(ngx_int_t status, ngx_http_push_channel_t *ch, ngx_http_request_t *r));
  
  //channel actions
  ngx_http_push_channel_t *(*get_channel)(ngx_str_t *id, time_t channel_timeout, ngx_int_t (*callback)(ngx_http_push_channel_t *channel));
  ngx_http_push_channel_t *(*find_channel)(ngx_str_t *id, time_t channel_timeout, ngx_int_t (*callback)(ngx_http_push_channel_t *channel));
  
  ngx_int_t (*delete_channel)(ngx_str_t *channel_id);
  ngx_http_push_msg_t *(*get_channel_message)(ngx_http_push_channel_t *channel, ngx_http_push_msg_id_t *msgid, ngx_int_t *msg_search_outcome, ngx_http_push_loc_conf_t *cf);
  
  void (*reserve_message)(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg);
  void (*release_message)(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg);
  
  //channel properties
  ngx_int_t (*channel_subscribers)(ngx_http_push_channel_t * channel);
  ngx_int_t (*channel_worker_subscribers)(ngx_http_push_subscriber_t * worker_sentinel);
  ngx_http_push_subscriber_t *(*next_subscriber)(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *sentinel, ngx_http_push_subscriber_t *cur, int release_previous);
  ngx_int_t (*release_subscriber_sentinel)(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *sentinel);
  
  void (*lock)(void); //legacy shared-memory store helpers
  void (*unlock)(void);
  void * (*alloc_locked)(size_t size, char * label);
  void (*free_locked)(void *ptr);
  
  //message actions and properties
  ngx_http_push_msg_t * (*create_message)(ngx_http_push_channel_t *channel, ngx_http_request_t *r);
  ngx_int_t (*delete_message)(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t force);
  ngx_int_t (*delete_message_locked)(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t force);
  ngx_int_t (*enqueue_message)(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_http_push_loc_conf_t *cf);
  ngx_str_t * (*message_etag)(ngx_http_push_msg_t *msg, ngx_pool_t *pool);
  ngx_str_t * (*message_content_type)(ngx_http_push_msg_t *msg, ngx_pool_t *pool);
  
  //ipc
  ngx_int_t (*send_worker_message)(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *subscriber_sentinel, ngx_pid_t pid, ngx_int_t worker_slot, ngx_http_push_msg_t *msg, ngx_int_t status_code);
  void (*receive_worker_message)(void);
} ngx_http_push_store_t;

