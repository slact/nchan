typedef ngx_int_t (*callback_pt)(ngx_int_t, void *, void *);
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
  ngx_int_t (*get_message) (ngx_str_t *, ngx_http_push_msg_id_t *, callback_pt, void *);
  ngx_int_t (*subscribe)   (ngx_str_t *, ngx_http_push_msg_id_t *, ngx_http_push_loc_conf_t *, callback_pt, void *);
  ngx_int_t (*publish)     (ngx_str_t *, ngx_http_push_msg_t *, ngx_http_push_loc_conf_t *, callback_pt, void *);
  ngx_int_t (*delete_channel)(ngx_str_t *channel_id);
  
  //channel actions
  ngx_http_push_channel_t *(*find_channel)(ngx_str_t *, time_t, callback_pt, void*);
  
  
  
  //message actions and properties
  ngx_str_t * (*message_etag)(ngx_http_push_msg_t *msg, ngx_pool_t *pool);
  ngx_str_t * (*message_content_type)(ngx_http_push_msg_t *msg, ngx_pool_t *pool);
  
  //ipc
  ngx_int_t (*send_worker_message)(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *subscriber_sentinel, ngx_pid_t pid, ngx_int_t worker_slot, ngx_http_push_msg_t *msg, ngx_int_t status_code);
  void (*receive_worker_message)(void);
} ngx_http_push_store_t;

