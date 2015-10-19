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
  ngx_int_t (*subscribe)   (ngx_str_t *, ngx_http_push_msg_id_t *, subscriber_t *, callback_pt, void *);
  ngx_int_t (*publish)     (ngx_str_t *, ngx_http_push_msg_t *, ngx_http_push_loc_conf_t *, callback_pt, void *);
  
  ngx_int_t (*delete_channel)(ngx_str_t *, callback_pt, void *);
  
  //channel actions
  ngx_int_t (*find_channel)(ngx_str_t *, callback_pt, void*);
  
  
  
  //message actions and properties
  ngx_str_t * (*message_etag)(ngx_http_push_msg_t *msg, ngx_pool_t *pool);
  ngx_str_t * (*message_content_type)(ngx_http_push_msg_t *msg, ngx_pool_t *pool);

} nchan_store_t;