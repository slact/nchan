static ngx_int_t ngx_http_push_store_redis_init_module(ngx_cycle_t *cycle) {
  return NGX_OK;
}

static ngx_int_t ngx_http_push_store_redis_init_worker(ngx_cycle_t *cycle) {
  ngx_http_push_redis = redisAsyncConnect("127.0.0.1", 6379);
  return NGX_OK;
}


ngx_http_push_store_t  ngx_http_push_store_redis = {
    //init
    &ngx_http_push_store_redis_init_module,
    &ngx_http_push_store_redis_init_worker,
    NULL,//&ngx_http_push_store_redis_init_postconfig,
    NULL,//&ngx_http_push_store_redis_create_main_conf,
    
    //shutdown
    NULL,//&ngx_http_push_store_redis_exit_worker,
    NULL,//&ngx_http_push_store_redis_exit_master,
  
    NULL,//&ngx_http_push_store_redis_get_channel, //creates channel if not found
    NULL,//&ngx_http_push_store_redis_find_channel, //returns channel or NULL if not found
    NULL,//&ngx_http_push_store_redis_get_message,
    NULL,//&ngx_http_push_store_redis_reserve_message,
    NULL,//&ngx_http_push_store_redis_release_message,
    
    //pub/sub
    NULL,
    NULL,
    
    //channel properties
    NULL,//&ngx_http_push_store_channel_subscribers,
};
