ngx_module_t  ngx_http_push_module;

static ngx_int_t ngx_http_push_init_module(ngx_cycle_t *cycle) {
  ngx_core_conf_t                *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  ngx_http_push_worker_processes = ccf->worker_processes;
  //initialize subscriber queues
  //pool, please
  if((ngx_http_push_pool = ngx_create_pool(NGX_CYCLE_POOL_SIZE, cycle->log))==NULL) { //I trust the cycle pool size to be a well-tuned one.
    return NGX_ERROR; 
  }
  
  //initialize storage engine
  return ngx_http_push_store->init_module(cycle);
}

static ngx_int_t ngx_http_push_init_worker(ngx_cycle_t *cycle) {
  if (ngx_process != NGX_PROCESS_WORKER) {
    //not a worker, stop initializing stuff.
    return NGX_OK;
  }
  
  if(ngx_http_push_store->init_worker(cycle)!=NGX_OK) {
    return NGX_ERROR;
  }
  return NGX_OK;
}

static ngx_int_t ngx_http_push_postconfig(ngx_conf_t *cf) {
  return ngx_http_push_store->init_postconfig(cf);
}

//main config
static void * ngx_http_push_create_main_conf(ngx_conf_t *cf) {
  ngx_http_push_main_conf_t      *mcf = ngx_pcalloc(cf->pool, sizeof(*mcf));
  if(mcf == NULL) {
    return NGX_CONF_ERROR;
  }
  
  ngx_http_push_store->create_main_conf(cf, mcf);
  
  return mcf;
}

//location config stuff
static void *ngx_http_push_create_loc_conf(ngx_conf_t *cf) {
  ngx_http_push_loc_conf_t       *lcf = ngx_pcalloc(cf->pool, sizeof(*lcf));
  if(lcf == NULL) {
    return NGX_CONF_ERROR;
  }
  lcf->buffer_timeout=NGX_CONF_UNSET;
  lcf->max_messages=NGX_CONF_UNSET;
  lcf->min_messages=NGX_CONF_UNSET;
  lcf->subscriber_concurrency=NGX_CONF_UNSET;
  lcf->subscriber_poll_mechanism=NGX_CONF_UNSET;
  lcf->subscriber_timeout=NGX_CONF_UNSET;
  lcf->authorize_channel=NGX_CONF_UNSET;
  lcf->delete_oldest_received_message=NGX_CONF_UNSET;
  lcf->max_channel_id_length=NGX_CONF_UNSET;
  lcf->max_channel_subscribers=NGX_CONF_UNSET;
  lcf->ignore_queue_on_no_cache=NGX_CONF_UNSET;
  lcf->channel_timeout=NGX_CONF_UNSET;
  lcf->channel_group.data=NULL;
  return lcf;
}

static char *  ngx_http_push_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child) {
  ngx_http_push_loc_conf_t       *prev = parent, *conf = child;
  ngx_conf_merge_sec_value(conf->buffer_timeout, prev->buffer_timeout, NGX_HTTP_PUSH_DEFAULT_BUFFER_TIMEOUT);
  ngx_conf_merge_value(conf->max_messages, prev->max_messages, NGX_HTTP_PUSH_DEFAULT_MAX_MESSAGES);
  ngx_conf_merge_value(conf->min_messages, prev->min_messages, NGX_HTTP_PUSH_DEFAULT_MIN_MESSAGES);
  ngx_conf_merge_value(conf->subscriber_concurrency, prev->subscriber_concurrency, NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_BROADCAST);
  ngx_conf_merge_value(conf->subscriber_poll_mechanism, prev->subscriber_poll_mechanism, NGX_HTTP_PUSH_MECHANISM_LONGPOLL);
  ngx_conf_merge_sec_value(conf->subscriber_timeout, prev->subscriber_timeout, NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_TIMEOUT);
  ngx_conf_merge_value(conf->authorize_channel, prev->authorize_channel, 0);
  ngx_conf_merge_value(conf->delete_oldest_received_message, prev->delete_oldest_received_message, 0);
  ngx_conf_merge_value(conf->max_channel_id_length, prev->max_channel_id_length, NGX_HTTP_PUSH_MAX_CHANNEL_ID_LENGTH);
  ngx_conf_merge_value(conf->max_channel_subscribers, prev->max_channel_subscribers, 0);
  ngx_conf_merge_value(conf->ignore_queue_on_no_cache, prev->ignore_queue_on_no_cache, 0);
  ngx_conf_merge_value(conf->channel_timeout, prev->channel_timeout, NGX_HTTP_PUSH_DEFAULT_CHANNEL_TIMEOUT);
  ngx_conf_merge_str_value(conf->channel_group, prev->channel_group, "");
  
  //sanity checks
  if(conf->max_messages < conf->min_messages) {
    //min/max buffer size makes sense?
    ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "push_max_message_buffer_length cannot be smaller than push_min_message_buffer_length.");
    return NGX_CONF_ERROR;
  }
  
  return NGX_CONF_OK;
}

static ngx_str_t  ngx_http_push_channel_id = ngx_string("push_channel_id"); //channel id variable
//publisher and subscriber handlers now.
static char *ngx_http_push_setup_handler(ngx_conf_t *cf, void * conf, ngx_int_t (*handler)(ngx_http_request_t *)) {
  ngx_http_core_loc_conf_t       *clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
  ngx_http_push_loc_conf_t       *plcf = conf;
  clcf->handler = handler;
  clcf->if_modified_since = NGX_HTTP_IMS_OFF;
  plcf->index = ngx_http_get_variable_index(cf, &ngx_http_push_channel_id);
  if (plcf->index == NGX_ERROR) {
    return NGX_CONF_ERROR;
  }
  return NGX_CONF_OK;
}

typedef struct {
  char                           *str;
  ngx_int_t                       val;
} ngx_http_push_strval_t;

static ngx_int_t ngx_http_push_strval(ngx_str_t string, ngx_http_push_strval_t strval[], ngx_int_t strval_len, ngx_int_t *val) {
  ngx_int_t                      i;
  for(i=0; i<strval_len; i++) {
    if(ngx_strncasecmp(string.data, (u_char *)strval[i].str, string.len)==0) {
      *val = strval[i].val;
      return NGX_OK;
    }
  }
  return NGX_DONE; //nothing matched
}

static char *ngx_http_push_set_subscriber_concurrency(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  static ngx_http_push_strval_t  concurrency[] = {
    { "first"    , NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_FIRSTIN   },
    { "last"     , NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_LASTIN    },
    { "broadcast", NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_BROADCAST }
  };
  ngx_int_t                      *field = (ngx_int_t *) ((char *) conf + cmd->offset);
  
  if (*field != NGX_CONF_UNSET) {
    return "is duplicate";
  }
  
  ngx_str_t                   value = (((ngx_str_t *) cf->args->elts)[1]);
  if(ngx_http_push_strval(value, concurrency, 3, field)!=NGX_OK) {
    ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "invalid push_subscriber_concurrency value: %V", &value);
    return NGX_CONF_ERROR;
  }

  return NGX_CONF_OK;
}

static char *ngx_http_push_publisher(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  return ngx_http_push_setup_handler(cf, conf, &ngx_http_push_publisher_handler);
}

static char *ngx_http_push_subscriber(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  static ngx_http_push_strval_t  mech[] = {
    { "interval-poll", NGX_HTTP_PUSH_MECHANISM_INTERVALPOLL },
    { "long-poll"    , NGX_HTTP_PUSH_MECHANISM_LONGPOLL     }  
  };
  ngx_int_t                      *field = (ngx_int_t *) ((char *) conf + cmd->offset);
  if (*field != NGX_CONF_UNSET) {
    return "is duplicate";
  }
  
  if(cf->args->nelts==1) { //no argument given
    *field = NGX_HTTP_PUSH_MECHANISM_LONGPOLL; //default
  }
  else {
    ngx_str_t                   value = (((ngx_str_t *) cf->args->elts)[1]);
    if(ngx_http_push_strval(value, mech, 2, field)!=NGX_OK) {
      ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "invalid push_subscriber value: %V", &value);
      return NGX_CONF_ERROR;
    }
  }
  
  return ngx_http_push_setup_handler(cf, conf, &ngx_http_push_subscriber_handler);
}

static void ngx_http_push_exit_worker(ngx_cycle_t *cycle) {
  ngx_http_push_store->exit_worker(cycle);
}

static void ngx_http_push_exit_master(ngx_cycle_t *cycle) {
  ngx_http_push_store->exit_master(cycle);
}

static char *ngx_http_push_set_message_buffer_length(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  char                           *p = conf;
  ngx_int_t                      *min, *max;
  ngx_str_t                      *value;
  ngx_int_t                       intval;
  min = (ngx_int_t *) (p + offsetof(ngx_http_push_loc_conf_t, min_messages));
  max = (ngx_int_t *) (p + offsetof(ngx_http_push_loc_conf_t, max_messages));
  if(*min != NGX_CONF_UNSET || *max != NGX_CONF_UNSET) {
    return "is duplicate";
  }
  value = cf->args->elts;
  if((intval = ngx_atoi(value[1].data, value[1].len))==NGX_ERROR) {
    return "invalid number";
  }
  *min = intval;
  *max = intval;
  
  return NGX_CONF_OK;
}

static char *ngx_http_push_store_messages_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  char    *p = conf;
  ngx_str_t *val = cf->args->elts;

  
  if (ngx_strcasecmp(val[1].data, (u_char *) "off") == 0) {
    ngx_int_t *min, *max;
    min = (ngx_int_t *) (p + offsetof(ngx_http_push_loc_conf_t, min_messages));
    max = (ngx_int_t *) (p + offsetof(ngx_http_push_loc_conf_t, max_messages));
    *min=0;
    *max=0;
  }
  return NGX_CONF_OK;
}

static ngx_command_t  ngx_http_push_commands[] = {

    { ngx_string("push_message_timeout"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_sec_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, buffer_timeout),
      NULL },

    { ngx_string("push_max_reserved_memory"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_push_main_conf_t, shm_size),
      NULL },
    
  { ngx_string("push_min_message_buffer_length"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, min_messages),
      NULL },
  
  { ngx_string("push_max_message_buffer_length"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, max_messages),
      NULL },
    
  { ngx_string("push_message_buffer_length"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_push_set_message_buffer_length,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },
    
  { ngx_string("push_delete_oldest_received_message"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_flag_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, delete_oldest_received_message),
      NULL },

  { ngx_string("push_publisher"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_push_publisher,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },
  
  { ngx_string("push_subscriber"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS|NGX_CONF_TAKE1,
      ngx_http_push_subscriber,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, subscriber_poll_mechanism),
      NULL },
  
    { ngx_string("push_subscriber_concurrency"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_push_set_subscriber_concurrency,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, subscriber_concurrency),
      NULL },
  
    { ngx_string("push_subscriber_timeout"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_sec_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, subscriber_timeout),
      NULL },
    
  { ngx_string("push_authorized_channels_only"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_flag_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, authorize_channel),
      NULL },
    
  { ngx_string("push_store_messages"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_push_store_messages_directive,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },
    
  { ngx_string("push_channel_group"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, channel_group),
      NULL },
    
  { ngx_string("push_max_channel_id_length"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, max_channel_id_length),
      NULL },
    
  { ngx_string("push_max_channel_subscribers"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_num_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, max_channel_subscribers),
      NULL },
    { ngx_string("push_ignore_queue_on_no_cache"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_flag_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, ignore_queue_on_no_cache),
      NULL },

    { ngx_string("push_channel_timeout"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_sec_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_push_loc_conf_t, channel_timeout),
      NULL },

    ngx_null_command
};

static ngx_http_module_t  ngx_http_push_module_ctx = {
    NULL,                                  /* preconfiguration */
    ngx_http_push_postconfig,              /* postconfiguration */
    ngx_http_push_create_main_conf,        /* create main configuration */
    NULL,                                  /* init main configuration */
    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */
    ngx_http_push_create_loc_conf,         /* create location configuration */
    ngx_http_push_merge_loc_conf,          /* merge location configuration */
};

ngx_module_t  ngx_http_push_module = {
    NGX_MODULE_V1,
    &ngx_http_push_module_ctx,             /* module context */
    ngx_http_push_commands,                /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    ngx_http_push_init_module,             /* init module */
    ngx_http_push_init_worker,             /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    ngx_http_push_exit_worker,             /* exit process */
    ngx_http_push_exit_master,             /* exit master */
    NGX_MODULE_V1_PADDING
};
