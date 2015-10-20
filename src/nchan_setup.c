ngx_module_t  nchan_module;

static ngx_int_t nchan_init_module(ngx_cycle_t *cycle) {
  ngx_core_conf_t                *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  nchan_worker_processes = ccf->worker_processes;
  //initialize subscriber queues
  //pool, please
  if((nchan_pool = ngx_create_pool(NGX_CYCLE_POOL_SIZE, cycle->log))==NULL) { //I trust the cycle pool size to be a well-tuned one.
    return NGX_ERROR; 
  }
  
  //initialize storage engines
  return nchan_store->init_module(cycle);
}

static ngx_int_t nchan_init_worker(ngx_cycle_t *cycle) {
  if (ngx_process != NGX_PROCESS_WORKER) {
    //not a worker, stop initializing stuff.
    return NGX_OK;
  }
  
  if(nchan_store->init_worker(cycle)!=NGX_OK) {
    return NGX_ERROR;
  }
  return NGX_OK;
}

static ngx_int_t nchan_postconfig(ngx_conf_t *cf) {
  return nchan_store->init_postconfig(cf);
}

//main config
static void * nchan_create_main_conf(ngx_conf_t *cf) {
  nchan_main_conf_t      *mcf = ngx_pcalloc(cf->pool, sizeof(*mcf));
  if(mcf == NULL) {
    return NGX_CONF_ERROR;
  }
  
  nchan_store->create_main_conf(cf, mcf);
  
  return mcf;
}

//location config stuff
static void *nchan_create_loc_conf(ngx_conf_t *cf) {
  nchan_loc_conf_t       *lcf = ngx_pcalloc(cf->pool, sizeof(*lcf));
  if(lcf == NULL) {
    return NGX_CONF_ERROR;
  }
  
  lcf->pub.http=0;
  lcf->pub.websocket=0;
  
  lcf->sub.poll=0;
  lcf->sub.longpoll=0;
  lcf->sub.eventsource=0;
  lcf->sub.websocket=0;
  
  lcf->buffer_timeout=NGX_CONF_UNSET;
  lcf->max_messages=NGX_CONF_UNSET;
  lcf->min_messages=NGX_CONF_UNSET;
  lcf->subscriber_concurrency=NGX_CONF_UNSET;
  
  lcf->subscriber_timeout=NGX_CONF_UNSET;
  lcf->authorize_channel=NGX_CONF_UNSET;
  lcf->delete_oldest_received_message=NGX_CONF_UNSET;
  lcf->max_channel_id_length=NGX_CONF_UNSET;
  lcf->max_channel_subscribers=NGX_CONF_UNSET;
  lcf->ignore_queue_on_no_cache=NGX_CONF_UNSET;
  lcf->channel_timeout=NGX_CONF_UNSET;
  lcf->channel_group.data=NULL;
  lcf->storage_engine.data=NULL;
  return lcf;
}

static ngx_int_t nchan_strmatch(ngx_str_t *val, ngx_int_t n, ...) {
  u_char   *match;
  va_list   args;
  ngx_int_t i;
  va_start(args, n);  
  for(i=0; i<n; i++) {
    match = va_arg(args, u_char *);
    if(ngx_strncasecmp(val->data, match, val->len)==0) {
      return 1;
    }
  }
  va_end(args);
  return 0;
}

static char *  nchan_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child) {
  nchan_loc_conf_t       *prev = parent, *conf = child;
  
  //publisher types
  ngx_conf_merge_bitmask_value(conf->pub.http, prev->pub.http, 0);
  ngx_conf_merge_bitmask_value(conf->pub.websocket, prev->pub.websocket, 0);
  
  //subscriber types
  ngx_conf_merge_bitmask_value(conf->sub.poll, prev->sub.poll, 0);
  ngx_conf_merge_bitmask_value(conf->sub.longpoll, prev->sub.longpoll, 0);
  ngx_conf_merge_bitmask_value(conf->sub.eventsource, prev->sub.eventsource, 0);
  ngx_conf_merge_bitmask_value(conf->sub.websocket, prev->sub.websocket, 0);
  
  ngx_conf_merge_sec_value(conf->buffer_timeout, prev->buffer_timeout, NCHAN_DEFAULT_BUFFER_TIMEOUT);
  ngx_conf_merge_value(conf->max_messages, prev->max_messages, NCHAN_DEFAULT_MAX_MESSAGES);
  ngx_conf_merge_value(conf->min_messages, prev->min_messages, NCHAN_DEFAULT_MIN_MESSAGES);
  ngx_conf_merge_value(conf->subscriber_concurrency, prev->subscriber_concurrency, NCHAN_SUBSCRIBER_CONCURRENCY_BROADCAST);
  
  ngx_conf_merge_sec_value(conf->subscriber_timeout, prev->subscriber_timeout, NCHAN_DEFAULT_SUBSCRIBER_TIMEOUT);
  ngx_conf_merge_value(conf->authorize_channel, prev->authorize_channel, 0);
  ngx_conf_merge_value(conf->delete_oldest_received_message, prev->delete_oldest_received_message, 0);
  ngx_conf_merge_value(conf->max_channel_id_length, prev->max_channel_id_length, NCHAN_MAX_CHANNEL_ID_LENGTH);
  ngx_conf_merge_value(conf->max_channel_subscribers, prev->max_channel_subscribers, 0);
  ngx_conf_merge_value(conf->ignore_queue_on_no_cache, prev->ignore_queue_on_no_cache, 0);
  ngx_conf_merge_value(conf->channel_timeout, prev->channel_timeout, NCHAN_DEFAULT_CHANNEL_TIMEOUT);
  ngx_conf_merge_str_value(conf->channel_group, prev->channel_group, "");
  ngx_conf_merge_str_value(conf->storage_engine, prev->storage_engine, "");
  
  //sanity checks
  if(conf->max_messages < conf->min_messages) {
    //min/max buffer size makes sense?
    ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "push_max_message_buffer_length cannot be smaller than push_min_message_buffer_length.");
    return NGX_CONF_ERROR;
  }
  
  return NGX_CONF_OK;
}

static ngx_str_t  nchan_channel_id = ngx_string("push_channel_id"); //channel id variable
//publisher and subscriber handlers now.
static char *nchan_setup_handler(ngx_conf_t *cf, void * conf, ngx_int_t (*handler)(ngx_http_request_t *)) {
  ngx_http_core_loc_conf_t       *clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
  nchan_loc_conf_t       *plcf = conf;
  clcf->handler = handler;
  clcf->if_modified_since = NGX_HTTP_IMS_OFF;
  plcf->index = ngx_http_get_variable_index(cf, &nchan_channel_id);
  if (plcf->index == NGX_ERROR) {
    return NGX_CONF_ERROR;
  }
  return NGX_CONF_OK;
}

typedef struct {
  char                           *str;
  ngx_int_t                       val;
} nchan_strval_t;

static ngx_int_t nchan_strval(ngx_str_t string, nchan_strval_t strval[], ngx_int_t *val) {
  ngx_int_t                      i;
  for(i=0; &strval[i] != NULL; i++) {
    if(ngx_strncasecmp(string.data, (u_char *)strval[i].str, string.len)==0) {
      *val = strval[i].val;
      return NGX_OK;
    }
  }
  return NGX_DONE; //nothing matched
}

static char *nchan_set_subscriber_concurrency(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  static nchan_strval_t  concurrency[] = {
    { "first"    , NCHAN_SUBSCRIBER_CONCURRENCY_FIRSTIN   },
    { "last"     , NCHAN_SUBSCRIBER_CONCURRENCY_LASTIN    },
    { "broadcast", NCHAN_SUBSCRIBER_CONCURRENCY_BROADCAST },
    {NULL}
  };
  ngx_int_t                      *field = (ngx_int_t *) ((char *) conf + cmd->offset);
  
  if (*field != NGX_CONF_UNSET) {
    return "is duplicate";
  }
  
  ngx_str_t                   value = (((ngx_str_t *) cf->args->elts)[1]);
  if(nchan_strval(value, concurrency,field)!=NGX_OK) {
    ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "invalid push_subscriber_concurrency value: %V", &value);
    return NGX_CONF_ERROR;
  }

  return NGX_CONF_OK;
}

static char *nchan_set_storage_engine(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  static nchan_strval_t  concurrency[] = {
    { "first"    , NCHAN_SUBSCRIBER_CONCURRENCY_FIRSTIN   },
    { "last"     , NCHAN_SUBSCRIBER_CONCURRENCY_LASTIN    },
    { "broadcast", NCHAN_SUBSCRIBER_CONCURRENCY_BROADCAST },
    {NULL}
  };
  ngx_int_t                      *field = (ngx_int_t *) ((char *) conf + cmd->offset);
  
  if (*field != NGX_CONF_UNSET) {
    return "is duplicate";
  }
  
  ngx_str_t                   value = (((ngx_str_t *) cf->args->elts)[1]);
  if(nchan_strval(value, concurrency, field)!=NGX_OK) {
    ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "invalid push_subscriber_concurrency value: %V", &value);
    return NGX_CONF_ERROR;
  }

  return NGX_CONF_OK;
}


static char *nchan_publisher(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  return nchan_setup_handler(cf, conf, &nchan_publisher_handler);
}



static char *nchan_subscriber(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t     *lcf = conf;
  ngx_str_t            *val;
  ngx_int_t             i;
  
  nchan_conf_subscriber_types_t *subt = &lcf->sub;
  
  if(cf->args->nelts == 1){ //no arguments
    subt->poll=0;
    subt->longpoll=1;
    subt->websocket=1;
    subt->eventsource=1;
  }
  else {
    for(i=1; i < cf->args->nelts; i++) {
      val = &((ngx_str_t *) cf->args->elts)[i];
      if(nchan_strmatch(val, 2, "longpoll", "long-poll")) {
        subt->longpoll=1;
      }
      else if(nchan_strmatch(val, 4, "poll", "interval-poll", "intervalpoll", "http")) {
        subt->poll=1;
      }
      else if(nchan_strmatch(val, 3, "websocket", "websockets", "ws")) {
        subt->websocket=1;
      }
      else if(nchan_strmatch(val, 4, "eventsource", "event-source", "es", "sse")) {
        subt->eventsource=1;
      }
      else if(nchan_strmatch(val, 2, "none", "off")) {
        subt->poll=0;
        subt->longpoll=0;
        subt->websocket=0;
        subt->eventsource=0;
      }
      else {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "invalid %V value: %V", &cmd->name, val);
        return NGX_CONF_ERROR;
      }
    }
  }
  return nchan_setup_handler(cf, conf, &nchan_subscriber_handler);
}

static void nchan_exit_worker(ngx_cycle_t *cycle) {
  nchan_store->exit_worker(cycle);
  ngx_destroy_pool(nchan_pool); // just for this worker
}

static void nchan_exit_master(ngx_cycle_t *cycle) {
  nchan_store->exit_master(cycle);
  ngx_destroy_pool(nchan_pool);
}

static char *nchan_set_message_buffer_length(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  char                           *p = conf;
  ngx_int_t                      *min, *max;
  ngx_str_t                      *value;
  ngx_int_t                       intval;
  min = (ngx_int_t *) (p + offsetof(nchan_loc_conf_t, min_messages));
  max = (ngx_int_t *) (p + offsetof(nchan_loc_conf_t, max_messages));
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

static char *nchan_store_messages_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  char    *p = conf;
  ngx_str_t *val = cf->args->elts;

  
  if (ngx_strcasecmp(val[1].data, (u_char *) "off") == 0) {
    ngx_int_t *min, *max;
    min = (ngx_int_t *) (p + offsetof(nchan_loc_conf_t, min_messages));
    max = (ngx_int_t *) (p + offsetof(nchan_loc_conf_t, max_messages));
    *min=0;
    *max=0;
  }
  return NGX_CONF_OK;
}

#include "nchan_config_commands.c" //hideous but hey, it works

static ngx_http_module_t  nchan_module_ctx = {
    NULL,                                  /* preconfiguration */
    nchan_postconfig,              /* postconfiguration */
    nchan_create_main_conf,        /* create main configuration */
    NULL,                                  /* init main configuration */
    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */
    nchan_create_loc_conf,         /* create location configuration */
    nchan_merge_loc_conf,          /* merge location configuration */
};

ngx_module_t  nchan_module = {
    NGX_MODULE_V1,
    &nchan_module_ctx,                     /* module context */
    nchan_commands,                /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    nchan_init_module,             /* init module */
    nchan_init_worker,             /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    nchan_exit_worker,             /* exit process */
    nchan_exit_master,             /* exit master */
    NGX_MODULE_V1_PADDING
};
