#include <nchan_websocket_publisher.h>
#include <nchan_types.h>
#include <util/nchan_output.h>
#include <nchan_variables.h>
#include <store/memory/store.h>
#include <store/redis/store.h>
#if (NGX_ZLIB)
#include <zlib.h>
#endif

static ngx_str_t      DEFAULT_CHANNEL_EVENT_STRING = ngx_string("$nchan_channel_event $nchan_channel_id");

nchan_store_t   *default_storage_engine = &nchan_store_memory;
ngx_flag_t       global_redis_enabled = 0;
ngx_flag_t       global_zstream_needed = 0;

#define MERGE_UNSET_CONF(conf, prev, unset, default)         \
if (conf == unset) {                                         \
  conf = (prev == unset) ? default : prev;                   \
}

#define MERGE_CONF(cf, prev_cf, name) if((cf)->name == NULL) { (cf)->name = (prev_cf)->name; }

static ngx_int_t nchan_init_module(ngx_cycle_t *cycle) {
  ngx_core_conf_t         *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  nchan_worker_processes = ccf->worker_processes;
  
  //initialize storage engines
  nchan_store_memory.init_module(cycle);
  if(global_redis_enabled) {
    nchan_store_redis.init_module(cycle);
  }
  return NGX_OK;
}

static ngx_int_t nchan_init_worker(ngx_cycle_t *cycle) {
  if (ngx_process != NGX_PROCESS_WORKER && ngx_process != NGX_PROCESS_SINGLE) {
    //not a worker, stop initializing stuff.
    return NGX_OK;
  }
  
  if(nchan_store_memory.init_worker(cycle)!=NGX_OK) {
    return NGX_ERROR;
  }
  
  if(global_redis_enabled && nchan_store_redis.init_worker(cycle)!=NGX_OK) {
    return NGX_ERROR;
  }
  
  nchan_websocket_publisher_llist_init();
  nchan_output_init();
  
  return NGX_OK;
}

static ngx_int_t nchan_postconfig(ngx_conf_t *cf) {
  if(nchan_store_memory.init_postconfig(cf)!=NGX_OK) {
    return NGX_ERROR;
  }
  if(global_redis_enabled && nchan_store_redis.init_postconfig(cf)!=NGX_OK) {
    return NGX_ERROR;
  }
  
#if (NGX_ZLIB)
  if(global_zstream_needed) {
    nchan_main_conf_t  *mcf = ngx_http_conf_get_module_main_conf(cf, ngx_nchan_module);
    nchan_common_deflate_init(mcf);
  }
#endif
  
  return NGX_OK;
}

//main config
static void * nchan_create_main_conf(ngx_conf_t *cf) {
  nchan_main_conf_t      *mcf = ngx_pcalloc(cf->pool, sizeof(*mcf));
  if(mcf == NULL) {
    return NGX_CONF_ERROR;
  }
  
  static ngx_path_init_t nchan_temp_path = { ngx_string(NGX_HTTP_CLIENT_TEMP_PATH), { 0, 0, 0 } };
  ngx_conf_merge_path_value(cf, &mcf->message_temp_path, NULL, &nchan_temp_path);
  
  nchan_store_memory.create_main_conf(cf, mcf);
  nchan_store_redis.create_main_conf(cf, mcf);
  
#if (NGX_ZLIB)
  mcf->zlib_params.level = Z_DEFAULT_COMPRESSION;
  mcf->zlib_params.windowBits = -10;
  mcf->zlib_params.memLevel = 8;
  mcf->zlib_params.strategy = Z_DEFAULT_STRATEGY;
#endif
  
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
  lcf->sub.http_chunked=0;
  
  // lcf->group is already zeroed
  lcf->group.enable_accounting = NGX_CONF_UNSET;
  
  lcf->shared_data_index=NGX_CONF_UNSET;
  
  lcf->authorize_request_url = NULL;
  lcf->publisher_upstream_request_url = NULL;
  lcf->unsubscribe_request_url = NULL;
  lcf->subscribe_request_url = NULL;
  lcf->channel_group = NULL;
  
  lcf->message_timeout=NGX_CONF_UNSET;
  lcf->max_messages=NGX_CONF_UNSET;
  
  lcf->complex_message_timeout = NULL;
  lcf->complex_max_messages = NULL;
  
  lcf->subscriber_first_message=NCHAN_SUBSCRIBER_FIRST_MESSAGE_UNSET;
  
  lcf->subscriber_timeout=NGX_CONF_UNSET;
  lcf->subscribe_only_existing_channel=NGX_CONF_UNSET;
  lcf->redis_idle_channel_cache_timeout=NGX_CONF_UNSET;
  lcf->max_channel_id_length=NGX_CONF_UNSET;
  lcf->max_channel_subscribers=NGX_CONF_UNSET;
  lcf->channel_timeout=NGX_CONF_UNSET;
  lcf->storage_engine=NULL;
  
  lcf->websocket_ping_interval=NGX_CONF_UNSET;
  
  lcf->msg_in_etag_only = NGX_CONF_UNSET;
  
  lcf->channel_events_channel_id = NULL;
  lcf->channel_event_string = NULL;
  
  lcf->websocket_heartbeat.enabled=NGX_CONF_UNSET;
  
  lcf->message_compression = NCHAN_MSG_COMPRESSION_INVALID;
  
  lcf->longpoll_multimsg=NGX_CONF_UNSET;
  lcf->longpoll_multimsg_use_raw_stream_separator=NGX_CONF_UNSET;
  
  ngx_memzero(&lcf->pub_chid, sizeof(nchan_complex_value_arr_t));
  ngx_memzero(&lcf->sub_chid, sizeof(nchan_complex_value_arr_t));
  ngx_memzero(&lcf->pubsub_chid, sizeof(nchan_complex_value_arr_t));
  ngx_memzero(&lcf->last_message_id, sizeof(nchan_complex_value_arr_t));
  
  ngx_memzero(&lcf->redis, sizeof(lcf->redis));
  lcf->redis.url_enabled=NGX_CONF_UNSET;
  lcf->redis.ping_interval = NGX_CONF_UNSET;
  lcf->redis.upstream_inheritable=NGX_CONF_UNSET;
  lcf->redis.storage_mode = REDIS_MODE_CONF_UNSET;
  lcf->redis.after_connect_wait_time = NGX_CONF_UNSET;
  
  lcf->request_handler = NULL;
  return lcf;
}

static char * create_complex_value_from_ngx_str(ngx_conf_t *cf, ngx_http_complex_value_t **dst_cv, ngx_str_t *str) {
  ngx_http_complex_value_t           *cv;
  ngx_http_compile_complex_value_t    ccv;
  
  cv = ngx_palloc(cf->pool, sizeof(*cv));
  if (cv == NULL) {
    ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "unable to allocate space for complex value");
    return NGX_CONF_ERROR;
  }
  
  ngx_memzero(&ccv, sizeof(ccv));
  
  ccv.cf = cf;
  ccv.value = str;
  ccv.complex_value = cv;
  
  if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
    return NGX_CONF_ERROR;
  }
  
  *dst_cv = cv;
  return NGX_CONF_OK;
}

static int is_pub_location(nchan_loc_conf_t *lcf) {
  return lcf->pub.http || lcf->pub.websocket;
}
static int is_sub_location(nchan_loc_conf_t *lcf) {
  nchan_conf_subscriber_types_t s = lcf->sub;
  return s.poll || s.http_raw_stream || s.longpoll || s.http_chunked || s.http_multipart || s.eventsource || s.websocket;
}
static int is_group_location(nchan_loc_conf_t *lcf) {
  return lcf->group.get || lcf->group.set || lcf->group.delete;
}

static int is_valid_location(ngx_conf_t *cf, nchan_loc_conf_t *lcf) {
  
  if(is_group_location(lcf)) {
    if(is_pub_location(lcf) && is_sub_location(lcf)) {
      ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "Can't have a publisher and subscriber location and also be a group access location (nchan_group + nchan_publisher, nchan_subscriber or nchan_pubsub)");
      return 0;
    }
    else if(is_pub_location(lcf)) {
      ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "Can't have a publisher location and also be a group access location (nchan_group + nchan_publisher)");
      return 0;
    }
    else if(is_sub_location(lcf)) {
      ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "Can't have a subscriber location and also be a group access location (nchan_group + nchan_subscriber)");
      return 0;
    }
  }
  return 1;
}

static char *ngx_conf_set_redis_upstream(ngx_conf_t *cf, ngx_str_t *url, void *conf) {  
  ngx_url_t             upstream_url;
  nchan_loc_conf_t     *lcf = conf;
  if (lcf->redis.upstream) {
    return "is duplicate";
  }
  
  ngx_memzero(&upstream_url, sizeof(upstream_url));
  upstream_url.url = *url;
  upstream_url.no_resolve = 1;
  
  if ((lcf->redis.upstream = ngx_http_upstream_add(cf, &upstream_url, 0)) == NULL) {
    return NGX_CONF_ERROR;
  }
  
  lcf->redis.enabled = 1;
  global_redis_enabled = 1;
  nchan_store_redis_add_server_conf(cf, &lcf->redis, lcf);
  
  return NGX_CONF_OK;
}

static char *nchan_setup_handler(ngx_conf_t *cf, ngx_int_t (*handler)(ngx_http_request_t *)) {
  ngx_http_core_loc_conf_t       *clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
  //nchan_loc_conf_t               *plcf = conf;
  clcf->handler = handler;
  clcf->if_modified_since = NGX_HTTP_IMS_OFF;
  
  return NGX_CONF_OK;
}

static char * nchan_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child) {
  nchan_loc_conf_t       *prev = parent, *conf = child;
  
  //publisher types
  ngx_conf_merge_bitmask_value(conf->pub.http, prev->pub.http, 0);
  ngx_conf_merge_bitmask_value(conf->pub.websocket, prev->pub.websocket, 0);
  
  //subscriber types
  ngx_conf_merge_bitmask_value(conf->sub.poll, prev->sub.poll, 0);
  ngx_conf_merge_bitmask_value(conf->sub.longpoll, prev->sub.longpoll, 0);
  ngx_conf_merge_bitmask_value(conf->sub.eventsource, prev->sub.eventsource, 0);
  ngx_conf_merge_bitmask_value(conf->sub.http_chunked, prev->sub.http_chunked, 0);
  ngx_conf_merge_bitmask_value(conf->sub.websocket, prev->sub.websocket, 0);
  
  //group request types
  ngx_conf_merge_bitmask_value(conf->group.get, prev->group.get, 0);
  ngx_conf_merge_bitmask_value(conf->group.set, prev->group.set, 0);
  ngx_conf_merge_bitmask_value(conf->group.delete, prev->group.delete, 0);
  
  ngx_conf_merge_value(conf->group.enable_accounting, prev->group.enable_accounting, 0);
  
  //validate location
  if(!is_valid_location(cf, conf)) {
    return NGX_CONF_ERROR;
  }
  
  MERGE_UNSET_CONF(conf->message_compression, prev->message_compression, NCHAN_MSG_COMPRESSION_INVALID, NCHAN_MSG_NO_COMPRESSION);
  
  ngx_conf_merge_sec_value(conf->message_timeout, prev->message_timeout, NCHAN_DEFAULT_MESSAGE_TIMEOUT);
  ngx_conf_merge_value(conf->max_messages, prev->max_messages, NCHAN_DEFAULT_MAX_MESSAGES);
  
  MERGE_CONF(conf, prev, complex_message_timeout);
  MERGE_CONF(conf, prev, complex_max_messages);
  
  if (conf->subscriber_first_message == NCHAN_SUBSCRIBER_FIRST_MESSAGE_UNSET) {
    conf->subscriber_first_message = (prev->subscriber_first_message == NCHAN_SUBSCRIBER_FIRST_MESSAGE_UNSET) ? NCHAN_SUBSCRIBER_DEFAULT_FIRST_MESSAGE : prev->subscriber_first_message;
  }
  
  ngx_conf_merge_sec_value(conf->websocket_ping_interval, prev->websocket_ping_interval, NCHAN_DEFAULT_WEBSOCKET_PING_INTERVAL);
  
  ngx_conf_merge_sec_value(conf->subscriber_timeout, prev->subscriber_timeout, NCHAN_DEFAULT_SUBSCRIBER_TIMEOUT);
  ngx_conf_merge_sec_value(conf->redis_idle_channel_cache_timeout, prev->redis_idle_channel_cache_timeout, NCHAN_DEFAULT_REDIS_IDLE_CHANNEL_CACHE_TIMEOUT);
  
  ngx_conf_merge_value(conf->subscribe_only_existing_channel, prev->subscribe_only_existing_channel, 0);
  ngx_conf_merge_value(conf->max_channel_id_length, prev->max_channel_id_length, NCHAN_MAX_CHANNEL_ID_LENGTH);
  ngx_conf_merge_value(conf->max_channel_subscribers, prev->max_channel_subscribers, 0);
  ngx_conf_merge_value(conf->channel_timeout, prev->channel_timeout, NCHAN_DEFAULT_CHANNEL_TIMEOUT);
  
  ngx_conf_merge_str_value(conf->subscriber_http_raw_stream_separator, prev->subscriber_http_raw_stream_separator, "\n");
  
  ngx_conf_merge_str_value(conf->channel_id_split_delimiter, prev->channel_id_split_delimiter, "");
  MERGE_CONF(conf, prev, allow_origin);
  ngx_conf_merge_str_value(conf->eventsource_event, prev->eventsource_event, "");
  ngx_conf_merge_str_value(conf->custom_msgtag_header, prev->custom_msgtag_header, "");
  ngx_conf_merge_value(conf->msg_in_etag_only, prev->msg_in_etag_only, 0);
  ngx_conf_merge_value(conf->longpoll_multimsg, prev->longpoll_multimsg, 0);
  ngx_conf_merge_value(conf->longpoll_multimsg_use_raw_stream_separator, prev->longpoll_multimsg_use_raw_stream_separator, 0);
  
  ngx_conf_merge_value(conf->websocket_heartbeat.enabled, prev->websocket_heartbeat.enabled, 0);
  MERGE_CONF(conf, prev, websocket_heartbeat.in);
  MERGE_CONF(conf, prev, websocket_heartbeat.out);
  
  MERGE_CONF(conf, prev, channel_events_channel_id);
  MERGE_CONF(conf, prev, channel_event_string);
  
  if(conf->channel_event_string == NULL) { //still null? use the default string
    if(create_complex_value_from_ngx_str(cf, &conf->channel_event_string, &DEFAULT_CHANNEL_EVENT_STRING) == NGX_CONF_ERROR) {
      return NGX_CONF_ERROR;
    }
  }

  if(conf->storage_engine == NULL) {
    conf->storage_engine = prev->storage_engine ? prev->storage_engine : default_storage_engine;
  }

  MERGE_CONF(conf, prev, authorize_request_url);
  MERGE_CONF(conf, prev, publisher_upstream_request_url);
  MERGE_CONF(conf, prev, unsubscribe_request_url);
  MERGE_CONF(conf, prev, subscribe_request_url);
  MERGE_CONF(conf, prev, channel_group);
  
  MERGE_CONF(conf, prev, group.max_channels);
  MERGE_CONF(conf, prev, group.max_subscribers);
  MERGE_CONF(conf, prev, group.max_messages);
  MERGE_CONF(conf, prev, group.max_messages_shm_bytes);
  MERGE_CONF(conf, prev, group.max_messages_file_bytes);
  
  if(conf->pub_chid.n == 0) {
    conf->pub_chid = prev->pub_chid;
  }
  if(conf->sub_chid.n == 0) {
    conf->sub_chid = prev->sub_chid;
  }
  if(conf->pubsub_chid.n == 0) {
    conf->pubsub_chid = prev->pubsub_chid;
  }
  if(conf->last_message_id.n == 0) {
    conf->last_message_id = prev->last_message_id;
  }
  if(conf->last_message_id.n == 0) { //if it's still null
    ngx_str_t      first_choice_msgid = ngx_string("$http_last_event_id");
    ngx_str_t      second_choice_msgid = ngx_string("$arg_last_event_id");
    
    if(create_complex_value_from_ngx_str(cf, &conf->last_message_id.cv[0], &first_choice_msgid) == NGX_CONF_ERROR) {
      return NGX_CONF_ERROR;
    }
    if(create_complex_value_from_ngx_str(cf, &conf->last_message_id.cv[1], &second_choice_msgid) == NGX_CONF_ERROR) {
      return NGX_CONF_ERROR;
    }
    conf->last_message_id.n = 2;
  }
  
  ngx_conf_merge_value(conf->redis.url_enabled, prev->redis.url_enabled, 0);
  ngx_conf_merge_value(conf->redis.upstream_inheritable, prev->redis.upstream_inheritable, 0);
  ngx_conf_merge_str_value(conf->redis.url, prev->redis.url, NCHAN_REDIS_DEFAULT_URL);
  ngx_conf_merge_str_value(conf->redis.namespace, prev->redis.namespace, "");
  ngx_conf_merge_value(conf->redis.ping_interval, prev->redis.ping_interval, NCHAN_REDIS_DEFAULT_PING_INTERVAL_TIME);
  ngx_conf_merge_sec_value(conf->redis.after_connect_wait_time, prev->redis.after_connect_wait_time, 0);
  
  if(conf->redis.url_enabled) {
    conf->redis.enabled = 1;
    nchan_store_redis_add_server_conf(cf, &conf->redis, conf);
  }
  if(conf->redis.upstream_inheritable && !conf->redis.upstream && prev->redis.upstream && prev->redis.upstream_url.len > 0) {
    conf->redis.upstream_url = prev->redis.upstream_url;
    ngx_conf_set_redis_upstream(cf, &conf->redis.upstream_url, conf);
  }
  if(conf->redis.storage_mode == REDIS_MODE_CONF_UNSET) {
    conf->redis.storage_mode = prev->redis.storage_mode == REDIS_MODE_CONF_UNSET ? REDIS_MODE_DISTRIBUTED : prev->redis.storage_mode;
  }
  
  if(prev->request_handler != NULL && conf->request_handler == NULL) {
    conf->request_handler = prev->request_handler;
  }
  if(conf->request_handler != NULL) {
    nchan_setup_handler(cf, conf->request_handler);
  }
  
  return NGX_CONF_OK;
}

static char *nchan_set_storage_engine(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t     *lcf = conf;
  ngx_str_t            *val = &((ngx_str_t *) cf->args->elts)[1];
  
  if(nchan_strmatch(val, 1, "memory")) {
    lcf->storage_engine = &nchan_store_memory;
  }
  else if(nchan_strmatch(val, 1, "redis")) {
    lcf->storage_engine = &nchan_store_redis;
    global_redis_enabled = 1;
  }
  else {
    ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "invalid %V value: %V", &cmd->name, val);
    return NGX_CONF_ERROR;
  }

  return NGX_CONF_OK;
}

#define WEBSOCKET_STRINGS "websocket", "ws", "websockets"
#define WEBSOCKET_STRINGS_N 3

#define EVENTSOURCE_STRINGS "eventsource", "event-source", "es", "sse"
#define EVENTSOURCE_STRINGS_N 4

#define HTTP_CHUNKED_STRINGS "chunked", "http-chunked"
#define HTTP_CHUNKED_STRINGS_N 2

#define HTTP_MULTIPART_STRINGS "multipart", "multipart/mixed", "http-multipart", "multipart-mixed"
#define HTTP_MULTIPART_STRINGS_N 4

#define LONGPOLL_STRINGS "longpoll", "long-poll"
#define LONGPOLL_STRINGS_N 2

#define INTERVALPOLL_STRINGS "poll", "interval-poll", "intervalpoll", "http"
#define INTERVALPOLL_STRINGS_N 4

#define HTTP_RAW_STREAM_STRINGS "http-raw-stream"
#define HTTP_RAW_STREAM_STRINGS_N 1

#define DISABLED_STRINGS "none", "off", "disabled"
#define DISABLED_STRINGS_N 3

static char *nchan_publisher_directive_parse(ngx_conf_t *cf, ngx_command_t *cmd, void *conf, ngx_int_t fail) {
  nchan_loc_conf_t     *lcf = conf;
  ngx_str_t            *val;
  ngx_uint_t            i;
  
  
  nchan_conf_publisher_types_t *pubt = &lcf->pub;
  
  if(cf->args->nelts == 1){ //no arguments
    pubt->http=1;
    pubt->websocket=1;
  }
  else {
    for(i=1; i < cf->args->nelts; i++) {
      val = &((ngx_str_t *) cf->args->elts)[i];
      if(nchan_strmatch(val, 1, "http")) {
        pubt->http=1;
      }
      else if(nchan_strmatch(val, WEBSOCKET_STRINGS_N, WEBSOCKET_STRINGS)) {
        pubt->websocket=1;
      }
      else{
         if(fail) {
          ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "invalid %V value: %V", &cmd->name, val);
         }
        return NGX_CONF_ERROR;
      }
    }
  }
  
  if(!is_valid_location(cf, lcf)) {
    return NGX_CONF_ERROR;
  }
  lcf->request_handler = &nchan_pubsub_handler;
  return NGX_CONF_OK;
}

static char *nchan_publisher_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  return nchan_publisher_directive_parse(cf, cmd, conf, 1);
}

static char *nchan_subscriber_directive_parse(ngx_conf_t *cf, ngx_command_t *cmd, void *conf, ngx_int_t fail) {
  nchan_loc_conf_t     *lcf = conf;
  ngx_str_t            *val;
  ngx_uint_t            i;
  
  nchan_conf_subscriber_types_t *subt = &lcf->sub;
  
  if(cf->args->nelts == 1){ //no arguments
    subt->poll=0;
    subt->http_raw_stream = 0;
    subt->longpoll=1;
    subt->websocket=1;
    subt->eventsource=1;
    subt->http_chunked=1;
    subt->http_multipart=1;
  }
  else {
    for(i=1; i < cf->args->nelts; i++) {
      val = &((ngx_str_t *) cf->args->elts)[i];
      if(nchan_strmatch(val, LONGPOLL_STRINGS_N, LONGPOLL_STRINGS)) {
        subt->longpoll=1;
      }
      else if(nchan_strmatch(val, INTERVALPOLL_STRINGS_N, INTERVALPOLL_STRINGS)) {
        subt->poll=1;
      }
      else if(nchan_strmatch(val, HTTP_RAW_STREAM_STRINGS_N, HTTP_RAW_STREAM_STRINGS)) {
        subt->http_raw_stream=1;
      }
      else if(nchan_strmatch(val, HTTP_CHUNKED_STRINGS_N, HTTP_CHUNKED_STRINGS)) {
        subt->http_chunked=1;
      }
      else if(nchan_strmatch(val, HTTP_MULTIPART_STRINGS_N, HTTP_MULTIPART_STRINGS)) {
        subt->http_multipart=1;
      }
      else if(nchan_strmatch(val, WEBSOCKET_STRINGS_N, WEBSOCKET_STRINGS)) {
        subt->websocket=1;
      }
      else if(nchan_strmatch(val, EVENTSOURCE_STRINGS_N, EVENTSOURCE_STRINGS)) {
        subt->eventsource=1;
      }
      else if(nchan_strmatch(val, DISABLED_STRINGS_N, DISABLED_STRINGS)) {
        subt->poll=0;
        subt->longpoll=0;
        subt->websocket=0;
        subt->eventsource=0;
        subt->http_chunked=0;
        subt->http_multipart=0;
      }
      else {
        if(fail) {
          ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "invalid %V value: %V", &cmd->name, val);
        }
        return NGX_CONF_ERROR;
      }
    }
  }
  
  if(!is_valid_location(cf, lcf)) {
    return NGX_CONF_ERROR;
  }
  lcf->request_handler = &nchan_pubsub_handler;
  return NGX_CONF_OK;
}

static char *nchan_subscriber_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  return nchan_subscriber_directive_parse(cf, cmd, conf, 1);
}

static char *nchan_pubsub_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_str_t            *val;
  ngx_uint_t            i;
  nchan_loc_conf_t     *lcf = conf;
  nchan_publisher_directive_parse(cf, cmd, conf, 0);
  nchan_subscriber_directive_parse(cf, cmd, conf, 0);
  for(i=1; i < cf->args->nelts; i++) {
    val = &((ngx_str_t *) cf->args->elts)[i];
    if(! nchan_strmatch(val, 
      WEBSOCKET_STRINGS_N + EVENTSOURCE_STRINGS_N + HTTP_CHUNKED_STRINGS_N + HTTP_MULTIPART_STRINGS_N + LONGPOLL_STRINGS_N + INTERVALPOLL_STRINGS_N + HTTP_RAW_STREAM_STRINGS_N + DISABLED_STRINGS_N,
      WEBSOCKET_STRINGS, EVENTSOURCE_STRINGS, HTTP_CHUNKED_STRINGS, HTTP_MULTIPART_STRINGS, LONGPOLL_STRINGS, INTERVALPOLL_STRINGS, HTTP_RAW_STREAM_STRINGS, DISABLED_STRINGS)) {
      ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "invalid %V value: %V", &cmd->name, val);
      return NGX_CONF_ERROR;
    }
  }
  
  if(!is_valid_location(cf, lcf)) {
    return NGX_CONF_ERROR;
  }
  
  return NGX_CONF_OK;
}

static char *nchan_group_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t     *lcf = conf;
  ngx_str_t            *val;
  ngx_uint_t            i;
  nchan_conf_group_t   *group = &lcf->group;
  
  if(cf->args->nelts == 1){ //no arguments
    group->get=1;
    group->set=1;
    group->delete=1;
  }
  else {
    for(i=1; i < cf->args->nelts; i++) {
      val = &((ngx_str_t *) cf->args->elts)[i];
      if(nchan_strmatch(val, 1, "get")) {
        group->get=1;
      }
      else if(nchan_strmatch(val, 1, "set")) {
        group->set=1;
      }
      else if(nchan_strmatch(val, 1, "delete")) {
        group->delete=1;
      }
      else if(nchan_strmatch(val, 1, "off")) {
        group->get=0;
        group->set=0;
        group->delete=0;
      }
      else {
        ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "invalid %V value: %V", &cmd->name, val);
        return NGX_CONF_ERROR;
      }
    }
  }
  
  if(!is_valid_location(cf, lcf)) {
    return NGX_CONF_ERROR;
  }
  lcf->request_handler = &nchan_group_handler;
  return NGX_CONF_OK;
}

static char *nchan_subscriber_first_message_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t   *lcf = (nchan_loc_conf_t *)conf;
  ngx_str_t          *val = &((ngx_str_t *) cf->args->elts)[1];
  if(nchan_strmatch(val, 1, "oldest")) {
    lcf->subscriber_first_message = 1;
  }
  else if(nchan_strmatch(val, 1, "newest")) {
    lcf->subscriber_first_message = 0;
  }
  else {
    //maybe a number?
    ngx_str_t num = *val;
    int       sign = 1;
    ngx_int_t n;
    
    if(num.len > 0 && num.data[0] == '-') {
      num.len--;
      num.data++;
      sign = -1;
    }
    if((n = ngx_atoi(num.data, num.len)) == NGX_ERROR) {
      ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "invalid %V value: %V, must be 'oldest', 'newest', or a number", &cmd->name, val);
      return NGX_CONF_ERROR;
    }
    if (n > 32) {
      ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "invalid %V value: %V, must be 'oldest', 'newest', or a number between -32 and 32", &cmd->name, val);
      return NGX_CONF_ERROR;
    }
    lcf->subscriber_first_message = n * sign;
  }
  return NGX_CONF_OK;
}

static char *nchan_websocket_heartbeat_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t   *lcf = (nchan_loc_conf_t *)conf;
  ngx_str_t          *heartbeat_in = &((ngx_str_t *) cf->args->elts)[1];
  ngx_str_t          *heartbeat_out = &((ngx_str_t *) cf->args->elts)[2];
  ngx_str_t          *in, *out;
  lcf->websocket_heartbeat.enabled = 1;
  
  in = ngx_pcalloc(cf->pool, sizeof(ngx_str_t) + heartbeat_in->len);
  in->data = (u_char *)&in[1];
  in->len = heartbeat_in->len;
  ngx_memcpy(in->data, heartbeat_in->data, heartbeat_in->len);
  lcf->websocket_heartbeat.in = in;
  
  out = ngx_pcalloc(cf->pool, sizeof(ngx_str_t) + heartbeat_out->len);
  out->data = (u_char *)&out[1];
  out->len = heartbeat_out->len;
  ngx_memcpy(out->data, heartbeat_out->data, heartbeat_out->len);
  lcf->websocket_heartbeat.out = out;
  
  return NGX_CONF_OK;
}

static char *nchan_conf_deflate_compression_level_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
#if (NGX_ZLIB)
  nchan_main_conf_t  *mcf = (nchan_main_conf_t *)conf;
  ngx_int_t           np;
  ngx_str_t           *value = cf->args->elts;
  np = ngx_atoi(value[1].data, value[1].len);
  if (np == NGX_ERROR) {
    return "invalid number";
  }
  if(np < 0 || np > 9) {
    return "must be between 0 and 9";
  }
  
  mcf->zlib_params.level = np;
#endif
  return NGX_CONF_OK;
}

static char *nchan_conf_deflate_compression_strategy_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
#if (NGX_ZLIB)
  nchan_main_conf_t   *mcf = (nchan_main_conf_t *)conf;
  ngx_str_t           *val = cf->args->elts;
  if(nchan_strmatch(val, 1, "default")) {
    mcf->zlib_params.strategy = Z_DEFAULT_STRATEGY;
  }
  else if(nchan_strmatch(val, 1, "filtered")) {
    mcf->zlib_params.strategy = Z_FILTERED;
  }
  else if(nchan_strmatch(val, 1, "huffman-only")) {
    mcf->zlib_params.strategy = Z_HUFFMAN_ONLY;
  }
  else if(nchan_strmatch(val, 1, "rle")) {
    mcf->zlib_params.strategy = Z_RLE;
  }
  else if(nchan_strmatch(val, 1, "fixed")) {
    mcf->zlib_params.strategy = Z_FIXED;
  }
  else {
    return "invalid compression strategy";
  }
#endif
  return NGX_CONF_OK;
}

static char *nchan_conf_deflate_compression_window_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
#if (NGX_ZLIB)
  nchan_main_conf_t  *mcf = (nchan_main_conf_t *)conf;
  ngx_int_t           np;
  ngx_str_t           *value = cf->args->elts;
  np = ngx_atoi(value[1].data, value[1].len);
  if (np == NGX_ERROR) {
    return "invalid number";
  }
  if(np < 9 || np > 15) {
    return "must be between 9 and 15";
  }
  
  mcf->zlib_params.windowBits = -np;
#endif
  return NGX_CONF_OK;
}

static char *nchan_conf_deflate_compression_memlevel_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
#if (NGX_ZLIB)
  nchan_main_conf_t  *mcf = (nchan_main_conf_t *)conf;
  ngx_int_t           np;
  ngx_str_t           *value = cf->args->elts;
  np = ngx_atoi(value[1].data, value[1].len);
  if (np == NGX_ERROR) {
    return "invalid number";
  }
  if(np < 9 || np > 15) {
    return "must be between 1 and 9";
  }
  
  mcf->zlib_params.windowBits = -np;
#endif
  return NGX_CONF_OK;
}


static void nchan_exit_worker(ngx_cycle_t *cycle) {
  if(global_redis_enabled) {
    redis_store_prepare_to_exit_worker();
  }
  nchan_store_memory.exit_worker(cycle);
  if(global_redis_enabled) {
    nchan_store_redis.exit_worker(cycle);
  }
  nchan_output_shutdown();
#if (NGX_ZLIB)
  if(global_zstream_needed) {
    nchan_common_deflate_shutdown();
  }
#endif
#if NCHAN_SUBSCRIBER_LEAK_DEBUG
  subscriber_debug_assert_isempty();
#endif
}

static void nchan_exit_master(ngx_cycle_t *cycle) {
  nchan_store_memory.exit_master(cycle);
  if(global_redis_enabled) {
    nchan_store_redis.exit_master(cycle);
  }
#if (NGX_ZLIB)
  if(global_zstream_needed) {
    nchan_common_deflate_shutdown();
  }
#endif
}

static char *nchan_set_complex_value_array(ngx_conf_t *cf, ngx_command_t *cmd, void *conf, nchan_complex_value_arr_t *chid) {
  ngx_uint_t                          i;
  ngx_str_t                          *value;
  ngx_http_complex_value_t          **cv;
  ngx_http_compile_complex_value_t    ccv;  
  
  chid->n = cf->args->nelts - 1;
  for(i=1; i < cf->args->nelts && i <= NCHAN_COMPLEX_VALUE_ARRAY_MAX; i++) {
    value = &((ngx_str_t *) cf->args->elts)[i];
    
    cv = &chid->cv[i-1];
    *cv = ngx_palloc(cf->pool, sizeof(ngx_http_complex_value_t));
    if (*cv == NULL) {
      return NGX_CONF_ERROR;
    }
    
    ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));
    ccv.cf = cf;
    ccv.value = value;
    ccv.complex_value = *cv;
    
    if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
      return NGX_CONF_ERROR;
    }
  }
  
  return NGX_CONF_OK;
}

static char *nchan_set_pub_channel_id(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  return nchan_set_complex_value_array(cf, cmd, conf, &((nchan_loc_conf_t *)conf)->pub_chid);
}

static char *nchan_set_sub_channel_id(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  return nchan_set_complex_value_array(cf, cmd, conf, &((nchan_loc_conf_t *)conf)->sub_chid);
}

static char *nchan_set_pubsub_channel_id(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  return nchan_set_complex_value_array(cf, cmd, conf, &((nchan_loc_conf_t *)conf)->pubsub_chid);
}

static char *nchan_subscriber_last_message_id(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  return nchan_set_complex_value_array(cf, cmd, conf, &((nchan_loc_conf_t *)conf)->last_message_id);
}

static char *nchan_set_channel_events_channel_id(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_str_t                          *value = &((ngx_str_t *) cf->args->elts)[1];
  ngx_http_complex_value_t          **cv = &((nchan_loc_conf_t *)conf)->channel_events_channel_id;
  ngx_http_compile_complex_value_t    ccv;  
  
  *cv = ngx_palloc(cf->pool, sizeof(ngx_http_complex_value_t));
  if (*cv == NULL) {
    return NGX_CONF_ERROR;
  }
  
  ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));
  ccv.cf = cf;
  ccv.value = value;
  ccv.complex_value = *cv;
  
  if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
    return NGX_CONF_ERROR;
  }
  
  return NGX_CONF_OK;
}

static char *nchan_store_messages_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  char    *p = conf;
  ngx_str_t *val = cf->args->elts;

  
  if (ngx_strcasecmp(val[1].data, (u_char *) "off") == 0) {
    ngx_int_t *max;
    max = (ngx_int_t *) (p + offsetof(nchan_loc_conf_t, max_messages));
    *max=0;
  }
  return NGX_CONF_OK;
}

static char *nchan_set_message_buffer_length(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t    *lcf = conf;
  ngx_str_t *val = cf->args->elts;
  ngx_str_t *arg = &val[1];
  
  if(memchr(arg->data, '$', arg->len)) {
    //complex
    lcf->max_messages = NGX_CONF_UNSET;
    cmd->offset = offsetof(nchan_loc_conf_t, complex_max_messages);
    ngx_http_set_complex_value_slot(cf, cmd, conf);
    memstore_reserve_conf_shared_data(lcf);
  }
  else {
    //simple
    lcf->complex_max_messages = NULL;
    cmd->offset = offsetof(nchan_loc_conf_t, max_messages);
    ngx_conf_set_num_slot(cf, cmd, conf);
  }
  return NGX_CONF_OK;
}

static char *ngx_http_set_unsubscribe_request_url(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
#if nginx_version >= 1003015
  return ngx_http_set_complex_value_slot(cf, cmd, conf);
#else
  ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "%V not available on nginx version: %s, must be at least 1.3.15", &cmd->name, NGINX_VERSION);
  return NGX_CONF_ERROR;
#endif
}

static char *nchan_set_message_timeout(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t    *lcf = conf;
  ngx_str_t *val = cf->args->elts;
  ngx_str_t *arg = &val[1];
  
  if(memchr(arg->data, '$', arg->len)) {
    //complex
    lcf->message_timeout = NGX_CONF_UNSET;
    cmd->offset = offsetof(nchan_loc_conf_t, complex_message_timeout);
    ngx_http_set_complex_value_slot(cf, cmd, conf);
    memstore_reserve_conf_shared_data(lcf);
  }
  else {
    //simple
    lcf->complex_message_timeout = NULL;
    cmd->offset = offsetof(nchan_loc_conf_t, message_timeout);
    ngx_conf_set_sec_slot(cf, cmd, conf);
  }
  return NGX_CONF_OK;
}

static char *nchan_ignore_obsolete_setting(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "ignoring obsolete nchan config directive '%V'.", &cmd->name);
  return NGX_CONF_OK;
}

static char *nchan_ignore_subscriber_concurrency(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_str_t          *val = &((ngx_str_t *) cf->args->elts)[1];
  if(!nchan_strmatch(val, 1, "broadcast")) {
    ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "ignoring obsolete nchan config directive '%V %V;'. Only 'broadcast' is currently supported.", &cmd->name, val);
  }
  return NGX_CONF_OK;
}

static char *nchan_set_raw_subscriber_separator(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_str_t          *val = &((ngx_str_t *) cf->args->elts)[1];
  nchan_loc_conf_t   *lcf = conf;
  ngx_str_t          *cf_val = &lcf->subscriber_http_raw_stream_separator;
  
  if( val->data[val->len - 1] != '\n' ) { //must end in a newline
    u_char   *cur;
    if((cur = ngx_palloc(cf->pool, val->len + 1)) == NULL) {
      return NGX_CONF_ERROR;
    }
    ngx_memcpy(cur, val->data, val->len);
    cur[val->len] = '\n';
    cf_val->len = val->len + 1;
    cf_val->data = cur;
  }
  else {
    *cf_val = *val;
  }
  return NGX_CONF_OK;
}

static char *nchan_set_message_compression_slot(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_str_t          *val = &((ngx_str_t *) cf->args->elts)[1];
  nchan_loc_conf_t   *lcf = conf;
#if (NGX_ZLIB)
  if(nchan_strmatch(val, 1, "on")) {
    lcf->message_compression = 1;
    global_zstream_needed = 1;
  }
  else if(nchan_strmatch(val, 1, "off")) {
    lcf->message_compression = 0;
  }
  else {
    return "invalid value: must be 'on' or 'off'";
  }
  return NGX_CONF_OK;
#else
  return "cannot use compression, Nginx was built without zlib";
#endif
}

static char *nchan_set_longpoll_multipart(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_str_t          *val = &((ngx_str_t *) cf->args->elts)[1];
  nchan_loc_conf_t   *lcf = conf;
  if(nchan_strmatch(val, 1, "on")) {
    lcf->longpoll_multimsg = 1;
  }
  else if(nchan_strmatch(val, 1, "off")) {
    lcf->longpoll_multimsg = 0;
  }
  else if(nchan_strmatch(val, 1, "raw")) {
    lcf->longpoll_multimsg = 1;
    lcf->longpoll_multimsg_use_raw_stream_separator = 1;
  }
  else {
    ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "invalid value for %V: %V;'. Must be 'on', 'off', or 'raw'", &cmd->name, val);
    return NGX_CONF_ERROR;
  }
  return NGX_CONF_OK;
}

static char *ngx_conf_enable_redis(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  char                *rc;
  ngx_flag_t          *fp;
  char                *p = conf;
  nchan_loc_conf_t    *lcf = conf;
  
  rc = ngx_conf_set_flag_slot(cf, cmd, conf);
  if(rc == NGX_CONF_ERROR) {
    return rc;
  }
  fp = (ngx_flag_t *) (p + cmd->offset);
  
  if(*fp) {
    if(!lcf->redis.enabled) {
      lcf->redis.enabled = 1;
      nchan_store_redis_add_server_conf(cf, &lcf->redis, lcf);
    }
    global_redis_enabled = 1;
  }
  else {
    nchan_store_redis_remove_server_conf(cf, &lcf->redis);
  }
  
  return rc;
}

static char *nchan_stub_status_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t    *lcf = conf;
  nchan_stub_status_enabled = 1;
  lcf->request_handler = &nchan_stub_status_handler;
  return NGX_CONF_OK;
}


static ngx_int_t nchan_upstream_dummy_roundrobin_init(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us) {
  return NGX_OK;
}

static char *ngx_conf_upstream_redis_server(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_http_upstream_srv_conf_t        *uscf;
  ngx_str_t                           *value;
  ngx_http_upstream_server_t          *usrv;
  
  uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);  
  
  if(uscf->servers == NULL) {
        uscf->servers = ngx_array_create(cf->pool, 4, sizeof(ngx_http_upstream_server_t));
  }
  if ((usrv = ngx_array_push(uscf->servers)) == NULL) {
    return NGX_CONF_ERROR;
  }
  value = cf->args->elts;
  ngx_memzero(usrv, sizeof(*usrv));
#if nginx_version >= 1007002
  usrv->name = value[1];
#endif
  usrv->addrs = ngx_pcalloc(cf->pool, sizeof(ngx_addr_t));
  usrv->addrs->name = value[1];
  
  uscf->peer.init_upstream = nchan_upstream_dummy_roundrobin_init;
  return NGX_CONF_OK;
}

static char *ngx_conf_set_redis_url(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t  *lcf = conf;
  
  if(lcf->redis.upstream) {
    return "can't be set here: already using nchan_redis_pass";
  }
  
  return ngx_conf_set_str_slot(cf, cmd, conf);
}

static char *ngx_conf_set_redis_namespace_slot(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  //raise(SIGSTOP);
  ngx_str_t *val = cf->args->elts;
  ngx_str_t *arg = &val[1];

  if(memchr(arg->data, '{', arg->len)) {
    return "can't contain character '{'";
  }
  
  if(memchr(arg->data, '}', arg->len)) {
    return "can't contain character '}'";
  }
  
  if(arg->len > 0 && arg->data[arg->len-1] != ':') {
    u_char  *nns;
    if((nns = ngx_palloc(cf->pool, sizeof(arg->len + 2))) == NULL) {
      return "couldn't allocate redis namespace data";
    }
    ngx_memcpy(nns, arg->data, arg->len);
    nns[arg->len]=':';
    nns[arg->len+1]='\0';
    arg->len++;
    arg->data=nns;
  }
  
  return ngx_conf_set_str_slot(cf, cmd, conf);
}

static char *ngx_conf_set_redis_storage_mode_slot(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  char                         *p = conf;
  ngx_str_t                    *val = cf->args->elts;
  ngx_str_t                    *arg = &val[1];
  
  nchan_redis_storage_mode_t   *field;

  field = (nchan_redis_storage_mode_t *) (p + cmd->offset);
  
  if(*field != REDIS_MODE_CONF_UNSET) {
    return "is duplicate";
  }
  
  if(nchan_strmatch(arg, 1, "backup")) {
    *field = REDIS_MODE_BACKUP;
  }
  else if(nchan_strmatch(arg, 1, "distributed")) {
    *field = REDIS_MODE_DISTRIBUTED;
  }
  else {
    return "is invalid, must be either 'distributed' or 'backup'";
  }
  
  return NGX_CONF_OK;
}

static char *ngx_conf_set_redis_upstream_pass(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t  *lcf = conf;
  ngx_str_t         *value = cf->args->elts;
  
  lcf->redis.upstream_url = value[1];
  return ngx_conf_set_redis_upstream(cf, &value[1], conf);
}


#include "nchan_config_commands.c" //hideous but hey, it works

static ngx_http_module_t  nchan_module_ctx = {
    nchan_add_variables,           /* preconfiguration */
    nchan_postconfig,              /* postconfiguration */
    nchan_create_main_conf,        /* create main configuration */
    NULL,                          /* init main configuration */
    NULL,                          /* create server configuration */
    NULL,                          /* merge server configuration */
    nchan_create_loc_conf,         /* create location configuration */
    nchan_merge_loc_conf,          /* merge location configuration */
};

ngx_module_t  ngx_nchan_module = {
    NGX_MODULE_V1,
    &nchan_module_ctx,             /* module context */
    nchan_commands,                /* module directives */
    NGX_HTTP_MODULE,               /* module type */
    NULL,                          /* init master */
    nchan_init_module,             /* init module */
    nchan_init_worker,             /* init process */
    NULL,                          /* init thread */
    NULL,                          /* exit thread */
    nchan_exit_worker,             /* exit process */
    nchan_exit_master,             /* exit master */
    NGX_MODULE_V1_PADDING
};
