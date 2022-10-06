#include <nchan_websocket_publisher.h>
#include <nchan_types.h>
#include <util/nchan_output.h>
#include <nchan_variables.h>
#include <store/memory/store.h>
#include <store/redis/store.h>
#if (NGX_ZLIB)
#include <zlib.h>
#endif

static char *nchan_set_complex_value_array(ngx_conf_t *cf, ngx_command_t *cmd, void *conf, nchan_complex_value_arr_t *chid);
static ngx_int_t set_complex_value_array_size1(ngx_conf_t *cf, nchan_complex_value_arr_t *chid, char *val);

static ngx_str_t      DEFAULT_CHANNEL_EVENT_STRING = ngx_string("$nchan_channel_event $nchan_channel_id");

static ngx_str_t      DEFAULT_SUBSCRIBER_INFO_STRING = ngx_string("$nchan_subscriber_type $remote_addr:$remote_port $http_user_agent $server_name $request_uri $pid");

nchan_store_t   *default_storage_engine = &nchan_store_memory;
ngx_flag_t       global_nchan_enabled = 0;
ngx_flag_t       global_redis_enabled = 0;
ngx_flag_t       global_zstream_needed = 0;
ngx_flag_t       global_benchmark_enabled = 0;
void            *global_owner_cycle = NULL;

#define MERGE_UNSET_CONF(conf, prev, unset, default)         \
if (conf == unset) {                                         \
  conf = (prev == unset) ? default : prev;                   \
}

#define MERGE_CONF(cf, prev_cf, name) if((cf)->name == NULL) { (cf)->name = (prev_cf)->name; }

static ngx_int_t nchan_init_module(ngx_cycle_t *cycle) {
  if(global_owner_cycle && global_owner_cycle != ngx_cycle) {
    global_nchan_enabled = 0;
    global_redis_enabled = 0;
    global_zstream_needed = 0;
    global_benchmark_enabled = 0;
  }
  global_owner_cycle = (void *)ngx_cycle;
  
  if(!global_nchan_enabled) {
    return NGX_OK;
  }
  ngx_core_conf_t         *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  nchan_worker_processes = ccf->worker_processes;
  
  //initialize storage engines
  nchan_store_memory.init_module(cycle);
  if(global_benchmark_enabled) {
    nchan_benchmark_init_module(cycle);
  }
  if(global_redis_enabled) {
    nchan_store_redis.init_module(cycle);
  }
  return NGX_OK;
}

static ngx_int_t nchan_init_worker(ngx_cycle_t *cycle) {
  if(!global_nchan_enabled) {
    return NGX_OK;
  }
  if (ngx_process != NGX_PROCESS_WORKER && ngx_process != NGX_PROCESS_SINGLE) {
    //not a worker, stop initializing stuff.
    return NGX_OK;
  }
  
  if(nchan_stats_init_worker(cycle) != NGX_OK) {
    return NGX_ERROR;
  }
  
  if(nchan_store_memory.init_worker(cycle)!=NGX_OK) {
    return NGX_ERROR;
  }
  if(global_benchmark_enabled) {
    nchan_benchmark_init_worker(cycle);
  }
  
  if(global_redis_enabled && nchan_store_redis.init_worker(cycle)!=NGX_OK) {
    return NGX_ERROR;
  }
  
  nchan_websocket_publisher_llist_init();
  nchan_output_init();
  
  return NGX_OK;
}

static ngx_int_t nchan_preconfig(ngx_conf_t *cf) {
  global_owner_cycle = (void *)ngx_cycle;
  global_nchan_enabled = 0;
  return nchan_add_variables(cf);
}

static ngx_int_t nchan_postconfig(ngx_conf_t *cf) {
  global_owner_cycle = (void *)ngx_cycle;
  if(nchan_stats_init_postconfig(cf, nchan_stub_status_enabled) != NGX_OK) {
    return NGX_ERROR;
  }
  
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
  
  global_nchan_enabled = 1;
  
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
  mcf->zlib_params.windowBits = 10;
  mcf->zlib_params.memLevel = 8;
  mcf->zlib_params.strategy = Z_DEFAULT_STRATEGY;
#endif
  
  return mcf;
}

static void *nchan_create_srv_conf(ngx_conf_t *cf) {
  nchan_srv_conf_t       *scf = ngx_pcalloc(cf->pool, sizeof(*scf));
  if(scf == NULL) {
    return NGX_CONF_ERROR;
  }
  scf->redis.retry_commands = NGX_CONF_UNSET;
  scf->redis.retry_commands_max_wait = NGX_CONF_UNSET_MSEC;
  scf->redis.node_connect_timeout = NGX_CONF_UNSET_MSEC;
  scf->redis.cluster_connect_timeout = NGX_CONF_UNSET_MSEC;
  
  scf->redis.reconnect_delay = NCHAN_CONF_UNSEC_BACKOFF;
  scf->redis.cluster_recovery_delay = NCHAN_CONF_UNSEC_BACKOFF;
  scf->redis.cluster_check_interval = NCHAN_CONF_UNSEC_BACKOFF;
  scf->redis.idle_channel_ttl = NCHAN_CONF_UNSEC_BACKOFF;
  
  scf->redis.idle_channel_ttl_safety_margin = NGX_CONF_UNSET_MSEC;
  
  scf->redis.cluster_max_failing_msec = NGX_CONF_UNSET_MSEC;
  scf->redis.command_timeout = NGX_CONF_UNSET_MSEC;
  scf->redis.load_scripts_unconditionally = NGX_CONF_UNSET;
  scf->redis.accurate_subscriber_count = NGX_CONF_UNSET;
  scf->redis.master_weight = NGX_CONF_UNSET;
  scf->redis.slave_weight = NGX_CONF_UNSET;
  scf->redis.blacklist_count = NGX_CONF_UNSET;
  scf->redis.blacklist = NULL;
  scf->redis.tls.enabled = NGX_CONF_UNSET;
  scf->redis.tls.verify_certificate = NGX_CONF_UNSET;
  
  scf->redis.stats.enabled = NGX_CONF_UNSET;
  scf->redis.stats.max_detached_time_sec = NGX_CONF_UNSET;
  
  scf->upstream_nchan_loc_conf = NULL;
  return scf;
}

static char *nchan_merge_srv_conf(ngx_conf_t *cf, void *parent, void *child) {
  nchan_srv_conf_t       *prev = parent, *conf = child;
  
  ngx_conf_merge_value(conf->redis.retry_commands, prev->redis.retry_commands, NCHAN_DEFAULT_REDIS_CAN_RETRY_COMMANDS);
  
  ngx_conf_merge_msec_value(conf->redis.retry_commands_max_wait, prev->redis.retry_commands_max_wait, NCHAN_DEFAULT_REDIS_RETRY_COMMANDS_MAX_WAIT_MSEC);
  
  ngx_conf_merge_msec_value(conf->redis.node_connect_timeout, prev->redis.node_connect_timeout, NCHAN_DEFAULT_REDIS_NODE_CONNECT_TIMEOUT_MSEC);
  ngx_conf_merge_msec_value(conf->redis.cluster_connect_timeout, prev->redis.cluster_connect_timeout, NCHAN_DEFAULT_REDIS_CLUSTER_CONNECT_TIMEOUT_MSEC);
  
  ngx_conf_merge_msec_value(conf->redis.cluster_max_failing_msec, prev->redis.cluster_max_failing_msec, NCHAN_DEFAULT_REDIS_CLUSTER_MAX_FAILING_TIME_MSEC);
  
  ngx_conf_merge_msec_value(conf->redis.command_timeout, prev->redis.command_timeout, NCHAN_DEFAULT_REDIS_COMMAND_TIMEOUT_MSEC);
  
  nchan_conf_merge_backoff_value(&conf->redis.reconnect_delay, &prev->redis.reconnect_delay, &NCHAN_REDIS_DEFAULT_RECONNECT_DELAY);
  
  nchan_conf_merge_backoff_value(&conf->redis.cluster_recovery_delay, &prev->redis.cluster_recovery_delay, &NCHAN_REDIS_DEFAULT_CLUSTER_RECOVERY_DELAY);
  
  nchan_conf_merge_backoff_value(&conf->redis.cluster_check_interval, &prev->redis.cluster_check_interval, &NCHAN_REDIS_DEFAULT_CLUSTER_CHECK_INTERVAL);

  nchan_conf_merge_backoff_value(&conf->redis.idle_channel_ttl, &prev->redis.idle_channel_ttl, &NCHAN_REDIS_DEFAULT_IDLE_CHANNEL_TTL);
  
  ngx_conf_merge_msec_value(conf->redis.idle_channel_ttl_safety_margin, prev->redis.idle_channel_ttl_safety_margin, NCHAN_REDIS_IDLE_CHANNEL_TTL_SAFETY_MARGIN_MSEC);
  
  ngx_conf_merge_value(conf->redis.load_scripts_unconditionally, prev->redis.load_scripts_unconditionally, 0);
  
  ngx_conf_merge_value(conf->redis.accurate_subscriber_count, prev->redis.accurate_subscriber_count, 0);
  
  ngx_conf_merge_value(conf->redis.stats.enabled, prev->redis.stats.enabled, NGX_CONF_UNSET);
  //default to unset to enable only if at least 1 stats location is configured
  
  ngx_conf_merge_value(conf->redis.stats.max_detached_time_sec, prev->redis.stats.max_detached_time_sec, NCHAN_REDIS_DEFAULT_STATS_MAX_DETACHED_TIME_SEC);
  
  ngx_conf_merge_value(conf->redis.master_weight, prev->redis.master_weight, 1);
  ngx_conf_merge_value(conf->redis.slave_weight, prev->redis.slave_weight, 1);
  ngx_conf_merge_value(conf->redis.blacklist_count, prev->redis.blacklist_count, 0);
  if(conf->redis.blacklist == NULL) {
    conf->redis.blacklist = prev->redis.blacklist;
  }
  ngx_conf_merge_value(conf->redis.tls.enabled, prev->redis.tls.enabled, 0);
  ngx_conf_merge_value(conf->redis.tls.verify_certificate, prev->redis.tls.verify_certificate, 1);
  ngx_conf_merge_str_value(conf->redis.tls.trusted_certificate, prev->redis.tls.trusted_certificate, "");
    ngx_conf_merge_str_value(conf->redis.tls.trusted_certificate_path, prev->redis.tls.trusted_certificate_path, "");
  ngx_conf_merge_str_value(conf->redis.tls.client_certificate, prev->redis.tls.client_certificate, "");
  ngx_conf_merge_str_value(conf->redis.tls.client_certificate_key, prev->redis.tls.client_certificate_key, "");
  ngx_conf_merge_str_value(conf->redis.tls.server_name, prev->redis.tls.server_name, "");
  ngx_conf_merge_str_value(conf->redis.tls.ciphers, prev->redis.tls.ciphers, "");
  
  ngx_conf_merge_str_value(conf->redis.username, prev->redis.username, "");
  ngx_conf_merge_str_value(conf->redis.password, prev->redis.password, "");
  
  return NGX_CONF_OK;
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
  
  lcf->subscriber_info_string=NULL;
  lcf->subscriber_info_location=NGX_CONF_UNSET;
  
  lcf->subscriber_timeout=NGX_CONF_UNSET;
  lcf->subscribe_only_existing_channel=NGX_CONF_UNSET;
  lcf->redis_idle_channel_cache_timeout=NGX_CONF_UNSET;
  lcf->max_channel_id_length=NGX_CONF_UNSET;
  lcf->max_channel_subscribers=NGX_CONF_UNSET;
  lcf->channel_timeout=NGX_CONF_UNSET;
  lcf->storage_engine=NULL;
  
  lcf->websocket_ping_interval=NGX_CONF_UNSET;
  
  lcf->eventsource_ping.interval=NGX_CONF_UNSET;
  
  lcf->msg_in_etag_only = NGX_CONF_UNSET;
  
  lcf->allow_origin = NULL;
  lcf->allow_credentials = NGX_CONF_UNSET;
  
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
  lcf->redis.nostore_fastpublish = NGX_CONF_UNSET;
  lcf->redis.privdata = NULL;
  lcf->redis.nodeset = NULL;
  lcf->redis.stats.upstream_name = NULL;
  
  lcf->request_handler = NULL;
  
  lcf->benchmark.time = NGX_CONF_UNSET;
  lcf->benchmark.msgs_per_minute = NGX_CONF_UNSET;
  lcf->benchmark.msg_padding = NGX_CONF_UNSET;
  lcf->benchmark.channels = NGX_CONF_UNSET;
  lcf->benchmark.subscribers_per_channel = NGX_CONF_UNSET;
  lcf->benchmark.subscriber_distribution = NCHAN_BENCHMARK_SUBSCRIBER_DISTRIBUTION_UNSET;
  lcf->benchmark.publisher_distribution = NCHAN_BENCHMARK_PUBLISHER_DISTRIBUTION_UNSET;
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

static int is_redis_stats_location(nchan_loc_conf_t *lcf) {
  return lcf->redis.stats.upstream_name != NULL;
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
    else if(is_redis_stats_location(lcf)) {
      ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "Can't have a redis stats location and also be a group access location (nchan_group + nchan_subscriber)");
      return 0;
    }
  }
  if(is_redis_stats_location(lcf)) {
    if (is_group_location(lcf) || is_sub_location(lcf) || is_pub_location(lcf)) {
      ngx_conf_log_error(NGX_LOG_ERR, cf, 0, "Can't have a redis stats location and also a group, publisher, or subscriber location.");
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
  nchan_store_redis_add_active_loc_conf(cf, lcf);
  
  return NGX_CONF_OK;
}

static char *nchan_setup_handler(ngx_conf_t *cf, ngx_int_t (*handler)(ngx_http_request_t *)) {
  ngx_http_core_loc_conf_t       *clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
  //nchan_loc_conf_t               *plcf = conf;
  clcf->handler = handler;
  clcf->if_modified_since = NGX_HTTP_IMS_OFF;
  
  return NGX_CONF_OK;
}

static nchan_loc_conf_t *nchan_loc_conf_get_upstream_lcf(nchan_loc_conf_t *conf, nchan_loc_conf_t *prev) {
  nchan_redis_conf_t *rcf = &conf->redis, *prev_rcf = &prev->redis;
  if(rcf->upstream == prev_rcf->upstream || rcf->upstream == NULL) {
    //same or no upstream, so don't bother
    return NULL;
  }
  else {
    assert(rcf->upstream);
    nchan_srv_conf_t      *upstream_scf = ngx_http_conf_upstream_srv_conf(rcf->upstream, ngx_nchan_module);
    if(upstream_scf && upstream_scf->upstream_nchan_loc_conf) {
      return upstream_scf->upstream_nchan_loc_conf;
    }
    else {
      //ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "nchan upstream srv_conf loc_conf ptr is null");
      return NULL;
    }
  }
}


static char * nchan_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child) {
  nchan_loc_conf_t       *prev = parent, *conf = child;
  nchan_loc_conf_t       *up = nchan_loc_conf_get_upstream_lcf(conf, prev);
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
  
  MERGE_CONF(conf, prev, redis.stats.upstream_name);
  
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
  
  MERGE_CONF(conf, prev, subscriber_info_string);
  if(conf->subscriber_info_string == NULL) { //still null? use the default string
    if(create_complex_value_from_ngx_str(cf, &conf->subscriber_info_string, &DEFAULT_SUBSCRIBER_INFO_STRING) == NGX_CONF_ERROR) {
      return NGX_CONF_ERROR;
    }
  }
  
  ngx_conf_merge_value(conf->subscriber_info_location, prev->subscriber_info_location, 0);
  
  ngx_conf_merge_sec_value(conf->websocket_ping_interval, prev->websocket_ping_interval, NCHAN_DEFAULT_SUBSCRIBER_PING_INTERVAL);
  
  ngx_conf_merge_sec_value(conf->eventsource_ping.interval, prev->eventsource_ping.interval, NCHAN_DEFAULT_SUBSCRIBER_PING_INTERVAL);
  ngx_conf_merge_str_value(conf->eventsource_ping.data, prev->eventsource_ping.comment, "");
  ngx_conf_merge_str_value(conf->eventsource_ping.event, prev->eventsource_ping.event, "ping");
  ngx_conf_merge_str_value(conf->eventsource_ping.data, prev->eventsource_ping.data, "");
  
  ngx_conf_merge_sec_value(conf->subscriber_timeout, prev->subscriber_timeout, NCHAN_DEFAULT_SUBSCRIBER_TIMEOUT);
  ngx_conf_merge_sec_value(conf->redis_idle_channel_cache_timeout, prev->redis_idle_channel_cache_timeout, NCHAN_DEFAULT_REDIS_IDLE_CHANNEL_CACHE_TIMEOUT);
  
  ngx_conf_merge_value(conf->subscribe_only_existing_channel, prev->subscribe_only_existing_channel, 0);
  ngx_conf_merge_value(conf->max_channel_id_length, prev->max_channel_id_length, NCHAN_MAX_CHANNEL_ID_LENGTH);
  ngx_conf_merge_value(conf->max_channel_subscribers, prev->max_channel_subscribers, 0);
  ngx_conf_merge_value(conf->channel_timeout, prev->channel_timeout, NCHAN_DEFAULT_CHANNEL_TIMEOUT);
  
  ngx_conf_merge_str_value(conf->subscriber_http_raw_stream_separator, prev->subscriber_http_raw_stream_separator, "\n");
  
  ngx_conf_merge_str_value(conf->channel_id_split_delimiter, prev->channel_id_split_delimiter, "");
  MERGE_CONF(conf, prev, allow_origin);
  ngx_conf_merge_value(conf->allow_credentials, prev->allow_credentials, 1);
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
  
  if(up && up->redis.namespace.len > 0) { //upstream has a namespace set
    ngx_conf_merge_str_value(conf->redis.namespace, up->redis.namespace, ""); 
  }
  else {
    ngx_conf_merge_str_value(conf->redis.namespace, prev->redis.namespace, "");
  }
  
  if(up)
    ngx_conf_merge_value(conf->redis.ping_interval, up->redis.ping_interval, NGX_CONF_UNSET);
  ngx_conf_merge_value(conf->redis.ping_interval, prev->redis.ping_interval, NCHAN_REDIS_DEFAULT_PING_INTERVAL_TIME);
  
  if(up)
    ngx_conf_merge_value(conf->redis.nostore_fastpublish, up->redis.nostore_fastpublish, NGX_CONF_UNSET);
  ngx_conf_merge_value(conf->redis.nostore_fastpublish, prev->redis.nostore_fastpublish, 0);
  
  if(conf->redis.url_enabled) {
    conf->redis.enabled = 1;
    nchan_store_redis_add_active_loc_conf(cf, conf);
  }
  if(conf->redis.upstream_inheritable && !conf->redis.upstream && prev->redis.upstream && prev->redis.upstream_url.len > 0) {
    conf->redis.upstream_url = prev->redis.upstream_url;
    ngx_conf_set_redis_upstream(cf, &conf->redis.upstream_url, conf);
  }
  
  if(up)
    MERGE_UNSET_CONF(conf->redis.storage_mode, up->redis.storage_mode, REDIS_MODE_CONF_UNSET, REDIS_MODE_CONF_UNSET);
  MERGE_UNSET_CONF(conf->redis.storage_mode, prev->redis.storage_mode, REDIS_MODE_CONF_UNSET, REDIS_MODE_DISTRIBUTED);
  
  if(prev->request_handler != NULL && conf->request_handler == NULL) {
    conf->request_handler = prev->request_handler;
  }
  if(conf->request_handler != NULL) {
    nchan_setup_handler(cf, conf->request_handler);
  }
  
  ngx_conf_merge_value(conf->benchmark.time, prev->benchmark.time, 10);
  ngx_conf_merge_value(conf->benchmark.msgs_per_minute, prev->benchmark.msgs_per_minute, 120);
  ngx_conf_merge_value(conf->benchmark.msg_padding, prev->benchmark.msg_padding, 0);
  ngx_conf_merge_value(conf->benchmark.channels, prev->benchmark.channels, 1000);
  ngx_conf_merge_value(conf->benchmark.subscribers_per_channel, prev->benchmark.subscribers_per_channel, 100);
  MERGE_UNSET_CONF(conf->benchmark.subscriber_distribution, prev->benchmark.subscriber_distribution, NCHAN_BENCHMARK_SUBSCRIBER_DISTRIBUTION_UNSET, NCHAN_BENCHMARK_SUBSCRIBER_DISTRIBUTION_RANDOM);
  MERGE_UNSET_CONF(conf->benchmark.publisher_distribution, prev->benchmark.publisher_distribution, NCHAN_BENCHMARK_PUBLISHER_DISTRIBUTION_UNSET, NCHAN_BENCHMARK_PUBLISHER_DISTRIBUTION_RANDOM);
  
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

static char *nchan_subscriber_info_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t     *lcf = conf;
  nchan_conf_subscriber_types_t *subt = &lcf->sub;
  
  // doesn't make sense to have longpoll be the default HTTP subscriber, since channel info locations are likely to be curl'd by developers
  subt->poll=0;
  subt->http_raw_stream = 1;
  subt->longpoll=0;
  subt->websocket=1;
  subt->eventsource=1;
  subt->http_chunked=1;
  subt->http_multipart=1;
  
  lcf->subscriber_info_location = 1;
  
  lcf->message_timeout=10;
  lcf->complex_message_timeout = NULL;
  
  lcf->request_handler = &nchan_subscriber_info_handler;
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

static ngx_int_t set_complex_value(ngx_conf_t *cf, ngx_http_complex_value_t **cv, char *val) {
  ngx_http_compile_complex_value_t    ccv;
  ngx_str_t                          *value = ngx_palloc(cf->pool, sizeof(ngx_str_t));;
  if(value == NULL) {
    return NGX_ERROR;
  }
  value->data = (u_char *)val;
  value->len = strlen(val);
  *cv = ngx_palloc(cf->pool, sizeof(ngx_http_complex_value_t));
  if (*cv == NULL) {
    return NGX_ERROR;
  } 
  ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));
  ccv.cf = cf;
  ccv.value = value;
  ccv.complex_value = *cv;
  if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
    return NGX_ERROR;
  }
  
  return NGX_OK;
}

static ngx_int_t set_complex_value_array_size1(ngx_conf_t *cf, nchan_complex_value_arr_t *chid, char *val) {
  chid->n = 1;
  return set_complex_value(cf, &chid->cv[0], val);
}

static char *nchan_benchmark_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t     *lcf = conf;
  global_benchmark_enabled = 1;
  lcf->request_handler = &nchan_benchmark_handler;
  
  
  //set group
  if(set_complex_value(cf, &lcf->channel_group, "benchmark") != NGX_OK) {
    return "error setting benchmark channel group";
  }
  //set pub and sub channel ids
  if(set_complex_value_array_size1(cf, &lcf->pub_chid, "control") != NGX_OK) {
    return "error setting benchmark control channel";
  }
  if(set_complex_value_array_size1(cf, &lcf->sub_chid, "data") != NGX_OK) {
    return "error setting benchmark data channel";
  }
  
  lcf->sub.websocket = 1;
  lcf->pub.websocket = 1;
  
  return NGX_CONF_OK;
}

static char *nchan_benchmark_subscriber_distribution_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_str_t          *val = &((ngx_str_t *) cf->args->elts)[1];
  nchan_loc_conf_t   *lcf = conf;
  if(nchan_strmatch(val, 1, "random")) {
    lcf->benchmark.subscriber_distribution = NCHAN_BENCHMARK_SUBSCRIBER_DISTRIBUTION_RANDOM;
  }
  else if(nchan_strmatch(val, 2, "optimal", "best")) {
    lcf->benchmark.subscriber_distribution = NCHAN_BENCHMARK_SUBSCRIBER_DISTRIBUTION_OPTIMAL;
  }
  else {
    return "invalid value, must be \"random\" or \"optimal\"";
  }
  return NGX_CONF_OK;
}
static char *nchan_benchmark_publisher_distribution_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_str_t          *val = &((ngx_str_t *) cf->args->elts)[1];
  nchan_loc_conf_t   *lcf = conf;
  if(nchan_strmatch(val, 1, "random")) {
    lcf->benchmark.publisher_distribution = NCHAN_BENCHMARK_PUBLISHER_DISTRIBUTION_RANDOM;
  }
  else if(nchan_strmatch(val, 2, "optimal", "best")) {
    lcf->benchmark.publisher_distribution = NCHAN_BENCHMARK_PUBLISHER_DISTRIBUTION_OPTIMAL;
  }
  else {
    return "invalid value, must be \"random\" or \"optimal\"";
  }
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
  
  mcf->zlib_params.windowBits = np;
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
  if(np < 1 || np > 9) {
    return "must be between 1 and 9";
  }
  
  mcf->zlib_params.memLevel = np;
#endif
  return NGX_CONF_OK;
}


static void nchan_exit_worker(ngx_cycle_t *cycle) {
  if(!global_nchan_enabled) {
    return;
  }
  
  nchan_stats_exit_worker(cycle);
  
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
  if(!global_nchan_enabled) {
    return;
  }
  nchan_stats_exit_master(cycle);
  
  if(global_benchmark_enabled) {
    nchan_benchmark_exit_master(cycle);
  }
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
  nchan_loc_conf_t *lcf = conf;
  return nchan_set_complex_value_array(cf, cmd, conf, &lcf->sub_chid);
}

static char *nchan_set_pubsub_channel_id(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t *lcf = conf;
  return nchan_set_complex_value_array(cf, cmd, conf, &lcf->pubsub_chid);
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
  
  if( val->len && val->data[val->len - 1] != '\n' ) { //must end in a newline if not empty
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

static char *ngx_conf_set_redis_subscribe_weights(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_int_t  master = NGX_CONF_UNSET;
  ngx_int_t  slave = NGX_CONF_UNSET;
  ngx_str_t *val = cf->args->elts;
  ngx_str_t *cur;
  unsigned   i;
  nchan_srv_conf_t *scf = conf;
  for(i=1; i < cf->args->nelts; i++) {
    cur = &val[i];
    if(nchan_str_after(&cur, "master=")) {
      if((master = ngx_atoi(cur->data, cur->len)) == NGX_ERROR) {
        return "has invalid weight for master";
      }
    }
    else if(nchan_str_after(&cur, "slave=")) {
      if((slave = ngx_atoi(cur->data, cur->len)) == NGX_ERROR) {
        return "has invalid weight for slave";
      }
    }
  }
  
  if(master != NGX_CONF_UNSET) {
    scf->redis.master_weight = master;
  }
  if(slave != NGX_CONF_UNSET) {
    scf->redis.slave_weight = slave;
  }
  
  return NGX_CONF_OK;
}

static char *ngx_conf_set_jitter(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_str_t          *val = &((ngx_str_t *) cf->args->elts)[1];
  char               *p = conf;
  double             *field = (double *) (p + cmd->offset);
  double              fp;
  
  if((fp = nchan_atof(val->data, val->len)) == NGX_ERROR) {
    return "invalid value, must be a non-negative floating-point number";
  }

  if(fp >= 1) {
    return "jitter multiplier cannot exceed 1";
  }
  if(fp < 0) {
    return "jitter multiplier cannot be less than 0";
  }
  *field = fp;
  return NGX_CONF_OK;
}

static char *ngx_conf_set_exponential_backoff(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_str_t          *val = &((ngx_str_t *) cf->args->elts)[1];
  char               *p = conf;
  double             *field = (double *) (p + cmd->offset);
  double              fp;
  
  if((fp = nchan_atof(val->data, val->len)) == NGX_ERROR) {
    return "invalid value, must be a non-negative floating-point number";
  }

  if(fp < 0) {
    return "value cannot be less than 0";
  }
  
  *field = fp;
  return NGX_CONF_OK;
}

static char *ngx_conf_enable_redis(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  char                *rc;
  ngx_flag_t          *fp;
  char                *p = conf;
  nchan_loc_conf_t    *lcf = conf;
  
  ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "Use of %V is discouraged in favor of nchan_redis_pass.", &cmd->name);
  
  rc = ngx_conf_set_flag_slot(cf, cmd, conf);
  if(rc == NGX_CONF_ERROR) {
    return rc;
  }
  fp = (ngx_flag_t *) (p + cmd->offset);
  
  if(*fp) {
    if(!lcf->redis.enabled) {
      lcf->redis.enabled = 1;
      nchan_store_redis_add_active_loc_conf(cf, lcf);
    }
    global_redis_enabled = 1;
  }
  else {
    nchan_store_redis_remove_active_loc_conf(cf, lcf);
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
  nchan_loc_conf_t                    *lcf = conf;
  uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);  
  nchan_srv_conf_t                    *scf = NULL;
  scf = ngx_http_conf_upstream_srv_conf(uscf, ngx_nchan_module);
  if(scf->upstream_nchan_loc_conf) {
    assert(scf->upstream_nchan_loc_conf == lcf);
  }
  else {
    //is this even a safe technique? it might break in the future...
    scf->upstream_nchan_loc_conf = lcf;
  }
  
  
  if(uscf->servers == NULL) {
        uscf->servers = ngx_array_create(cf->pool, 4, sizeof(ngx_http_upstream_server_t));
  }
  if ((usrv = ngx_array_push(uscf->servers)) == NULL) {
    return NGX_CONF_ERROR;
  }
  value = cf->args->elts;
  
  if(!nchan_store_redis_validate_url(&value[1])) {
    return "url is invalid";
  }
  
  ngx_memzero(usrv, sizeof(*usrv));
#if nginx_version >= 1007002
  usrv->name = value[1];
#endif
  usrv->addrs = ngx_pcalloc(cf->pool, sizeof(ngx_addr_t));
  usrv->addrs->name = value[1];
  
  uscf->peer.init_upstream = nchan_upstream_dummy_roundrobin_init;
  return NGX_CONF_OK;
}

static void ipv6_prefix_size_to_mask(int prefix_size, struct in6_addr *mask) {
  ngx_memzero(mask, sizeof(*mask));
  int i;
  for(i=0; prefix_size > 0; prefix_size-=8, i++) {
    mask->s6_addr[i]= (prefix_size >= 8) ? 0xFF : (unsigned long)(0xFFU << (8 - prefix_size));
  }
}

static void ipv4_prefix_size_to_mask(int prefix_size, struct in_addr *mask) {
  mask->s_addr= (in_addr_t )(prefix_size > 0 ? htonl(~((1 << (32 - prefix_size)) - 1)) : 0);
}

static char *ngx_conf_set_redis_ip_blacklist(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_srv_conf_t    *scf = conf;
  ngx_str_t           *cur;
  ngx_str_t           *val = cf->args->elts;
  int                  i;
  nchan_redis_ip_range_t *blacklist = ngx_palloc(cf->pool, sizeof(*blacklist) * (cf->args->nelts - 1));
  if(blacklist == NULL) {
    return "couldn't allocate Redis server blacklist";
  }
  
  scf->redis.blacklist = blacklist;
  scf->redis.blacklist_count = cf->args->nelts - 1;
  
  for(i=1; i <= scf->redis.blacklist_count; i++) {
    cur = &val[i];
    nchan_redis_ip_range_t *entry = &blacklist[i-1];
    entry->str = *cur;
    int                     prefix_size;
    u_char                 *slash = memchr(cur->data, '/', cur->len);
    if(slash) {
      prefix_size = ngx_atoi(slash+1, cur->len - (slash + 1 - cur->data));
      if(prefix_size == NGX_ERROR) {
        return "invalid CIDR range prefix size";
      }
    }
    else {
      prefix_size = -1;
      slash = &cur->data[cur->len];
    }
    
    char buf[64];
    ngx_memzero(buf, sizeof(buf));
    memcpy(buf, cur->data, (slash - cur->data));
    
    struct addrinfo hints = {0};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_PASSIVE;
    
    struct addrinfo *res;
    if(getaddrinfo(buf, NULL, &hints, &res) != 0) {
      return "unable to parse IP address";
    }
    entry->family = res->ai_family;
    if(entry->family == AF_INET) {
      struct sockaddr_in *sa = (struct sockaddr_in *)res->ai_addr;
      entry->addr.ipv4 = sa->sin_addr;
      entry->addr_block.ipv4 = sa->sin_addr;
    }
#ifdef AF_INET6
    else if(entry->family == AF_INET6) {
      struct sockaddr_in6 *sa = (struct sockaddr_in6 *)res->ai_addr;
      entry->addr.ipv6 = sa->sin6_addr;
      entry->addr_block.ipv6 = sa->sin6_addr;
    }
#endif
    else {
      return "invalid address family";
    }
    
    if(prefix_size == 0) {
      return "netmask size of 0 would block everything";
    }
    
    if(prefix_size == -1) {
      //no prefix size given, assume we're blacklisting single ip.
      //prefix size depends on ipv4 or ipv6
      prefix_size = entry->family == AF_INET ? 32 : 128;
    }
    entry->prefix_size = prefix_size;
    
    if(entry->family == AF_INET) {
      if(prefix_size > 32) {
        return "netmask size cannot exceed 32 for IPv4";
      }
      ipv4_prefix_size_to_mask(prefix_size, &entry->mask.ipv4);
      entry->addr_block.ipv4.s_addr &= entry->mask.ipv4.s_addr;
      
    }
#ifdef AF_INET6
    else if(entry->family == AF_INET6) {
      if(prefix_size > 128) {
        return "netmask size cannot exceed 128 for IPv4";
      }
      ipv6_prefix_size_to_mask(prefix_size, &entry->mask.ipv6);
      unsigned j;
      uint8_t *addr = entry->addr_block.ipv6.s6_addr;
      uint8_t *mask = entry->mask.ipv6.s6_addr;
      for(j=0; j<sizeof(entry->addr_block.ipv6.s6_addr); j++) {
        addr[j] &= mask[j];
      }
    }
#endif
    else {
      return "invalid address family";
    }
    freeaddrinfo(res);
  }
  return NGX_OK;
}

static char *ngx_conf_set_str_slot_no_newlines(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_str_t *val = cf->args->elts;
  ngx_str_t *arg = &val[1];
  
  if(nchan_ngx_str_substr(arg, "\n")) {
    return "can't contain any newline characters";
  }
  
  return ngx_conf_set_str_slot(cf, cmd, conf);
}

static char *ngx_conf_set_redis_url(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t  *lcf = conf;
  ngx_str_t *val = cf->args->elts;
  ngx_str_t *arg = &val[1];
  
  ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "Use of %V is discouraged in favor of an upstream { } block with nchan_redis_server %V;", &cmd->name, arg);
  
  if(lcf->redis.upstream) {
    return "can't be set here: already using nchan_redis_pass";
  }
  if(!nchan_store_redis_validate_url(arg)) {
    return "url is invalid";
  }
  
  return ngx_conf_set_str_slot(cf, cmd, conf);
}

static char *ngx_conf_process_redis_namespace_slot(ngx_conf_t *cf, void *post, void *fld) {
  ngx_str_t *arg = fld;

  if(memchr(arg->data, '{', arg->len)) {
    return "can't contain character '{'";
  }
  
  if(memchr(arg->data, '}', arg->len)) {
    return "can't contain character '}'";
  }
  
  if(arg->len > 0 && arg->data[arg->len-1] != ':') {
    u_char  *nns;
    if((nns = ngx_palloc(cf->pool, arg->len + 2)) == NULL) {
      return "couldn't allocate redis namespace data";
    }
    ngx_memcpy(nns, arg->data, arg->len);
    nns[arg->len]=':';
    nns[arg->len+1]='\0';
    arg->len++;
    arg->data=nns;
  }
  
  return NGX_CONF_OK;
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
  else if(nchan_strmatch(arg, 1, "nostore") ||  nchan_strmatch(arg, 1, "distributed-nostore")) {
    *field = REDIS_MODE_DISTRIBUTED_NOSTORE;
  }
  else {
    return "is invalid, must be one of 'distributed',  'backup' or 'nostore'";
  }
  
  return NGX_CONF_OK;
}

static char *ngx_conf_set_redis_upstream_pass(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t  *lcf = conf;
  ngx_str_t         *value = cf->args->elts;
  
  lcf->redis.upstream_url = value[1];
  return ngx_conf_set_redis_upstream(cf, &value[1], conf);
}


static char *nchan_redis_stats_directive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  nchan_loc_conf_t     *lcf = conf;
  
  ngx_http_set_complex_value_slot(cf, cmd, conf);
  
  if(!is_valid_location(cf, lcf)) {
    return NGX_CONF_ERROR;
  }
  
  nchan_redis_stats_enabled = 1;
  
  lcf->request_handler = &nchan_redis_stats_handler;
  return NGX_CONF_OK;
}

#include "nchan_config_commands.c" //hideous but hey, it works

static ngx_http_module_t  nchan_module_ctx = {
    nchan_preconfig,               /* preconfiguration */
    nchan_postconfig,              /* postconfiguration */
    nchan_create_main_conf,        /* create main configuration */
    NULL,                          /* init main configuration */
    nchan_create_srv_conf,         /* create server configuration */
    nchan_merge_srv_conf,          /* merge server configuration */
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
