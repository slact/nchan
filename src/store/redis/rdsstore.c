#include <nchan_module.h>

#include <assert.h>
#include <netinet/ip.h>
#include "store-private.h"
#include "store.h"
#include "cluster.h"

#include "redis_nginx_adapter.h"

#include <util/nchan_msg.h>
#include <util/nchan_rbtree.h>
#include <store/store_common.h>

#include <store/memory/store.h>

#include "redis_lua_commands.h"

#define REDIS_CHANNEL_EMPTY_BUT_SUBSCRIBED_TTL_STEP 600 //10min
#define REDIS_CHANNEL_EMPTY_BUT_SUBSCRIBED_TTL_MAX 2628000 //whole month

#define REDIS_LUA_HASH_LENGTH 40

#define REDIS_RECONNECT_TIME 5000

#define REDIS_STALL_CHECK_TIME 0 //disable for now

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "REDISTORE: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDISTORE: " fmt, ##args)

#define REDIS_CONNECTION_FOR_PUBLISH_WAIT 5000

u_char            redis_subscriber_id[255];
size_t            redis_subscriber_id_len;

static rbtree_seed_t              redis_data_tree;
static rdstore_channel_head_t    *chanhead_hash = NULL;
static size_t                     redis_publish_message_msgkey_size;

redis_connection_status_t redis_connection_status(nchan_loc_conf_t *cf) {
  rdstore_data_t  *rdata = cf->redis.privdata;
  return rdata->status;
}

void redis_store_expire_on_connected_callback(ngx_event_t *ev) {
  callback_chain_t  *d = ev->data;
  
  if(d->prev)
    d->prev->next = d->next;
  if(d->next)
    d->next->prev = d->prev;
  if(d->rdata->on_connected.head == d)
    d->rdata->on_connected.head = d->next;
  if(d->rdata->on_connected.tail == d)
    d->rdata->on_connected.tail = d->prev;
  
  d->cb(NGX_DECLINED, d->rdata, d->pd);
  ngx_free(d);
}

ngx_int_t redis_store_callback_on_connected(nchan_loc_conf_t *cf, ngx_msec_t max_wait, callback_pt cb, void *privdata) {
  rdstore_data_t    *rdata = cf->redis.privdata;
  callback_chain_t  *d;
  
  if(rdata->status == CONNECTED) {
    cb(NGX_OK, rdata, privdata);
  }
  
  d = ngx_alloc(sizeof(*d), ngx_cycle->log);
  
  d->cb = cb;
  d->pd = privdata;
  
  d->rdata = rdata;
  d->next = NULL;
  ngx_memzero(&d->timeout_ev, sizeof(d->timeout_ev));
  
  if(max_wait > 0) {
    nchan_init_timer(&d->timeout_ev, redis_store_expire_on_connected_callback, d);
    ngx_add_timer(&d->timeout_ev, max_wait);
  }
  
  d->prev = rdata->on_connected.tail;
  if(rdata->on_connected.tail)
    rdata->on_connected.tail->next = d;
  rdata->on_connected.tail = d;
  if(rdata->on_connected.head == NULL)
    rdata->on_connected.head = d;
  return NGX_OK;
}

#define CHANNEL_HASH_FIND(id_buf, p)    HASH_FIND( hh, chanhead_hash, (id_buf)->data, (id_buf)->len, p)
#define CHANNEL_HASH_ADD(chanhead)      HASH_ADD_KEYPTR( hh, chanhead_hash, (chanhead->id).data, (chanhead->id).len, chanhead)
#define CHANNEL_HASH_DEL(chanhead)      HASH_DEL( chanhead_hash, chanhead)

#include <stdbool.h>
#include "cmp.h"

#define redis_command(rdata, cb, pd, fmt, args...)                   \
  do {                                                               \
    if(redis_ensure_connected(rdata) == NGX_OK) {                    \
      rdata->pending_commands++;                                     \
      nchan_update_stub_status(redis_pending_commands, 1);           \
      redisAsyncCommand((rdata)->ctx, cb, pd, fmt, ##args);          \
    } else {                                                         \
      ERR("Can't run redis command: no connection to redis server.");\
    }                                                                \
  }while(0)                                                          \

#define redis_subscriber_command(rdata, cb, pd, fmt, args...)        \
  do {                                                               \
    if(redis_ensure_connected(rdata) == NGX_OK) {                    \
      redisAsyncCommand((rdata)->sub_ctx, cb, pd, fmt, ##args);      \
    } else {                                                         \
      ERR("Can't run redis command: no connection to redis server.");\
    }                                                                \
  }while(0)                                                          \
  
#define redis_sync_command(rdata, fmt, args...)                      \
  do {                                                               \
    if((rdata)->sync_ctx == NULL) {                                  \
      redis_nginx_open_sync_context(((rdata)->connect_params.peername.len > 0 ? &(rdata)->connect_params.peername : &(rdata)->connect_params.hostname), (rdata)->connect_params.port, (rdata)->connect_params.db, &(rdata)->connect_params.password, &(rdata)->sync_ctx); \
    }                                                                \
    if((rdata)->sync_ctx) {                                          \
      redisCommand((rdata)->sync_ctx, fmt, ##args);                  \
    } else {                                                         \
      ERR("Can't run redis command: no connection to redis server.");\
    }                                                                \
  }while(0)
  

#define redis_script(script_name, rdata, cb, pd, fmt, args...)                         \
  redis_command(rdata, cb, pd, "EVALSHA %s " fmt, redis_lua_scripts.script_name.hash, ##args)

#define redis_sync_script(script_name, rdata, fmt, args...)                    \
  redis_sync_command(rdata, "EVALSHA %s " fmt, redis_lua_scripts.script_name.hash, ##args)
  

#define nchan_redis_script(script_name, rdata, cb, pd, channel_id, fmt, args...)       \
  redis_script(script_name, rdata, cb, pd, "0 %b %b " fmt, STR(&rdata->namespace), STR(channel_id), ##args)

#define nchan_redis_sync_script(script_name, rdata, channel_id, fmt, args...)  \
  redis_sync_script(script_name, rdata, "0 %b %b " fmt, STR(&rdata->namespace), STR(channel_id), ##args)
  
  

  
#define CHECK_REPLY_STR(reply) ((reply)->type == REDIS_REPLY_STRING)
#define CHECK_REPLY_STRVAL(reply, v) ( CHECK_REPLY_STR(reply) && ngx_strcmp((reply)->str, v) == 0 )
#define CHECK_REPLY_STRNVAL(reply, v, n) ( CHECK_REPLY_STR(reply) && ngx_strncmp((reply)->str, v, n) == 0 )
#define CHECK_REPLY_INT(reply) ((reply)->type == REDIS_REPLY_INTEGER)
#define CHECK_REPLY_INTVAL(reply, v) ( CHECK_REPLY_INT(reply) && (reply)->integer == v )
#define CHECK_REPLY_ARRAY_MIN_SIZE(reply, size) ( (reply)->type == REDIS_REPLY_ARRAY && (reply)->elements >= (unsigned )size )
#define CHECK_REPLY_NIL(reply) ((reply)->type == REDIS_REPLY_NIL)
#define CHECK_REPLY_INT_OR_STR(reply) ((reply)->type == REDIS_REPLY_INTEGER || (reply)->type == REDIS_REPLY_STRING)
  
static ngx_int_t nchan_store_publish_generic(ngx_str_t *, rdstore_data_t *, nchan_msg_t *, ngx_int_t, const ngx_str_t *);

static rdstore_channel_head_t * nchan_store_get_chanhead(ngx_str_t *channel_id, rdstore_data_t *rdata);

static ngx_buf_t *set_buf(ngx_buf_t *buf, u_char *start, off_t len){
  ngx_memzero(buf, sizeof(*buf));
  buf->start = start;
  buf->end = start + len;
  buf->pos = buf->start;
  buf->last = buf->end;
  return buf;
}

static int ngx_strmatch(ngx_str_t *str, char *match) {
  return ngx_strncmp(str->data, match, str->len) == 0;
}

static int ngx_str_chop_if_startswith(ngx_str_t *str, char *match) {
  char *cur, *max = (char *)str->data + str->len;
  for(cur = (char *)str->data; cur < max; cur++, match++) {
    if(*match == '\0') {
      str->len -= (u_char *)cur - str->data;
      str->data = (u_char *)cur;
      return 1;
    }
    else if(*match != *cur)
      break;
  }
  return 0;
}

static u_char *fwd_buf(ngx_buf_t *buf, size_t sz) {
  u_char *ret = buf->pos;
  buf->pos += sz;
  return ret;
}

static void fwd_buf_to_str(ngx_buf_t *buf, size_t sz, ngx_str_t *str) {
  str->data = fwd_buf(buf, sz);
  str->len = sz;;
}

static bool ngx_buf_reader(cmp_ctx_t *ctx, void *data, size_t limit) {
  ngx_buf_t   *buf=(ngx_buf_t *)ctx->buf;
  if(buf->pos + limit > buf->last){
    return false;
  }
  ngx_memcpy(data, buf->pos, limit);
  buf->pos += limit;
  return true;
}
static size_t ngx_buf_writer(cmp_ctx_t *ctx, const void *data, size_t count) {
  return 0;
}

ngx_int_t parse_redis_url(ngx_str_t *url, redis_connect_params_t *rcp) {

  u_char                  *cur, *last, *ret;
  
  cur = url->data;
  last = url->data + url->len;
  
  
  //ignore redis://
  if(ngx_strnstr(cur, "redis://", 8) != NULL) {
    cur += 8;
  }
  
  if(cur[0] == ':') {
    cur++;
    if((ret = ngx_strlchr(cur, last, '@')) == NULL) {
      rcp->password.data = NULL;
      rcp->password.len = 0;
    }
    else {
      rcp->password.data = cur;
      rcp->password.len = ret - cur;
      cur = ret + 1;
    }
  }
  else {
    rcp->password.data = NULL;
    rcp->password.len = 0;
  }
  
  ///port:host
  if((ret = ngx_strlchr(cur, last, ':')) == NULL) {
    //just host
    rcp->port = 6379;
    if((ret = ngx_strlchr(cur, last, '/')) == NULL) {
      ret = last;
    }
    rcp->hostname.data = cur;
    rcp->hostname.len = ret - cur;
  }
  else {
    rcp->hostname.data = cur;
    rcp->hostname.len = ret - cur;
    cur = ret + 1;
    
    //port
    if((ret = ngx_strlchr(cur, last, '/')) == NULL) {
      ret = last;
    }
    
    rcp->port = ngx_atoi(cur, ret-cur);
    if(rcp->port == NGX_ERROR) {
      rcp->port = 6379;
    }
  }
  cur = ret;
  
  if(cur[0] == '/') {
    cur++;
    rcp->db = ngx_atoi(cur, last-cur);
    if(rcp->db == NGX_ERROR) {
      rcp->db = 0;
    }
  }
  else {
    rcp->db = 0;
  }
  
  return NGX_OK;
}

static void free_chanhead(rdstore_channel_head_t *ch);

static void redis_store_reap_chanhead(rdstore_channel_head_t *ch) {
  rdstore_data_t   *rdata;
  if(!ch->shutting_down) {
    assert(ch->sub_count == 0 && ch->fetching_message_count == 0);
  }
  
  DBG("reap channel %V", &ch->id);

  rdata = redis_cluster_rdata_from_channel(ch);
  if(ch->pubsub_status == SUBBED) {
    assert(ch->rdt->storage_mode == REDIS_MODE_DISTRIBUTED);
    ch->pubsub_status = UNSUBBING;
    redis_subscriber_command(rdata, NULL, NULL, "UNSUBSCRIBE %b{channel:%b}:pubsub", STR(&rdata->namespace), STR(&ch->id));
  }
  
  if(ch->rd_prev) {
    ch->rd_prev->rd_next = ch->rd_next;
  }
  if(ch->rd_next) {
    ch->rd_next->rd_prev = ch->rd_prev;
  }
  if(rdata->channels_head == ch) {
    rdata->channels_head = ch->rd_next;
  }
  
  if(rdata->almost_deleted_channels_head) {
    rdata->almost_deleted_channels_head->rd_prev = ch;
    ch->rd_next = rdata->almost_deleted_channels_head;
    ch->rd_prev = NULL;
  }
  else {
    ch->rd_next = NULL;
    ch->rd_prev = NULL;
  }
  
  rdata->almost_deleted_channels_head = ch;
  
  if(rdata->node.cluster && rdata->node.cluster->orphan_channels_head == ch) {
    rdata->node.cluster->orphan_channels_head = ch->rd_next;
  }
  
  DBG("chanhead %p (%V) is empty and expired. delete.", ch, &ch->id);
  if(ch->keepalive_timer.timer_set) {
    ngx_del_timer(&ch->keepalive_timer);
  }
  stop_spooler(&ch->spooler, 1);
  CHANNEL_HASH_DEL(ch);
  
  free_chanhead(ch);
}

static void free_chanhead(rdstore_channel_head_t *ch) {
  rdstore_data_t   *rdata = redis_cluster_rdata_from_channel(ch);
  if(ch->rd_prev) {
    ch->rd_prev->rd_next = ch->rd_next;
  }
  if(ch->rd_next) {
    ch->rd_next->rd_prev = ch->rd_prev;
  }
  if(rdata->almost_deleted_channels_head == ch) {
    rdata->almost_deleted_channels_head = ch->rd_next;
  }

  DBG("freed channel %V %p", &ch->id, ch);
  
  ngx_free(ch);
}

static ngx_int_t nchan_redis_chanhead_ready_to_reap(rdstore_channel_head_t *ch, uint8_t force) {
  if(!force) {
    if(ch->status != INACTIVE) {
      return NGX_DECLINED;
    }
    
    if(ch->reserved > 0 ) {
      DBG("not yet time to reap %V, %i reservations left", &ch->id, ch->reserved);
      return NGX_DECLINED;
    }
    
    if(ch->gc_time - ngx_time() > 0) {
      DBG("not yet time to reap %V, %i sec left", &ch->id, ch->gc_time - ngx_time());
      return NGX_DECLINED;
    }
    
    if (ch->sub_count > 0) { //there are subscribers
      DBG("not ready to reap %V, %i subs left", &ch->id, ch->sub_count);
      return NGX_DECLINED;
    }
    
    if (ch->fetching_message_count > 0) { //there are subscribers
      DBG("not ready to reap %V, fetching %i messages", &ch->id, ch->fetching_message_count);
      return NGX_DECLINED;
    }
    
    //if(ch->pubsub_status == SUBBING) {
    //  return NGX_DECLINED;
    //}
    
    //DBG("ok to delete channel %V", &ch->id);
    return NGX_OK;
  }
  else {
    //force delete is always ok
    return NGX_OK;
  }
}

static redisAsyncContext **whichRedisContext(rdstore_data_t *rdata, const redisAsyncContext *ac) {
  if(rdata->ctx == ac) {
    return &rdata->ctx;
  }
  else if(rdata->sub_ctx == ac) {
    return &rdata->sub_ctx;
  }
  return NULL;
}

static void rdt_set_status(rdstore_data_t *rdata, redis_connection_status_t status, const redisAsyncContext *ac) {
  redis_connection_status_t prev_status = rdata->status;
  
  if(rdata->node.cluster) {
    redis_cluster_node_change_status(rdata, status);
  }
  
  rdata->status = status;
  
  if(status == DISCONNECTED) {
    if(!rdata->shutting_down && !rdata->reconnect_timer.timer_set) {
      ngx_add_timer(&rdata->reconnect_timer, REDIS_RECONNECT_TIME);
    }
    
    ngx_memzero(&rdata->detailed_status, sizeof(rdata->detailed_status));
    
    //clear the resolved peername -- it should get re-resolved on reconnect
    rdata->connect_params.peername.len = 0;
    
    if(rdata->ping_timer.timer_set) {
      ngx_del_timer(&rdata->ping_timer);
    }
    if(rdata->stall_timer.timer_set) {
      ngx_del_timer(&rdata->stall_timer);
    }
    
    if(prev_status == CONNECTED) {
      rdstore_channel_head_t   *cur;
      
      //nchan_update_stub_status(redis_pending_commands, -(rdata->pending_commands)); // not  necessary, callbacks will be called
      nchan_update_stub_status(redis_connected_servers, -1);
      
      if(!rdata->node.cluster) {
        //not in a cluster -- disconnect all subs right away
        for(cur = rdata->channels_head; cur != NULL; cur = cur->rd_next) {
          cur->spooler.fn->broadcast_status(&cur->spooler, NGX_HTTP_GONE, &NCHAN_HTTP_STATUS_410);
          if(!cur->in_gc_reaper) {
            redis_chanhead_gc_add(cur, 0, "redis connection gone");
          }
        }
        
        nchan_reaper_flush(&rdata->chanhead_reaper);
        
        while((cur = rdata->almost_deleted_channels_head) != NULL) {
          free_chanhead(cur);
        }
      }
    }
    
    if(ac) {
      redisAsyncContext **pac = whichRedisContext(rdata, ac);
      if(pac)
        *pac = NULL;
    }
  }
  else if(status == CONNECTED && prev_status != CONNECTED) {
    callback_chain_t    *cur, *next;
    
    if(rdata->generation == 0) {
      nchan_log_notice("Established connection to redis at %V.", rdata->connect_url);
    }
    else {
      nchan_log_warning("Re-established connection to redis at %V.", rdata->connect_url);
    }
    
    nchan_update_stub_status(redis_connected_servers, 1);
    rdata->time_connected = ngx_time();
    
    if(!rdata->ping_timer.timer_set && rdata->ping_interval > 0) {
      ngx_add_timer(&rdata->ping_timer, rdata->ping_interval * 1000);
    }
    
    rdata->pending_commands = 0;
    if(!rdata->stall_timer.timer_set && REDIS_STALL_CHECK_TIME > 0) {
      ngx_add_timer(&rdata->stall_timer, REDIS_STALL_CHECK_TIME);
    }
    
    //on_connected callbacks
    cur = rdata->on_connected.head;
    rdata->on_connected.head = NULL;
    rdata->on_connected.tail = NULL;
    
    while(cur != NULL) {
      next = cur->next;
      cur->cb(NGX_OK, rdata, cur->pd);
      if(cur->timeout_ev.timer_set) {
        ngx_del_timer(&cur->timeout_ev);
      }
      ngx_free(cur);
      cur = next;
    }
    rdata->generation++;
  }
}

void __rdt_process_detailed_status(rdstore_data_t *rdata) {
 if(rdata->detailed_status.connection_established
  && rdata->detailed_status.authenticated
  && rdata->detailed_status.not_loading_data
  && rdata->detailed_status.loaded_scripts
  && rdata->detailed_status.cluster_checked) {
   rdt_set_status(rdata, CONNECTED, NULL);
 }
}

static void redis_reconnect_timer_handler(ngx_event_t *ev) {
  if(!ev->timedout || ngx_exiting || ngx_quit)
    return;
  ev->timedout = 0;
  redis_ensure_connected((rdstore_data_t *)ev->data);
}

static void redis_ping_callback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply         *reply = (redisReply *)r;
  rdstore_data_t     *rdata = c->data;
  
  rdata->pending_commands--;
  nchan_update_stub_status(redis_pending_commands, -1);
  
  if(redisReplyOk(c, r)) {
    if(CHECK_REPLY_INT(reply)) {
      if(reply->integer < 1) {
        ERR("failed to forward ping to sub_ctx");
      }
    }
    else {
      ERR("unexpected reply type for redis_ping_callback");
    }
  }
}

static void redis_ping_timer_handler(ngx_event_t *ev) {
  rdstore_data_t  *rdata = ev->data, *cmd_rdata;
  if(!ev->timedout || ngx_exiting || ngx_quit)
    return;
  
  ev->timedout = 0;
  if(rdata->status == CONNECTED && rdata->ctx && rdata->sub_ctx) {
    if((cmd_rdata = redis_cluster_rdata_from_cstr(rdata, redis_subscriber_id)) != NULL) { //works right for clusters only if redis_subscriber_id has a curlybraced {...} string in it
      redis_command(cmd_rdata, redis_ping_callback, NULL, "PUBLISH %b%s ping", STR(&cmd_rdata->namespace), redis_subscriber_id);
    }
    else {
      //TODO: what to do?...
    }
    if(rdata->ping_interval > 0) {
      ngx_add_timer(ev, rdata->ping_interval * 1000);
    }
  }
}

static ngx_int_t redis_data_tree_connector(rbtree_seed_t *seed, rdstore_data_t *rdata, ngx_int_t *total_rc) {
  ngx_int_t  rc = redis_ensure_connected(rdata);
  if(rc != NGX_OK) {
    *total_rc = rc;
  }
  return NGX_OK;
}

static ngx_int_t nchan_store_init_worker(ngx_cycle_t *cycle) {
  ngx_int_t rc = NGX_OK;
  u_char *cur;
  ngx_memzero(&redis_subscriber_id, sizeof(redis_subscriber_id));
  cur = ngx_snprintf(redis_subscriber_id, 512, "nchan_worker:{%i:time:%i}", ngx_pid, ngx_time());
  redis_subscriber_id_len = cur - redis_subscriber_id;
  
  //DBG("worker id %s len %i", redis_subscriber_id, redis_subscriber_id_len);
  
  redis_nginx_init();
  
  rbtree_walk(&redis_data_tree, (rbtree_walk_callback_pt )redis_data_tree_connector, &rc);
  return rc;
}

void redisCheckErrorCallback(redisAsyncContext *c, void *r, void *privdata) {
  redisReplyOk(c, r);
}
int redisReplyOk(redisAsyncContext *c, void *r) {
  static const ngx_str_t script_error_start= ngx_string("ERR Error running script (call to f_");
  redisReply *reply = (redisReply *)r;
  if(reply == NULL) { //redis disconnected?...
    if(c->err) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "connection to redis failed while waiting for reply - %s", c->errstr);
    }
    else {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "got a NULL redis reply for unknown reason");
    }
    return 0;
  }
  else if(reply->type == REDIS_REPLY_ERROR) {
    if(ngx_strncmp(reply->str, script_error_start.data, script_error_start.len) == 0 && (unsigned ) reply->len > script_error_start.len + REDIS_LUA_HASH_LENGTH) {
      char *hash = &reply->str[script_error_start.len];
      redis_lua_script_t  *script;
      REDIS_LUA_SCRIPTS_EACH(script) {
        if (ngx_strncmp(script->hash, hash, REDIS_LUA_HASH_LENGTH)==0) {
          ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS SCRIPT ERROR: %s :%s", script->name, &reply->str[script_error_start.len + REDIS_LUA_HASH_LENGTH + 2]);
          return 0;
        }
      }
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS SCRIPT ERROR: (unknown): %s", reply->str);
    }
    else {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS REPLY ERROR: %s", reply->str);
    }
    return 0;
  }
  else {
    return 1;
  }
}

static void redisEchoCallback(redisAsyncContext *ac, void *r, void *privdata) {
  redisReply      *reply = r;
  rdstore_data_t  *rdata;
  unsigned    i;
  //nchan_channel_t * channel = (nchan_channel_t *)privdata;
  if(ac) {
    rdata = ac->data;
    if(ac->err) {
      if(rdata->status != DISCONNECTED) {
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "connection to redis failed - %s", ac->errstr);
        rdt_set_status(rdata, DISCONNECTED, ac);
      }
      return;
    }
  }
  else {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "connection to redis was terminated");
    return;
  }
  if(reply == NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS REPLY is NULL");
    return;
  }  
  
  switch(reply->type) {
    case REDIS_REPLY_STATUS:
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS_REPLY_STATUS  %s", reply->str);
      break;
      
    case REDIS_REPLY_ERROR:
      redisCheckErrorCallback(ac, r, privdata);
      break;
      
    case REDIS_REPLY_INTEGER:
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS_REPLY_INTEGER: %i", reply->integer);
      break;
      
    case REDIS_REPLY_NIL:
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS_REPLY_NIL: nil");
      break;
      
    case REDIS_REPLY_STRING:
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS_REPLY_STRING: %s", reply->str);
      break;
      
    case REDIS_REPLY_ARRAY:
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS_REPLY_ARRAY: %i", reply->elements);
      for(i=0; i< reply->elements; i++) {
        redisEchoCallback(ac, reply->element[i], "  ");
      }
      break;
  }
  //redis_subscriber_command(NULL, NULL, "UNSUBSCRIBE {channel:%b}:pubsub", str(&(channel->id)));
}

static void redis_load_script_callback(redisAsyncContext *ac, void *r, void *privdata) {
  redis_lua_script_t  *script = privdata;

  redisReply *reply = r;
  if (reply == NULL) return;
  switch(reply->type) {
    case REDIS_REPLY_ERROR:
      nchan_log_error("Failed loading redis lua script %s :%s", script->name, reply->str);
      break;
    case REDIS_REPLY_STRING:
      if(ngx_strncmp(reply->str, script->hash, REDIS_LUA_HASH_LENGTH)!=0) {
        nchan_log_error("Redis lua script %s has unexpected hash %s (expected %s)", script->name, reply->str, script->hash);
      }
      else {
        //everything went well
        rdstore_data_t  *rdata = ac->data;
        if(rdata->status == LOADING_SCRIPTS) {
          rdata->scripts_loaded_count++;
          if(rdata->scripts_loaded_count == redis_lua_scripts_count) {
            rdata_set_status_flag(rdata, loaded_scripts, 1);
          }
        }
      }
      break;
  }
}

static void redisInitScripts(rdstore_data_t *rdata){
  redis_lua_script_t  *script;
  if(rdata->ctx == NULL) {
    ERR("unable to init lua scripts: redis connection not initialized.");
  }
  else {
    rdt_set_status(rdata, LOADING_SCRIPTS, NULL);
    rdata->scripts_loaded_count = 0;
    REDIS_LUA_SCRIPTS_EACH(script) {
      redisAsyncCommand(rdata->ctx, &redis_load_script_callback, script, "SCRIPT LOAD %s", script->script);
    }
  }
}

static void redis_subscriber_callback(redisAsyncContext *c, void *r, void *privdata);

static void redis_nginx_connect_event_handler(const redisAsyncContext *ac, int status) {
  //ERR("redis_nginx_connect_event_handler %v: %i", rdt.connect_url, status);
  rdstore_data_t    *rdata = ac->data;
  if(status != REDIS_OK) {
    if(rdata->status != DISCONNECTED) {
      char *action = rdata->generation == 0 ? "connect" : "reconnect";
      if(ac->errstr) {
        nchan_log_error("Can't %s to redis at %V: %s.", action, rdata->connect_url, ac->errstr);
      }
      else {
        nchan_log_error("Can't %s to redis at %V.", action, rdata->connect_url);
      }
    }
    rdt_set_status(rdata, DISCONNECTED, ac);
  }
}

static void redis_nginx_disconnect_event_handler(const redisAsyncContext *ac, int status) {
  rdstore_data_t    *rdata = ac->data;
  
  DBG("connection to redis for %V closed: %s", rdata->connect_url, ac->errstr);
  
  if(rdata->status == CONNECTED && !ngx_exiting && !ngx_quit && !rdata->shutting_down) {
    if(ac->err) {
      nchan_log_error("Lost connection to redis at %V: %s.", rdata->connect_url, ac->errstr);
    }
    else {
      nchan_log_error("Lost connection to redis at %V.", rdata->connect_url);
    }
  }
  rdt_set_status(rdata, DISCONNECTED, ac);
  
}

static void redis_get_server_info(redisAsyncContext *ac);

static void redis_check_if_still_loading_handler(ngx_event_t *ev) {
  DBG("still loading?,,.");
  rdstore_data_t   *rdata = ev->data;
  if(rdata->status != DISCONNECTED && rdata->ctx) {
    redis_get_server_info(rdata->ctx);
  }
  
  ngx_free(ev);
}

void redis_get_server_info_callback(redisAsyncContext *ac, void *rep, void *privdata);


static void redis_get_server_info(redisAsyncContext *ac) {
  redisAsyncCommand(ac, redis_get_server_info_callback, NULL, "INFO");
}




void redis_get_server_info_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply             *reply = rep;
  rdstore_data_t         *rdata = ac->data;
  
  //DBG("redis_get_server_info_callback %p", ac);
  if(ac->err || !redisReplyOk(ac, rep) || reply->type != REDIS_REPLY_STRING) {
    return;
  }
  
  //is it loading?
  if(ngx_strstrn((u_char *)reply->str, "loading:1", 8)) {
    nchan_log_warning("Redis server at %V is still loading data.", rdata->connect_url);
    ngx_event_t      *evt = ngx_calloc(sizeof(*evt), ngx_cycle->log);
    nchan_init_timer(evt, redis_check_if_still_loading_handler, rdata);
    rdt_set_status(rdata, LOADING, ac);
    ngx_add_timer(evt, 1000);
  }
  else {
    rdata_set_status_flag(rdata, not_loading_data, 1);
    if (ac == rdata->ctx && ngx_strstrn((u_char *)reply->str, "cluster_enabled:1", 16)) {
      //it's part of a cluster
      DBG("is part of a cluster. learn more.");
      redis_get_cluster_info(rdata);
    }
    else {
      rdata_set_status_flag(rdata, cluster_checked, 1);
    }
    
    redisInitScripts(rdata);
    if(rdata->sub_ctx) {
      //ERR("rdata->sub_ctx OK, subscribing for %V", rdata->connect_url);
      if(redis_cluster_rdata_from_cstr(rdata, redis_subscriber_id) == rdata) { //works only if redis_subsriber_id has a curlybraced {...} string in it
        redisAsyncCommand(rdata->sub_ctx, redis_subscriber_callback, NULL, "SUBSCRIBE %b%s", STR(&rdata->namespace), redis_subscriber_id);
      }
    }
    else {
      ERR("rdata->sub_ctx NULL, can't subscribe for %V", rdata->connect_url);
    }
  }
}

void redis_nginx_select_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply             *reply = rep;
  rdstore_data_t         *rdata = ac->data;
  if ((reply == NULL) || (reply->type == REDIS_REPLY_ERROR)) {
    if(rdata->status == CONNECTING) {
      ERR("could not select redis database");
    }
    rdt_set_status(rdata, DISCONNECTED, ac);
    redisAsyncFree(ac);
  }
  else if(rdata->ctx && rdata->sub_ctx && rdata->status == CONNECTING && !rdata->ctx->err && !rdata->sub_ctx->err) {
    rdt_set_status(rdata, AUTHENTICATING, NULL);
    
    if(rdata->ctx == ac) {
      redis_get_server_info(ac);
    }
  }
}

void redis_nginx_auth_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  redisReply             *reply = rep;
  rdstore_data_t         *rdata = ac->data;
  if ((reply == NULL) || (reply->type == REDIS_REPLY_ERROR)) {
    if(rdata->status == CONNECTING) {
      ERR("AUTH command failed, probably because the password is incorrect");
      rdt_set_status(rdata, DISCONNECTED, ac);
    }
  }
  else {
    rdata_set_status_flag(rdata, authenticated, 1); 
  }
}

static ngx_int_t rdata_set_peername(rdstore_data_t *rdata, redisAsyncContext *ctx) {
  char                  *ipstr = (char *)rdata->connect_params.peername.data;
  struct sockaddr_in    *s4;
  struct sockaddr_in6   *s6;
  // deal with both IPv4 and IPv6:
  switch(ctx->c.sockaddr.sa_family) {
    case AF_INET:
      s4 = (struct sockaddr_in *)&ctx->c.sockaddr;
      inet_ntop(AF_INET, &s4->sin_addr, ipstr, INET6_ADDRSTRLEN);
      break;
    case AF_INET6:
      s6 = (struct sockaddr_in6 *)&ctx->c.sockaddr;
      inet_ntop(AF_INET6, &s6->sin6_addr, ipstr, INET6_ADDRSTRLEN);
      break;
    case AF_UNSPEC:
      DBG("sockaddr info not available");
      return NGX_ERROR;
    default:
      DBG("unexpected sockaddr af family");
      return NGX_ERROR;
  }
  
  rdata->connect_params.peername.len = strlen(ipstr);
  
  DBG("got peername %V", &rdata->connect_params.peername);
  return NGX_OK;
}

static int redis_initialize_ctx(redisAsyncContext **ctx, rdstore_data_t *rdata) {
  int have_peername = rdata->connect_params.peername.len > 0;
  
  if(*ctx == NULL) {
    DBG("connect to %V port %i", have_peername ? &rdata->connect_params.peername : &rdata->connect_params.hostname, rdata->connect_params.port);
    redis_nginx_open_context(have_peername ? &rdata->connect_params.peername : &rdata->connect_params.hostname, rdata->connect_params.port, rdata->connect_params.db, &rdata->connect_params.password, rdata, ctx);
    if(*ctx != NULL) {
      rdata_set_status_flag(rdata, connection_established, 1);
      if(!have_peername) {
        rdata_set_peername(rdata, *ctx);
      }
      
      if(rdata->connect_params.password.len > 0) {
        redisAsyncCommand(*ctx, redis_nginx_auth_callback, NULL, "AUTH %b", STR(&rdata->connect_params.password));
      }
      else {
        rdata_set_status_flag(rdata, authenticated, 1);
      }
      
      if(rdata->connect_params.db > 0) {
        redisAsyncCommand(*ctx, redis_nginx_select_callback, NULL, "SELECT %d", rdata->connect_params.db);
      }
      else {
        redis_get_server_info(*ctx);
      }
      redisAsyncSetConnectCallback(*ctx, redis_nginx_connect_event_handler);
      redisAsyncSetDisconnectCallback(*ctx, redis_nginx_disconnect_event_handler);
      return 1;
    }
    else {
      ERR("can't initialize redis ctx %p", ctx);
      return 0;
    }
  }
  else {
    return 0;
  }
}

ngx_int_t redis_ensure_connected(rdstore_data_t *rdata) {
  int connecting = 0;
  
  if(redis_initialize_ctx(&rdata->ctx, rdata)) {
    connecting = 1;
  }
  
  if(redis_initialize_ctx(&rdata->sub_ctx, rdata)) {
    connecting = 1;
  }
    
  if(rdata->ctx && rdata->sub_ctx) {
    if(connecting) {
      rdt_set_status(rdata, CONNECTING, NULL);
    }
    return NGX_OK;
  }
  else {
    return NGX_DECLINED;
  }

}

static ngx_int_t msg_from_redis_get_message_reply(nchan_msg_t *msg, nchan_compressed_msg_t *cmsg, ngx_str_t *content_type, ngx_str_t *eventsource_event, redisReply *r, uint16_t offset);

#define SLOW_REDIS_REPLY 100 //ms

static ngx_int_t log_redis_reply(char *name, ngx_msec_t t) {
  ngx_msec_t   dt = ngx_current_msec - t;
  if(dt >= SLOW_REDIS_REPLY) {
    DBG("redis command %s took %i msec", name, dt);
  }
  return NGX_OK;
}

static ngx_int_t redisReply_to_int(redisReply *el, ngx_int_t *integer) {
  if(CHECK_REPLY_INT(el)) {
    *integer=el->integer;
  }
  else if(CHECK_REPLY_STR(el)) {
    *integer=ngx_atoi((u_char *)el->str, el->len);
  }
  else {
    return NGX_ERROR;
  }
  return NGX_OK;
}

typedef struct {
  ngx_msec_t                    t;
  char                         *name;
  ngx_str_t                     channel_id;
  nchan_msg_id_t               *msg_id;
  ngx_str_t                     msg_key;
  rdstore_data_t               *rdata;
} redis_get_message_from_key_data_t;

static void get_msg_from_msgkey_callback(redisAsyncContext *c, void *r, void *privdata);

static void get_msg_from_msgkey_send(rdstore_data_t *rdata, void *pd) {
  redis_get_message_from_key_data_t *d = pd;
  if(rdata) {
    redis_script(get_message_from_key, rdata, &get_msg_from_msgkey_callback, d, "1 %b", STR(&d->msg_key));
  }
  else {
    ngx_free(d);
  }
}

static void get_msg_from_msgkey_callback(redisAsyncContext *c, void *r, void *privdata) {
  redis_get_message_from_key_data_t *d = (redis_get_message_from_key_data_t *)privdata;
  redisReply           *reply = r;
  nchan_msg_t           msg;
  nchan_compressed_msg_t cmsg;
  ngx_str_t             content_type;
  ngx_str_t             eventsource_event;
  ngx_str_t            *chid = &d->channel_id;
  rdstore_data_t       *rdata = c->data;
  
  rdata->pending_commands--;
  nchan_update_stub_status(redis_pending_commands, -1);
  
  DBG("get_msg_from_msgkey_callback");
  
  log_redis_reply(d->name, d->t);
  
  if(!clusterKeySlotOk(c, r)) {
    cluster_add_retry_command_with_channel_id(rdata->node.cluster, chid, get_msg_from_msgkey_send, d);
    return;
  }
  
  if(reply) {
    if(chid == NULL) {
      ERR("get_msg_from_msgkey channel id is NULL");
      return;
    }
    if(msg_from_redis_get_message_reply(&msg, &cmsg, &content_type, &eventsource_event, reply, 0) != NGX_OK) {
      ERR("invalid message or message absent after get_msg_from_key");
      return;
    }
    nchan_store_publish_generic(chid, d->rdata, &msg, 0, NULL);
  }
  else {
    //reply is NULL
  }
  ngx_free(d);
}

static bool cmp_err(cmp_ctx_t *cmp) {
  ERR("msgpack parsing error: %s", cmp_strerror(cmp));
  return false;
}

static bool cmp_to_str(cmp_ctx_t *cmp, ngx_str_t *str) {
  ngx_buf_t      *mpbuf =(ngx_buf_t *)cmp->buf;
  uint32_t        sz;
  
  if(cmp_read_str_size(cmp, &sz)) {
    fwd_buf_to_str(mpbuf, sz, str);
    return true;
  }
  else {
    cmp_err(cmp);
    return false;
  }
}

static bool cmp_to_msg(cmp_ctx_t *cmp, nchan_msg_t *msg, nchan_compressed_msg_t *cmsg, ngx_str_t *content_type, ngx_str_t *eventsource_event) {
  ngx_buf_t  *mpb = (ngx_buf_t *)cmp->buf;
  uint32_t    sz;
  uint64_t    msgtag;
  int32_t     ttl;
  int32_t     compression;
  //ttl 
  if(!cmp_read_int(cmp, &ttl)) {
    return cmp_err(cmp);
  }
  assert(ttl >= 0);
  if(ttl == 0) {
    ttl++; // less than a second left for this message... give it a second's lease on life
  }
  msg->expires = ngx_time() + ttl;
  
  //msg id
  if(!cmp_read_uinteger(cmp, (uint64_t *)&msg->id.time)) {
    return cmp_err(cmp);
  }
  if(!cmp_read_uinteger(cmp, &msgtag)) {
    return cmp_err(cmp);
  }
  else {
    msg->id.tag.fixed[0] = msgtag;
    msg->id.tagactive = 0;
    msg->id.tagcount = 1;
  }
  
  //msg prev_id
  if(!cmp_read_uinteger(cmp, (uint64_t *)&msg->prev_id.time)) {
    return cmp_err(cmp);
  }
  if(!cmp_read_uinteger(cmp, &msgtag)) {
    return cmp_err(cmp);
  }
  else {
    msg->prev_id.tag.fixed[0] = msgtag;
    msg->prev_id.tagactive = 0;
    msg->prev_id.tagcount = 1;
  }
  
  //message data
  if(!cmp_read_str_size(cmp, &sz)) {
    return cmp_err(cmp);
  }
  set_buf(&msg->buf, mpb->pos, sz);
  fwd_buf(mpb, sz);
  msg->buf.memory = 1;
  msg->buf.last_buf = 1;
  msg->buf.last_in_chain = 1;

  //content-type
  if(!cmp_read_str_size(cmp, &sz)) {
    return cmp_err(cmp);
  }
  fwd_buf_to_str(mpb, sz, content_type);
  msg->content_type = sz > 0 ? content_type : NULL;
  
  //eventsource_event
  if(!cmp_read_str_size(cmp, &sz)) {
    return cmp_err(cmp);
  }
  fwd_buf_to_str(mpb, sz, eventsource_event);
  msg->eventsource_event = sz > 0 ? eventsource_event : NULL;
  
  //compression
  if(!cmp_read_int(cmp, &compression)) {
    msg->compressed = NULL;
  }
  if(compression > 0) {
    msg->compressed = cmsg;
    ngx_memzero(&cmsg->buf, sizeof(cmsg->buf));
    cmsg->compression = compression;
  }
  else {
    msg->compressed = NULL;
  }
  
  return true;
}

static ngx_int_t get_msg_from_msgkey(ngx_str_t *channel_id, rdstore_data_t *rdata, nchan_msg_id_t *msgid, ngx_str_t *msg_redis_hash_key) {
  rdstore_channel_head_t              *head;
  redis_get_message_from_key_data_t   *d;
  DBG("Get message from msgkey %V", msg_redis_hash_key);
  
  head = nchan_store_get_chanhead(channel_id, rdata);
  if(head->sub_count == 0) {
    DBG("Nobody wants this message we'll need to grab with an HMGET");
    return NGX_OK;
  }
  
  if((d=ngx_alloc(sizeof(*d) + (u_char)channel_id->len + (u_char)msg_redis_hash_key->len, ngx_cycle->log)) == 0) {
    ERR("unable to allocate memory for callback data for message hmget");
    return NGX_ERROR;
  }
  d->channel_id.data = (u_char *)&d[1];
  nchan_strcpy(&d->channel_id, channel_id, 0);
  
  d->msg_key.data = d->channel_id.data + d->channel_id.len;
  nchan_strcpy(&d->msg_key, msg_redis_hash_key, 0);
  
  d->t = ngx_current_msec;
  d->rdata = rdata;
  
  d->name = "get_message_from_key";
  
  //d->hcln = put_current_subscribers_in_limbo(head);
  //assert(d->hcln != 0);
  if((rdata = redis_cluster_rdata_from_key(rdata, msg_redis_hash_key)) == NULL) {
    return NGX_ERROR;
  }
  get_msg_from_msgkey_send(rdata, d);

  return NGX_OK;
}

static ngx_int_t redis_subscriber_register(rdstore_channel_head_t *chanhead, subscriber_t *sub);

static int str_match_redis_subscriber_channel(ngx_str_t *pubsub_channel, ngx_str_t *ns) {
  ngx_str_t psch = *pubsub_channel;
  if(pubsub_channel->len != ns->len + redis_subscriber_id_len || psch.len < ns->len) {
    return 0;
  }
  if(ngx_memcmp(psch.data, ns->data, ns->len) != 0) {
    return 0;
  }
  psch.data += ns->len;
  psch.len -= ns->len;
  
  return ngx_strmatch(&psch, (char *)redis_subscriber_id);
}

static ngx_str_t *get_channel_id_from_pubsub_channel(ngx_str_t *pubsub_channel, ngx_str_t *ns, ngx_str_t *str_in) {
  if(str_match_redis_subscriber_channel(pubsub_channel, ns)) {
    return NULL;
  }
  str_in->data = pubsub_channel->data + ns->len + 9; //"<namespace>{channel:"
  str_in->len = pubsub_channel->len - ns->len - 9;
  str_in->len -= 8;//"}:pubsub"
  return str_in;
}

static rdstore_channel_head_t *find_chanhead_for_pubsub_callback(ngx_str_t *chid) {
  rdstore_channel_head_t *head;
  CHANNEL_HASH_FIND(chid, head);
  return head;
}

static void redis_subscriber_callback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply             *reply = r;
  redisReply             *el = NULL;
  nchan_msg_t             msg;
  nchan_compressed_msg_t  cmsg;
  ngx_str_t               content_type;
  ngx_str_t               eventsource_event;
  ngx_str_t               chid_str;
  ngx_str_t              *chid = NULL;
  ngx_str_t               pubsub_channel; 
  ngx_str_t               msg_redis_hash_key = ngx_null_string;
  ngx_uint_t              subscriber_id;
  
  ngx_buf_t               mpbuf;
  cmp_ctx_t               cmp;
  
  rdstore_channel_head_t     *chanhead = NULL;
  rdstore_data_t             *rdata = c->data;
  
  msg.expires = 0;
  msg.refcount = 0;
  msg.parent = NULL;
  msg.storage = NCHAN_MSG_STACK;

  if(reply == NULL) return;
  
  if(CHECK_REPLY_ARRAY_MIN_SIZE(reply, 3)
   && CHECK_REPLY_STR(reply->element[0])
   && CHECK_REPLY_STR(reply->element[1])) {
    pubsub_channel.data = (u_char *)reply->element[1]->str;
    pubsub_channel.len = reply->element[1]->len;
    chid = get_channel_id_from_pubsub_channel(&pubsub_channel, &rdata->namespace, &chid_str);
  }
  else {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "no PUBSUB message, something else");
    redisEchoCallback(c,r,privdata);
    return;
  }
  
  if(CHECK_REPLY_STRVAL(reply->element[0], "message") && CHECK_REPLY_STR(reply->element[2])) {
    
    //reply->element[1] is the pubsub channel name
    el = reply->element[2];
    
    if(CHECK_REPLY_STRVAL(el, "ping") && str_match_redis_subscriber_channel(&pubsub_channel, &rdata->namespace)) {
      DBG("got pinged");
    }
    else if(CHECK_REPLY_STR(el)) {
      uint32_t    array_sz;
      unsigned    chid_present = 0;
      ngx_str_t   extracted_channel_id;
      unsigned    msgbuf_size_changed = 0;
      int64_t     msgbuf_size = 0;
      //maybe a message?
      set_buf(&mpbuf, (u_char *)el->str, el->len);
      cmp_init(&cmp, &mpbuf, ngx_buf_reader, NULL, ngx_buf_writer);
      if(cmp_read_array(&cmp, &array_sz)) {
        
        if(array_sz != 0) {
          uint32_t      sz;
          ngx_str_t     msg_type;
          cmp_read_str_size(&cmp ,&sz);
          fwd_buf_to_str(&mpbuf, sz, &msg_type);
          
          if(ngx_str_chop_if_startswith(&msg_type, "max_msgs+")) {
            if(cmp_read_integer(&cmp, &msgbuf_size))
              msgbuf_size_changed = 1; 
            else
              cmp_err(&cmp);
          }
          
          if(ngx_str_chop_if_startswith(&msg_type, "ch+")) {
            if(cmp_read_str_size(&cmp, &sz)) {
              fwd_buf_to_str(&mpbuf, sz, &extracted_channel_id);
              chid = &extracted_channel_id;
            }
            else {
              cmp_err(&cmp);
            }
          }
          //else {
          //  chid already set from the pubsub channel name
          //}
          
          if(msgbuf_size_changed && (chanhead = nchan_store_get_chanhead(chid, rdata)) != NULL) {
            chanhead->spooler.fn->broadcast_notice(&chanhead->spooler, NCHAN_NOTICE_REDIS_CHANNEL_MESSAGE_BUFFER_SIZE_CHANGE, (void *)(intptr_t )msgbuf_size);
          }
          
          if(!chanhead) {
            chanhead = find_chanhead_for_pubsub_callback(chid);
          }
          
          if(ngx_strmatch(&msg_type, "msg")) {
            assert(array_sz >= 9 + msgbuf_size_changed + chid_present);
            if(chanhead && cmp_to_msg(&cmp, &msg, &cmsg, &content_type, &eventsource_event)) {
              //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "got msg %V", msgid_to_str(&msg));
              nchan_store_publish_generic(chid, chanhead ? chanhead->rdt : rdata, &msg, 0, NULL);
            }
            else {
              ERR("thought there'd be a channel for msg");
            }
          }
          else if(ngx_strmatch(&msg_type, "msgkey")) {
            assert(array_sz == 4 + msgbuf_size_changed + chid_present);
            if(chanhead != NULL) {
              uint64_t              msgtag;
              nchan_msg_id_t        msgid;
              
              if(!cmp_read_uinteger(&cmp, (uint64_t *)&msgid.time)) {
                cmp_err(&cmp);
                return;
              }
              
              if(!cmp_read_uinteger(&cmp, &msgtag)) {
                cmp_err(&cmp);
                return;
              }
              else {
                msgid.tag.fixed[0] = msgtag;
                msgid.tagactive = 0;
                msgid.tagcount = 1;
              }
              
              if(cmp_to_str(&cmp, &msg_redis_hash_key)) {
                get_msg_from_msgkey(chid, chanhead ? chanhead->rdt : rdata, &msgid, &msg_redis_hash_key);
              }
            }
            else {
              ERR("thought there'd be a channel id around for msgkey");
            }
          }
          else if(ngx_strmatch(&msg_type, "alert") && array_sz > 1) {
            ngx_str_t    alerttype;
            
            if(!cmp_to_str(&cmp, &alerttype)) {
              return;
            }
            
            if(ngx_strmatch(&alerttype, "delete channel") && array_sz > 2) {
              if(cmp_to_str(&cmp, &extracted_channel_id)) {
                rdstore_channel_head_t *doomed_channel;
                nchan_store_publish_generic(&extracted_channel_id, rdata, NULL, NGX_HTTP_GONE, &NCHAN_HTTP_STATUS_410);
                doomed_channel = nchan_store_get_chanhead(&extracted_channel_id, rdata);
                redis_chanhead_gc_add(doomed_channel, 0, "channel deleted");
              }
              else {
                ERR("unexpected \"delete channel\" msgpack message from redis");
              }
            }
            else if(ngx_strmatch(&alerttype, "unsub one") && array_sz > 3) {
              if(cmp_to_str(&cmp, &extracted_channel_id)) {
                cmp_to_str(&cmp, &extracted_channel_id);
                cmp_read_uinteger(&cmp, (uint64_t *)&subscriber_id);
                //TODO
              }
              ERR("unsub one not yet implemented");
              assert(0);
            }
            else if(ngx_strmatch(&alerttype, "unsub all") && array_sz > 1) {
              if(cmp_to_str(&cmp, &extracted_channel_id)) {
                nchan_store_publish_generic(&extracted_channel_id, rdata, NULL, NGX_HTTP_CONFLICT, &NCHAN_HTTP_STATUS_409);
              }
            }
            else if(ngx_strmatch(&alerttype, "unsub all except")) {
              if(cmp_to_str(&cmp, &extracted_channel_id)) {
                cmp_read_uinteger(&cmp, (uint64_t *)&subscriber_id);
                //TODO
              }
              ERR("unsub all except not yet  implemented");
              assert(0);
            }
            else {
              ERR("unexpected msgpack alert from redis");
              assert(0);
            }
          }
          else {
            ERR("unexpected msgpack message from redis");
            assert(0);
          }
        }
        else {
          ERR("unexpected msgpack object from redis");
          assert(0);
        }
      }
      else {
        ERR("invalid msgpack message from redis: %s", cmp_strerror(&cmp));
        assert(0);
      }
    }
    else { //not a string
      redisEchoCallback(c, el, NULL);
    }
  }

  else if(CHECK_REPLY_STRVAL(reply->element[0], "subscribe") && CHECK_REPLY_INT(reply->element[2])) {
    
    if(chid) {
      chanhead = find_chanhead_for_pubsub_callback(chid);
      if(chanhead != NULL) {
        if(chanhead->pubsub_status != SUBBING) {
          ERR("expected previous pubsub_status for channel %p (id: %V) to be SUBBING (%i), was %i", chanhead, &chanhead->id, SUBBING, chanhead->pubsub_status);
        }
        chanhead->pubsub_status = SUBBED;
        
        switch(chanhead->status) {
          case NOTREADY:
            chanhead->status = READY;
            chanhead->spooler.fn->handle_channel_status_change(&chanhead->spooler);
            //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "REDIS: PUB/SUB subscribed to %s, chanhead %p now READY.", reply->element[1]->str, chanhead);
            break;
          case READY:
            ERR("REDIS: PUB/SUB already subscribed to %s, chanhead %p (id %V) already READY.", reply->element[1]->str, chanhead, &chanhead->id);
            break;
          case INACTIVE:
            ERR("REDIS: PUB/SUB already unsubscribed from %s, chanhead %p (id %V) INACTIVE.", reply->element[1]->str, chanhead, &chanhead->id);
            break;
          default:
            ERR("REDIS: PUB/SUB really unexpected chanhead status %i", chanhead->status);
            assert(0);
            //not sposed to happen
        }
      }
      else {
        ERR("received SUBSCRIBE acknowledgement for unknown channel %V", chid);
      }
    }
    else {
      DBG("subscribed to worker channel %s", redis_subscriber_id);
    }
    
    DBG("REDIS: PUB/SUB subscribed to %s (%i total)", reply->element[1]->str, reply->element[2]->integer);
  }
  else if(CHECK_REPLY_STRVAL(reply->element[0], "unsubscribe") && CHECK_REPLY_INT(reply->element[2])) {
    
    if(chid) {
      DBG("received UNSUBSCRIBE acknowledgement for channel %V", chid);
    }
    else {
      DBG("received UNSUBSCRIBE acknowledgement for worker channel %s", redis_subscriber_id);
    }
  }
  
  else {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "Unexpected PUBSUB message %s", reply->element[0]);
    redisEchoCallback(c,r,privdata);
  }
}

static ngx_int_t redis_subscriber_register(rdstore_channel_head_t *chanhead, subscriber_t *sub);
static ngx_int_t redis_subscriber_unregister(rdstore_channel_head_t *chanhead, subscriber_t *sub, uint8_t shutting_down);
static void spooler_add_handler(channel_spooler_t *spl, subscriber_t *sub, void *privdata) {
  rdstore_channel_head_t *head = (rdstore_channel_head_t *)privdata;
  head->sub_count++;
  if(sub->type == INTERNAL) {
    head->internal_sub_count++;
  }
  redis_subscriber_register(head, sub);
}

static void spooler_dequeue_handler(channel_spooler_t *spl, subscriber_t *sub, void *privdata) {
  //need individual subscriber
  //TODO
  rdstore_channel_head_t *head = (rdstore_channel_head_t *)privdata;
  
  head->sub_count--;
  if(sub->type == INTERNAL) {
    head->internal_sub_count--;
  }
  
  if(head->rdt->status == CONNECTED) {
    redis_subscriber_unregister(head, sub, head->shutting_down);
  }
  
  if(head->sub_count == 0 && head->fetching_message_count == 0) {
    redis_chanhead_gc_add(head, 0, "sub count == 0 and fetching_message_count == 0 after spooler dequeue");
  }
  
}

static void spooler_use_handler(channel_spooler_t *spl, void *d) {
  //nothing. 
}

void spooler_get_message_start_handler(channel_spooler_t *spl, void *pd) {
  ((rdstore_channel_head_t *)pd)->fetching_message_count++;
}

void spooler_get_message_finish_handler(channel_spooler_t *spl, void *pd) {
  ((rdstore_channel_head_t *)pd)->fetching_message_count--;
  assert(((rdstore_channel_head_t *)pd)->fetching_message_count >= 0);
}

static ngx_int_t start_chanhead_spooler(rdstore_channel_head_t *head) {
  static uint8_t channel_buffer_complete = 1;
  static channel_spooler_handlers_t handlers = {
    spooler_add_handler,
    spooler_dequeue_handler,
    NULL,
    spooler_use_handler,
    spooler_get_message_start_handler,
    spooler_get_message_finish_handler
  };
  start_spooler(&head->spooler, &head->id, &head->status, &channel_buffer_complete, &nchan_store_redis, head->rdt->lcf, FETCH, &handlers, head);
  return NGX_OK;
}

static void redis_subscriber_register_cb(redisAsyncContext *c, void *vr, void *privdata);

typedef struct {
  rdstore_channel_head_t *chanhead;
  unsigned                generation;
  subscriber_t           *sub;
} redis_subscriber_register_t;

static void redis_subscriber_register_send(rdstore_data_t *rdata, void *pd) {
  redis_subscriber_register_t   *d = pd;
  if(rdata) {
    d->chanhead->reserved++;
    nchan_redis_script(subscriber_register, rdata, &redis_subscriber_register_cb, d, &d->chanhead->id,
                       "- %i %i",
                       REDIS_CHANNEL_EMPTY_BUT_SUBSCRIBED_TTL_STEP,
                       ngx_time()
                      );
  }
  else {
    d->sub->fn->release(d->sub, 0);
    ngx_free(d);
  }
}


static ngx_int_t redis_subscriber_register(rdstore_channel_head_t *chanhead, subscriber_t *sub) {
  redis_subscriber_register_t *sdata=NULL;
  rdstore_data_t              *rdata;
  if((sdata = ngx_alloc(sizeof(*sdata), ngx_cycle->log)) == NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "No memory for sdata. Part IV, subparagraph 12 of the Cryptic Error Series.");
    return NGX_ERROR;
  }
  sdata->chanhead = chanhead;
  sdata->generation = chanhead->generation;
  sdata->sub = sub;
  
  sub->fn->reserve(sub);
  
  //input: keys: [], values: [channel_id, subscriber_id, active_ttl]
  //  'subscriber_id' can be '-' for new id, or an existing id
  //  'active_ttl' is channel ttl with non-zero subscribers. -1 to persist, >0 ttl in sec
  //output: subscriber_id, num_current_subscribers, next_keepalive_time
  if((rdata = redis_cluster_rdata_from_channel(chanhead)) == NULL) {
    return NGX_ERROR;
  }
  redis_subscriber_register_send(rdata, sdata);
  
  return NGX_OK;
}

static void redis_subscriber_register_cb(redisAsyncContext *c, void *vr, void *privdata) {
  redis_subscriber_register_t *sdata= (redis_subscriber_register_t *) privdata;
  redisReply                  *reply = (redisReply *)vr;
  rdstore_data_t              *rdata = c->data;
  int                          keepalive_ttl;
  
  rdata->pending_commands--;
  nchan_update_stub_status(redis_pending_commands, -1);
  
  sdata->chanhead->reserved--;
  sdata->sub->fn->release(sdata->sub, 0);
  
  if(!clusterKeySlotOk(c, reply)) {
    cluster_add_retry_command_with_chanhead(sdata->chanhead, redis_subscriber_register_send, sdata);
    return; 
  }
  
  if (!redisReplyOk(c, reply)) {
    ngx_free(sdata);
    return;
  }
  
  if ( !CHECK_REPLY_ARRAY_MIN_SIZE(reply, 3) || !CHECK_REPLY_INT(reply->element[1]) || !CHECK_REPLY_INT(reply->element[2])) {
    //no good
    redisEchoCallback(c,reply,privdata);
    return;
  }
  if(sdata->generation == sdata->chanhead->generation) {
    //is the subscriber     
    //TODO: set subscriber id
    //sdata->sub->id = reply->element[1]->integer;
  }
  keepalive_ttl = reply->element[2]->integer;
  if(keepalive_ttl > 0) {
    if(!sdata->chanhead->keepalive_timer.timer_set) {
      ngx_add_timer(&sdata->chanhead->keepalive_timer, keepalive_ttl * 1000);
    }
  }
  ngx_free(sdata);
}


typedef struct {
  ngx_str_t    *channel_id;
  time_t        channel_timeout;
} subscriber_unregister_data_t;

static void redis_subscriber_unregister_cb(redisAsyncContext *c, void *r, void *privdata);
static void redis_subscriber_unregister_send(rdstore_data_t *rdata, void *pd) {
  //input: keys: [], values: [namespace, channel_id, subscriber_id, empty_ttl]
  // 'subscriber_id' is an existing id
  // 'empty_ttl' is channel ttl when without subscribers. 0 to delete immediately, -1 to persist, >0 ttl in sec
  //output: subscriber_id, num_current_subscribers
  if(rdata) {
    nchan_redis_script( subscriber_unregister,rdata, &redis_subscriber_unregister_cb, NULL, 
      ((subscriber_unregister_data_t *)pd)->channel_id, "%i %i", 
                       0/*TODO: sub->id*/,
                       ((subscriber_unregister_data_t *)pd)->channel_timeout
                      );
  }
}

static void redis_subscriber_unregister_cb(redisAsyncContext *c, void *r, void *privdata) {
  redisReply      *reply = r;
  rdstore_data_t  *rdata = c->data;
  
  rdata->pending_commands--;
  nchan_update_stub_status(redis_pending_commands, -1);
  
  if(reply && reply->type == REDIS_REPLY_ERROR) {
    ngx_str_t    errstr;
    ngx_str_t    countstr;
    ngx_str_t    channel_id;
    ngx_int_t    channel_timeout;
    
    errstr.data = (u_char *)reply->str;
    errstr.len = strlen(reply->str);
    
    if(ngx_str_chop_if_startswith(&errstr, "CLUSTER KEYSLOT ERROR. ")) {
      
      nchan_scan_until_chr_on_line(&errstr, &countstr, ' ');
      channel_timeout = ngx_atoi(countstr.data, countstr.len);
      channel_id = errstr;
      
      subscriber_unregister_data_t  *d = cluster_retry_palloc(rdata->node.cluster, sizeof(*d) + sizeof(ngx_str_t) + channel_id.len);
      if(!d) {
        ERR("can't allocate add_fakesub_data for CLUSTER KEYSLOT ERROR retry");
        return;
      }
      d->channel_timeout = channel_timeout;
      d->channel_id = (ngx_str_t *)&d[1];
      d->channel_id->data = (u_char *)&d->channel_id[1];
      nchan_strcpy(d->channel_id, &channel_id, 0);
      cluster_add_retry_command_with_channel_id(rdata->node.cluster, &channel_id, redis_subscriber_unregister_send, d);
      
      return;
    }
    
  }
  redisCheckErrorCallback(c, r, privdata);
}

static ngx_int_t redis_subscriber_unregister(rdstore_channel_head_t *chanhead, subscriber_t *sub, uint8_t shutting_down) {
  nchan_loc_conf_t  *cf = sub->cf;
  rdstore_data_t    *rdata;
  
  if((rdata = redis_cluster_rdata_from_channel(chanhead)) == NULL) {
    return NGX_ERROR;
  }
  if(!shutting_down) {
    subscriber_unregister_data_t d;
    d.channel_id = &chanhead->id;
    d.channel_timeout = cf->channel_timeout;
    redis_subscriber_unregister_send(rdata, &d);
  }
  else {
    nchan_redis_sync_script(subscriber_unregister, rdata, &chanhead->id, "%i %i", 
                            0/*TODO: sub->id*/,
                            cf->channel_timeout
                           );
  }
  return NGX_OK;
}


static void redisChannelKeepaliveCallback(redisAsyncContext *c, void *vr, void *privdata);

static void redisChannelKeepaliveCallback_send(rdstore_data_t *rdata, void *pd) {
  rdstore_channel_head_t   *head = pd;
  time_t                    ttl;
  if(rdata) {
    head->reserved++;
    ttl = REDIS_CHANNEL_EMPTY_BUT_SUBSCRIBED_TTL_STEP * (1+head->keepalive_times_sent);
    if(ttl > REDIS_CHANNEL_EMPTY_BUT_SUBSCRIBED_TTL_MAX) { //1 week at most
      ttl = REDIS_CHANNEL_EMPTY_BUT_SUBSCRIBED_TTL_MAX;
    }
    nchan_redis_script(channel_keepalive, rdata, &redisChannelKeepaliveCallback, head, &head->id, "%i", ttl);
  }
}

static void redisChannelKeepaliveCallback(redisAsyncContext *c, void *vr, void *privdata) {
  rdstore_channel_head_t   *head = (rdstore_channel_head_t *)privdata;
  redisReply               *reply = (redisReply *)vr;
  rdstore_data_t           *rdata = c->data;
  
  head->reserved--;
  rdata->pending_commands--;
  nchan_update_stub_status(redis_pending_commands, -1);
  head->keepalive_times_sent++;
  
  if(!clusterKeySlotOk(c, vr)) {
    cluster_add_retry_command_with_chanhead(head, redisChannelKeepaliveCallback_send, head);
    return;
  }
  
  if(redisReplyOk(c, vr)) {
    assert(CHECK_REPLY_INT(reply));
    
    //reply->integer == -1 means "let it disappear" (see channel_keepalive.lua)
    
    if(reply->integer != -1 && !head->keepalive_timer.timer_set) {
      ngx_add_timer(&head->keepalive_timer, reply->integer * 1000);
    }
  }
  
}

static void redis_channel_keepalive_timer_handler(ngx_event_t *ev) {
  rdstore_channel_head_t   *head = ev->data;
  rdstore_data_t           *rdata;
  if(ev->timedout) {
    ev->timedout=0;
    if((rdata = redis_cluster_rdata_from_channel(head)) != NULL) {
      redisChannelKeepaliveCallback_send(rdata, head);
    }
  }
}

void redis_associate_chanhead_with_rdata(rdstore_channel_head_t *head, rdstore_data_t *rdata) {
  head->rd_prev = NULL;
  head->rd_next = rdata->channels_head;
  if(rdata->channels_head) {
    rdata->channels_head->rd_prev = head;
  }
  rdata->channels_head = head;
}

ngx_int_t ensure_chanhead_pubsub_subscribed_if_needed(rdstore_channel_head_t *ch) {
  rdstore_data_t     *rdata;
  if(ch->pubsub_status != SUBBED && ch->pubsub_status != SUBBING && ch->rdt->storage_mode == REDIS_MODE_DISTRIBUTED && (rdata = redis_cluster_rdata_from_channel(ch)) != NULL) {
    DBG("SUBSCRIBING to %V{channel:%V}:pubsub", &rdata->namespace, &ch->id);
    ch->pubsub_status = SUBBING;
    redis_subscriber_command(rdata, redis_subscriber_callback, NULL, "SUBSCRIBE %b{channel:%b}:pubsub", STR(&rdata->namespace), STR(&ch->id));
  }
  return NGX_OK;
}

ngx_int_t redis_chanhead_catch_up_after_reconnect(rdstore_channel_head_t *ch) {
  return spooler_catch_up(&ch->spooler);
}

static rdstore_channel_head_t *create_chanhead(ngx_str_t *channel_id, rdstore_data_t *rdata) {
  rdstore_channel_head_t   *head;
  
  head=ngx_calloc(sizeof(*head) + sizeof(u_char)*(channel_id->len), ngx_cycle->log);
  if(head==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "can't allocate memory for (new) channel subscriber head");
    return NULL;
  }
  head->id.len = channel_id->len;
  head->id.data = (u_char *)&head[1];
  ngx_memcpy(head->id.data, channel_id->data, channel_id->len);
  head->sub_count=0;
  head->fetching_message_count=0;
  head->redis_subscriber_privdata = NULL;
  head->status = NOTREADY;
  head->pubsub_status = UNSUBBED;
  head->generation = 0;
  head->last_msgid.time=0;
  head->last_msgid.tag.fixed[0]=0;
  head->last_msgid.tagcount = 1;
  head->last_msgid.tagactive = 0;
  head->shutting_down = 0;
  head->reserved = 0;
  head->keepalive_times_sent = 0;
  
  head->in_gc_reaper = NULL;
  
  if(head->id.len >= 5 && ngx_strncmp(head->id.data, "meta/", 5) == 0) {
    head->meta = 1;
  }
  else {
    head->meta = 0;
  }

  ngx_memzero(&head->keepalive_timer, sizeof(head->keepalive_timer));
  nchan_init_timer(&head->keepalive_timer, redis_channel_keepalive_timer_handler, head);
  
  if(channel_id->len > 2) { // absolutely no multiplexed channels allowed
    assert(ngx_strncmp(head->id.data, "m/", 2) != 0);
  }
  
  head->rd_next = NULL;
  head->rd_prev = NULL;
  head->rdt = rdata;
  if(rdata->node.cluster) {
    head->cluster.enabled = 1;
    redis_cluster_associate_chanhead_with_rdata(head);
  }
  else {
    redis_associate_chanhead_with_rdata(head, rdata);
  }
  
  if(head->rdt->storage_mode == REDIS_MODE_BACKUP) {
    head->status = READY;
  }
  
  head->spooler.running=0;
  start_chanhead_spooler(head);
  if(head->meta) {
    head->spooler.publish_events = 0;
  }
  
  ensure_chanhead_pubsub_subscribed_if_needed(head);
  CHANNEL_HASH_ADD(head);
  
  return head;
}

static rdstore_channel_head_t * nchan_store_get_chanhead(ngx_str_t *channel_id, rdstore_data_t *rdata) {
  rdstore_channel_head_t    *head;
  
  CHANNEL_HASH_FIND(channel_id, head);
  if(head==NULL) {
    head = create_chanhead(channel_id, rdata);
  }
  if(head == NULL) {
    ERR("can't create chanhead for redis store");
    return NULL;
  }
  
  if (head->status == INACTIVE) { //recycled chanhead
    ensure_chanhead_pubsub_subscribed_if_needed(head);
    redis_chanhead_gc_withdraw(head);
    
    if(head->rdt->storage_mode == REDIS_MODE_BACKUP) {
      head->status = READY;
    }
    else {
      head->status = head->pubsub_status == SUBBED ? READY : NOTREADY;
    }
  }

  if(!head->spooler.running) {
    DBG("Spooler for channel %p %V wasn't running. start it.", head, &head->id);
    start_chanhead_spooler(head);
  }
  
  return head;
}

nchan_reaper_t *rdstore_get_chanhead_reaper(rdstore_channel_head_t *ch) {
  if(ch->cluster.enabled) {
    rdstore_data_t *rdata;
    if((rdata = redis_cluster_rdata_from_channel(ch)) != NULL && rdata->status == CONNECTED) {
      return &rdata->chanhead_reaper;
    }
    else {
      return &ch->rdt->node.cluster->chanhead_reaper;
    }
  }
  else {
    return &ch->rdt->chanhead_reaper;
  }
}

ngx_int_t redis_chanhead_gc_add_to_reaper(nchan_reaper_t *reaper, rdstore_channel_head_t *head, ngx_int_t expire, const char *reason) {
  assert(head->sub_count == 0);
    
  if(head->in_gc_reaper && head->in_gc_reaper != reaper) {
    redis_chanhead_gc_withdraw(head);
  }
    
  if(!head->in_gc_reaper) {
    assert(head->status != INACTIVE);
    head->status = INACTIVE;
    head->gc_time = ngx_time() + (expire == 0 ? NCHAN_CHANHEAD_EXPIRE_SEC : expire);
    head->in_gc_reaper = reaper;
    
    nchan_reaper_add(reaper, head);
    
    DBG("gc_add chanhead %V to %s (%s)", &head->id, reaper->name, reason);
  }
  else {
    assert(head->in_gc_reaper == reaper);
    ERR("gc_add chanhead %V to %s: already added (%s)", &head->id, reaper->name, reason);
  }

  return NGX_OK;
}


ngx_int_t redis_chanhead_gc_add(rdstore_channel_head_t *head, ngx_int_t expire, const char *reason) {
  return redis_chanhead_gc_add_to_reaper(rdstore_get_chanhead_reaper(head), head, expire, reason);
}

ngx_int_t redis_chanhead_gc_withdraw(rdstore_channel_head_t *chanhead) {
  if(chanhead->in_gc_reaper) {
    DBG("gc_withdraw chanhead %s from %V", chanhead->in_gc_reaper->name, &chanhead->id);
    assert(chanhead->status == INACTIVE);
    
    nchan_reaper_withdraw(chanhead->in_gc_reaper, chanhead);
    chanhead->in_gc_reaper = NULL;
  }
  else {
    DBG("gc_withdraw chanhead (%V), but not in gc reaper", &chanhead->id);
  }
  return NGX_OK;
}

static ngx_int_t nchan_store_publish_generic(ngx_str_t *channel_id, rdstore_data_t *rdata, nchan_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line){
  rdstore_channel_head_t   *head;
  ngx_int_t                 ret;
  //redis_channel_head_cleanup_t *hcln;
  
  head = nchan_store_get_chanhead(channel_id, rdata);
  
  if(head->sub_count > 0) {
    if(msg) {
      assert(msg->id.tagcount == 1);
      head->last_msgid.time = msg->id.time;
      head->last_msgid.tag.fixed[0] = msg->id.tag.fixed[0];
      head->last_msgid.tagcount = 1;
      head->last_msgid.tagactive = 0;
      
      head->spooler.fn->respond_message(&head->spooler, msg);
    }
    else {
      head->spooler.fn->broadcast_status(&head->spooler, status_code, status_line);
    }
    ret= NGX_OK;
  }
  else {
    ret= NCHAN_MESSAGE_QUEUED;
  }
  return ret;
}

static ngx_int_t redis_array_to_channel(redisReply *r, nchan_channel_t *ch) {
  ngx_str_t       msgid;
  nchan_msg_id_t  zeroid = NCHAN_OLDEST_MSGID;
  
  if ( CHECK_REPLY_ARRAY_MIN_SIZE(r, 5)
    && CHECK_REPLY_INT(r->element[0])
    && CHECK_REPLY_INT(r->element[1])
    && CHECK_REPLY_INT(r->element[2])
    && CHECK_REPLY_STR(r->element[3])
    && CHECK_REPLY_INT(r->element[4])) {
    
    //channel info
    ch->expires = ngx_time() + r->element[0]->integer;
    ch->last_seen = r->element[1]->integer;
    ch->subscribers = r->element[2]->integer;
  
    msgid.data = (u_char *)r->element[3]->str;
    msgid.len = r->element[3]->len;
      
    if(msgid.len == 0) {
      ch->last_published_msg_id = zeroid;
    }
    else if(nchan_parse_compound_msgid(&ch->last_published_msg_id, &msgid, 1) != NGX_OK) {
      ERR("failed to parse last-msgid %V from redis", &msgid);
    }
  
    ch->messages = r->element[4]->integer;
    
    //no id?..
    ch->id.len=0;
    ch->id.data=NULL;
    
    //queued messages
    if( CHECK_REPLY_ARRAY_MIN_SIZE(r, 6)
      && CHECK_REPLY_INT(r->element[5])) {
      
      ch->messages = r->element[5]->integer;
    }
    
    return NGX_OK;
  }
  else if(CHECK_REPLY_NIL(r)) {
    return NGX_DECLINED;
  }
  else {
    return NGX_ERROR;
  }
}

typedef struct {
  ngx_msec_t           t;
  char                *name;
  ngx_str_t           *channel_id;
  callback_pt          callback;
  void                *privdata;
} redis_channel_callback_data_t;

#define CREATE_CALLBACK_DATA(d, rdata, namestr, channel_id, callback, privdata) \
  do {                                                                       \
    if ((d = ngx_alloc(sizeof(*d) + ((rdata->node.cluster != NULL) ? (sizeof(*channel_id) + channel_id->len) : 0), ngx_cycle->log)) == NULL) { \
      ERR("Can't allocate redis %s channel callback data", namestr);         \
      return NGX_ERROR;                                                      \
    }                                                                        \
    d->t = ngx_current_msec;                                                 \
    d->name = namestr;                                                       \
    if(rdata->node.cluster != NULL) {                                        \
      /* might need to use channel id later to retry the command */          \
      d->channel_id = (ngx_str_t *)&d[1];                                    \
      d->channel_id->data = (u_char *)&d->channel_id[1];                     \
      nchan_strcpy(d->channel_id, channel_id, 0);                            \
    }                                                                        \
    else {                                                                   \
      d->channel_id = channel_id;                                            \
    }                                                                        \
    d->callback = callback;                                                  \
    d->privdata = privdata;                                                  \
  } while(0)

static void redisChannelInfoCallback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply *reply=r;
  redis_channel_callback_data_t *d=(redis_channel_callback_data_t *)privdata;
  nchan_channel_t channel;
  ngx_memzero(&channel, sizeof(channel)); // for ddebugging. this should be removed later.
  
  log_redis_reply(d->name, d->t);
  
  if(d->callback) {
    if(reply) {
      switch(redis_array_to_channel(reply, &channel)) {
        case NGX_OK:
          d->callback(NGX_OK, &channel, d->privdata);
          break;
        case NGX_DECLINED: //not found
          d->callback(NGX_OK, NULL, d->privdata);
          break;
        case NGX_ERROR:
        default:
          redisEchoCallback(c, r, privdata);
          d->callback(NGX_ERROR, NULL, d->privdata);
      }
    }
    else {
      d->callback(NGX_ERROR, NULL, d->privdata);
    }
  }
}

static void redisChannelDeleteCallback(redisAsyncContext *c, void *r, void *privdata);

static void nchan_store_delete_channel_send(rdstore_data_t *rdata, void *pd) {
  redis_channel_callback_data_t *d = pd;
  if(rdata) {
    nchan_redis_script(delete, rdata, &redisChannelDeleteCallback, d, d->channel_id, "");
  }
  else {
    redisChannelDeleteCallback(NULL, NULL, d);
  }
}

static void redisChannelDeleteCallback(redisAsyncContext *ac, void *r, void *privdata) {
  rdstore_data_t  *rdata;
  
  nchan_update_stub_status(redis_pending_commands, -1);
  if(ac) {
    rdata = ac->data;
    rdata->pending_commands--;
    
    if(!clusterKeySlotOk(ac, r)) {
      redis_channel_callback_data_t  *d = privdata;
      
      cluster_add_retry_command_with_channel_id(rdata->node.cluster, d->channel_id, nchan_store_delete_channel_send, privdata);
      return;
    }
  }
  redisChannelInfoCallback(ac, r, privdata);
  ngx_free(privdata);
}

static ngx_int_t nchan_store_delete_channel(ngx_str_t *channel_id, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  redis_channel_callback_data_t *d;
  rdstore_data_t                *rdata = cf->redis.privdata;
  
  CREATE_CALLBACK_DATA(d, rdata, "delete", channel_id, callback, privdata);
  
  if((rdata = redis_cluster_rdata_from_channel_id(rdata, channel_id)) == NULL) {
    return NGX_ERROR;
  }
  
  nchan_store_delete_channel_send(rdata, d);
  
  return NGX_OK;
}


static void redisChannelFindCallback(redisAsyncContext *c, void *r, void *privdata);

static void nchan_store_find_channel_send(rdstore_data_t *rdata, void *pd) {
  redis_channel_callback_data_t *d = pd;
  if(rdata) {
    nchan_redis_script(find_channel, rdata, &redisChannelFindCallback, d, d->channel_id, "");
  }
  else {
    redisChannelFindCallback(NULL, NULL, d);
  }
}

static void redisChannelFindCallback(redisAsyncContext *ac, void *r, void *privdata) {
  rdstore_data_t                 *rdata = NULL;
  
  if(ac) {
    rdata = ac->data;
    rdata->pending_commands--;
    nchan_update_stub_status(redis_pending_commands, -1);
  }
  
  if(ac && !clusterKeySlotOk(ac, r)) {
    redis_channel_callback_data_t  *d = privdata;
    cluster_add_retry_command_with_channel_id(rdata->node.cluster, d->channel_id, nchan_store_find_channel_send, privdata);
    return;
  }
  
  redisChannelInfoCallback(ac, r, privdata);
  ngx_free(privdata);
}

static ngx_int_t nchan_store_find_channel(ngx_str_t *channel_id, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  redis_channel_callback_data_t *d;
  rdstore_data_t                *rdata = cf->redis.privdata;
  CREATE_CALLBACK_DATA(d, rdata, "find_channel", channel_id, callback, privdata);
  
  if((rdata = redis_cluster_rdata_from_channel_id(rdata, channel_id)) == NULL) {
    return NGX_ERROR;
  }
  nchan_store_find_channel_send(rdata, d);
  
  return NGX_OK;
}

static ngx_int_t msg_from_redis_get_message_reply(nchan_msg_t *msg, nchan_compressed_msg_t *cmsg, ngx_str_t *content_type, ngx_str_t *eventsource_event, redisReply *r, uint16_t offset) {
  
  redisReply         **els = r->element;
  size_t               content_type_len = 0, es_event_len = 0;
  ngx_int_t            time_int, ttl;
  ngx_int_t            compression;
  
  if(CHECK_REPLY_ARRAY_MIN_SIZE(r, offset + 8)
   && CHECK_REPLY_INT(els[offset])            //msg TTL
   && CHECK_REPLY_INT_OR_STR(els[offset+1])   //id - time
   && CHECK_REPLY_INT_OR_STR(els[offset+2])   //id - tag
   && CHECK_REPLY_INT_OR_STR(els[offset+3])   //prev_id - time
   && CHECK_REPLY_INT_OR_STR(els[offset+4])   //prev_id - tag
   && CHECK_REPLY_STR(els[offset+5])   //message
   && CHECK_REPLY_STR(els[offset+6])   //content-type
   && CHECK_REPLY_STR(els[offset+7])  //eventsource event
  ) {
  
    content_type_len=els[offset+6]->len;
    es_event_len = els[offset+7]->len;
    
    ngx_memzero(msg, sizeof(*msg));
    
    msg->buf.start = msg->buf.pos = (u_char *)els[offset+5]->str;
    msg->buf.end = msg->buf.last = msg->buf.start + els[offset+5]->len;
    msg->buf.memory = 1;
    msg->buf.last_buf = 1;
    msg->buf.last_in_chain = 1;
    
    if(redisReply_to_int(els[offset], &ttl) != NGX_OK) {
      ERR("invalid ttl integer value in msg response from redis");
      return NGX_ERROR;
    }
    assert(ttl >= 0);
    if(ttl == 0)
      ttl++; // less than a second left for this message... give it a second's lease on life
    
    msg->expires = ngx_time() + ttl;
    
    msg->compressed = NULL;
    if(r->elements >= (uint16_t )(offset + 8)) {
      if(!CHECK_REPLY_INT_OR_STR(els[offset+8]) || redisReply_to_int(els[offset+8], &compression) != NGX_OK) {
        ERR("invalid compression type integer value in msg response from redis");
        return NGX_ERROR;
      }
      if((nchan_msg_compression_type_t )compression != NCHAN_MSG_COMPRESSION_INVALID && (nchan_msg_compression_type_t )compression != NCHAN_MSG_NO_COMPRESSION) {
        msg->compressed = cmsg;
        ngx_memzero(&cmsg->buf, sizeof(cmsg->buf));
        cmsg->compression = (nchan_msg_compression_type_t )compression;
      }
    }
    
    if(content_type_len > 0) {
      msg->content_type = content_type;
      msg->content_type->len=content_type_len;
      msg->content_type->data=(u_char *)els[offset+6]->str;
    }
    else {
      msg->content_type = NULL;
    }
    
    if(es_event_len > 0) {
      msg->eventsource_event = eventsource_event;
      msg->eventsource_event->len=es_event_len;
      msg->eventsource_event->data=(u_char *)els[offset+7]->str;
    }
    else {
      msg->eventsource_event = NULL;
    }
    
    if(redisReply_to_int(els[offset+1], &time_int) == NGX_OK) {
      msg->id.time = time_int;
    }
    else {
      msg->id.time = 0;
      ERR("invalid msg time from redis");
    }
    
    redisReply_to_int(els[offset+2], (ngx_int_t *)&msg->id.tag.fixed[0]); // tag is a uint, meh.
    msg->id.tagcount = 1;
    msg->id.tagactive = 0;
    
    redisReply_to_int(els[offset+3], &time_int);
    msg->prev_id.time = time_int;
    redisReply_to_int(els[offset+4], (ngx_int_t *)&msg->prev_id.tag.fixed[0]);
    msg->prev_id.tagcount = 1;
    msg->prev_id.tagactive = 0;
    
    return NGX_OK;
  }
  else {
    ERR("invalid message redis reply");
    return NGX_ERROR;
  }
}

typedef struct {
  ngx_msec_t              t;
  char                   *name;
  ngx_str_t              *channel_id;
  nchan_msg_tiny_id_t     msg_id;
  callback_pt             callback;
  void                   *privdata;
} redis_get_message_data_t;

static void redis_get_message_callback(redisAsyncContext *c, void *r, void *privdata);

static void nchan_store_async_get_message_send(rdstore_data_t *rdata, void *pd) {
  redis_get_message_data_t           *d = pd;
  //input:  keys: [], values: [namespace, channel_id, msg_time, msg_tag, no_msgid_order, create_channel_ttl]
  //output: result_code, msg_ttl, msg_time, msg_tag, prev_msg_time, prev_msg_tag, message, content_type, eventsource_event, channel_subscriber_count
  if(rdata) {
    nchan_redis_script(get_message, rdata, &redis_get_message_callback, d, d->channel_id, "%i %i FILO 0", 
                       d->msg_id.time, 
                       d->msg_id.tag
                      );
  }
  else {
    //TODO: pass on a get_msg error status maybe?
    ngx_free(d);
  }
}

static void redis_get_message_callback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply                *reply= r;
  redis_get_message_data_t  *d= (redis_get_message_data_t *)privdata;
  nchan_msg_t                msg;
  nchan_compressed_msg_t     cmsg;
  ngx_str_t                  content_type;
  ngx_str_t                  eventsource_event;
  rdstore_data_t            *rdata = c->data;
  
  rdata->pending_commands--;
  nchan_update_stub_status(redis_pending_commands, -1);
  
  if(!clusterKeySlotOk(c, r)) {
    cluster_add_retry_command_with_channel_id(rdata->node.cluster, d->channel_id, nchan_store_async_get_message_send, d);
    return;
  }
  
  if(d == NULL) {
    ERR("redis_get_mesage_callback has NULL userdata");
    return;
  }
  
  log_redis_reply(d->name, d->t);
  
  //output: result_code, msg_time, msg_tag, message, content_type,  channel-subscriber-count
  // result_code can be: 200 - ok, 403 - channel not found, 404 - not found, 410 - gone, 418 - not yet available
  
  if (!redisReplyOk(c, r) || !CHECK_REPLY_ARRAY_MIN_SIZE(reply, 1) || !CHECK_REPLY_INT(reply->element[0]) ) {
    //no good
    ngx_free(d);
    return;
  }
  
  switch(reply->element[0]->integer) {
    case 200: //ok
      if(msg_from_redis_get_message_reply(&msg, &cmsg, &content_type, &eventsource_event, reply, 1) == NGX_OK) {
        d->callback(MSG_FOUND, &msg, d->privdata);
      }
      break;
    case 403: //channel not found
    case 404: //not found
      d->callback(MSG_NOTFOUND, NULL, d->privdata);
      break;
    case 410: //gone
      d->callback(MSG_EXPIRED, NULL, d->privdata);
      break;
    case 418: //not yet available
      d->callback(MSG_EXPECTED, NULL, d->privdata);
      break;
  }
  
  ngx_free(d);
}

static ngx_int_t nchan_store_async_get_message(ngx_str_t *channel_id, nchan_msg_id_t *msg_id, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  redis_get_message_data_t           *d;
  rdstore_data_t                     *rdata = cf->redis.privdata;
  if(callback==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "no callback given for async get_message. someone's using the API wrong!");
    return NGX_ERROR;
  }
  
  assert(msg_id->tagcount == 1);
  
  CREATE_CALLBACK_DATA(d, rdata, "get_message", channel_id, callback, privdata);
  d->msg_id.time = msg_id->time;
  d->msg_id.tag = msg_id->tag.fixed[0];
  
  if((rdata = redis_cluster_rdata_from_channel_id(rdata, channel_id)) == NULL) {
    return NGX_ERROR;
  }
  nchan_store_async_get_message_send(rdata, d);
  return NGX_OK; //async only now!
}


typedef struct nchan_redis_conf_ll_s nchan_redis_conf_ll_t;
struct nchan_redis_conf_ll_s {
  nchan_redis_conf_t     *cf;
  nchan_loc_conf_t       *loc_conf;
  nchan_redis_conf_ll_t  *next;
};

nchan_redis_conf_ll_t   *redis_conf_head;

ngx_int_t nchan_store_redis_add_server_conf(ngx_conf_t *cf, nchan_redis_conf_t *rcf, nchan_loc_conf_t *loc_conf) {
  nchan_redis_conf_ll_t  *rcf_ll = ngx_palloc(cf->pool, sizeof(*rcf_ll));
  rcf_ll->cf = rcf;
  rcf_ll->loc_conf = loc_conf;
  rcf_ll->next = redis_conf_head;
  redis_conf_head = rcf_ll;
  return NGX_OK;
}

ngx_int_t nchan_store_redis_remove_server_conf(ngx_conf_t *cf, nchan_redis_conf_t *rcf) {
  nchan_redis_conf_ll_t  *cur, *prev;
  
  for(cur = redis_conf_head, prev = NULL; cur != NULL; prev = cur, cur = cur->next) {
    if(cur->cf == rcf) { //found it
      if(prev == NULL) {
        redis_conf_head = cur->next;
      }
      else {
        prev->next = cur->next;
      }
      //don't need to ngx_pfree
      return NGX_OK;
    }
  }
  return NGX_OK;
}

//initialization
static ngx_int_t nchan_store_init_module(ngx_cycle_t *cycle) {
  ngx_core_conf_t                *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  nchan_worker_processes = ccf->worker_processes;
  return NGX_OK;
}


//rbtree for all redis connections
static void *redis_data_rbtree_node_id(void *data) {
  return &((rdstore_data_t *)data)->connect_params;
}
static uint32_t redis_data_rbtree_bucketer(void *vid) {
  redis_connect_params_t   *id = (redis_connect_params_t *)vid;
  return (uint32_t )id->port;
}
static ngx_int_t redis_data_rbtree_compare(void *v1, void *v2) {
  redis_connect_params_t   *id1 = (redis_connect_params_t *)v1;
  redis_connect_params_t   *id2 = (redis_connect_params_t *)v2;
  
  if(id1->port > id2->port)
    return 1;
  else if (id1->port < id2->port)
    return -1;
  
  if(id1->db > id2->db)
    return 1;
  else if (id1->db < id2->db)
    return -1;
  
  if(id1->hostname.len > id2->hostname.len)
    return 1;
  else if(id1->hostname.len < id2->hostname.len)
    return -1;
  
  return ngx_strncmp(id1->hostname.data, id2->hostname.data, id1->hostname.len);
}

ngx_int_t rdstore_initialize_chanhead_reaper(nchan_reaper_t *reaper, char *name) {
  
  nchan_reaper_start(reaper, 
              name, 
              offsetof(rdstore_channel_head_t, gc_prev), 
              offsetof(rdstore_channel_head_t, gc_next), 
  (ngx_int_t (*)(void *, uint8_t)) nchan_redis_chanhead_ready_to_reap,
  (void (*)(void *)) redis_store_reap_chanhead,
              4
  );
    
  return NGX_OK;
}

static void redis_stall_timer_handler(ngx_event_t *ev) {
  /*rdstore_data_t  *rdata = ev->data;
  
  
  if(!ev->timedout || ngx_exiting || ngx_quit || rdata->shutting_down)
    return;
  //ERR("redis_stall_timer_handler PRE  ctr: %d ctr_while_checking: %d, chk: %i", rdata->stall_counter, rdata->stall_counter_while_checking, rdata->stall_count_check);
  ev->timedout = 0;
  if(rdata->status != CONNECTED) {
   return;
  }
  if( 0 ) { //stalled
    if(rdata->stall_counter > 0) {
      //yep, it stalled
      if(rdata->ctx) {
        redisAsyncFree(rdata->ctx);
        rdata->ctx = NULL;
      }
      if(rdata->sub_ctx) {
        redisAsyncFree(rdata->sub_ctx);
        rdata->sub_ctx = NULL;
      }
      if(rdata->sync_ctx) {
        redisFree(rdata->sync_ctx);
        rdata->sync_ctx = NULL;
      }
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "Detected stalled connection to Redis server %V.", rdata->connect_url);
      rdt_set_status(rdata, DISCONNECTED, NULL);
      //ERR("redis_stall_timer_handler DSCN ctr: %d ctr_while_checking: %d, chk: %i", rdata->stall_counter, rdata->stall_counter_while_checking, rdata->stall_count_check);
      return;
    }
    else {
      //ERR("all good");
    }
  }
  else if(rdata->stall_counter != 0) {
    //ERR("will check");
    rdata->stall_count_check = 1;
  }
  //ERR("redis_stall_timer_handler POST ctr: %d ctr_while_checking: %d, chk: %i", rdata->stall_counter, rdata->stall_counter_while_checking, rdata->stall_count_check);
  if(REDIS_STALL_CHECK_TIME > 0) {
    ngx_add_timer(ev, REDIS_STALL_CHECK_TIME);
  }
  */
}

rdstore_data_t *redis_create_rdata(ngx_str_t *url, redis_connect_params_t *rcp, nchan_redis_conf_t *rcf, nchan_loc_conf_t *lcf) {
  ngx_rbtree_node_t     *node;
  rdstore_data_t        *rdata;
  size_t                 reaper_name_len;
  char                  *reaper_name;
  
  struct rdata_blob_s{
    rdstore_data_t   rdata;
    u_char           peername[INET6_ADDRSTRLEN + 2];
  } *blob;
  
  reaper_name_len = strlen("redis chanhead ()  ") + url->len;
  
  if((node = rbtree_create_node(&redis_data_tree, sizeof(*blob) + reaper_name_len)) == NULL) {
    ERR("can't create rbtree node for redis connection");
    return NULL;
  }
  
  blob = (struct rdata_blob_s *)rbtree_data_from_node(node);
  rdata = &blob->rdata;
  ngx_memzero(rdata, sizeof(*rdata));
  rdata->connect_params = *rcp;
  
  rdata->connect_params.peername.len = 0;
  rdata->connect_params.peername.data = blob->peername;
  
  rdata->status = DISCONNECTED;
  rdata->time_connected = 0;
  rdata->generation = 0;
  rdata->shutting_down = 0;
  rdata->lcf = lcf;
  nchan_init_timer(&rdata->reconnect_timer, redis_reconnect_timer_handler, rdata);
  nchan_init_timer(&rdata->ping_timer, redis_ping_timer_handler, rdata);

  rdata->pending_commands = 0;  
  nchan_init_timer(&rdata->stall_timer, redis_stall_timer_handler, rdata);
  
  rdata->channels_head = NULL;
  rdata->almost_deleted_channels_head = NULL;
  
  reaper_name = (char *)&blob[1];
  ngx_sprintf((u_char *)reaper_name, "redis chanhead (%V)%Z", url);
  rdstore_initialize_chanhead_reaper(&rdata->chanhead_reaper, reaper_name);
  
  rdata->ping_interval = rcf->ping_interval;
  rdata->connect_url = url;
  rdata->namespace = rcf->namespace;
  rdata->storage_mode = rcf->storage_mode;
  assert(rdata->storage_mode != REDIS_MODE_CONF_UNSET);
  
  if(rbtree_insert_node(&redis_data_tree, node) != NGX_OK) {
    ERR("couldn't insert redis date node");
    rbtree_destroy_node(&redis_data_tree, node);
    return NULL;
  }
  
  return rdata;
}

rdstore_data_t *find_rdata_by_url(ngx_str_t *url) {
  redis_connect_params_t rcp;
  rcp.peername.len = 0;
  rcp.peername.data = NULL;
  parse_redis_url(url, &rcp);
  return find_rdata_by_connect_params(&rcp);
}

rdstore_data_t *find_rdata_by_connect_params(redis_connect_params_t *rcp) {
  rdstore_data_t        *rdata;
  ngx_rbtree_node_t     *node;
  if((node = rbtree_find_node(&redis_data_tree, rcp)) == NULL) {
    return NULL;
  }
  rdata = (rdstore_data_t *)rbtree_data_from_node(node);
  return rdata;
}

ngx_int_t redis_add_connection_data(nchan_redis_conf_t *rcf, nchan_loc_conf_t *lcf, ngx_str_t *override_url) {
  rdstore_data_t          *rdata;
  redis_connect_params_t   rcp;
  static ngx_str_t         default_redis_url = ngx_string(NCHAN_REDIS_DEFAULT_URL);
  ngx_str_t               *url;
  
  if(rcf->url.len == 0) {
    rcf->url = default_redis_url;
  }
  url = override_url ? override_url : &rcf->url;
  
  if(url->len == 0) {
    url = &default_redis_url;
  }
  
  parse_redis_url(url, &rcp);
  rcp.peername.len = 0;
  rcp.peername.data = NULL;
  
  if((rdata = find_rdata_by_connect_params(&rcp)) == NULL) {
    rdata = redis_create_rdata(url, &rcp, rcf, lcf);
  }
  else {
    if(rcf->ping_interval > 0 && rcf->ping_interval < rdata->ping_interval) {
      //shorter ping interval wins
      rdata->ping_interval = rcf->ping_interval;
    }
  }
  
  rcf->privdata = rdata;
  
  return NGX_OK;
}

static ngx_int_t nchan_store_init_postconfig(ngx_conf_t *cf) {
  nchan_redis_conf_t    *rcf;
  nchan_redis_conf_ll_t *cur;
  nchan_main_conf_t     *mcf = ngx_http_conf_get_module_main_conf(cf, ngx_nchan_module);
  
  if(mcf->redis_publish_message_msgkey_size == NGX_CONF_UNSET_SIZE) {
    mcf->redis_publish_message_msgkey_size = NCHAN_REDIS_DEFAULT_PUBSUB_MESSAGE_MSGKEY_SIZE;
  }
  redis_publish_message_msgkey_size = mcf->redis_publish_message_msgkey_size;
  
  rbtree_init(&redis_data_tree, "redis connection data", redis_data_rbtree_node_id, redis_data_rbtree_bucketer, redis_data_rbtree_compare);
  
  redis_cluster_init_postconfig(cf);
  
  for(cur = redis_conf_head; cur != NULL; cur = cur->next) {
    rcf = cur->cf;
    if(!rcf->enabled) {
      ERR("there's a non-enabled redis_conf_t here");
      continue;
    }
    
    if(rcf->upstream) {
      ngx_uint_t                   i;
      ngx_array_t                 *servers = rcf->upstream->servers;
      ngx_http_upstream_server_t  *usrv = servers->elts;
      ngx_str_t                   *upstream_url;
      
      for(i=0; i < servers->nelts; i++) {
#if nginx_version >= 1007002
        upstream_url = &usrv[i].name;
#else
        upstream_url = &usrv[i].addrs->name;
#endif
        redis_add_connection_data(rcf, cur->loc_conf, upstream_url);
      }
    }
    else {
      redis_add_connection_data(rcf, cur->loc_conf, NULL);
    }
  }
  
  return NGX_OK;
}

static void nchan_store_create_main_conf(ngx_conf_t *cf, nchan_main_conf_t *mcf) {
  mcf->redis_publish_message_msgkey_size=NGX_CONF_UNSET_SIZE;
  
  //reset redis_conf_head for reloads
  redis_conf_head = NULL;
}

void redis_store_prepare_to_exit_worker() {
  rdstore_channel_head_t    *cur, *tmp;
  HASH_ITER(hh, chanhead_hash, cur, tmp) {
    cur->shutting_down = 1;
  }
}

static ngx_int_t redis_data_tree_exiter_stage1(rbtree_seed_t *seed, rdstore_data_t *rdata, void *pd) {
  rdata->shutting_down = 1;
  
  callback_chain_t     *ccur, *cnext;  
  for(ccur = rdata->on_connected.head; ccur != NULL; ccur = cnext) {
    cnext = ccur->next;
    ccur->cb(NGX_ABORT, rdata, ccur->pd);
    if(ccur->timeout_ev.timer_set) {
      ngx_del_timer(&ccur->timeout_ev);
    }
    ngx_free(ccur);
  }
  rdata->on_connected.head = NULL;
  rdata->on_connected.tail = NULL;

  return NGX_OK;
}

static ngx_int_t redis_data_tree_exiter_stage2(rbtree_seed_t *seed, rdstore_data_t *rdata, unsigned *chanheads) {
  
  *chanheads += rdata->chanhead_reaper.count;
  
  nchan_reaper_stop(&rdata->chanhead_reaper);
  
  if(rdata->ctx)
    redis_nginx_force_close_context(&rdata->ctx);
  if(rdata->sub_ctx)
    redis_nginx_force_close_context(&rdata->sub_ctx);
  if(rdata->sync_ctx) {
    redisFree(rdata->sync_ctx);
    rdata->sync_ctx = NULL;
  }

  return NGX_OK;
}

static ngx_int_t redis_data_tree_exiter_stage3(rbtree_seed_t *seed, rdstore_data_t *rdata, unsigned *chanheads) {
  
  DBG("exiting3 rdata %p %V", rdata, rdata->connect_url);
  
  if(rdata->ctx)
    redis_nginx_force_close_context(&rdata->ctx);
  if(rdata->sub_ctx)
    redis_nginx_force_close_context(&rdata->sub_ctx);
  if(rdata->sync_ctx) {
    redisFree(rdata->sync_ctx);
    rdata->sync_ctx = NULL;
  }
  if(rdata->ping_timer.timer_set) {
    ngx_del_timer(&rdata->ping_timer);
  }
  if(rdata->stall_timer.timer_set) {
    ngx_del_timer(&rdata->stall_timer);
  }
  if(rdata->reconnect_timer.timer_set) {
    ngx_del_timer(&rdata->reconnect_timer);
  }

  return NGX_OK;
}

static void nchan_store_exit_worker(ngx_cycle_t *cycle) {
  rdstore_channel_head_t     *cur, *tmp;
  unsigned                    chanheads = 0;
  DBG("redis exit worker");
  rbtree_walk(&redis_data_tree, (rbtree_walk_callback_pt )redis_data_tree_exiter_stage1, NULL);
  
  HASH_ITER(hh, chanhead_hash, cur, tmp) {
    cur->shutting_down = 1;
    if(!cur->in_gc_reaper) {
      cur->spooler.fn->broadcast_status(&cur->spooler, NGX_HTTP_GONE, &NCHAN_HTTP_STATUS_410);
      redis_chanhead_gc_add(cur, 0, "exit worker");
    }
  }
  
  rbtree_walk(&redis_data_tree, (rbtree_walk_callback_pt )redis_data_tree_exiter_stage2, &chanheads);
  
  rbtree_empty(&redis_data_tree, (rbtree_walk_callback_pt )redis_data_tree_exiter_stage3, NULL);
  nchan_exit_notice_about_remaining_things("redis channel", "", chanheads);
  
  redis_cluster_exit_worker(cycle);
}

static void nchan_store_exit_master(ngx_cycle_t *cycle) {
  rbtree_empty(&redis_data_tree, NULL, NULL);
}

typedef struct {
  ngx_str_t                   *channel_id;
  subscriber_t                *sub;
  unsigned                     allocd:1;
} redis_subscribe_data_t;

static ngx_int_t nchan_store_subscribe_continued(redis_subscribe_data_t *d);

static ngx_int_t subscribe_existing_channel_callback(ngx_int_t status, void *ch, void *d) {
  nchan_channel_t              *channel = (nchan_channel_t *)ch;
  redis_subscribe_data_t       *data = (redis_subscribe_data_t *)d;
  
  if(channel == NULL) {
    data->sub->fn->respond_status(data->sub, NGX_HTTP_FORBIDDEN, NULL, NULL);
    data->sub->fn->release(data->sub, 0);
  }
  else {
    nchan_store_subscribe_continued(d);
  }
  assert(data->allocd);
  ngx_free(data);
  
  return NGX_OK;
}

static ngx_int_t nchan_store_subscribe(ngx_str_t *channel_id, subscriber_t *sub) {
  redis_subscribe_data_t        d_data;
  redis_subscribe_data_t       *d = NULL;

  assert(sub->last_msgid.tagcount == 1);
  
  if(!sub->cf->subscribe_only_existing_channel) {
    d_data.allocd = 0;
    d_data.channel_id = channel_id;
    d_data.sub = sub;
    nchan_store_subscribe_continued(&d_data);
  }
  else {
    if((d=ngx_alloc(sizeof(*d) + sizeof(ngx_str_t) + channel_id->len, ngx_cycle->log))==NULL) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "can't allocate redis get_message callback data");
      return NGX_ERROR;
    }
    d->allocd = 1;
    d->channel_id=(ngx_str_t *)&d[1];
    d->channel_id->len = channel_id->len;
    d->channel_id->data = (u_char *)&(d->channel_id)[1];
    ngx_memcpy(d->channel_id->data, channel_id->data, channel_id->len);
    d->sub = sub;
    nchan_store_find_channel(channel_id, sub->cf, subscribe_existing_channel_callback, d);
  }

  return NGX_OK;
}

static ngx_int_t nchan_store_subscribe_continued(redis_subscribe_data_t *d) {
  //nchan_loc_conf_t           *cf = d->sub->cf;
  rdstore_channel_head_t       *ch;
  //ngx_int_t                   create_channel_ttl = cf->subscribe_only_existing_channel==1 ? 0 : cf->channel_timeout;
  rdstore_data_t               *rdata = d->sub->cf->redis.privdata;
  ngx_int_t                     rc;
  
  ch = nchan_store_get_chanhead(d->channel_id, rdata);
  
  assert(ch != NULL);
  
  rc = ch->spooler.fn->add(&ch->spooler, d->sub);
  //redisAsyncCommand(rds_ctx(), &redis_getmessage_callback, (void *)d, "EVALSHA %s 0 %b %i %i %s %i", redis_lua_scripts.get_message.hash, STR(d->channel_id), d->msg_id->time, d->msg_id->tag[0], "FILO", create_channel_ttl);
  return rc;
}

typedef struct {
  ngx_msec_t            t;
  char                 *name;
  ngx_str_t            *channel_id;
  time_t                msg_time;
  nchan_msg_t          *msg;
  unsigned              shared_msg:1;
  time_t                message_timeout;
  ngx_int_t             max_messages;
  nchan_msg_compression_type_t compression;
  ngx_int_t             msglen;
  callback_pt           callback;
  void                 *privdata;
} redis_publish_callback_data_t;

static void redisPublishCallback(redisAsyncContext *, void *, void *);

static void redis_publish_message_send(rdstore_data_t *rdata, void *pd) {
  redis_publish_callback_data_t  *d = pd;
  ngx_int_t                       mmapped = 0;
  ngx_buf_t                      *buf;
  ngx_str_t                       msgstr;
  nchan_msg_t                    *msg = d->msg;
  const ngx_str_t                 empty=ngx_string("");
  
  buf = &msg->buf;
  if(ngx_buf_in_memory(buf)) {
    msgstr.data = buf->pos;
    msgstr.len = buf->last - msgstr.data;
  }
  else { //in a file
    ngx_fd_t fd = buf->file->fd == NGX_INVALID_FILE ? nchan_fdcache_get(&buf->file->name) : buf->file->fd;
    
    msgstr.len = buf->file_last - buf->file_pos;
    msgstr.data = mmap(NULL, msgstr.len, PROT_READ, MAP_SHARED, fd, 0);
    if (msgstr.data != MAP_FAILED) {
      mmapped = 1;
    }
    else {
      msgstr.data = NULL;
      msgstr.len = 0;
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, ngx_errno, "Redis store: Couldn't mmap file %V", &buf->file->name);
    }
  }
  d->msglen = msgstr.len;
  
  
  //input:  keys: [], values: [namespace, channel_id, time, message, content_type, eventsource_event, compression, msg_ttl, max_msg_buf_size, pubsub_msgpacked_size_cutoff]
  //output: message_time, message_tag, channel_hash {ttl, time_last_seen, subscribers, messages}
  nchan_redis_script(publish, rdata, &redisPublishCallback, d, d->channel_id, 
                     "%i %b %b %b %i %i %i %i", 
                     msg->id.time, 
                     STR(&msgstr), 
                     STR((msg->content_type ? msg->content_type : &empty)), 
                     STR((msg->eventsource_event ? msg->eventsource_event : &empty)), 
                     d->compression,
                     d->message_timeout, 
                     d->max_messages, 
                     redis_publish_message_msgkey_size
                    );
  if(mmapped && munmap(msgstr.data, msgstr.len) == -1) {
    ERR("munmap was a problem");
  }
}

static ngx_int_t redis_publish_message_send_when_connected(ngx_int_t status, void *rd, void *pd) {
  rdstore_data_t                 *rdata = rd, *prev_rdata = rd;
  redis_publish_callback_data_t  *d = pd;
  
  if(status != NGX_OK) {
    d->callback(NGX_HTTP_SERVICE_UNAVAILABLE, NULL, d->privdata);
    ngx_free(d);
    return NGX_OK;
  }
  
  assert(rdata->status == CONNECTED);
  if((rdata = redis_cluster_rdata_from_channel_id(rdata, d->channel_id)) == NULL) {
    ERR("redis_publish_message_send_when_connected cluster rdata is null");
    if(d->shared_msg) {
      msg_release(d->msg, "redis publish");
    }
    d->callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, d->privdata);
    ngx_free(d);
    return NGX_ERROR;
  }
  
  if(rdata != prev_rdata) {
    //it's a cluster, and we need a different node
    if(rdata->status != CONNECTED) {
      //and it's not ready yet...
      nchan_loc_conf_t           fake_cf;
      fake_cf.redis.privdata = rdata;
      return redis_store_callback_on_connected(&fake_cf, REDIS_CONNECTION_FOR_PUBLISH_WAIT, redis_publish_message_send_when_connected, d);
    }
  }
  
  redis_publish_message_send(rdata, d);
  return NGX_OK;
}

static ngx_int_t nchan_store_publish_message(ngx_str_t *channel_id, nchan_msg_t *msg, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  redis_publish_callback_data_t  *d=NULL;
  rdstore_data_t                 *rdata = cf->redis.privdata;
  
  assert(callback != NULL);

  CREATE_CALLBACK_DATA(d, rdata, "publish_message", channel_id, callback, privdata);
  
  d->msg_time=msg->id.time;
  if(d->msg_time == 0) {
    d->msg_time = ngx_time();
  }
  d->msg = msg;
  d->shared_msg = msg->storage == NCHAN_MSG_SHARED;
  d->message_timeout = nchan_loc_conf_message_timeout(cf);
  d->max_messages = nchan_loc_conf_max_messages(cf);
  d->compression = cf->message_compression;
  
  assert(msg->id.tagcount == 1);
  
  if(rdata->status != CONNECTED) {
    if(d->shared_msg) {
      msg_reserve(d->msg, "redis publish");
    }
    redis_store_callback_on_connected(cf, REDIS_CONNECTION_FOR_PUBLISH_WAIT, redis_publish_message_send_when_connected, d);
  }
  else {
    if((rdata = redis_cluster_rdata_from_channel_id(rdata, channel_id)) == NULL) {
      return NGX_ERROR;
    }
    if(d->shared_msg) {
      msg_reserve(d->msg, "redis publish");
    }
    redis_publish_message_send(rdata, d);
  }
  
  return NGX_OK;
}

static void redisPublishCallback(redisAsyncContext *c, void *r, void *privdata) {
  redis_publish_callback_data_t *d=(redis_publish_callback_data_t *)privdata;
  redisReply                    *reply=r;
  redisReply                    *cur;
  nchan_channel_t                ch;
  
  rdstore_data_t                *rdata = c->data;
  rdata->pending_commands--;
  nchan_update_stub_status(redis_pending_commands, -1);
  
  if(!clusterKeySlotOk(c, r)) {
    if(d->shared_msg) {
      cluster_add_retry_command_with_channel_id(rdata->node.cluster, d->channel_id, redis_publish_message_send, d);
    }
    else {
      //message probably isn't available anymore...
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis store received cluster MOVE/ASK error while publishing, and can't retry publishing after reconfiguring cluster.");
      d->callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, d->privdata);
      ngx_free(d);
    }
    return;
  }
  
  if(d->shared_msg) {
    msg_release(d->msg, "redis publish");
  }
  
  ngx_memzero(&ch, sizeof(ch)); //for debugging basically. should be removed in the future and zeroed as-needed
  
  if(reply && CHECK_REPLY_ARRAY_MIN_SIZE(reply, 2)) {
    cur=reply->element[0];
    switch(redis_array_to_channel(cur, &ch)) {
      case NGX_OK:
        d->callback(ch.subscribers > 0 ? NCHAN_MESSAGE_RECEIVED : NCHAN_MESSAGE_QUEUED, &ch, d->privdata);
        break;
      case NGX_DECLINED: //not found
        d->callback(NGX_OK, NULL, d->privdata);
        break;
      case NGX_ERROR:
      default:
        redisEchoCallback(c, r, privdata);
        d->callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, d->privdata);
    }
  }
  else {
    redisEchoCallback(c, r, privdata);
    d->callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, d->privdata);
  }
  ngx_free(d);
}



typedef struct {
  ngx_str_t      *channel_id;
  ngx_int_t       count;
} add_fakesub_data_t;


static void nchan_store_redis_add_fakesub_callback(redisAsyncContext *c, void *r, void *privdata);
static void nchan_store_redis_add_fakesub_send(rdstore_data_t *rdata, void *pd) {
  if(rdata) {
    nchan_redis_script(add_fakesub, rdata, &nchan_store_redis_add_fakesub_callback, NULL, 
                       ((add_fakesub_data_t *)pd)->channel_id,
                       "%i %i",
                       ((add_fakesub_data_t *)pd)->count,
                       ngx_time()
                      );
  }
}

static void nchan_store_redis_add_fakesub_callback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply      *reply = r;
  rdstore_data_t  *rdata = c->data;
  
  rdata->pending_commands--;
  nchan_update_stub_status(redis_pending_commands, -1);
  
  if(reply && reply->type == REDIS_REPLY_ERROR) {
    ngx_str_t    errstr;
    ngx_str_t    countstr;
    ngx_str_t    channel_id;
    intptr_t     count;
    
    errstr.data = (u_char *)reply->str;
    errstr.len = strlen(reply->str);
    
    if(ngx_str_chop_if_startswith(&errstr, "CLUSTER KEYSLOT ERROR. ")) {
      
      nchan_scan_until_chr_on_line(&errstr, &countstr, ' ');
      count = ngx_atoi(countstr.data, countstr.len);
      channel_id = errstr;
      
      add_fakesub_data_t  *d = cluster_retry_palloc(rdata->node.cluster, sizeof(*d) + sizeof(ngx_str_t) + channel_id.len);
      if(!d) {
        ERR("can't allocate add_fakesub_data for CLUSTER KEYSLOT ERROR retry");
        return;
      }
      d->count = count;
      d->channel_id = (ngx_str_t *)&d[1];
      d->channel_id->data = (u_char *)&d->channel_id[1];
      nchan_strcpy(d->channel_id, &channel_id, 0);
      cluster_add_retry_command_with_channel_id(rdata->node.cluster, &channel_id, nchan_store_redis_add_fakesub_send, d);
      
      return;
    }
    
  }
  redisCheckErrorCallback(c, r, privdata);
}

ngx_int_t nchan_store_redis_fakesub_add(ngx_str_t *channel_id, nchan_loc_conf_t *cf, ngx_int_t count, uint8_t shutting_down) {
  rdstore_data_t                 *rdata;
  
  if((rdata = redis_cluster_rdata_from_channel_id(cf->redis.privdata, channel_id)) == NULL) {
    return NGX_ERROR;
  }
  if(!shutting_down) {
    add_fakesub_data_t   data = {channel_id, count};
    nchan_store_redis_add_fakesub_send(rdata, &data);
  }
  else {
    redis_sync_command(rdata, "EVALSHA %s 0 %b %i", redis_lua_scripts.add_fakesub.hash, STR(channel_id), count);
  }
  return NGX_OK;
}

int nchan_store_redis_ready(nchan_loc_conf_t *cf) {
  rdstore_data_t    *rdt;
  time_t             wait = cf->redis.after_connect_wait_time;
  
  if(wait == 0) { //no need to wait
    return 1;
  }
  
  rdt = cf->redis.privdata;
  
  if(rdt == NULL
  || rdt->status != CONNECTED
  || rdt->time_connected == 0) {
    return 0;
  }
  
  return ngx_time() > rdt->time_connected + wait;
}

nchan_store_t nchan_store_redis = {
  //init
  &nchan_store_init_module,
  &nchan_store_init_worker,
  &nchan_store_init_postconfig,
  &nchan_store_create_main_conf,
  
  //shutdown
  &nchan_store_exit_worker,
  &nchan_store_exit_master,
  
  //async-friendly functions with callbacks
  &nchan_store_async_get_message, //+callback
  &nchan_store_subscribe, //+callback
  &nchan_store_publish_message, //+callback
  
  &nchan_store_delete_channel, //+callback
  &nchan_store_find_channel, //+callback
  
  NULL, //get_group
  NULL,
  NULL
};

