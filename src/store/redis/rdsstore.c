#include <nchan_module.h>
#include <store/redis/store.h>
#include <assert.h>
#include <netinet/ip.h>
#include "store-private.h"

#include "redis_nginx_adapter.h"

#include <util/nchan_msg.h>
#include <util/nchan_rbtree.h>
#include <store/store_common.h>

#include <store/memory/store.h>

#include "redis_nodeset.h"
#include "redis_lua_commands.h"

#define REDIS_CHANNEL_KEEPALIVE_NOTREADY_RETRY_TIME 5000

#define REDIS_STALL_CHECK_TIME 0 //disable for now

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "REDISTORE: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDISTORE: " fmt, ##args)

#define REDIS_CONNECTION_FOR_PUBLISH_WAIT 5000

u_char            redis_subscriber_id[512];
size_t            redis_subscriber_id_len;

static rdstore_channel_head_t    *chanhead_hash = NULL;
static size_t                     redis_publish_message_msgkey_size;

#define CHANNEL_HASH_FIND(id_buf, p)    HASH_FIND( hh, chanhead_hash, (id_buf)->data, (id_buf)->len, p)
#define CHANNEL_HASH_ADD(chanhead)      HASH_ADD_KEYPTR( hh, chanhead_hash, (chanhead->id).data, (chanhead->id).len, chanhead)
#define CHANNEL_HASH_DEL(chanhead)      HASH_DEL( chanhead_hash, chanhead)

#include <stdbool.h>
#include "cmp.h"


#define redis_subscriber_command(node, cb, pd, fmt, args...)        \
  do {                                                               \
    if((node)->state >= REDIS_NODE_READY) {                    \
      redisAsyncCommand((node)->ctx.pubsub, cb, pd, fmt, ##args);      \
    } else {                                                         \
      ERR("Can't run redis command: no connection to redis server.");\
    }                                                                \
  }while(0)                                                          \
  
  
  
#define redis_sync_command(node, fmt, args...)                       \
  do {                                                               \
    if((node)->ctx.sync == NULL) {                                   \
      (node)->ctx.sync = node_connect_sync_context(node);            \
    }                                                                \
    if((node)->ctx.sync) {                                           \
      redisCommand((node)->ctx.sync, fmt, ##args);                   \
    } else {                                                         \
      ERR("Can't run redis command: no connection to redis server.");\
    }                                                                \
  }while(0)

#define redis_sync_script(script_name, node, fmt, args...)                    \
  redis_sync_command(node, "EVALSHA %s " fmt, redis_lua_scripts.script_name.hash, ##args)

#define nchan_redis_sync_script(script_name, node, channel_id, fmt, args...)  \
  redis_sync_script(script_name, node, "0 %b %b " fmt, STR(node->nodeset->settings.namespace), STR(channel_id), ##args)

  
  
#define redis_command(node, cb, pd, fmt, args...)                 \
  do {                                                               \
    if(node->state >= REDIS_NODE_READY) {                            \
      if((cb) != NULL) {                                             \
        /* a reply is expected, so track this command */             \
        node_command_sent(node);                                     \
      }                                                              \
      redisAsyncCommand((node)->ctx.cmd, cb, pd, fmt, ##args);       \
    } else {                                                         \
      node_log_error(node, "Can't run redis command: no connection to redis server.");\
    }                                                                \
  }while(0)                                                          \
  
#define redis_script(script_name, node, cb, pd, fmt, args...)                         \
  redis_command(node, cb, pd, "EVALSHA %s " fmt, redis_lua_scripts.script_name.hash, ##args)

#define nchan_redis_script(script_name, node, cb, pd, channel_id, fmt, args...)       \
  redis_script(script_name, node, cb, pd, "0 %b %b " fmt, STR((node)->nodeset->settings.namespace), STR(channel_id), ##args)

  
#define CHECK_REPLY_STR(reply) ((reply)->type == REDIS_REPLY_STRING)
#define CHECK_REPLY_STRVAL(reply, v) ( CHECK_REPLY_STR(reply) && ngx_strcmp((reply)->str, v) == 0 )
#define CHECK_REPLY_STRNVAL(reply, v, n) ( CHECK_REPLY_STR(reply) && ngx_strncmp((reply)->str, v, n) == 0 )
#define CHECK_REPLY_STATUSVAL(reply, v) ( (reply)->type == REDIS_REPLY_STATUS && ngx_strcmp((reply)->str, v) == 0 )
#define CHECK_REPLY_INT(reply) ((reply)->type == REDIS_REPLY_INTEGER)
#define CHECK_REPLY_INTVAL(reply, v) ( CHECK_REPLY_INT(reply) && (reply)->integer == v )
#define CHECK_REPLY_ARRAY_MIN_SIZE(reply, size) ( (reply)->type == REDIS_REPLY_ARRAY && (reply)->elements >= (unsigned )size )
#define CHECK_REPLY_NIL(reply) ((reply)->type == REDIS_REPLY_NIL)
#define CHECK_REPLY_INT_OR_STR(reply) ((reply)->type == REDIS_REPLY_INTEGER || (reply)->type == REDIS_REPLY_STRING)
  
static ngx_int_t nchan_store_publish_generic(ngx_str_t *, redis_nodeset_t *, nchan_msg_t *, ngx_int_t, const ngx_str_t *);

static rdstore_channel_head_t * nchan_store_get_chanhead(ngx_str_t *channel_id, redis_nodeset_t *);

const nchan_backoff_settings_t NCHAN_REDIS_DEFAULT_IDLE_CHANNEL_TTL = {
  .min = NCHAN_REDIS_DEFAULT_IDLE_CHANNEL_TTL_MIN_MSEC,
  .backoff_multiplier = NCHAN_REDIS_DEFAULT_IDLE_CHANNEL_TTL_BACKOFF_MULTIPLIER,
  .jitter_multiplier = NCHAN_REDIS_DEFAULT_IDLE_CHANNEL_TTL_JITTER_MULTIPLIER,
  .max = NCHAN_REDIS_DEFAULT_IDLE_CHANNEL_TTL_MAX_MSEC
};

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

int nchan_store_redis_validate_url(ngx_str_t *url) {
  redis_connect_params_t rcp;
  return parse_redis_url(url, &rcp) == NGX_OK;
}

ngx_int_t parse_redis_url(ngx_str_t *url, redis_connect_params_t *rcp) {
  u_char                  *cur, *last, *ret;
  
  cur = url->data;
  last = url->data + url->len;
  
  
  //ignore redis://
  rcp->use_tls = 0;
  if(ngx_strnstr(cur, "redis://", 8) != NULL) {
    cur += 8;
  }
  else if(ngx_strnstr(cur, "rediss://", 9) != NULL) {
    cur += 9;
    rcp->use_tls = 1;
  }
  
  //username:password@
  if((ret = ngx_strlchr(cur, last, '@')) != NULL) {
    u_char *split = ngx_strlchr(cur, ret, ':');
    if(!split) {
      return NGX_ERROR;
    }
    rcp->username.len = (split - cur);
    rcp->username.data = rcp->username.len == 0 ? NULL : cur;
    
    rcp->password.len = (ret - split - 1);
    rcp->password.data = rcp->password.len == 0 ? NULL : &split[1];
    
    cur = ret + 1;
  }
  else {
    rcp->username.len = 0;
    rcp->username.data = NULL;
    rcp->password.len = 0;
    rcp->password.data = NULL;
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
      return NGX_ERROR;
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

static void redis_store_reap_chanhead(rdstore_channel_head_t *ch) {
  if(!ch->shutting_down) {
    assert(ch->sub_count == 0 && ch->fetching_message_count == 0);
  }
  
  DBG("reap channel %V", &ch->id);

  if(ch->pubsub_status == REDIS_PUBSUB_SUBSCRIBED) {
    redis_node_t *pubsub_node = ch->redis.node.pubsub;
    assert(ch->redis.nodeset->settings.storage_mode >= REDIS_MODE_DISTRIBUTED);
    assert(pubsub_node);
    redis_chanhead_set_pubsub_status(ch, NULL, REDIS_PUBSUB_UNSUBSCRIBED);
    //node_log_error(pubsub_node, "UNSUBSCRIBE start %V", &ch->redis.pubsub_id);
    node_pubsub_time_start(pubsub_node, NCHAN_REDIS_CMD_PUBSUB_UNSUBSCRIBE);
    redis_subscriber_command(pubsub_node, NULL, NULL, "%s %b", pubsub_node->nodeset->use_spublish ? "sunsubscribe" : "unsubscribe", STR(&ch->redis.pubsub_id));
  }

  DBG("chanhead %p (%V) is empty and expired. unsubscribe, then delete.", ch, &ch->id);
  
  if(ch->keepalive_timer.timer_set) {
    ngx_del_timer(&ch->keepalive_timer);
  }
  
  nodeset_dissociate_chanhead(ch);

  stop_spooler(&ch->spooler, 1);
  CHANNEL_HASH_DEL(ch);
  
  ngx_free(ch);
}

static ngx_int_t nchan_redis_chanhead_ready_to_reap(rdstore_channel_head_t *ch, uint8_t force) {
  if(force) {
    //force delete is always ok
    return NGX_OK;
  }
  
  if(ch->status != INACTIVE) {
    return NGX_DECLINED;
  }
  
  if(ch->reserved > 0 ) {
    DBG("not yet time to reap %V, %i reservations left", &ch->id, ch->reserved);
    return NGX_DECLINED;
  }
  
  if(ch->gc.time - ngx_time() > 0) {
    DBG("not yet time to reap %V, %i sec left", &ch->id, ch->gc.time - ngx_time());
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
  
  //DBG("ok to delete channel %V", &ch->id);
  return NGX_OK;
}

static void redis_subscriber_callback(redisAsyncContext *c, void *r, void *privdata);

static ngx_int_t nchan_store_init_worker(ngx_cycle_t *cycle) {
  ngx_int_t rc = NGX_OK;
  
  u_char randbytes[16];
  u_char randstr[33];
  int use_randbytes = 0;
#if (NGX_OPENSSL)
  if (RAND_bytes(randbytes, 16) == 1) {
    use_randbytes = 1;
  }
#endif
  if(use_randbytes) {
    ngx_hex_dump(randstr, randbytes, 16);
    randstr[32]='\0';
  }
  else {
    ngx_sprintf(randstr, "%xi%Z", ngx_random());
  }
  
  u_char *cur;
  cur = ngx_snprintf(redis_subscriber_id, 512, "nchan_worker:{%i:time:%i:%s}%Z", ngx_pid, ngx_time(), randstr);
  redis_subscriber_id_len = cur - redis_subscriber_id;
  
  //DBG("worker id %s len %i", redis_subscriber_id, redis_subscriber_id_len);
  
  redis_nginx_init();
  
  nodeset_initialize((char *)redis_subscriber_id, redis_subscriber_callback);
  nodeset_connect_all();
  
  //OLD
  //rbtree_walk(&redis_data_tree, (rbtree_walk_callback_pt )redis_data_tree_connector, &rc);
  return rc;
}

void redisCheckErrorCallback(redisAsyncContext *c, void *r, void *privdata) {
  redisReplyOk(c, r);
}
int redisReplyOk(redisAsyncContext *ac, void *r) {
  redis_node_t         *node = ac->data;
  redisReply *reply = (redisReply *)r;
  if(reply == NULL) { //redis disconnected?...
    if(ac->err) {
      node_log_error(node, "connection to redis failed while waiting for reply - %s", ac->errstr);
    }
    else {
      node_log_error(node, "got a NULL redis reply for unknown reason");
    }
    return 0;
  }
  else if(reply->type == REDIS_REPLY_ERROR) {
    char *str = reply->str;
    
    //is there a script hash inside?
    redis_lua_script_t  *script;
    REDIS_LUA_SCRIPTS_EACH(script) {
      if (ngx_strstr((u_char *)str, script->hash)) {
        node_log_error(node, "REDIS SCRIPT ERROR: %s.lua : %s", script->name, str);
        return 0;
      }
    }
    
    //nope, didn't match a script  
    node_log_error(node, "REDIS REPLY ERROR: %s", str);
    return 0;
  }
  else {
    return 1;
  }
}

void redisEchoCallback(redisAsyncContext *ac, void *r, void *privdata) {
  redisReply      *reply = r;
  redis_node_t    *node = NULL;
  unsigned    i;
  //nchan_channel_t * channel = (nchan_channel_t *)privdata;
  if(ac) {
    node = ac->data;
    if(ac->err) {
      node_log_error(node, "connection to redis failed - %s", ac->errstr);
      return;
    }
  }
  else {
    node_log_error(node, "connection to redis was terminated");
    return;
  }
  if(reply == NULL) {
    node_log_error(node, "REDIS REPLY is NULL");
    return;
  }  
  
  switch(reply->type) {
    case REDIS_REPLY_STATUS:
      node_log_error(node, "REDIS_REPLY_STATUS  %s", reply->str);
      break;
      
    case REDIS_REPLY_ERROR:
      redisCheckErrorCallback(ac, r, privdata);
      break;
      
    case REDIS_REPLY_INTEGER:
      node_log_error(node, "REDIS_REPLY_INTEGER: %i", reply->integer);
      break;
      
    case REDIS_REPLY_NIL:
      node_log_error(node, "REDIS_REPLY_NIL: nil");
      break;
      
    case REDIS_REPLY_STRING:
      node_log_error(node, "REDIS_REPLY_STRING: %s", reply->str);
      break;
      
    case REDIS_REPLY_ARRAY:
      node_log_error(node, "REDIS_REPLY_ARRAY: %i", reply->elements);
      for(i=0; i< reply->elements; i++) {
        redisEchoCallback(ac, reply->element[i], "  ");
      }
      break;
  }
  //redis_subscriber_command(NULL, NULL, "UNSUBSCRIBE {channel:%b}:pubsub", str(&(channel->id)));
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

static ngx_int_t redisReply_to_ngx_int(redisReply *el, ngx_int_t *integer) {
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
} redis_get_message_from_key_data_t;

static void get_msg_from_msgkey_callback(redisAsyncContext *ac, void *r, void *privdata);

static ngx_int_t get_msg_from_msgkey_send(redis_nodeset_t *ns, void *pd) {
  redis_get_message_from_key_data_t *d = pd;
  if(nodeset_ready(ns)) {
    redis_node_t  *node = nodeset_node_find_by_key(ns, &d->msg_key);
    node_command_time_start(node, NCHAN_REDIS_CMD_CHANNEL_GET_LARGE_MESSAGE);
    redis_script(get_message_from_key, node, &get_msg_from_msgkey_callback, d, "1 %b", STR(&d->msg_key));
  }
  else {
    ngx_free(d);
  }
  return NGX_OK;
}

static void get_msg_from_msgkey_callback(redisAsyncContext *ac, void *r, void *privdata) {
  redis_get_message_from_key_data_t *d = (redis_get_message_from_key_data_t *)privdata;
  redisReply           *reply = r;
  nchan_msg_t           msg;
  nchan_compressed_msg_t cmsg;
  ngx_str_t             content_type;
  ngx_str_t             eventsource_event;
  ngx_str_t            *chid = &d->channel_id;
  redis_node_t         *node = ac->data;
  
  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CHANNEL_GET_LARGE_MESSAGE);
  
  DBG("get_msg_from_msgkey_callback");
  
  log_redis_reply(d->name, d->t);
  
  if(!nodeset_node_reply_keyslot_ok(node, reply) && nodeset_node_can_retry_commands(node)) {
    nodeset_callback_on_ready(node->nodeset, get_msg_from_msgkey_send, d);
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
    nchan_store_publish_generic(chid, node->nodeset, &msg, 0, NULL);
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

static ngx_int_t get_msg_from_msgkey(ngx_str_t *channel_id, redis_nodeset_t *nodeset, nchan_msg_id_t *msgid, ngx_str_t *msg_redis_hash_key) {
  rdstore_channel_head_t              *head;
  redis_get_message_from_key_data_t   *d;
  DBG("Get message from msgkey %V", msg_redis_hash_key);
  
  head = nchan_store_get_chanhead(channel_id, nodeset);
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
  
  d->name = "get_message_from_key";
  
  //d->hcln = put_current_subscribers_in_limbo(head);
  //assert(d->hcln != 0);
  get_msg_from_msgkey_send(nodeset, d);

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

ngx_int_t redis_chanhead_set_pubsub_status(rdstore_channel_head_t *chanhead, redis_node_t *node, redis_pubsub_status_t status) {
  assert(chanhead);
  
  switch(status) {
    case REDIS_PUBSUB_UNSUBSCRIBED:
      if(chanhead->pubsub_status == REDIS_PUBSUB_UNSUBSCRIBED) {
        node_log_warning(node, "channel %V got double UNSUBSCRIBED", &chanhead->id);
      }
      if(chanhead->pubsub_status == REDIS_PUBSUB_SUBSCRIBING) {
        node_log_error(node, "channel %V is SUBSCRIBING, but status was set to UNSUBSCRIBED", &chanhead->id);
      }
      chanhead->pubsub_status = REDIS_PUBSUB_UNSUBSCRIBED;
      
      nodeset_node_dissociate_pubsub_chanhead(chanhead);
      
      if(chanhead->redis.slist.in_disconnected_pubsub_list == 0) {
        nchan_slist_append(&chanhead->redis.nodeset->channels.disconnected_pubsub, chanhead);
        chanhead->redis.slist.in_disconnected_pubsub_list = 1;
      }
      
      if(chanhead->redis.nodeset->settings.storage_mode == REDIS_MODE_BACKUP && chanhead->status == READY) {
        chanhead->status = NOTREADY;
        chanhead->spooler.fn->handle_channel_status_change(&chanhead->spooler);
      }
      
      break;
    
    case REDIS_PUBSUB_SUBSCRIBING:
      if(chanhead->pubsub_status != REDIS_PUBSUB_UNSUBSCRIBED) {
        nchan_log_error("Redis chanhead %V pubsub status set to SUBSCRIBING when prev status was not UNSUBSCRIBED (%i)", &chanhead->id, chanhead->pubsub_status);
      }
      chanhead->pubsub_status = REDIS_PUBSUB_SUBSCRIBING;
      break;
    
    case REDIS_PUBSUB_SUBSCRIBED:
      assert(node);
      if(chanhead->pubsub_status != REDIS_PUBSUB_SUBSCRIBING) {
        node_log_error(node, "expected previous pubsub_status for channel %p (id: %V) to be REDIS_PUBSUB_SUBSCRIBING (%i), was %i", chanhead, &chanhead->id, REDIS_PUBSUB_SUBSCRIBING, chanhead->pubsub_status);
      }
      chanhead->pubsub_status = REDIS_PUBSUB_SUBSCRIBED;
      nodeset_node_associate_pubsub_chanhead(node, chanhead);
      
      switch(chanhead->status) {
        case NOTREADY:
          chanhead->status = READY;
          chanhead->spooler.fn->handle_channel_status_change(&chanhead->spooler);
          break;
        
        case READY:
          break;
        
        case INACTIVE:
          // this is fine, inactive channels can be pubsubbed, they will be garbage collected
          // later if needed
          break;
          
        default:
          node_log_error(node, "REDIS: PUB/SUB really unexpected chanhead status %i", chanhead->status);
          //not sposed to happen
          raise(SIGABRT);
          break;
      }
      break;
      
    
  }
  
  return NGX_OK;
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
  redis_node_t               *node = c->data;
  redis_nodeset_t            *nodeset = node->nodeset;
  ngx_str_t                  *namespace = nodeset->settings.namespace;
  
  msg.expires = 0;
  msg.refcount = 0;
  msg.parent = NULL;
  msg.storage = NCHAN_MSG_STACK;

  if(reply == NULL) return;
  
  if(CHECK_REPLY_ARRAY_MIN_SIZE(reply, 3)
   && CHECK_REPLY_STR(reply->element[0])
   && (CHECK_REPLY_STR(reply->element[1]) || CHECK_REPLY_INT(reply->element[1]))) {
    pubsub_channel.data = (u_char *)reply->element[1]->str;
    pubsub_channel.len = reply->element[1]->len;
    chid = get_channel_id_from_pubsub_channel(&pubsub_channel, namespace, &chid_str);
  }
  else {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "no PUBSUB message, something else");
    redisEchoCallback(c,r,privdata);
    return;
  }
  
  if((
      CHECK_REPLY_STRVAL(reply->element[0], "message")
      || CHECK_REPLY_STRVAL(reply->element[0], "smessage")
     )
    && CHECK_REPLY_STR(reply->element[2])) {
    
    //reply->element[1] is the pubsub channel name
    el = reply->element[2];
    
    if(CHECK_REPLY_STRVAL(el, "ping") && str_match_redis_subscriber_channel(&pubsub_channel, namespace)) {
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
          
          if(msgbuf_size_changed && (chanhead = nchan_store_get_chanhead(chid, nodeset)) != NULL) {
            chanhead->spooler.fn->broadcast_notice(&chanhead->spooler, NCHAN_NOTICE_REDIS_CHANNEL_MESSAGE_BUFFER_SIZE_CHANGE, (void *)(intptr_t )msgbuf_size);
          }
          
          if(!chanhead) {
            chanhead = find_chanhead_for_pubsub_callback(chid);
          }
          
          if(ngx_strmatch(&msg_type, "msg")) {
            assert(array_sz >= 9 + msgbuf_size_changed + chid_present);
            if(chanhead && cmp_to_msg(&cmp, &msg, &cmsg, &content_type, &eventsource_event)) {
              //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "got msg %V", msgid_to_str(&msg));
              nchan_store_publish_generic(chid, chanhead ? chanhead->redis.nodeset : nodeset, &msg, 0, NULL);
            }
            else {
              node_log_debug(node, "thought there'd be a channel (%V) for msg", chid);
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
                get_msg_from_msgkey(chid, chanhead ? chanhead->redis.nodeset : node->nodeset, &msgid, &msg_redis_hash_key);
              }
            }
            else {
              node_log_error(node, "thought there'd be a channel id around for msgkey");
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
                nchan_store_publish_generic(&extracted_channel_id, nodeset, NULL, NGX_HTTP_GONE, &NCHAN_HTTP_STATUS_410);
                doomed_channel = nchan_store_get_chanhead(&extracted_channel_id, nodeset);
                redis_chanhead_gc_add(doomed_channel, 0, "channel deleted");
              }
              else {
                node_log_error(node, "unexpected \"delete channel\" msgpack message from redis");
              }
            }
            else if(ngx_strmatch(&alerttype, "unsub one") && array_sz > 3) {
              if(cmp_to_str(&cmp, &extracted_channel_id)) {
                cmp_to_str(&cmp, &extracted_channel_id);
                cmp_read_uinteger(&cmp, (uint64_t *)&subscriber_id);
                //TODO
              }
              node_log_error(node, "unsub one not yet implemented");
            }
            else if(ngx_strmatch(&alerttype, "unsub all") && array_sz > 1) {
              if(cmp_to_str(&cmp, &extracted_channel_id)) {
                nchan_store_publish_generic(&extracted_channel_id, nodeset, NULL, NGX_HTTP_CONFLICT, &NCHAN_HTTP_STATUS_409);
              }
            }
            else if(ngx_strmatch(&alerttype, "unsub all except")) {
              if(cmp_to_str(&cmp, &extracted_channel_id)) {
                cmp_read_uinteger(&cmp, (uint64_t *)&subscriber_id);
                //TODO
              }
              node_log_error(node, "unsub all except not yet implemented");
            }
            else if(ngx_strmatch(&alerttype, "subscriber info")) {
              uint64_t request_id;
              cmp_read_uinteger(&cmp, &request_id);
              
              if((chanhead = nchan_store_get_chanhead(chid, nodeset)) == NULL) {
                node_log_error(node, "received invalid subscriber info notice with bad channel name");
              }
              else {
                chanhead->spooler.fn->broadcast_notice(&chanhead->spooler, NCHAN_NOTICE_SUBSCRIBER_INFO_REQUEST, (void *)(intptr_t )request_id);
              }
            }
            
            else {
              node_log_error(node, "unexpected msgpack alert from redis");
            }
          }
          else {
            node_log_error(node, "unexpected msgpack message from redis");
          }
        }
        else {
          node_log_error(node, "unexpected msgpack object from redis");
        }
      }
      else {
        node_log_error(node, "invalid msgpack message from redis: %s", cmp_strerror(&cmp));
      }
    }

    else { //not a string
      redisEchoCallback(c, el, NULL);
    }
  }

  else if((CHECK_REPLY_STRVAL(reply->element[0], "subscribe") || CHECK_REPLY_STRVAL(reply->element[0], "ssubscribe")) && CHECK_REPLY_INT(reply->element[2])) {
    if(chid) {
      node_pubsub_time_finish_relaxed(node, NCHAN_REDIS_CMD_PUBSUB_SUBSCRIBE);
      chanhead = find_chanhead_for_pubsub_callback(chid);
      if(chanhead != NULL) {
        redis_chanhead_set_pubsub_status(chanhead, node, REDIS_PUBSUB_SUBSCRIBED);
      }
      else {
        node_log_error(node, "received SUBSCRIBE acknowledgement for unknown channel %V", chid);
      }
    }
    else {
      DBG("subscribed to worker channel %s", redis_subscriber_id);
    }
    
    DBG("REDIS: PUB/SUB subscribed to %s (%i total)", reply->element[1]->str, reply->element[2]->integer);
  }
  else if((CHECK_REPLY_STRVAL(reply->element[0], "unsubscribe") || CHECK_REPLY_STRVAL(reply->element[0], "sunsubscribe")) && CHECK_REPLY_INT(reply->element[2])) {
    
    if(chid) {
      DBG("received UNSUBSCRIBE acknowledgement for channel %V", chid);
      node_pubsub_time_finish_relaxed(node, NCHAN_REDIS_CMD_PUBSUB_UNSUBSCRIBE);
      chanhead = find_chanhead_for_pubsub_callback(chid);
      if(chanhead != NULL) {
        if(chanhead->pubsub_status != REDIS_PUBSUB_UNSUBSCRIBED) {
          //didn't expect to receive an UNSUBSCRIBE -- but it does happen when a node is going down or being failed over
          redis_chanhead_set_pubsub_status(chanhead, node, REDIS_PUBSUB_UNSUBSCRIBED);
        }
      }
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
  
  redis_subscriber_unregister(head, sub, head->shutting_down);
  
  if(head->sub_count == 0 && head->fetching_message_count == 0) {
    redis_chanhead_gc_add(head, 0, "sub count == 0 and fetching_message_count == 0 after spooler dequeue");
  }
  
}

static void spooler_use_handler(channel_spooler_t *spl, void *d) {
  //nothing. 
}

static void spooler_get_message_start_handler(channel_spooler_t *spl, void *pd) {
  ((rdstore_channel_head_t *)pd)->fetching_message_count++;
}

static void spooler_get_message_finish_handler(channel_spooler_t *spl, void *pd) {
  ((rdstore_channel_head_t *)pd)->fetching_message_count--;
  assert(((rdstore_channel_head_t *)pd)->fetching_message_count >= 0);
}

static ngx_int_t start_chanhead_spooler(rdstore_channel_head_t *head) {
  static uint8_t channel_buffer_complete = 1;
  spooler_fetching_strategy_t spooling_strategy;
  static channel_spooler_handlers_t handlers = {
    spooler_add_handler,
    spooler_dequeue_handler,
    NULL,
    spooler_use_handler,
    spooler_get_message_start_handler,
    spooler_get_message_finish_handler
  };
  nchan_loc_conf_t *lcf = head->redis.nodeset->first_loc_conf; //any loc_conf that refers to this nodeset will work. 
  //the spooler needs it to pass to get_message calls, which in rdstore's case only cares about the nodeset referenced in the loc_conf
  if(head->redis.nodeset->settings.storage_mode == REDIS_MODE_DISTRIBUTED_NOSTORE) {
    spooling_strategy = NCHAN_SPOOL_PASSTHROUGH;
  }
  else {
    spooling_strategy = NCHAN_SPOOL_FETCH;
  }
  
  start_spooler(&head->spooler, &head->id, &head->status, &channel_buffer_complete, &nchan_store_redis, lcf, spooling_strategy, &handlers, head);
  return NGX_OK;
}

static void redis_update_channel_keepalive_timer(rdstore_channel_head_t *ch, int keepalive_from_redis_msec) {
  ngx_msec_t new_interval = keepalive_from_redis_msec;
  if(keepalive_from_redis_msec < 0) {
    //timer doesn't need to be set
    return;
  }
  
  // use the TTL provided. it might be the one we gave it, or it might be longer
  ch->keepalive_interval = new_interval;
    
  if(ch->keepalive_timer.timer_set) {
    ngx_del_timer(&ch->keepalive_timer);
  }
  ngx_add_timer(&ch->keepalive_timer, ch->keepalive_interval);
}

static void redis_subscriber_register_cb(redisAsyncContext *c, void *vr, void *privdata);

typedef struct {
  rdstore_channel_head_t *chanhead;
  unsigned                generation;
  subscriber_t           *sub;
} redis_subscriber_register_t;

static ngx_int_t redis_subscriber_register_send(redis_nodeset_t *nodeset, void *pd) {
  redis_subscriber_register_t   *d = pd;
  if(nodeset_ready(nodeset)) {
    d->chanhead->reserved++;
    redis_node_t *node = nodeset_node_find_by_chanhead(d->chanhead);
    node_command_time_start(node, NCHAN_REDIS_CMD_CHANNEL_SUBSCRIBE);
    
    nchan_redis_script(subscriber_register, node, &redis_subscriber_register_cb, d, &d->chanhead->id,
                       "- %i %i %i 1",
                       d->chanhead->keepalive_interval,
                       nodeset->settings.idle_channel_ttl_safety_margin,
                       ngx_time()
                      );
  }
  else {
    d->sub->fn->release(d->sub, 0);
    ngx_free(d);
  }
  return NGX_OK;
}


static ngx_int_t redis_subscriber_register(rdstore_channel_head_t *chanhead, subscriber_t *sub) {
  redis_subscriber_register_t *sdata=NULL;
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
  redis_subscriber_register_send(chanhead->redis.nodeset, sdata);
  
  return NGX_OK;
}

static ngx_int_t redis_subscriber_register_send_retry_wrapper(redis_nodeset_t *nodeset, void *pd) {
  redis_subscriber_register_t   *d = pd;
  d->chanhead->reserved--;
  return redis_subscriber_register_send(nodeset, pd);
}

static void redis_subscriber_register_cb(redisAsyncContext *c, void *vr, void *privdata) {
  redis_subscriber_register_t *sdata= (redis_subscriber_register_t *) privdata;
  redisReply                  *reply = (redisReply *)vr;
  redis_node_t                *node = c->data;
  
  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CHANNEL_SUBSCRIBE);
  
  sdata->chanhead->reserved--;
  
  if(!nodeset_node_reply_keyslot_ok(node, reply) && nodeset_node_can_retry_commands(node)) {
    sdata->chanhead->reserved++;
    nodeset_callback_on_ready(node->nodeset, redis_subscriber_register_send_retry_wrapper, sdata);
    return; 
  }
  
  if (!redisReplyOk(c, reply)) {
    //TODO: fail less silently, maybe retry subscriber registration?
    sdata->sub->fn->release(sdata->sub, 0);
    ngx_free(sdata);
    return;
  }
  
  if(  CHECK_REPLY_ARRAY_MIN_SIZE(reply, 4)
    && CHECK_REPLY_INT(reply->element[3])
  ) {
    //notify about channel buffer size if it's present
    sdata->sub->fn->notify(sdata->sub, NCHAN_NOTICE_REDIS_CHANNEL_MESSAGE_BUFFER_SIZE_CHANGE, (void *)(intptr_t )reply->element[3]->integer);
  }
  
  if(sdata->generation == sdata->chanhead->generation) {
    //is the subscriber     
    //TODO: set subscriber id
    //sdata->sub->id = reply->element[1]->integer;
  }
  
  sdata->sub->fn->release(sdata->sub, 0);
  sdata->sub = NULL; //don't use it anymore, it might have been freed
  
  if ( !CHECK_REPLY_ARRAY_MIN_SIZE(reply, 3) || !CHECK_REPLY_INT(reply->element[1]) || !CHECK_REPLY_INT(reply->element[2])) {
    //no good
    //TODO: fail less silently, maybe retry subscriber registration?
    redisEchoCallback(c,reply,privdata);
    ngx_free(sdata);
    return;
  }
  
  redis_update_channel_keepalive_timer(sdata->chanhead, reply->element[2]->integer);
  
  ngx_free(sdata);
}


typedef struct {
  ngx_str_t    *channel_id;
  time_t        channel_timeout;
  unsigned      allocd:1;
} subscriber_unregister_data_t;

static void redis_subscriber_unregister_cb(redisAsyncContext *c, void *r, void *privdata);
static ngx_int_t redis_subscriber_unregister_send(redis_nodeset_t *nodeset, void *pd) {
  //input: keys: [], values: [namespace, channel_id, subscriber_id, empty_ttl]
  // 'subscriber_id' is an existing id
  // 'empty_ttl' is channel ttl when without subscribers. 0 to delete immediately, -1 to persist, >0 ttl in sec
  //output: subscriber_id, num_current_subscribers
  subscriber_unregister_data_t *d = pd;
  if(nodeset_ready(nodeset)) {
    redis_node_t *node = nodeset_node_find_by_channel_id(nodeset, d->channel_id);
    node_command_time_start(node, NCHAN_REDIS_CMD_CHANNEL_UNSUBSCRIBE);
    nchan_redis_script( subscriber_unregister, node, &redis_subscriber_unregister_cb, NULL, 
      d->channel_id, "%i %i", 
                       0/*TODO: sub->id*/,
                       d->channel_timeout
                      );
  }
  if(d->allocd) {
    ngx_free(d);
  }
  return NGX_OK;
}

static void redis_subscriber_unregister_cb(redisAsyncContext *c, void *r, void *privdata) {
  redisReply      *reply = r;
  redis_node_t    *node = c->data;
  
  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CHANNEL_UNSUBSCRIBE);
  
  if(reply && reply->type == REDIS_REPLY_ERROR) {
    ngx_str_t    errstr;
    ngx_str_t    countstr;
    ngx_str_t    channel_id;
    ngx_int_t    channel_timeout;
    
    errstr.data = (u_char *)reply->str;
    errstr.len = strlen(reply->str);
    
    if(ngx_str_chop_if_startswith(&errstr, "CLUSTER KEYSLOT ERROR. ")) {
      nodeset_node_keyslot_changed(node, "CLUSTER KEYSLOT error");
      nchan_scan_until_chr_on_line(&errstr, &countstr, ' ');
      channel_timeout = ngx_atoi(countstr.data, countstr.len);
      channel_id = errstr;
      
      subscriber_unregister_data_t  *d = ngx_alloc(sizeof(*d) + sizeof(ngx_str_t) + channel_id.len, ngx_cycle->log);
      if(!d) {
        ERR("can't allocate add_fakesub_data for CLUSTER KEYSLOT ERROR retry");
        return;
      }
      d->channel_timeout = channel_timeout;
      d->channel_id = (ngx_str_t *)&d[1];
      d->channel_id->data = (u_char *)&d->channel_id[1];
      d->allocd = 1;
      nchan_strcpy(d->channel_id, &channel_id, 0);
      nodeset_callback_on_ready(node->nodeset, redis_subscriber_unregister_send, d);
      return;
    }
    
  }
  redisCheckErrorCallback(c, r, privdata);
}

static ngx_int_t redis_subscriber_unregister(rdstore_channel_head_t *chanhead, subscriber_t *sub, uint8_t shutting_down) {
  nchan_loc_conf_t  *cf = sub->cf;
  
  if(!shutting_down) {
    subscriber_unregister_data_t d;
    d.channel_id = &chanhead->id;
    d.channel_timeout = cf->channel_timeout;
    d.allocd = 0;
    redis_subscriber_unregister_send(chanhead->redis.nodeset, &d);
  }
  else {
    if(nodeset_ready(chanhead->redis.nodeset)) {
      redis_node_t   *node = nodeset_node_find_by_chanhead(chanhead);
      nchan_redis_sync_script(subscriber_unregister, node, &chanhead->id, "%i %i", 
                              0/*TODO: sub->id*/,
                              cf->channel_timeout
                            );
    }
  }
  return NGX_OK;
}


static void redisChannelKeepaliveCallback(redisAsyncContext *c, void *vr, void *privdata);

static ngx_int_t redisChannelKeepaliveCallback_send(redis_nodeset_t *ns, void *pd) {
  rdstore_channel_head_t   *head = pd;
  //TODO: optimize this
  redis_node_t *node = nodeset_node_find_by_channel_id(head->redis.nodeset, &head->id);
  if(nodeset_ready(ns)) {
    head->reserved++;
    
    nchan_set_next_backoff(&head->keepalive_interval, &ns->settings.idle_channel_ttl);
    node_command_time_start(node, NCHAN_REDIS_CMD_CHANNEL_KEEPALIVE);
    nchan_redis_script(channel_keepalive, node, &redisChannelKeepaliveCallback, head, &head->id, "%i %i", head->keepalive_interval, ns->settings.idle_channel_ttl_safety_margin);
  }
  return NGX_OK;
}

static ngx_int_t redisChannelKeepaliveCallback_retry_wrapper(redis_nodeset_t *ns, void *pd) {
  rdstore_channel_head_t   *head = pd;
  head->reserved--;
  return redisChannelKeepaliveCallback_send(ns, pd);
}

static void redisChannelKeepaliveCallback(redisAsyncContext *c, void *vr, void *privdata) {
  rdstore_channel_head_t   *head = (rdstore_channel_head_t *)privdata;
  redisReply               *reply = (redisReply *)vr;
  redis_node_t             *node = c->data;
  
  head->reserved--;
  
  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CHANNEL_KEEPALIVE);
  
  if(!nodeset_node_reply_keyslot_ok(node, reply) && nodeset_node_can_retry_commands(node)) {
    head->reserved++;
    nodeset_callback_on_ready(node->nodeset, redisChannelKeepaliveCallback_retry_wrapper, head);
    return;
  }
  
  if(!redisReplyOk(c, reply)) {
    node_log_error(node, "bad channel keepalive reply for channel %V", &head->id);
    if(!head->keepalive_timer.timer_set) {
      ngx_add_timer(&head->keepalive_timer, head->keepalive_interval);
    }
    return;
  }

  assert(CHECK_REPLY_INT(reply));
  
  redis_update_channel_keepalive_timer(head, reply->integer);
}

static void redis_channel_keepalive_timer_handler(ngx_event_t *ev) {
  rdstore_channel_head_t   *head = ev->data;
  if(ev->timedout) {
    ev->timedout=0;
    if(head->pubsub_status != REDIS_PUBSUB_SUBSCRIBED || head->status == NOTREADY) {
      //no use trying to keepalive a not-ready (possibly disconnected) chanhead
      DBG("Tried sending channel keepalive when channel is not ready");
      ngx_add_timer(ev, REDIS_CHANNEL_KEEPALIVE_NOTREADY_RETRY_TIME); //retry after reconnect timeout
    }
    else {
      redisChannelKeepaliveCallback_send(head->redis.nodeset, head);
    }
  }
}

ngx_int_t ensure_chanhead_pubsub_subscribed_if_needed(rdstore_channel_head_t *ch) {
  redis_node_t       *pubsub_node;
  if( ch->pubsub_status != REDIS_PUBSUB_SUBSCRIBED && ch->pubsub_status != REDIS_PUBSUB_SUBSCRIBING
   && ch->redis.nodeset->settings.storage_mode >= REDIS_MODE_DISTRIBUTED
   && nodeset_ready(ch->redis.nodeset)
  ) {
    pubsub_node = nodeset_node_pubsub_find_by_chanhead(ch);
    
    redis_chanhead_set_pubsub_status(ch, pubsub_node, REDIS_PUBSUB_SUBSCRIBING);
    node_pubsub_time_start(pubsub_node, NCHAN_REDIS_CMD_PUBSUB_SUBSCRIBE);
    redis_subscriber_command(pubsub_node, redis_subscriber_callback, pubsub_node, "%s %b", pubsub_node->nodeset->use_spublish ? "SSUBSCRIBE" : "SUBSCRIBE", STR(&ch->redis.pubsub_id));
  }
  return NGX_OK;
}

ngx_int_t redis_chanhead_catch_up_after_reconnect(rdstore_channel_head_t *ch) {
  return spooler_catch_up(&ch->spooler);
}

static rdstore_channel_head_t *create_chanhead(ngx_str_t *channel_id, redis_nodeset_t *ns) {
  rdstore_channel_head_t   *head;
  
  size_t pubsub_id_len = strlen("{channel:}:pubsub") + channel_id->len + ns->settings.namespace->len + 1;
  
  head=ngx_calloc(sizeof(*head) + sizeof(u_char)*(channel_id->len) + pubsub_id_len, ngx_cycle->log);
  if(head==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "can't allocate memory for (new) channel subscriber head");
    return NULL;
  }
  head->id.len = channel_id->len;
  head->id.data = (u_char *)&head[1];
  ngx_memcpy(head->id.data, channel_id->data, channel_id->len);
  head->redis.pubsub_id.data = &head->id.data[head->id.len];
  ngx_sprintf(head->redis.pubsub_id.data, "%V{channel:%V}:pubsub%Z", ns->settings.namespace, channel_id);
  head->redis.pubsub_id.len=pubsub_id_len-1; //sans null
  head->sub_count=0;
  head->fetching_message_count=0;
  head->redis_subscriber_privdata = NULL;
  head->status = NOTREADY;
  head->pubsub_status = REDIS_PUBSUB_UNSUBSCRIBED;
  head->generation = 0;
  head->last_msgid.time=0;
  head->last_msgid.tag.fixed[0]=0;
  head->last_msgid.tagcount = 1;
  head->last_msgid.tagactive = 0;
  head->shutting_down = 0;
  head->reserved = 0;
  head->keepalive_interval = 0;
  
  head->gc.in_reaper = 0;
  head->gc.time = 0;
  head->gc.prev = NULL;
  head->gc.next = NULL;
  
  if(head->id.len >= 5 && ngx_strncmp(head->id.data, "meta/", 5) == 0) {
    head->meta = 1;
  }
  else {
    head->meta = 0;
  }

  ngx_memzero(&head->keepalive_timer, sizeof(head->keepalive_timer));
  nchan_init_timer(&head->keepalive_timer, redis_channel_keepalive_timer_handler, head);
  nchan_set_next_backoff(&head->keepalive_interval, &ns->settings.idle_channel_ttl);
  if(channel_id->len > 2) { // absolutely no multiplexed channels allowed
    assert(ngx_strncmp(head->id.data, "m/", 2) != 0);
  }
  
  head->redis.nodeset = ns;
  head->redis.generation = 0;
  head->redis.node.cmd = NULL;
  head->redis.node.pubsub = NULL;
  ngx_memzero(&head->redis.slist, sizeof(head->redis.slist));
  
  if(head->redis.nodeset->settings.storage_mode == REDIS_MODE_BACKUP) {
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

static rdstore_channel_head_t * nchan_store_get_chanhead(ngx_str_t *channel_id, redis_nodeset_t *nodeset) {
  rdstore_channel_head_t    *head;
  
  CHANNEL_HASH_FIND(channel_id, head); //BUG: this doesn't account for namespacing!!
  if(head==NULL) {
    head = create_chanhead(channel_id, nodeset);
  }
  if(head == NULL) {
    ERR("can't create chanhead for redis store");
    return NULL;
  }
  
  if (head->status == INACTIVE) { //recycled chanhead
    ensure_chanhead_pubsub_subscribed_if_needed(head);
    redis_chanhead_gc_withdraw(head);
    
    if(head->redis.nodeset->settings.storage_mode == REDIS_MODE_BACKUP) {
      head->status = READY;
    }
    else {
      head->status = head->pubsub_status == REDIS_PUBSUB_SUBSCRIBED ? READY : NOTREADY;
    }
  }

  if(!head->spooler.running) {
    DBG("Spooler for channel %p %V wasn't running. start it.", head, &head->id);
    start_chanhead_spooler(head);
  }
  
  return head;
}

ngx_int_t redis_chanhead_gc_add(rdstore_channel_head_t *head, ngx_int_t expire, const char *reason) {
  assert(head->sub_count == 0);
  nchan_reaper_t *reaper = &head->redis.nodeset->chanhead_reaper;
  if(!head->gc.in_reaper) {
    assert(head->status != INACTIVE);
    head->status = INACTIVE;
    head->gc.time = ngx_time() + (expire == 0 ? NCHAN_CHANHEAD_EXPIRE_SEC : expire);
    head->gc.in_reaper = 1;
    
    nchan_reaper_add(reaper, head);
    
    DBG("gc_add chanhead %V to %s (%s)", &head->id, reaper->name, reason);
  }
  else {
    ERR("gc_add chanhead %V to %s: already added (%s)", &head->id, reaper->name, reason);
  }

  return NGX_OK;
}

ngx_int_t redis_chanhead_gc_withdraw(rdstore_channel_head_t *chanhead) {
  if(chanhead->gc.in_reaper) {
    nchan_reaper_t *reaper = &chanhead->redis.nodeset->chanhead_reaper;
    DBG("gc_withdraw chanhead %s from %V", reaper->name, &chanhead->id);
    assert(chanhead->status == INACTIVE);
    
    nchan_reaper_withdraw(reaper, chanhead);
    chanhead->gc.in_reaper = 0;
  }
  else {
    DBG("gc_withdraw chanhead (%V), but not in gc reaper", &chanhead->id);
  }
  return NGX_OK;
}

static ngx_int_t nchan_store_publish_generic(ngx_str_t *channel_id, redis_nodeset_t *nodeset, nchan_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line){
  rdstore_channel_head_t   *head;
  ngx_int_t                 ret;
  //redis_channel_head_cleanup_t *hcln;
  
  
  
  head = nchan_store_get_chanhead(channel_id, nodeset);
  
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

#define CREATE_CALLBACK_DATA(d, nodeset, cf, namestr, channel_id, callback, privdata) \
  do {                                                                       \
    if ((d = ngx_alloc(sizeof(*d) + ((nodeset)->cluster.enabled ? (sizeof(*channel_id) + channel_id->len) : 0), ngx_cycle->log)) == NULL) { \
      ERR("Can't allocate redis %s channel callback data", namestr);         \
      return NGX_ERROR;                                                      \
    }                                                                        \
    d->t = ngx_current_msec;                                                 \
    d->name = namestr;                                                       \
    if((nodeset)->cluster.enabled) {                                            \
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

static ngx_int_t nchan_store_delete_channel_send(redis_nodeset_t *ns, void *pd) {
  redis_channel_callback_data_t *d = pd;
  if(nodeset_ready(ns)) {
    redis_node_t *node = nodeset_node_find_by_channel_id(ns, d->channel_id);
    node_command_time_start(node, NCHAN_REDIS_CMD_CHANNEL_DELETE);
    nchan_redis_script(delete, node, &redisChannelDeleteCallback, d, d->channel_id, "%s %i",ns->use_spublish ? "SPUBLISH" : "PUBLISH", ns->settings.accurate_subscriber_count);
    return NGX_OK;
  }
  else {
    redisChannelDeleteCallback(NULL, NULL, d);
    return NGX_ERROR;
  }
}

static void redisChannelDeleteCallback(redisAsyncContext *ac, void *r, void *privdata) {
  redis_node_t  *node = ac ? ac->data : NULL;
  
  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CHANNEL_DELETE);
  if(ac) {
    if(!nodeset_node_reply_keyslot_ok(node, (redisReply *)r) && nodeset_node_can_retry_commands(node)) {
      nodeset_callback_on_ready(node->nodeset, nchan_store_delete_channel_send, privdata);
      return;
    }
  }
  redisChannelInfoCallback(ac, r, privdata);
  ngx_free(privdata);
}

static ngx_int_t nchan_store_delete_channel(ngx_str_t *channel_id, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  redis_channel_callback_data_t *d;
  redis_nodeset_t               *ns = nodeset_find(&cf->redis);
  CREATE_CALLBACK_DATA(d, ns, cf, "delete", channel_id, callback, privdata);

  return nchan_store_delete_channel_send(ns, d);
}


static void redisChannelFindCallback(redisAsyncContext *c, void *r, void *privdata);

static ngx_int_t nchan_store_find_channel_send(redis_nodeset_t *ns, void *pd) {
  redis_channel_callback_data_t *d = pd;
  if(nodeset_ready(ns)) {
    redis_node_t *node = nodeset_node_find_by_channel_id(ns, d->channel_id);
    node_command_time_start(node, NCHAN_REDIS_CMD_CHANNEL_FIND);
    nchan_redis_script(find_channel, node, &redisChannelFindCallback, d, d->channel_id, "%i", ns->settings.accurate_subscriber_count);
  }
  else {
    redisChannelFindCallback(NULL, NULL, d);
  }
  return NGX_OK;
}

static void redisChannelFindCallback(redisAsyncContext *ac, void *r, void *privdata) {
  redis_node_t                 *node = ac ? ac->data : NULL;
  
  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CHANNEL_FIND);
  
  if(ac) {
    node = ac->data;
    
    if(!nodeset_node_reply_keyslot_ok(node, (redisReply *)r) && nodeset_node_can_retry_commands(node)) {
      nodeset_callback_on_ready(node->nodeset, nchan_store_find_channel_send, privdata);
      return;
    }
  }
  
  redisChannelInfoCallback(ac, r, privdata);
  ngx_free(privdata);
}

static ngx_int_t nchan_store_find_channel(ngx_str_t *channel_id, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  redis_channel_callback_data_t *d;
  redis_nodeset_t               *ns = nodeset_find(&cf->redis);
  CREATE_CALLBACK_DATA(d, ns, cf, "find_channel", channel_id, callback, privdata);
  
  nchan_store_find_channel_send(ns, d);
  
  return NGX_OK;
}

static ngx_int_t msg_from_redis_get_message_reply(nchan_msg_t *msg, nchan_compressed_msg_t *cmsg, ngx_str_t *content_type, ngx_str_t *eventsource_event, redisReply *r, uint16_t offset) {
  
  redisReply         **els = r->element;
  size_t               content_type_len = 0, es_event_len = 0;
  ngx_int_t            time_int = 0, ttl;
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
    
    if(redisReply_to_ngx_int(els[offset], &ttl) != NGX_OK) {
      ERR("invalid ttl integer value in msg response from redis");
      return NGX_ERROR;
    }
    assert(ttl >= 0);
    if(ttl == 0)
      ttl++; // less than a second left for this message... give it a second's lease on life
    
    msg->expires = ngx_time() + ttl;
    
    msg->compressed = NULL;
    if(r->elements >= (uint16_t )(offset + 8)) {
      if(!CHECK_REPLY_INT_OR_STR(els[offset+8]) || redisReply_to_ngx_int(els[offset+8], &compression) != NGX_OK) {
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
    
    if(redisReply_to_ngx_int(els[offset+1], &time_int) == NGX_OK) {
      msg->id.time = time_int;
    }
    else {
      msg->id.time = 0;
      ERR("invalid msg time from redis");
    }
    
    redisReply_to_ngx_int(els[offset+2], (ngx_int_t *)&msg->id.tag.fixed[0]); // tag is a uint, meh.
    msg->id.tagcount = 1;
    msg->id.tagactive = 0;
    
    redisReply_to_ngx_int(els[offset+3], &time_int);
    msg->prev_id.time = time_int;
    redisReply_to_ngx_int(els[offset+4], (ngx_int_t *)&msg->prev_id.tag.fixed[0]);
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

static ngx_int_t nchan_store_async_get_message_send(redis_nodeset_t *ns, void *pd) {
  redis_get_message_data_t           *d = pd;
  //input:  keys: [], values: [namespace, channel_id, msg_time, msg_tag, no_msgid_order, create_channel_ttl]
  //output: result_code, msg_ttl, msg_time, msg_tag, prev_msg_time, prev_msg_tag, message, content_type, eventsource_event, channel_subscriber_count
  if(nodeset_ready(ns)) {
    redis_node_t *node = nodeset_node_find_by_channel_id(ns, d->channel_id);
    node_command_time_start(node, NCHAN_REDIS_CMD_CHANNEL_GET_MESSAGE);
    nchan_redis_script(get_message, node, &redis_get_message_callback, d, d->channel_id, "%i %i FILO 0", 
                       d->msg_id.time, 
                       d->msg_id.tag
                      );
  }
  else {
    //TODO: pass on a get_msg error status maybe?
    ngx_free(d);
  }
  return NGX_OK;
}

static void redis_get_message_callback(redisAsyncContext *ac, void *r, void *privdata) {
  redisReply                *reply= r;
  redis_get_message_data_t  *d= (redis_get_message_data_t *)privdata;
  nchan_msg_t                msg;
  nchan_compressed_msg_t     cmsg;
  ngx_str_t                  content_type;
  ngx_str_t                  eventsource_event;
  redis_node_t              *node = ac ? ac->data : NULL;

  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CHANNEL_GET_MESSAGE);
  
  if(d == NULL) {
    ERR("redis_get_mesage_callback has NULL userdata");
    return;
  }
  
  if(ac) {
    if(!nodeset_ready(node->nodeset) || (!nodeset_node_reply_keyslot_ok(node, reply) && nodeset_node_can_retry_commands(node))) {
      nodeset_callback_on_ready(node->nodeset, nchan_store_async_get_message_send, privdata);
      return;
    }
  
    log_redis_reply(d->name, d->t);
  
    //output: result_code, msg_ttl, msg_time, msg_tag, prev_msg_time, prev_msg_tag, message, content_type, eventsource_event, compression_type, channel_subscriber_count
    // result_code can be: 200 - ok, 403 - channel not found, 404 - not found, 410 - gone, 418 - not yet available
  
    if (!redisReplyOk(ac, r) || !CHECK_REPLY_ARRAY_MIN_SIZE(reply, 1) || !CHECK_REPLY_INT(reply->element[0]) ) {
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
  }
  else {
    ERR("redisAsyncContext NULL for redis_get_message_callback");
  }
  
  ngx_free(d);
}

static ngx_int_t nchan_store_async_get_message(ngx_str_t *channel_id, nchan_msg_id_t *msg_id, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  redis_get_message_data_t      *d;
  redis_nodeset_t               *ns = nodeset_find(&cf->redis);
  if(callback==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "no callback given for async get_message. someone's using the API wrong!");
    return NGX_ERROR;
  }
  
  assert(msg_id->tagcount == 1);
  
  CREATE_CALLBACK_DATA(d, ns, cf, "get_message", channel_id, callback, privdata);
  d->msg_id.time = msg_id->time;
  d->msg_id.tag = msg_id->tag.fixed[0];
  
  nchan_store_async_get_message_send(ns, d);
  return NGX_OK; //async only now!
}


typedef struct nchan_redis_conf_ll_s nchan_redis_conf_ll_t;
struct nchan_redis_conf_ll_s {
  nchan_loc_conf_t       *lcf;
  nchan_redis_conf_ll_t  *next;
};

nchan_redis_conf_ll_t   *redis_conf_head;

ngx_int_t nchan_store_redis_add_active_loc_conf(ngx_conf_t *cf, nchan_loc_conf_t *loc_conf) {
  nchan_redis_conf_ll_t  *rcf_ll = ngx_palloc(cf->pool, sizeof(*rcf_ll));
  rcf_ll->lcf = loc_conf;
  rcf_ll->next = redis_conf_head;
  redis_conf_head = rcf_ll;
  return NGX_OK;
}

ngx_int_t nchan_store_redis_remove_active_loc_conf(ngx_conf_t *cf, nchan_loc_conf_t *loc_conf) {
  nchan_redis_conf_ll_t  *cur, *prev;
  
  for(cur = redis_conf_head, prev = NULL; cur != NULL; prev = cur, cur = cur->next) {
    if(cur->lcf == loc_conf) { //found it
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


ngx_int_t rdstore_initialize_chanhead_reaper(nchan_reaper_t *reaper, char *name) {
  
  nchan_reaper_start(reaper, 
              name, 
              offsetof(rdstore_channel_head_t, gc.prev), 
              offsetof(rdstore_channel_head_t, gc.next), 
  (ngx_int_t (*)(void *, uint8_t)) nchan_redis_chanhead_ready_to_reap,
  (void (*)(void *)) redis_store_reap_chanhead,
              4
  );
    
  return NGX_OK;
}

static ngx_int_t nchan_store_init_redis_loc_conf_postconfig(nchan_loc_conf_t *lcf) {
  nchan_redis_conf_t    *rcf = &lcf->redis;
  
  assert(rcf->enabled);
  
  //server-scope loc_conf may have some undefined values (because it was never merged with a prev)
  //thus we must reduntantly check for unset values
  if(rcf->ping_interval == NGX_CONF_UNSET) {
    rcf->ping_interval = NCHAN_REDIS_DEFAULT_PING_INTERVAL_TIME;
  }
  if(rcf->storage_mode == REDIS_MODE_CONF_UNSET) {
    rcf->storage_mode = REDIS_MODE_DISTRIBUTED;
  }
  if(rcf->nostore_fastpublish == NGX_CONF_UNSET) {
    rcf->nostore_fastpublish = 0;
  }
  
  return NGX_OK;
}

static ngx_int_t nchan_store_init_postconfig(ngx_conf_t *cf) {
  nchan_loc_conf_t      *lcf;
  nchan_redis_conf_ll_t *cur;
  nchan_main_conf_t     *mcf = ngx_http_conf_get_module_main_conf(cf, ngx_nchan_module);
  redis_nodeset_t       *nodeset;
  
  if(mcf->redis_publish_message_msgkey_size == NGX_CONF_UNSET_SIZE) {
    mcf->redis_publish_message_msgkey_size = NCHAN_REDIS_DEFAULT_PUBSUB_MESSAGE_MSGKEY_SIZE;
  }
  redis_publish_message_msgkey_size = mcf->redis_publish_message_msgkey_size;
  
  for(cur = redis_conf_head; cur != NULL; cur = cur->next) {
    lcf = cur->lcf;
    nchan_store_init_redis_loc_conf_postconfig(lcf);
    
    if((nodeset = nodeset_find(&lcf->redis)) == NULL) {
      nodeset = nodeset_create(lcf);
    }
    if(!nodeset) {
      return NGX_ERROR;
    }
    rdstore_initialize_chanhead_reaper(&nodeset->chanhead_reaper, "Redis channel reaper");
  }
  
  return NGX_OK;
}

static void nchan_store_create_main_conf(ngx_conf_t *cf, nchan_main_conf_t *mcf) {
  mcf->redis_publish_message_msgkey_size=NGX_CONF_UNSET_SIZE;
  
  //reset redis_conf_head for reloads
  redis_conf_head = NULL;
  nodeset_destroy_all(); //reset all nodesets before loading config
}

void redis_store_prepare_to_exit_worker() {
  rdstore_channel_head_t    *cur, *tmp;
  HASH_ITER(hh, chanhead_hash, cur, tmp) {
    cur->shutting_down = 1;
  }
}

static void nodeset_exiter_stage1(redis_nodeset_t *ns, void *pd) {
  nodeset_abort_on_ready_callbacks(ns);
}
static void nodeset_exiter_stage2(redis_nodeset_t *ns, void *pd) {
  unsigned *chanheads = pd;
  *chanheads += ns->chanhead_reaper.count;
  nchan_reaper_stop(&ns->chanhead_reaper);
}

static void nchan_store_exit_worker(ngx_cycle_t *cycle) {
  rdstore_channel_head_t     *cur, *tmp;
  unsigned                    chanheads = 0;
  DBG("redis exit worker");
  
  //old
  //rbtree_walk(&redis_data_tree, (rbtree_walk_callback_pt )redis_data_tree_exiter_stage1, NULL);
  
  nodeset_each(nodeset_exiter_stage1, NULL);
  
  HASH_ITER(hh, chanhead_hash, cur, tmp) {
    cur->shutting_down = 1;
    if(!cur->gc.in_reaper) {
      cur->spooler.fn->broadcast_status(&cur->spooler, NGX_HTTP_GONE, &NCHAN_HTTP_STATUS_410);
      redis_chanhead_gc_add(cur, 0, "exit worker");
    }
  }
  
  nodeset_each(nodeset_exiter_stage2, &chanheads);
  
  //OLD
  //rbtree_walk(&redis_data_tree, (rbtree_walk_callback_pt )redis_data_tree_exiter_stage2, &chanheads);
  nodeset_destroy_all();
  
  //OLD
  //rbtree_empty(&redis_data_tree, (rbtree_walk_callback_pt )redis_data_tree_exiter_stage3, NULL);
  
  nchan_exit_notice_about_remaining_things("redis channel", "", chanheads);
}

static void nchan_store_exit_master(ngx_cycle_t *cycle) {
  nodeset_destroy_all();
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
  assert(d->sub->cf->redis.enabled);
  redis_nodeset_t              *nodeset = nodeset_find(&d->sub->cf->redis);
  ngx_int_t                     rc;
  
  ch = nchan_store_get_chanhead(d->channel_id, nodeset);
  
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
  unsigned              cluster_move_error:1;
  time_t                message_timeout;
  ngx_int_t             max_messages;
  nchan_msg_compression_type_t compression;
  ngx_int_t             msglen;
  callback_pt           callback;
  void                 *privdata;
  uint8_t               retry;
} redis_publish_callback_data_t;

static void redisPublishCallback(redisAsyncContext *, void *, void *);
static void redisPublishNostoreCallback(redisAsyncContext *, void *, void *);
static void redisPublishNostoreQueuedCheckCallback(redisAsyncContext *, void *, void *);
static ngx_int_t redis_publish_message_send(redis_nodeset_t *nodeset, void *pd);

static ngx_int_t redis_publish_message_nodeset_maybe_retry(redis_nodeset_t *ns, redis_publish_callback_data_t *d) {
  //retry maybe
  if(d->retry < REDIS_NODESET_NOT_READY_MAX_RETRIES) {
    d->retry++;
    nodeset_callback_on_ready(ns, redis_publish_message_send, d);
  }
  else {
    d->callback(NGX_HTTP_SERVICE_UNAVAILABLE, NULL, d->privdata);
    ngx_free(d);
  }
  return NGX_DECLINED;
}

static ngx_int_t redis_publish_message_send(redis_nodeset_t *nodeset, void *pd) {
  redis_publish_callback_data_t  *d = pd;
  ngx_int_t                       mmapped = 0;
  ngx_buf_t                      *buf;
  ngx_str_t                       msgstr;
  nchan_msg_t                    *msg = d->msg;
  const ngx_str_t                 empty=ngx_string("");
  
  if(!nodeset_ready(nodeset)) {
    return redis_publish_message_nodeset_maybe_retry(nodeset, d);
  }
  
  redis_node_t *node = nodeset_node_find_by_channel_id(nodeset, d->channel_id);
  
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
  
  node_command_time_start(node, NCHAN_REDIS_CMD_CHANNEL_PUBLISH);
  
  if(nodeset->settings.storage_mode == REDIS_MODE_DISTRIBUTED_NOSTORE) {
    //hand-roll the msgpacked message
    /*
    9A A3"msg" CE<ttl> CE<time> 00 00 00 DB<len><str> D9<len><str> D9<len><str> 0X
    |  |       |       |        |  |  |  |            |            |            |
    |  |       |       |        |  |  |  |            |            |    fixint0/1 compression
    |  |       |       |        |  |  |  |            |    bin8 <uint8 len> eventsource-event
    |  |       |       |        |  |  |  |     bin8 <uint8 len> content type
    |  |       |       |        |  |  |  bin32 <uint32 BE len> msg data
    |  |       |       |      fixint=0 tag=0, prev_time=0, prev_tag=0
    |  |       |    uint32 (BE) message time
    |  |     uint32 (BE) message ttl
    |  fixstr[3]
    fixarray[10]
    */
    
    uint32_t ttl, time;
    uint32_t msglen;
    uint8_t  content_type_len, eventsource_event_len, compression;
    char     zero='\0';
    int      fastpublish = nodeset->settings.nostore_fastpublish;
    void   (*publish_callback)(redisAsyncContext *, void *, void *) = NULL;
    
    
    ttl = htonl(d->message_timeout);
    time = htonl(msg->id.time);
    msglen = htonl(d->msglen);
    content_type_len = msg->content_type ? (msg->content_type->len > 255 ? 255 : msg->content_type->len) : 0;
    eventsource_event_len = msg->eventsource_event ? (msg->eventsource_event->len > 255 ? 255 : msg->eventsource_event->len) : 0;
    compression = d->compression;
    
    if(!fastpublish) {
      redis_command(node, NULL, d, "MULTI");
      publish_callback = redisPublishNostoreQueuedCheckCallback;
    }
    else {
      publish_callback = redisPublishNostoreCallback;
    }
    
    redis_command(node, publish_callback, d,
      //can't use the prebaked pubsub channel id because we don't have the chanhead here, just its id
      "%s %b{channel:%b}:pubsub "
      "\x9A\xA3msg\xCE%b\xCE%b%b%b%b\xDB%b%b\xD9%b%b\xD9%b%b%b",
      
      nodeset->use_spublish ? "SPUBLISH" : "PUBLISH",
      STR(nodeset->settings.namespace),
      STR(d->channel_id),
      
      (char *)&ttl, (size_t )4,
      (char *)&time, (size_t )4,
      (char *)&zero, (size_t )1,
      (char *)&zero, (size_t )1,
      (char *)&zero, (size_t )1,
      (char *)&msglen, (size_t )4,
      STR(&msgstr),
      (char *)&content_type_len, (size_t )1,
      STR((msg->content_type ? msg->content_type : &empty)),
      (char *)&eventsource_event_len, (size_t )1,
      STR((msg->eventsource_event ? msg->eventsource_event : &empty)),
      (char *)&compression, (size_t )1
    );
    if(!fastpublish) {
      nchan_redis_script(nostore_publish_multiexec_channel_info, node, publish_callback, d, d->channel_id, "%i", nodeset->settings.accurate_subscriber_count);
      redis_command(node, &redisPublishNostoreCallback, d, "EXEC");
    }
  }
  else {  
    //input:  keys: [], values: [namespace, channel_id, time, message, content_type, eventsource_event, compression, msg_ttl, max_msg_buf_size, pubsub_msgpacked_size_cutoff, , optimize_target, publish_command, use_accurate_subscriber_count]
    //output: message_time, message_tag, channel_hash {ttl, time_last_seen, subscribers, messages}
    nchan_redis_script(publish, node, &redisPublishCallback, d, d->channel_id, 
                      "%b %b %b %i %i %i %i %s %i",
                      STR(&msgstr), 
                      STR((msg->content_type ? msg->content_type : &empty)), 
                      STR((msg->eventsource_event ? msg->eventsource_event : &empty)), 
                      d->compression,
                      d->message_timeout, 
                      d->max_messages, 
                      redis_publish_message_msgkey_size,
                      nodeset->use_spublish ? "SPUBLISH" : "PUBLISH",
                      nodeset->settings.accurate_subscriber_count
                      );
  }
  if(mmapped && munmap(msgstr.data, msgstr.len) == -1) {
    ERR("munmap was a problem");
    return NGX_ERROR;
  }
  return NGX_OK;
}

static ngx_int_t nchan_store_publish_message(ngx_str_t *channel_id, nchan_msg_t *msg, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  redis_publish_callback_data_t  *d=NULL;
  redis_nodeset_t                *ns = nodeset_find(&cf->redis);
  assert(callback != NULL);

  CREATE_CALLBACK_DATA(d, ns, cf, "publish_message", channel_id, callback, privdata);
  
  d->msg_time=msg->id.time;
  if(d->msg_time == 0) {
    d->msg_time = ngx_time();
  }
  d->msg = msg;
  d->shared_msg = msg->storage == NCHAN_MSG_SHARED;
  d->message_timeout = nchan_loc_conf_message_timeout(cf);
  d->max_messages = nchan_loc_conf_max_messages(cf);
  d->compression = cf->message_compression;
  d->retry = 0;
  d->cluster_move_error = 0;
  
  assert(msg->id.tagcount == 1);
  
  if(d->shared_msg) {
    msg_reserve(d->msg, "redis publish");
  }
  redis_publish_message_send(ns, d);
  
  return NGX_OK;
}

static int64_t redisReply_to_int(redisReply *reply, int nil_value, int wrong_datatype_value) {
  switch(reply->type) {
    case REDIS_REPLY_INTEGER:
      return reply->integer;
    case REDIS_REPLY_STRING:
      return atol(reply->str);
    case REDIS_REPLY_NIL:
      return nil_value;
    default:
      return wrong_datatype_value;
  }
}

static void redisPublishNostoreCallback(redisAsyncContext *c, void *r, void *privdata) {
  redis_publish_callback_data_t *d=(redis_publish_callback_data_t *)privdata;
  redisReply                    *reply=r;
  redisReply                   **els;
  nchan_channel_t                ch;
  
  redis_node_t                 *node = c->data;
  
  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CHANNEL_PUBLISH);
  
  if(d->shared_msg) {
    msg_release(d->msg, "redis publish");
  }
  
  ngx_memzero(&ch, sizeof(ch)); //for debugging basically. should be removed in the future and zeroed as-needed
  if(d->cluster_move_error) {
    nodeset_node_keyslot_changed(node, "CLUSTER MOVE error");
    d->callback(NGX_HTTP_SERVICE_UNAVAILABLE, NULL, d->privdata);
  }
  else if(reply) {
    if(reply->type == REDIS_REPLY_ARRAY && reply->elements == 2 && reply->element[1]->type == REDIS_REPLY_ARRAY && reply->element[1]->elements == 2) {
      els = reply->element[1]->element;
      ch.last_seen = redisReply_to_int(els[0], 0, 0);
      ch.subscribers = redisReply_to_int(els[1], 0, 0);
      d->callback(ch.subscribers > 0 ? NCHAN_MESSAGE_RECEIVED : NCHAN_MESSAGE_QUEUED, &ch, d->privdata);
    }
    else if(reply->type == REDIS_REPLY_INTEGER) {
      ch.last_seen = 0;
      ch.subscribers = redisReply_to_int(reply, 0, 0);
      d->callback(ch.subscribers > 0 ? NCHAN_MESSAGE_RECEIVED : NCHAN_MESSAGE_QUEUED, &ch, d->privdata);
    }
    else {
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

static void redisPublishNostoreQueuedCheckCallback(redisAsyncContext *c, void *r, void *privdata) {
  redis_publish_callback_data_t *d=(redis_publish_callback_data_t *)privdata;
  redisReply                    *reply=r;
  
  redis_node_t                 *node = c->data;
  node_command_received(node);
  
  if(reply && !CHECK_REPLY_STATUSVAL(reply, "QUEUED")) {
    if(!nodeset_node_reply_keyslot_ok(node, reply) && nodeset_node_can_retry_commands(node)) {
      d->cluster_move_error = 1;
    }
    else {
      redisEchoCallback(c, r, privdata);
    }
  }
}

static void redisPublishCallback(redisAsyncContext *c, void *r, void *privdata) {
  redis_publish_callback_data_t *d=(redis_publish_callback_data_t *)privdata;
  redisReply                    *reply=r;
  redisReply                    *cur;
  nchan_channel_t                ch;
  
  redis_node_t                 *node = c->data;
  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CHANNEL_PUBLISH);
  if(!nodeset_node_reply_keyslot_ok(node, reply) && nodeset_node_can_retry_commands(node)) {
    if(d->shared_msg) {
      redis_publish_message_nodeset_maybe_retry(node->nodeset, d);
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
static ngx_int_t nchan_store_redis_add_fakesub_send(redis_nodeset_t *nodeset, void *pd) {
  add_fakesub_data_t *d = pd;
  if(nodeset_ready(nodeset)) {
    redis_node_t *node = nodeset_node_find_by_channel_id(nodeset, d->channel_id);
    node_command_time_start(node, NCHAN_REDIS_CMD_CHANNEL_CHANGE_SUBSCRIBER_COUNT);
    nchan_redis_script(add_fakesub, node, &nchan_store_redis_add_fakesub_callback, NULL, 
                       d->channel_id,
                       "%i %i %s",
                       d->count,
                       ngx_time(),
                       redis_subscriber_id
                      );
    return NGX_OK;
  }
  else {
    return NGX_ERROR;
  }
}

static ngx_int_t nchan_store_redis_add_fakesub_send_retry_wrapper(redis_nodeset_t *nodeset, void *pd) {
  add_fakesub_data_t *d = pd;
  ngx_int_t rc = nchan_store_redis_add_fakesub_send(nodeset, pd);
  ngx_free(d);
  return rc;
}

static void nchan_store_redis_add_fakesub_callback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply      *reply = r;
  redis_node_t    *node = c->data;
  
  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CHANNEL_CHANGE_SUBSCRIBER_COUNT);
  
  if(reply && reply->type == REDIS_REPLY_ERROR) {
    ngx_str_t    errstr;
    ngx_str_t    countstr;
    ngx_str_t    channel_id;
    intptr_t     count;
    
    errstr.data = (u_char *)reply->str;
    errstr.len = strlen(reply->str);
    
    if(ngx_str_chop_if_startswith(&errstr, "CLUSTER KEYSLOT ERROR. ")) {
      nodeset_node_keyslot_changed(node, "CLUSTER KEYSLOT error");
      nchan_scan_until_chr_on_line(&errstr, &countstr, ' ');
      count = ngx_atoi(countstr.data, countstr.len);
      channel_id = errstr;
      
      add_fakesub_data_t  *d = ngx_alloc(sizeof(*d) + sizeof(ngx_str_t) + channel_id.len, ngx_cycle->log);
      if(!d) {
        ERR("can't allocate add_fakesub_data for CLUSTER KEYSLOT ERROR retry");
        return;
      }
      d->count = count;
      d->channel_id = (ngx_str_t *)&d[1];
      d->channel_id->data = (u_char *)&d->channel_id[1];
      nchan_strcpy(d->channel_id, &channel_id, 0);
      nodeset_callback_on_ready(node->nodeset, nchan_store_redis_add_fakesub_send_retry_wrapper, d);
      
      return;
    }
    
  }
  redisCheckErrorCallback(c, r, privdata);
}

ngx_int_t nchan_store_redis_fakesub_add(ngx_str_t *channel_id, nchan_loc_conf_t *cf, ngx_int_t count, uint8_t shutting_down) {
  redis_nodeset_t  *nodeset = nodeset_find(&cf->redis);
  
  if(!shutting_down) {
    add_fakesub_data_t   data = {channel_id, count};
    nchan_store_redis_add_fakesub_send(nodeset, &data);
  }
  else {
    if(nodeset_ready(nodeset)) {
      redis_node_t *node = nodeset_node_find_by_channel_id(nodeset, channel_id);
      redis_sync_command(node, "EVALSHA %s 0 %b %i", redis_lua_scripts.add_fakesub.hash, STR(channel_id), count);
    }
  }
  return NGX_OK;
}

int nchan_store_redis_ready(nchan_loc_conf_t *cf) {
  redis_nodeset_t   *nodeset = nodeset_find(&cf->redis);
  return nodeset && nodeset_ready(nodeset);
}


typedef struct {
  callback_pt  cb;
  void        *pd;
} redis_subscriber_info_id_data_t;

static void get_subscriber_info_id_callback(redisAsyncContext *c, void *r, void *privdata);

static ngx_int_t nchan_store_get_subscriber_info_id(nchan_loc_conf_t *cf, callback_pt cb, void *pd) {
  redis_nodeset_t  *nodeset = nodeset_find(&cf->redis);
  
  if(!nodeset_ready(nodeset)) {
    return NGX_ERROR;
  }
  
  ngx_str_t request_id_key = ngx_string(NCHAN_REDIS_UNIQUE_REQUEST_ID_KEY);
  redis_node_t  *node = nodeset_node_find_by_key(nodeset, &request_id_key);
  if(!node) {
    return NGX_ERROR;
  }
  
  redis_subscriber_info_id_data_t *d = ngx_alloc(sizeof(*d), ngx_cycle->log);
  if(d == NULL) {
    return NGX_ERROR;
  }
  
  d->cb = cb;
  d->pd = pd;
  
  node_command_time_start(node, NCHAN_REDIS_CMD_CHANNEL_GET_SUBSCRIBER_INFO_ID);
  redis_script(get_subscriber_info_id, node, &get_subscriber_info_id_callback, d, "1 %b", STR(&request_id_key));
  
  return NGX_DONE;
}

static void get_subscriber_info_id_callback(redisAsyncContext *c, void *r, void *privdata) {
  redis_subscriber_info_id_data_t *d = privdata;
  redisReply                    *reply = r;
  
  redis_node_t                 *node = c->data;
  
  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CHANNEL_GET_SUBSCRIBER_INFO_ID);
  
  callback_pt  cb = d->cb;
  void        *cb_pd = d->pd;
  
  ngx_free(d);
  
  if (!redisReplyOk(c, reply)) {
    cb(NGX_ERROR, NULL, cb_pd);
    return;
  }
  
  int64_t new_id = redisReply_to_int(reply, 0, 0);
  cb(NGX_OK, (void *)(uintptr_t )new_id, cb_pd);
}

static void redis_request_subscriber_info_callback(redisAsyncContext *c, void *r, void *privdata) {
  redis_node_t                 *node = c->data;
  node_command_received(node);
  node_command_time_finish(node, NCHAN_REDIS_CMD_CHANNEL_REQUEST_SUBSCRIBER_INFO);
  redisReplyOk(c, r);
}

static ngx_int_t nchan_store_request_subscriber_info(ngx_str_t *channel_id, ngx_int_t request_id, nchan_loc_conf_t *cf, callback_pt cb, void *pd) {
  if(nchan_channel_id_is_multi(channel_id)) {
    ERR("redis nchan_store_request_subscriber_info can't handle multi-channel ids");
    return NGX_ERROR;
  }
  
  redis_nodeset_t               *nodeset = nodeset_find(&cf->redis);
  if(!nodeset) {
    ERR("redis nodeset not found for nchan_store_request_subscriber_info");
    return NGX_ERROR;
  }
  
  if(!nodeset_ready(nodeset)) {
    ERR("redis nodeset not ready for nchan_store_request_subscriber_info");
    return NGX_ERROR;
  }
  
  redis_node_t *node = nodeset_node_find_by_channel_id(nodeset, channel_id);
  if(!node) {
    ERR("couldn't find Redis node for nchan_store_request_subscriber_info");
    return NGX_ERROR;
  }
  
  node_command_time_start(node, NCHAN_REDIS_CMD_CHANNEL_REQUEST_SUBSCRIBER_INFO);
  nchan_redis_script(request_subscriber_info, node, &redis_request_subscriber_info_callback, NULL, channel_id, "%i", (int )request_id);
  
  return NGX_DONE;
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
  NULL, //set group
  NULL, //delete group
  
  &nchan_store_get_subscriber_info_id, //+callback
  &nchan_store_request_subscriber_info //+callback
  
};

