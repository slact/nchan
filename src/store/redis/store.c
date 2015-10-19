#include <nchan_module.h>

#include <assert.h>
#include "store.h"
#include <store/ngx_rwlock.h>
#include "redis_nginx_adapter.h"
#include "redis_lua_commands.h"


typedef struct nchan_store_channel_head_s nchan_store_channel_head_t;


#include "../spool.h"

typedef enum {INACTIVE, NOTREADY, READY} chanhead_pubsub_status_t;

struct nchan_store_channel_head_s {
  ngx_str_t                    id; //channel id
  channel_spooler_t            spooler;
  ngx_uint_t                   generation; //subscriber pool generation.
  chanhead_pubsub_status_t     status;
  ngx_uint_t                   sub_count;
  ngx_uint_t                   internal_sub_count;
  nchan_msg_id_t               last_msgid;
  void                        *redis_subscriber_privdata;
  nchan_llist_timed_t          cleanlink;
  UT_hash_handle               hh;
};

#define CHANNEL_HASH_FIND(id_buf, p)    HASH_FIND( hh, subhash, (id_buf)->data, (id_buf)->len, p)
#define CHANNEL_HASH_ADD(chanhead)      HASH_ADD_KEYPTR( hh, subhash, (chanhead->id).data, (chanhead->id).len, chanhead)
#define CHANNEL_HASH_DEL(chanhead)      HASH_DEL( subhash, chanhead)

#undef uthash_malloc
#undef uthash_free
#define uthash_malloc(sz) ngx_alloc(sz, ngx_cycle->log)
#define uthash_free(ptr,sz) ngx_free(ptr)


#include <msgpack.h>

#define REDIS_HOSTNAME "127.0.0.1"
#define REDIS_PORT 8537

#define STR(buf) (buf)->data, (buf)->len
#define BUF(buf) (buf)->pos, ((buf)->last - (buf)->pos)

#define NCHAN_DEFAULT_SUBSCRIBER_POOL_SIZE (5 * 1024)
#define NCHAN_DEFAULT_CHANHEAD_CLEANUP_INTERVAL 1000
#define NCHAN_CHANHEAD_EXPIRE_SEC 1

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, __VA_ARGS__)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)

static nchan_store_channel_head_t *subhash = NULL;

//garbage collection for channel heads
static ngx_event_t         chanhead_cleanup_timer = {0};
static nchan_llist_timed_t *chanhead_cleanup_head = NULL;
static nchan_llist_timed_t *chanhead_cleanup_tail = NULL;

static ngx_int_t chanhead_gc_add(nchan_store_channel_head_t *head);
static ngx_int_t chanhead_gc_withdraw(nchan_store_channel_head_t *chanhead);

static void nchan_store_chanhead_cleanup_timer_handler(ngx_event_t *);
static ngx_int_t nchan_store_publish_generic(ngx_str_t *, nchan_msg_t *, ngx_int_t, const ngx_str_t *);
static ngx_str_t * nchan_store_content_type_from_message(nchan_msg_t *, ngx_pool_t *);
static ngx_str_t * nchan_store_etag_from_message(nchan_msg_t *, ngx_pool_t *);

static nchan_store_channel_head_t * nchan_store_get_chanhead(ngx_str_t *channel_id);
static ngx_int_t nchan_store_init_worker(ngx_cycle_t *cycle) {
  redis_nginx_init();
  
  chanhead_cleanup_timer.data=NULL;
  chanhead_cleanup_timer.handler=&nchan_store_chanhead_cleanup_timer_handler;
  chanhead_cleanup_timer.log=ngx_cycle->log;
  
  return NGX_OK;
}

static void redisCheckErrorCallback(redisAsyncContext *c, void *r, void *privdata) {
  static const ngx_str_t script_error_start= ngx_string("ERR Error running script (call to f_");
  redisReply *reply = (redisReply *)r;
  if(reply != NULL && reply->type == REDIS_REPLY_ERROR) {
    if(ngx_strncmp(reply->str, script_error_start.data, script_error_start.len) == 0 && reply->len > script_error_start.len + REDIS_LUA_HASH_LENGTH) {
      char *hash = &reply->str[script_error_start.len];
      char * (*hashes)[]=(char* (*)[])&store_rds_lua_hashes;
      char * (*names)[]=(char* (*)[])&store_rds_lua_script_names;
      int n = sizeof(store_rds_lua_hashes)/sizeof(char*);
      int i;
      for(i=0; i<n; i++) {
        if (ngx_strncmp((*hashes)[i], hash, REDIS_LUA_HASH_LENGTH)==0) {
          ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS SCRIPT ERROR: %s :%s", (*names)[i], &reply->str[script_error_start.len + REDIS_LUA_HASH_LENGTH + 2]);
          return;
        }
      }
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS SCRIPT ERROR: %s", reply->str);
    }
    else {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS_REPLY_ERROR: %s", reply->str);
    }
  }
}

static void redisEchoCallback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply *reply = r;
  ngx_int_t   i;
  //nchan_channel_t * channel = (nchan_channel_t *)privdata;
  if (reply == NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS REPLY is NULL");
    return;
  }
  switch(reply->type) {
    case REDIS_REPLY_STATUS:
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REDIS_REPLY_STATUS  %s", reply->str);
      break;
      
    case REDIS_REPLY_ERROR:
      redisCheckErrorCallback(c, r, privdata);
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
        redisEchoCallback(c, reply->element[i], "  ");
      }
      break;
  }
  //redisAsyncCommand(rds_sub_ctx(), NULL, NULL, "UNSUBSCRIBE channel:%b:pubsub", str(&(channel->id)));
}

static void redis_load_script_callback(redisAsyncContext *c, void *r, void *privdata) {
  char* (*hashes)[]=(char* (*)[])&store_rds_lua_hashes;
  //char* (*scripts)[]=(char* (*)[])&store_rds_lua_scripts;
  char* (*names)[]=(char* (*)[])&store_rds_lua_script_names;
  uintptr_t i=(uintptr_t) privdata;
  char *hash=(*hashes)[i];

  redisReply *reply = r;
  if (reply == NULL) return;
  switch(reply->type) {
    case REDIS_REPLY_ERROR:
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan: Failed loading redis lua scripts %s :%s", (*names)[i], reply->str);
      break;
    case REDIS_REPLY_STRING:
      if(ngx_strncmp(reply->str, hash, REDIS_LUA_HASH_LENGTH)!=0) {
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan Redis lua script %s has unexpected hash %s (expected %s)", (*names)[i], reply->str, hash);
      }
      break;
  }
}

static void redisInitScripts(redisAsyncContext *c){
  uintptr_t i;
  char* (*scripts)[]=(char* (*)[])&store_rds_lua_scripts;
  for(i=0; i<sizeof(store_rds_lua_scripts)/sizeof(char*); i++) {
    redisAsyncCommand(c, &redis_load_script_callback, (void *)i, "SCRIPT LOAD %s", (*scripts)[i]);
  }
}

static redisAsyncContext * rds_sub_ctx(void);

static redisAsyncContext * rds_ctx(void){
  static redisAsyncContext *c = NULL;
  if(c==NULL) {
    //init redis
    redis_nginx_open_context((const char *)REDIS_HOSTNAME, REDIS_PORT, 1, &c);
    redisInitScripts(c);
  }
  rds_sub_ctx();
  return c;
}

/*
static ngx_int_t msgpack_obj_to_int(msgpack_object *o) {
  switch(o->type) {
    case MSGPACK_OBJECT_POSITIVE_INTEGER:
      return (ngx_int_t) o->via.u64;
    case MSGPACK_OBJECT_NEGATIVE_INTEGER:
      return (ngx_int_t) o->via.i64;
    case MSGPACK_OBJECT_RAW:
      return ngx_atoi((u_char *)o->via.raw.ptr, o->via.raw.size);
    default:
      return 0;
  }
}
*/

static void * ngx_store_alloc(size_t size) {
  return ngx_alloc(size, ngx_cycle->log);
}
static nchan_msg_t * msg_from_redis_get_message_reply(redisReply *r, ngx_int_t offset, void *(*allocator)(size_t size));

#define CHECK_REPLY_STR(reply) ((reply)->type == REDIS_REPLY_STRING)
#define CHECK_REPLY_STRVAL(reply, v) ( CHECK_REPLY_STR(reply) && ngx_strcmp((reply)->str, v) == 0 )
#define CHECK_REPLY_STRNVAL(reply, v, n) ( CHECK_REPLY_STR(reply) && ngx_strncmp((reply)->str, v, n) == 0 )
#define CHECK_REPLY_INT(reply) ((reply)->type == REDIS_REPLY_INTEGER)
#define CHECK_REPLY_INTVAL(reply, v) ( CHECK_REPLY_INT(reply) && (reply)->integer == v )
#define CHECK_REPLY_ARRAY_MIN_SIZE(reply, size) ( (reply)->type == REDIS_REPLY_ARRAY && (reply)->elements >= size )
#define CHECK_REPLY_NIL(reply) ((reply)->type == REDIS_REPLY_NIL)
#define CHECK_REPLY_INT_OR_STR(reply) ((reply)->type == REDIS_REPLY_INTEGER || (reply)->type == REDIS_REPLY_STRING)

#define SLOW_REDIS_REPLY 100

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
} redis_get_message_from_key_data_t;

static void redis_subscriber_messageHMGET_callback(redisAsyncContext *c, void *r, void *privdata) {
  redis_get_message_from_key_data_t *d = (redis_get_message_from_key_data_t *)privdata;
  redisReply           *reply = r;
  nchan_msg_t          *msg;
  ngx_str_t            *chid = &d->channel_id;
  DBG("Message HMGET callback");
  log_redis_reply(d->name, d->t);
  
  if(chid == NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "channel_id is null after HMGET");
    return;
  }
  if((msg = msg_from_redis_get_message_reply(reply, 0, ngx_store_alloc)) == NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "invalid message or message absent after HMGET");
    return;
  }
  
  //TODO: Complete this.
  ngx_free(msg);
  ngx_free(d);
}

#define CHECK_MSGPACK_STRVAL(obj, val) ( obj.type == MSGPACK_OBJECT_RAW && ngx_strncmp(obj.via.raw.ptr, val, obj.via.raw.size) == 0 )
static ngx_int_t msgpack_to_uint(msgpack_object *obj, ngx_uint_t *ret) {
  switch(obj->type) {
    case MSGPACK_OBJECT_RAW:
      *ret = (ngx_uint_t ) ngx_atoi((u_char *) obj->via.raw.ptr, obj->via.raw.size);
      break;
    case MSGPACK_OBJECT_POSITIVE_INTEGER:
      *ret = obj->via.u64;
      break;
    case MSGPACK_OBJECT_NEGATIVE_INTEGER:
      *ret = (ngx_uint_t) obj->via.i64;
      break;
    default:
      return NGX_ERROR;
  }
  return NGX_OK;
}

static ngx_int_t msgpack_to_int(msgpack_object *obj, ngx_int_t *ret) {
  ngx_uint_t    preret = NGX_ERROR;
  ngx_int_t     retcode = NGX_ERROR;
  retcode = msgpack_to_uint(obj, &preret);
  *ret = (ngx_int_t) preret;
  return retcode;
}

static ngx_int_t msgpack_to_time(msgpack_object *obj, time_t *ret) {
  switch(obj->type) {
    case MSGPACK_OBJECT_RAW:
      *ret = ngx_atotm((u_char *) obj->via.raw.ptr, obj->via.raw.size);
      break;
    case MSGPACK_OBJECT_POSITIVE_INTEGER:
      *ret = (time_t) obj->via.u64;
      break;
    case MSGPACK_OBJECT_NEGATIVE_INTEGER:
      *ret = (time_t) obj->via.i64;
      break;
    default:
      return NGX_ERROR;
  }
  return NGX_OK;
}

static ngx_int_t msgpack_to_str(msgpack_object *obj, ngx_str_t *ret) {
  if(obj->type == MSGPACK_OBJECT_RAW) {
    ret->len=obj->via.raw.size;
    ret->data=(u_char *)obj->via.raw.ptr;
    return NGX_OK;
  }
  else {
    ret->len=0;
    ret->data=NULL;
    return NGX_ERROR;
  }
}
static ngx_int_t msgpack_array_to_msg(msgpack_object *arr, ngx_uint_t offset, nchan_msg_t *msg, ngx_buf_t *buf) {
  msgpack_to_time(&arr->via.array.ptr[offset], &(msg->message_time));
  msgpack_to_int(&arr->via.array.ptr[offset+1], &(msg->message_tag));
  
  if(arr->via.array.ptr[offset+2].type == MSGPACK_OBJECT_RAW) {
    msg->buf = buf;
    buf->start = buf->pos = (u_char *)arr->via.array.ptr[offset+2].via.raw.ptr;
    buf->end = buf->last = (u_char *)(buf->start + arr->via.array.ptr[offset+2].via.raw.size);
    buf->memory = 1;
    buf->last_buf = 1;
    buf->last_in_chain = 1;
  }
  
  if(arr->via.array.ptr[offset+3].type == MSGPACK_OBJECT_RAW) {
    msg->content_type.len=arr->via.array.ptr[offset+3].via.raw.size;
    msg->content_type.data=(u_char *)arr->via.array.ptr[offset+3].via.raw.ptr;
  }
  return NGX_OK;
}


static ngx_int_t get_msg_from_msgkey(ngx_str_t *channel_id, nchan_msg_id_t *msgid, ngx_str_t *msg_redis_hash_key) {
  nchan_store_channel_head_t               *head;
  redis_get_message_from_key_data_t *d;
  DBG("Get message from msgkey %V", msg_redis_hash_key);
  
  head = nchan_store_get_chanhead(channel_id);
  if(head->sub_count == 0) {
    DBG("Nobody wants this message we'll need to grab with an HMGET");
    return NGX_OK;
  }
  
  if((d=ngx_alloc(sizeof(*d) + (u_char)channel_id->len, ngx_cycle->log)) == 0) {
    ERR("nchan: unable to allocate memory for callback data for message hmget");
    return NGX_ERROR;
  }
  d->channel_id.len = channel_id->len;
  d->channel_id.data = (u_char *)&d[1];
  ngx_memcpy(d->channel_id.data, channel_id->data, channel_id->len);
  d->t = ngx_current_msec;
  d->name = "HMGET message";
  
  //d->hcln = put_current_subscribers_in_limbo(head);
  //assert(d->hcln != 0);
  
  redisAsyncCommand(rds_ctx(), &redis_subscriber_messageHMGET_callback, d, "HMGET %b time tag data content_type", STR(msg_redis_hash_key));
  return NGX_OK;
}

static ngx_int_t redis_subscriber_register(nchan_store_channel_head_t *chanhead, subscriber_t *sub);

static void redis_subscriber_callback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply             *reply = r;
  redisReply             *el = NULL;
  nchan_msg_t     msg;
  ngx_buf_t               buf = {0};

  ngx_str_t               chid = {0};

  ngx_str_t               msg_redis_hash_key = {0};
  ngx_uint_t              subscriber_id;
  msgpack_unpacked        msgunpack;
  
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "redis_subscriber_callback,  privdata=%p", privdata);
  
  
  nchan_store_channel_head_t *chanhead = (nchan_store_channel_head_t *)privdata;
  
  msg.expires=0;
  msg.refcount=0;
  msg.buf=NULL;

  if(reply == NULL) return;
  if(CHECK_REPLY_ARRAY_MIN_SIZE(reply, 3)
    && CHECK_REPLY_STRVAL(reply->element[0], "message")
    && CHECK_REPLY_STR(reply->element[1])
    && CHECK_REPLY_STR(reply->element[2])) {

    //reply->element[1] is the pubsub channel name
    el = reply->element[2];
    
    if(CHECK_REPLY_STR(el)) {
      //maybe a message?
      msgpack_unpacked_init(&msgunpack);
      if(msgpack_unpack_next(&msgunpack, (char *)el->str, el->len, NULL)) {
        msgpack_object  obj = msgunpack.data;
        ngx_uint_t          asize;

        if(obj.type == MSGPACK_OBJECT_ARRAY && obj.via.array.size != 0) {
          asize = obj.via.array.size;
          msgpack_object msgtype = obj.via.array.ptr[0];

          if(CHECK_MSGPACK_STRVAL(msgtype, "msg")) {
            if(chanhead != NULL) {
              msgpack_array_to_msg(&obj, 1, &msg, &buf);
              //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "got msg %i:%i", msg.message_time, msg.message_tag);
              nchan_store_publish_generic(&chanhead->id, &msg, 0, NULL);
            }
            else {
              ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan: thought there'd be a channel id around for msg");
            }
          }
          else if(CHECK_MSGPACK_STRVAL(msgtype, "ch+msg")) {
            msgpack_to_str(&obj.via.array.ptr[1], &chid);
            msgpack_array_to_msg(&obj, 2, &msg, &buf);
            nchan_store_publish_generic(&chid, &msg, 0, NULL);
          }
          else if(CHECK_MSGPACK_STRVAL(msgtype, "msgkey")) {
            if(chanhead != NULL) {
              nchan_msg_id_t        msgid;
              
              msgpack_to_time(&obj.via.array.ptr[1], &msgid.time);
              msgpack_to_int(&obj.via.array.ptr[2], &msgid.tag);
              
              msgpack_to_str(&obj.via.array.ptr[3], &msg_redis_hash_key);
              get_msg_from_msgkey(&chanhead->id, &msgid, &msg_redis_hash_key);
            }
            else {
              ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan: thought there'd be a channel id around for msgkey");
            }
          }
          else if(CHECK_MSGPACK_STRVAL(msgtype, "ch+msgkey")) {
            nchan_msg_id_t        msgid;
            
            msgpack_to_str( &obj.via.array.ptr[1], &chid);
            msgpack_to_time(&obj.via.array.ptr[2], &msgid.time);
            msgpack_to_int( &obj.via.array.ptr[3], &msgid.tag);
            msgpack_to_str( &obj.via.array.ptr[4], &msg_redis_hash_key);
            get_msg_from_msgkey(&chid, &msgid, &msg_redis_hash_key);
          }
          
          else if(CHECK_MSGPACK_STRVAL(msgtype, "alert") && asize > 1) {
            msgpack_object alerttype = obj.via.array.ptr[1];

            if(CHECK_MSGPACK_STRVAL(alerttype, "delete channel") && asize > 2) {
              if(msgpack_to_str(&obj.via.array.ptr[2], &chid) == NGX_OK) {
                nchan_store_publish_generic(&chid, NULL, NGX_HTTP_GONE, &NCHAN_HTTP_STATUS_410);
              }
              else {
                ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan: unexpected \"delete channel\" msgpack message from redis");
              }
            }

            else if(CHECK_MSGPACK_STRVAL(alerttype, "unsub one") && asize > 3) {
              msgpack_to_str(&obj.via.array.ptr[2], &chid);
              msgpack_to_uint(&obj.via.array.ptr[3], &subscriber_id);
              //TODO
            }

            else if(CHECK_MSGPACK_STRVAL(alerttype, "unsub all") && asize > 1) {
              msgpack_to_str(&obj.via.array.ptr[1], &chid);
              nchan_store_publish_generic(&chid, NULL, NGX_HTTP_CONFLICT, &NCHAN_HTTP_STATUS_409);
            }

            else if(CHECK_MSGPACK_STRVAL(alerttype, "unsub all except")) {
              msgpack_to_str(&obj.via.array.ptr[2], &chid);
              msgpack_to_uint(&obj.via.array.ptr[3], &subscriber_id);
              //TODO
            }

            else {
              ERR("nchan: unexpected msgpack alert from redis: %s", (char *)el->str);
            }
          }
          else {
            ERR("nchan: unexpected msgpack message from redis: %s", (char *)el->str);
          }

        }
        else {
          ERR("nchan: unexpected msgpack object from redis: %s", (char *)el->str);
        }
      }
      else {
        ERR("nchan: invalid msgpack message from redis: %s", (char *)el->str);
      }
      msgpack_unpacked_destroy(&msgunpack);
    }
    else { //not a string
      redisEchoCallback(c, el, NULL);
    }
  }

  else if(CHECK_REPLY_ARRAY_MIN_SIZE(reply, 3)
    && CHECK_REPLY_STRVAL(reply->element[0], "subscribe")
    && CHECK_REPLY_STR(reply->element[1])
    && CHECK_REPLY_INT(reply->element[2])) {

    if(chanhead != NULL){
      switch(chanhead->status) {
        case NOTREADY:
          chanhead->status = READY;
          
          //TODO: register all subscribers
          
          //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "REDIS: PUB/SUB subscribed to %s, chanhead %p now READY.", reply->element[1]->str, chanhead);
          break;
        case READY:
          ERR("REDIS: PUB/SUB already subscribed to %s, chanhead %p (id %V) already READY.", reply->element[1]->str, chanhead, &chanhead->id);
          break;
        case INACTIVE:
          ERR("REDIS: PUB/SUB already unsubscribed from %s, chanhead %p (id %V) INACTIVE.", reply->element[1]->str, chanhead, &chanhead->id);
          break;
      }
    }
    
    DBG("REDIS: PUB/SUB subscribed to %s (%i total)", reply->element[1]->str, reply->element[2]->integer);
  }
  else if(CHECK_REPLY_ARRAY_MIN_SIZE(reply, 3)
    && CHECK_REPLY_STRVAL(reply->element[0], "unsubscribe")
    && CHECK_REPLY_STR(reply->element[1])
    && CHECK_REPLY_INT(reply->element[2])) {

    DBG("REDIS: PUB/SUB unsubscribed from %s (%i total)", reply->element[1]->str, reply->element[2]->integer);
  }
  
  else {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "no message, something else");
    redisEchoCallback(c,r,privdata);
  }
}

static u_char subscriber_id[255] = "";
static u_char subscriber_channel[255] = "";
static redisAsyncContext * rds_sub_ctx(void){
  static redisAsyncContext *c = NULL;
  if(subscriber_id[0] == 0) {
    ngx_snprintf(subscriber_id, 255, "worker:%i:time:%i", ngx_pid, ngx_time());
    ngx_snprintf(subscriber_channel, 255, "nchan:%s", subscriber_id);
  }
  if(c==NULL) {
    //init redis
    redis_nginx_open_context((const char *)"localhost", 8537, 1, &c);
    redisAsyncCommand(c, redis_subscriber_callback, NULL, "SUBSCRIBE %s", subscriber_channel);
  }
  return c;
}


static ngx_int_t redis_subscriber_register(nchan_store_channel_head_t *chanhead, subscriber_t *sub);
static ngx_int_t redis_subscriber_unregister(ngx_str_t *channel_id, subscriber_t *sub);
static void spooler_add_handler(channel_spooler_t *spl, subscriber_t *sub, void *privdata) {
  nchan_store_channel_head_t *head = (nchan_store_channel_head_t *)privdata;
  head->sub_count++;
  if(sub->type == INTERNAL) {
    head->internal_sub_count++;
  }
  redis_subscriber_register(head, sub);
}

static void spooler_dequeue_handler(channel_spooler_t *spl, subscriber_t *sub, void *privdata) {
  //need individual subscriber
  //TODO
  nchan_store_channel_head_t *head = (nchan_store_channel_head_t *)privdata;
  
  head->sub_count--;
  if(sub->type == INTERNAL) {
    head->internal_sub_count--;
  }
  
  redis_subscriber_unregister(head, sub);
}

static ngx_int_t start_chanhead_spooler(nchan_store_channel_head_t *head) {
  start_spooler(&head->spooler);
  head->spooler.set_add_handler(&head->spooler, spooler_add_handler, head);
  head->spooler.set_dequeue_handler(&head->spooler, spooler_dequeue_handler, head);
  return NGX_OK;
}

static void redis_subscriber_register_callback(redisAsyncContext *c, void *vr, void *privdata);

typedef struct {
  nchan_store_channel_head_t *chanhead;
  ngx_int_t            generation;
  subscriber_t        *sub;
} redis_subscriber_register_t;

static ngx_int_t redis_subscriber_register(nchan_store_channel_head_t *chanhead, subscriber_t *sub) {
  char                      *concurrency = NULL;
  redis_subscriber_register_t *sdata=NULL;
  
  
  switch (sub->cf->subscriber_concurrency) {
    case NCHAN_SUBSCRIBER_CONCURRENCY_BROADCAST:
      concurrency = "broadcast";
      break;
    case NCHAN_SUBSCRIBER_CONCURRENCY_LASTIN:
      concurrency = "FIFO";
      break;
    case NCHAN_SUBSCRIBER_CONCURRENCY_FIRSTIN:
      concurrency = "FILO";
      break;
    default:
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "unknown concurrency setting");
  }
  
  //input: keys: [], values: [channel_id, subscriber_id, channel_empty_ttl, active_ttl, concurrency]
  //  'subscriber_id' can be '-' for new id, or an existing id
  //  'active_ttl' is channel ttl with non-zero subscribers. -1 to persist, >0 ttl in sec
  //  'concurrency' can be 'FIFO', 'FILO', or 'broadcast'
  //output: subscriber_id, num_current_subscribers
  
  if((sdata = ngx_alloc(sizeof(*sdata), ngx_cycle->log)) == NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "No memory for sdata. Part IV of the Cryptic Error Series.");
    return NGX_ERROR;
  }
  sdata->chanhead = chanhead;
  sdata->generation = chanhead->generation;
  sdata->sub = sub;

  if (0 != 0) { //TODO: check the subscriber's id
    
    redisAsyncCommand(rds_ctx(), &redis_subscriber_register_callback, sdata, "EVALSHA %s 0 %b %i %i %s", store_rds_lua_hashes.subscriber_register, STR(&chanhead->id), 0 /*TODO: current sub's ID*/, -1, concurrency);
  }
  else {
    redisAsyncCommand(rds_ctx(), &redis_subscriber_register_callback, sdata, "EVALSHA %s 0 %b - %i %s", store_rds_lua_hashes.subscriber_register, STR(&chanhead->id), -1, concurrency);
  }
  return NGX_OK;
}

static void redis_subscriber_register_callback(redisAsyncContext *c, void *vr, void *privdata) {
  redis_subscriber_register_t *sdata= (redis_subscriber_register_t *) privdata;
  redisReply                *reply = (redisReply *)vr;
  
  if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
    redisEchoCallback(c,reply,privdata);
    return;
  }
  
  if ( !CHECK_REPLY_ARRAY_MIN_SIZE(reply, 2) || !CHECK_REPLY_INT(reply->element[1]) ) {
    //no good
    redisEchoCallback(c,reply,privdata);
    return;
  }
  if(sdata->generation == sdata->chanhead->generation) {
    //is the subscriber     
    //TODO: set subscriber id
    //sdata->sub->id = reply->element[1]->integer;
  }
  ngx_free(sdata);
}


static ngx_int_t redis_subscriber_unregister(ngx_str_t *channel_id, subscriber_t *sub) {
  nchan_loc_conf_t  *cf = sub->cf;
  //input: keys: [], values: [channel_id, subscriber_id, empty_ttl]
  // 'subscriber_id' is an existing id
  // 'empty_ttl' is channel ttl when without subscribers. 0 to delete immediately, -1 to persist, >0 ttl in sec
  //output: subscriber_id, num_current_subscribers
  redisAsyncCommand(rds_ctx(), &redisCheckErrorCallback, NULL, "EVALSHA %s 0 %b %i %i", store_rds_lua_hashes.subscriber_unregister, STR(channel_id), 0/*TODO: sub->id*/, cf->channel_timeout);
  return NGX_OK;
}

static nchan_store_channel_head_t *chanhead_redis_create(ngx_str_t *channel_id) {
  nchan_store_channel_head_t *head;
  
  head=ngx_calloc(sizeof(*head) + sizeof(u_char)*(channel_id->len), ngx_cycle->log);
  if(head==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "can't allocate memory for (new) channel subscriber head");
    return NULL;
  }
  head->id.len = channel_id->len;
  head->id.data = (u_char *)&head[1];
  ngx_memcpy(head->id.data, channel_id->data, channel_id->len);
  head->sub_count=0;
  head->redis_subscriber_privdata = NULL;
  head->status = NOTREADY;
  head->generation = 0;
  head->last_msgid.time=0;
  head->last_msgid.tag=0;
  
  head->spooler.running=0;
  start_chanhead_spooler(head);

  DBG("SUBSCRIBING to channel:pubsub:%V", channel_id);
  redisAsyncCommand(rds_sub_ctx(), redis_subscriber_callback, head, "SUBSCRIBE channel:pubsub:%b", STR(channel_id));
  CHANNEL_HASH_ADD(head);
  
  return head;
}

static nchan_store_channel_head_t * nchan_store_get_chanhead(ngx_str_t *channel_id) {
  nchan_store_channel_head_t     *head;
  
  CHANNEL_HASH_FIND(channel_id, head);
  if(head==NULL) {
    head = chanhead_redis_create(channel_id);
  }
  if(head == NULL) {
    ERR("can't create chanhead for redis store");
    return NULL;
  }
  
  if (head->status == INACTIVE) { //recycled chanhead
    chanhead_gc_withdraw(head);
    head->status = READY;
  }

  if(!head->spooler.running) {
    DBG("Spooler for channel %p %V wasn't running. start it.", head, &head->id);
    start_chanhead_spooler(head);
  }
  
  return head;
}

static ngx_int_t redis_subscriber_remove(subscriber_t *sub) {
  //remove subscriber from list
 //TODO: maybe?..
  return NGX_OK;
}

static ngx_int_t chanhead_gc_add(nchan_store_channel_head_t *head) {
  nchan_llist_timed_t         *chanhead_cleanlink;
  
  if(head->status != INACTIVE) {
    chanhead_cleanlink = &head->cleanlink;
    
    chanhead_cleanlink->data=(void *)head;
    chanhead_cleanlink->time=ngx_time();
    chanhead_cleanlink->prev=chanhead_cleanup_tail;
    if(chanhead_cleanup_tail != NULL) {
      chanhead_cleanup_tail->next=chanhead_cleanlink;
    }
    chanhead_cleanlink->next=NULL;
    chanhead_cleanup_tail=chanhead_cleanlink;
    if(chanhead_cleanup_head==NULL) {
      chanhead_cleanup_head = chanhead_cleanlink;
    }
    
    head->status = INACTIVE;
    
    DBG("gc_add chanhead %V", &head->id);
  }
  else {
    ERR("gc_add chanhead %V: already added", &head->id);
  }

  //initialize cleanup timer
  if(!chanhead_cleanup_timer.timer_set) {
    ngx_add_timer(&chanhead_cleanup_timer, NCHAN_DEFAULT_CHANHEAD_CLEANUP_INTERVAL);
  }
  return NGX_OK;
}

static ngx_int_t chanhead_gc_withdraw(nchan_store_channel_head_t *chanhead) {
  //remove from cleanup list if we're there
  nchan_llist_timed_t    *cl;
  DBG("gc_withdraw chanhead %V", &chanhead->id);
  if(chanhead->status == INACTIVE) {
    cl=&chanhead->cleanlink;
    if(cl->prev!=NULL)
      cl->prev->next=cl->next;
    if(cl->next!=NULL)
      cl->next->prev=cl->prev;
    if(chanhead_cleanup_head==cl)
      chanhead_cleanup_head=cl->next;
    if(chanhead_cleanup_tail==cl)
      chanhead_cleanup_tail=cl->prev;

    cl->prev = cl->next = NULL;
  }
  else {
    DBG("gc_withdraw chanhead %p (%V), but already inactive", chanhead, &chanhead->id);
  }
  return NGX_OK;
}

static void *put_current_subscribers_in_limbo(nchan_store_channel_head_t *head) {
  if(head==NULL) {
    ERR(" No head given. not putting anyone in limbo");
    return NULL;
  }
  
  if (head->sub_count == 0) { //no one is listening, no need to publish
    ERR(" No subscribers. not putting anyone in limbo");
    return NULL;
  }
  
  //TODO: set up detached (?) spooler?
  
  return NULL;
}

static void empty_callback(){}

static ngx_int_t publish_to_subscribers_in_limbo(nchan_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line) {
  /*
  if(msg) {
    head->last_msgid.time = msg->message_time;
    head->last_msgid.tag = msg->message_tag;
  }
  
  if(hcln->first_sub == NULL || hcln->sub_count == 0) {
    ERR("No one to publish to (in limbo)...");
    return NGX_OK; //nothing to do
  }
  
  chanhead_gc_add(head);
  for(sub = hcln->first_sub ; sub!=NULL; sub=next) {
    subscriber_t         *rsub = sub->subscriber;

    rsub->set_dequeue_callback(rsub, (subscriber_callback_pt )subscriber_publishing_cleanup_callback, 
    &sub->clndata);
    rsub->set_timeout_callback(rsub, (subscriber_callback_pt )empty_callback, NULL);
    
    next = sub->next; //becase the cleanup callback will dequeue this subscriber
    
    if(sub->clndata.shared != hcln) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "wrong shared cleanup for subscriber %p: should be %p, is %p", sub, hcln, sub->clndata.shared);
    }
    
    if(msg!=NULL) {
      rsub->respond_message(rsub, msg);
    }
    else {
      rsub->respond_status(rsub, status_code, status_line);
    }
  }
  */
  //head->generation++;
  return NGX_OK;
}

static ngx_int_t nchan_store_publish_generic(ngx_str_t *channel_id, nchan_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line){
  nchan_store_channel_head_t        *head;
  ngx_int_t                   ret;
  //redis_channel_head_cleanup_t *hcln;
  
  head = nchan_store_get_chanhead(channel_id);
  
  if(head->sub_count > 0) {
    if(msg) {
      head->last_msgid.time = msg->message_time;
      head->last_msgid.tag = msg->message_tag;
      head->spooler.respond_message(&head->spooler, msg);
    }
    else {
      head->spooler.respond_status(&head->spooler, status_code, status_line);
    }
    ret= NGX_OK;
  }
  else {
    ret= NCHAN_MESSAGE_QUEUED;
  }
  return ret;
}

static void handle_chanhead_gc_queue(ngx_int_t force_delete) {
  nchan_llist_timed_t    *cur, *next;
  nchan_store_channel_head_t   *ch = NULL;
  
  DBG("handle_chanhead_gc_queue");
  
  for(cur=chanhead_cleanup_head; cur != NULL; cur=next) {
    next=cur->next;
    if(force_delete || ngx_time() - cur->time > NCHAN_CHANHEAD_EXPIRE_SEC) {
      ch = (nchan_store_channel_head_t *)cur->data;
      
      if (ch->sub_count == 0) { //still no subscribers here

        //unsubscribe now
        DBG("UNSUBSCRIBING from channel:pubsub:%V", &ch->id);
        redisAsyncCommand(rds_sub_ctx(), NULL, NULL, "UNSUBSCRIBE channel:pubsub:%b", STR(&ch->id));
        DBG("chanhead %p (%V) is empty and expired. delete.", ch, &ch->id);
        CHANNEL_HASH_DEL(ch);
        ngx_free(ch);
      }
      else {
        ERR("chanhead %p (%V) is still in use.", ch, &ch->id);
      }
    }
    else {
      break;
    }
  }
  chanhead_cleanup_head=cur;
  if (cur==NULL) { //we went all the way to the end
    chanhead_cleanup_tail=NULL;
  }
  else {
    cur->prev=NULL;
  }
}

static void nchan_store_chanhead_cleanup_timer_handler(ngx_event_t *ev) {
  handle_chanhead_gc_queue(0);
  if (!(ngx_quit || ngx_terminate || ngx_exiting || chanhead_cleanup_head==NULL)) {
    ngx_add_timer(ev, NCHAN_DEFAULT_CHANHEAD_CLEANUP_INTERVAL);
  }
  else if(chanhead_cleanup_head==NULL) {
    DBG("chanhead gc queue looks empty, stop gc_queue handler");
  }
}

static ngx_int_t redis_array_to_channel(redisReply *r, nchan_channel_t *ch) {
  if ( CHECK_REPLY_ARRAY_MIN_SIZE(r, 4)
    && CHECK_REPLY_INT(r->element[0])
    && CHECK_REPLY_INT(r->element[1])
    && CHECK_REPLY_INT(r->element[2]) ) {
    
    //channel info
    ch->expires = ngx_time() + r->element[0]->integer;
    ch->last_seen = r->element[1]->integer;
    ch->subscribers = r->element[2]->integer;
    ch->messages = r->element[3]->integer;
    
    //no id?..
    ch->id.len=0;
    ch->id.data=NULL;
  
    //legacy
    ch->message_queue = NULL;
    
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

static void redisChannelInfoCallback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply *reply=r;
  redis_channel_callback_data_t *d=(redis_channel_callback_data_t *)privdata;
  nchan_channel_t channel = {{0}};
  
  log_redis_reply(d->name, d->t);
  
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
  }

  ngx_free(d);
}

static ngx_int_t nchan_store_delete_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  redis_channel_callback_data_t *d;
  if((d=ngx_alloc(sizeof(*d), ngx_cycle->log))==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "Failed to allocate memory for some callback data");
    return NGX_ERROR;
  }
  d->t = ngx_current_msec;
  d->name = "delete";
  d->channel_id = channel_id;
  d->callback = callback;
  d->privdata = privdata;
  
  redisAsyncCommand(rds_ctx(), &redisChannelInfoCallback, d, "EVALSHA %s 0 %b", store_rds_lua_hashes.delete, STR(channel_id));

  return NGX_OK;
}



static ngx_int_t nchan_store_find_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  redis_channel_callback_data_t *d;
  if((d=ngx_alloc(sizeof(*d), ngx_cycle->log))==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "Failed to allocate memory for some callback data");
    return NGX_ERROR;
  }
  d->t = ngx_current_msec;
  d->name = "find_channel";
  d->channel_id = channel_id;
  d->callback = callback;
  d->privdata = privdata;
  
  redisAsyncCommand(rds_ctx(), &redisChannelInfoCallback, d, "EVALSHA %s 0 %b", store_rds_lua_hashes.find_channel, STR(channel_id));
  
  return NGX_OK;
}





static nchan_msg_t * msg_from_redis_get_message_reply(redisReply *r, ngx_int_t offset, void *(*allocator)(size_t size)) {
  nchan_msg_t *msg=NULL;
  ngx_buf_t           *buf=NULL;
  redisReply         **els = r->element;
  size_t len = 0, content_type_len = 0;
  
  if(CHECK_REPLY_ARRAY_MIN_SIZE(r, offset + 4)
   && CHECK_REPLY_INT_OR_STR(els[offset])     //id - time
   && CHECK_REPLY_INT_OR_STR(els[offset+1])   //id - tag
   && CHECK_REPLY_STR(els[offset+2])   //message
   && CHECK_REPLY_STR(els[offset+3])){ //content-type
    len=els[offset+2]->len;
    content_type_len=els[offset+3]->len;
    if((msg=allocator(sizeof(*msg) + sizeof(ngx_buf_t) + len + content_type_len))==NULL) {
      ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "nchan: can't allocate memory for message from redis reply");
      return NULL;
    }
    ngx_memzero(msg, sizeof(*msg)+sizeof(ngx_buf_t));
    //set up message buffer;
    msg->buf = (void *)(&msg[1]);
    buf = msg->buf;
    buf->start = buf->pos = (void *)(&buf[1]);
    buf->end = buf->last = &buf->start[len];
    ngx_memcpy(buf->start, els[offset+2]->str, len);
    buf->memory = 1;
    buf->last_buf = 1;
    buf->last_in_chain = 1;
    
    if(content_type_len>0) {
      msg->content_type.len=content_type_len;
      msg->content_type.data=buf->end;
      ngx_memcpy(msg->content_type.data, els[offset+3]->str, content_type_len);
    }
    
    redisReply_to_int(els[offset+0], &msg->message_time);
    redisReply_to_int(els[offset+1], &msg->message_tag);
    return msg;
  }
  else {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "nchan: invalid message redis reply");
    return NULL;
  }
}

typedef struct {
  ngx_msec_t              t;
  char                   *name;
  ngx_str_t              *channel_id;
  nchan_msg_id_t *msg_id;
  callback_pt             callback;
  void                  *privdata;
} redis_get_message_data_t;

static void redis_get_message_callback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply                *reply= r;
  redis_get_message_data_t  *d= (redis_get_message_data_t *)privdata;
  nchan_msg_t       *msg=NULL;
  
  if(d == NULL) {
    ERR("redis_get_mesage_callback has NULL userdata");
    return;
  }
  
  log_redis_reply(d->name, d->t);
  
  //output: result_code, msg_time, msg_tag, message, content_type,  channel-subscriber-count
  // result_code can be: 200 - ok, 403 - channel not found, 404 - not found, 410 - gone, 418 - not yet available
  
  if ( !CHECK_REPLY_ARRAY_MIN_SIZE(reply, 1) || !CHECK_REPLY_INT(reply->element[0]) ) {
    //no good
    ngx_free(d);
    return;
  }
  
  switch(reply->element[0]->integer) {
    case 200: //ok
      if((msg=msg_from_redis_get_message_reply(reply, 1, &ngx_store_alloc))) {
        d->callback(NCHAN_MESSAGE_FOUND, msg, d->privdata);
      }
      break;
    case 403: //channel not found
    case 404: //not found
      d->callback(NCHAN_MESSAGE_NOTFOUND, NULL, d->privdata);
      break;
    case 410: //gone
      d->callback(NCHAN_MESSAGE_EXPIRED, NULL, d->privdata);
      break;
    case 418: //not yet available
      d->callback(NCHAN_MESSAGE_EXPECTED, NULL, d->privdata);
      break;
  }
  
  ngx_free(d);
}

static ngx_int_t nchan_store_async_get_message(ngx_str_t *channel_id, nchan_msg_id_t *msg_id, callback_pt callback, void *privdata) {
  redis_get_message_data_t           *d=NULL;
  if(callback==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "no callback given for async get_message. someone's using the API wrong!");
    return NGX_ERROR;
  }
  if((d=ngx_alloc(sizeof(*d), ngx_cycle->log))==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "Failed to allocate memory for some callback data");
    return NGX_ERROR;
  }
  d->channel_id = channel_id;
  d->msg_id = msg_id;
  d->callback = callback;
  d->privdata = privdata;
  
  d->t = ngx_current_msec;
  d->name = "get_message";
  
  //input:  keys: [], values: [channel_id, msg_time, msg_tag, no_msgid_order, create_channel_ttl, subscriber_channel]
  //subscriber channel is not given, because we don't care to subscribe
  redisAsyncCommand(rds_ctx(), &redis_get_message_callback, (void *)d, "EVALSHA %s 0 %b %i %i %s", store_rds_lua_hashes.get_message, STR(channel_id), msg_id->time, msg_id->tag, "FILO", 0);
  return NGX_OK; //async only now!
}

//initialization
static ngx_int_t nchan_store_init_module(ngx_cycle_t *cycle) {
  ngx_core_conf_t                *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  nchan_worker_processes = ccf->worker_processes;
  //initialize our little IPC
  return NGX_OK;
}

static ngx_int_t nchan_store_init_postconfig(ngx_conf_t *cf) {
  //nothing to do but be OK.
  return NGX_OK;
}

static void nchan_store_create_main_conf(ngx_conf_t *cf, nchan_main_conf_t *mcf) {
  mcf->shm_size=NGX_CONF_UNSET_SIZE;
}

static void nchan_store_exit_worker(ngx_cycle_t *cycle) {
  nchan_store_channel_head_t *cur, *tmp;
  redisAsyncContext *ctx;

  handle_chanhead_gc_queue(1);
  
  if((ctx=rds_ctx())!=NULL)
    redis_nginx_force_close_context(&ctx);
  if((ctx=rds_sub_ctx())!=NULL)
    redis_nginx_force_close_context(&ctx);
  
  HASH_ITER(hh, subhash, cur, tmp) {
    //any subscribers?
    //TODO: respond to all subscribers
    HASH_DEL(subhash, cur);
    ngx_free(cur);
  }

  if(chanhead_cleanup_timer.timer_set) {
    ngx_del_timer(&chanhead_cleanup_timer);
  }
}

static void nchan_store_exit_master(ngx_cycle_t *cycle) {
  //destroy channel tree in shared memory
  //nchan_walk_rbtree(nchan_movezig_channel_locked, nchan_shm_zone);
  //deinitialize IPC
  
}

static void subscriber_cleanup_callback(subscriber_t *rsub, void *foo) {
  /*
  redis_subscriber_t           *sub = cln->sub;
  nchan_store_channel_head_t         *head = shared->head;
  
  DBG("subscriber_cleanup_callback for %p on %V", sub, &head->id);
  
  ngx_int_t done;
  done = sub->prev==NULL && sub->next==NULL;
  
  redis_subscriber_remove(sub);
  sub->subscriber->dequeue(sub->subscriber);

  head->sub_count--;
  
  if(done) {
    //add chanhead to gc list
    head->sub=NULL;
    chanhead_gc_add(head);
  }
  */
}

static void redis_subscriber_timeout_handler(subscriber_t *rsub, void *foo) {
  //redis_subscriber_t         *sub = cln->sub;
  //ngx_pfree(cln->shared->pool, sub); //do we even want this?
}

static ngx_int_t redis_subscriber_create(nchan_store_channel_head_t *chanhead, subscriber_t *sub) {
  //this is the new shit
  void *foo = NULL;
  
  //TODO
  
  sub->set_dequeue_callback(sub, (subscriber_callback_pt )subscriber_cleanup_callback, &foo);
  
  sub->set_timeout_callback(sub, (subscriber_callback_pt )redis_subscriber_timeout_handler, &foo);
  
  return NGX_OK;
}

typedef struct {
  ngx_msec_t              t;
  char                   *name;
  ngx_str_t              *channel_id;
  nchan_msg_id_t *msg_id;
  callback_pt             callback;
  subscriber_t           *sub;
  nchan_store_channel_head_t    *chanhead;
  void                   *privdata;
} redis_subscribe_data_t;

static void redis_getmessage_callback(redisAsyncContext *c, void *vr, void *privdata) {
  redis_subscribe_data_t    *d = (redis_subscribe_data_t *) privdata;
  redisReply                *reply = (redisReply *)vr;
  subscriber_t              *sub = d->sub;
  nchan_loc_conf_t  *cf = sub->cf;
  ngx_int_t                  status=0;
  nchan_msg_t       *msg=NULL;
  
  sub->release(sub); //let the sub be destroyed if needed.
  
  //output: result_code, msg_time, msg_tag, message, content_type, channel-subscriber-count
  // result_code can be: 200 - ok, 403 - channel not found, 404 - not found, 410 - gone, 418 - not yet available
  DBG("redis getmessage callback for %s", d->name);
  log_redis_reply(d->name, d->t);
  
  if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
    redisEchoCallback(c,reply,privdata);
    ngx_free(d);
    return;
  }
  
  if ( !CHECK_REPLY_ARRAY_MIN_SIZE(reply, 1) || !CHECK_REPLY_INT(reply->element[0]) ) {
    //no good
    redisEchoCallback(c,reply,privdata);
    ERR("Invalid redis getmessage return data");
    ngx_free(d);
    return;
  }
  
  status = reply->element[0]->integer;
  
  switch(status) {
    ngx_int_t                   ret;
    nchan_store_channel_head_t        *chanhead=NULL;
    
    case 200: //ok
      msg = msg_from_redis_get_message_reply(reply, 1, &ngx_store_alloc);
      if(msg == NULL) {
        ERR("expected message, got NULL");
        ret = sub->respond_status(sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL);
        d->callback(ret, msg, privdata);
      }
      switch(cf->subscriber_concurrency) {
        case NCHAN_SUBSCRIBER_CONCURRENCY_LASTIN:
          //kick everyone elese out, then subscribe
          //TODO: profiling
          redisAsyncCommand(rds_ctx(), &redisEchoCallback, NULL, "EVALSHA %s 0 %b %i", store_rds_lua_hashes.publish_status, STR(d->channel_id), 409);
          //FALL-THROUGH to BROADCAST

        case NCHAN_SUBSCRIBER_CONCURRENCY_BROADCAST:
          DBG("message found; respond to subscriber");
          ret = sub->respond_message(sub, msg);
          d->callback(ret, msg, d->privdata);
          break;

        case NCHAN_SUBSCRIBER_CONCURRENCY_FIRSTIN:
          if(CHECK_REPLY_ARRAY_MIN_SIZE(reply, 6) && CHECK_REPLY_INT(reply->element[5]) && reply->element[5]->integer > 0 ) {
            ret = sub->respond_status(sub, NGX_HTTP_NOT_FOUND, &NCHAN_HTTP_STATUS_409);
            d->callback(ret, msg, d->privdata);
          }
          break;

        default:
          ERR("unexpected subscriber_concurrency config value");
      }
      break;
    case 418: //not yet available
    case 403: //channel not found (not authorized)
      // ♫ It's gonna be the future soon ♫
      
      if((chanhead = nchan_store_get_chanhead(d->channel_id))== NULL) {
        d->callback(NGX_ERROR, NULL, d->privdata);
      }
      else {
        ret = chanhead->spooler.add(&chanhead->spooler, sub);
        d->callback(ret == NGX_OK ? NGX_DONE : NGX_ERROR, NULL, d->privdata);
      }
      break;
    
    case 404: //not found
      sub->respond_status(sub, NGX_HTTP_NOT_FOUND, NULL);
      d->callback(NGX_HTTP_NOT_FOUND, NULL, d->privdata);
      break;
    case 410: //gone
      //subscriber wants an expired message
      sub->respond_status(sub, NGX_HTTP_NO_CONTENT, NULL);
      d->callback(NGX_HTTP_NO_CONTENT, NULL, d->privdata);
      break;
    default: //shouldn't be here!
      sub->respond_status(sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL);
      d->callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, d->privdata);
  }
  if(msg != NULL) {
    ngx_free(msg);
  }
  ngx_free(d);
}

static ngx_int_t nchan_store_subscribe(ngx_str_t *channel_id, nchan_msg_id_t *msg_id, subscriber_t *sub, callback_pt callback, void *privdata) {
  redis_subscribe_data_t       *d = NULL;
  ngx_int_t                     create_channel_ttl;
  nchan_loc_conf_t     *cf = sub->cf;
  assert(callback != NULL);
  
  if((d=ngx_calloc(sizeof(*d) + sizeof(ngx_str_t) + channel_id->len, ngx_cycle->log))==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "can't allocate redis get_message callback data");
    return callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, privdata);
  }
  
  d->channel_id=(ngx_str_t *)&d[1];
  d->channel_id->len = channel_id->len;
  d->channel_id->data = (u_char *)&(d->channel_id)[1];
  ngx_memcpy(d->channel_id->data, channel_id->data, channel_id->len);
  
  d->msg_id=msg_id;
  d->callback=callback;
  d->privdata=privdata;

  d->t = ngx_current_msec;
  d->name = "get_message (subscribe)";
  
  d->sub = sub;
  sub->reserve(sub);
  
  create_channel_ttl = cf->authorize_channel==1 ? 0 : cf->channel_timeout;
  
  //input:  keys: [], values: [channel_id, msg_time, msg_tag, no_msgid_order, create_channel_ttl]
  redisAsyncCommand(rds_ctx(), &redis_getmessage_callback, (void *)d, "EVALSHA %s 0 %b %i %i %s %i", store_rds_lua_hashes.get_message, STR(channel_id), msg_id->time, msg_id->tag, "FILO", create_channel_ttl);
  return NGX_OK; 
}

static ngx_str_t * nchan_store_etag_from_message(nchan_msg_t *msg, ngx_pool_t *pool){
  ngx_str_t *etag;
  if(pool!=NULL && (etag = ngx_palloc(pool, sizeof(*etag) + NGX_INT_T_LEN))==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan: unable to allocate memory for Etag header in pool");
    return NULL;
  }
  else if(pool==NULL && (etag = ngx_alloc(sizeof(*etag) + NGX_INT_T_LEN, ngx_cycle->log))==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan: unable to allocate memory for Etag header");
    return NULL;
  }
  etag->data = (u_char *)(etag+1);
  etag->len = ngx_sprintf(etag->data,"%ui", msg->message_tag)- etag->data;
  return etag;
}

static ngx_str_t * nchan_store_content_type_from_message(nchan_msg_t *msg, ngx_pool_t *pool){
  ngx_str_t *content_type;
  if(pool != NULL && (content_type = ngx_palloc(pool, sizeof(*content_type) + msg->content_type.len))==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan: unable to allocate memory for Content Type header in pool");
    return NULL;
  }
  else if(pool == NULL && (content_type = ngx_alloc(sizeof(*content_type) + msg->content_type.len, ngx_cycle->log))==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan: unable to allocate memory for Content Type header");
    return NULL;
  }
  content_type->data = (u_char *)(content_type+1);
  content_type->len = msg->content_type.len;
  ngx_memcpy(content_type->data, msg->content_type.data, content_type->len);
  return content_type;
}

typedef struct {
  ngx_msec_t            t;
  char                 *name;
  ngx_str_t            *channel_id;
  time_t                msg_time;
  nchan_msg_t  *msg;
  ngx_int_t             msglen;
  callback_pt           callback;
  void                 *privdata;
} redis_publish_callback_data_t;

static void redisPublishCallback(redisAsyncContext *, void *, void *);

static ngx_int_t nchan_store_publish_message(ngx_str_t *channel_id, nchan_msg_t *msg, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  redis_publish_callback_data_t  *d=NULL;
  u_char                         *msgstart;
  size_t                          msglen;
  ngx_int_t                       mmapped=0;
  ngx_buf_t                      *buf;
  
  assert(callback != NULL);

  if((d=ngx_calloc(sizeof(*d), ngx_cycle->log))==NULL) { //todo: allocate in request pool?...
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "can't allocate redis publish callback data");
    return callback(NGX_ERROR, NULL, privdata);
  }

  if(msg->message_time==0) {
    msg->message_time = ngx_time();
  }
  
  d->channel_id=channel_id;
  d->callback=callback;
  d->privdata=privdata;
  d->msg_time=msg->message_time;
  

  //nchan_store_publish_generic(channel_id, msg, 0, NULL);
  
  //input:  keys: [], values: [channel_id, time, message, content_type, msg_ttl, max_messages]
  //output: message_tag, channel_hash
  buf = msg->buf;
  if(ngx_buf_in_memory(buf)) {
    msgstart = buf->pos;
    msglen = buf->last - msgstart;
  }
  else { //in a file
    msglen = buf->file_last - buf->file_pos;
    msgstart = mmap(NULL, msglen, PROT_READ, MAP_SHARED, buf->file->fd, 0);
    if (msgstart != MAP_FAILED) {
      mmapped = 1;
    }
    else {
      msgstart = NULL;
      msglen = 0;
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "Couldn't mmap file");
    }
  }
  d->msglen = msglen;
  
  d->t = ngx_current_msec;
  d->name = "publish";
  
  redisAsyncCommand(rds_ctx(), &redisPublishCallback, (void *)d, "EVALSHA %s 0 %b %i %b %b %i %i", store_rds_lua_hashes.publish, STR(channel_id), msg->message_time, msgstart, msglen, STR(&(msg->content_type)), cf->buffer_timeout, cf->max_messages);
  if(mmapped && munmap(msgstart, msglen) == -1) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "munmap was a problem");
  }
  return NGX_OK;
}

static void redisPublishCallback(redisAsyncContext *c, void *r, void *privdata) {
  redis_publish_callback_data_t *d=(redis_publish_callback_data_t *)privdata;
  redisReply *reply=r;
  redisReply *cur;
  //nchan_msg_id_t msg_id;
  nchan_channel_t ch={{0}};

  if(CHECK_REPLY_ARRAY_MIN_SIZE(reply, 2)) {
    //msg_id.time=d->msg_time;
    //msg_id.tag=reply->element[0]->integer;

    cur=reply->element[1];
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

nchan_store_t  nchan_store_redis = {
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
    
    //message stuff
    &nchan_store_etag_from_message,
    &nchan_store_content_type_from_message,
    
};
