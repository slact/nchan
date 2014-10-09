#include <ngx_http_push_module.h>

#include <assert.h>
#include "store.h"
#include <store/ngx_rwlock.h>
#include <store/ngx_http_push_module_ipc.h>
#include "redis_nginx_adapter.h"
#include "redis_lua_commands.h"

#include <msgpack.h>

#define STR(buf) (buf)->data, (buf)->len
#define BUF(buf) (buf)->pos, ((buf)->last - (buf)->pos)
   
#define ENQUEUED_DBG "msg %p enqueued.  ref:%i, p:%p n:%p"
#define CREATED_DBG  "msg %p created    ref:%i, p:%p n:%p"
#define FREED_DBG    "msg %p freed.     ref:%i, p:%p n:%p"
#define RESERVED_DBG "msg %p reserved.  ref:%i, p:%p n:%p"
#define RELEASED_DBG "msg %p released.  ref:%i, p:%p n:%p"

#define NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_POOL_SIZE (5 * 1024)
#define NGX_HTTP_PUSH_DEFAULT_CHANHEAD_CLEANUP_INTERVAL 1000
#define NGX_HTTP_PUSH_CHANHEAD_EXPIRE_SEC 1

//#define DEBUG_SHM_ALLOC 1

static nhpm_channel_head_t *subhash = NULL;

//garbage collection for channel heads
static ngx_event_t         chanhead_cleanup_timer = {0};
static nhpm_llist_timed_t *chanhead_cleanup_head = NULL;
static nhpm_llist_timed_t *chanhead_cleanup_tail = NULL;

static void ngx_http_push_store_chanhead_cleanup_timer_handler(ngx_event_t *);
static ngx_int_t ngx_http_push_store_publish_raw(ngx_str_t *, ngx_http_push_msg_t *, ngx_int_t, const ngx_str_t *);
static ngx_str_t * ngx_http_push_store_content_type_from_message(ngx_http_push_msg_t *, ngx_pool_t *);
static ngx_str_t * ngx_http_push_store_etag_from_message(ngx_http_push_msg_t *, ngx_pool_t *);

static ngx_int_t ngx_http_push_store_init_worker(ngx_cycle_t *cycle) {
  redis_nginx_init();
  
  chanhead_cleanup_timer.data=NULL;
  chanhead_cleanup_timer.handler=&ngx_http_push_store_chanhead_cleanup_timer_handler;
  chanhead_cleanup_timer.log=ngx_cycle->log;
  
  return NGX_OK;
}

static void redisCheckErrorCallback(redisAsyncContext *c, void *r, void *privdata) {
  static const ngx_str_t script_error_start= ngx_string("ERR Error running script (call to f_");
  redisReply *reply = (redisReply *)r;
  if(reply != NULL && reply->type == REDIS_REPLY_ERROR) {
    if(ngx_strncmp(reply->str, script_error_start.data, script_error_start.len) == 0 && reply->len > script_error_start.len + REDIS_LUA_HASH_LENGTH) {
      char *hash = &reply->str[script_error_start.len];
      char * (*hashes)[]=(char* (*)[])&nhpm_rds_lua_hashes;
      char * (*names)[]=(char* (*)[])&nhpm_rds_lua_script_names;
      int n = sizeof(nhpm_rds_lua_hashes)/sizeof(char*);
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
  //ngx_http_push_channel_t * channel = (ngx_http_push_channel_t *)privdata;
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
  char* (*hashes)[]=(char* (*)[])&nhpm_rds_lua_hashes;
  //char* (*scripts)[]=(char* (*)[])&nhpm_rds_lua_scripts;
  char* (*names)[]=(char* (*)[])&nhpm_rds_lua_script_names;
  uintptr_t i=(uintptr_t) privdata;
  char *hash=(*hashes)[i];

  redisReply *reply = r;
  if (reply == NULL) return;
  switch(reply->type) {
    case REDIS_REPLY_ERROR:
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: Failed loading redis lua scripts %s :%s", (*names)[i], reply->str);
      break;
    case REDIS_REPLY_STRING:
      if(ngx_strncmp(reply->str, hash, REDIS_LUA_HASH_LENGTH)!=0) {
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module Redis lua script %s has unexpected hash %s (expected %s)", (*names)[i], reply->str, hash);
      }
      break;
  }
}

static void redisInitScripts(redisAsyncContext *c){
  uintptr_t i;
  char* (*scripts)[]=(char* (*)[])&nhpm_rds_lua_scripts;
  for(i=0; i<sizeof(nhpm_rds_lua_scripts)/sizeof(char*); i++) {
    redisAsyncCommand(c, &redis_load_script_callback, (void *)i, "SCRIPT LOAD %s", (*scripts)[i]);
  }
}

static redisAsyncContext * rds_sub_ctx(void);

static redisAsyncContext * rds_ctx(void){
  static redisAsyncContext *c = NULL;
  if(c==NULL) {
    //init redis
    redis_nginx_open_context((const char *)"localhost", 8537, 1, &c);
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

#define CHECK_REPLY_STR(reply) ((reply)->type == REDIS_REPLY_STRING)
#define CHECK_REPLY_STRVAL(reply, v) ( CHECK_REPLY_STR(reply) && ngx_strcmp((reply)->str, v) == 0 )
#define CHECK_REPLY_STRNVAL(reply, v, n) ( CHECK_REPLY_STR(reply) && ngx_strncmp((reply)->str, v, n) == 0 )
#define CHECK_REPLY_INT(reply) ((reply)->type == REDIS_REPLY_INTEGER)
#define CHECK_REPLY_INTVAL(reply, v) ( CHECK_REPLY_INT(reply) && (reply)->integer == v )
#define CHECK_REPLY_ARRAY_MIN_SIZE(reply, size) ( (reply)->type == REDIS_REPLY_ARRAY && (reply)->elements >= size )
#define CHECK_REPLY_NIL(reply) ((reply)->type == REDIS_REPLY_NIL)

static void redis_subscriber_callback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply *reply = r;
  redisReply *el = NULL;
  ngx_http_push_msg_t msg;
  ngx_buf_t           buf= {0};

  ngx_str_t           channel_id = {0};
  msgpack_unpacked    msgunpack;
  msgpack_object     *cur;
  
  if(reply == NULL) return;
  if(CHECK_REPLY_ARRAY_MIN_SIZE(reply, 3)
    && CHECK_REPLY_STRVAL(reply->element[0], "message")
    && CHECK_REPLY_STR(reply->element[1])
    && CHECK_REPLY_STR(reply->element[2])) {

    msg.expires=0;
    msg.refcount=0;
    msg.buf=NULL;
    //reply->element[1] is channel name
    el = reply->element[2];
    
    if(CHECK_REPLY_STRNVAL(el, "delete:", 7)) {
      //delete channel command
      channel_id.len=el->len - 7;
      channel_id.data=(u_char *)(el->str + 7);
      
      ngx_http_push_store_publish_raw(&channel_id, NULL, NGX_HTTP_GONE, &NGX_HTTP_PUSH_HTTP_STATUS_410);

    }
    else if(CHECK_REPLY_STR(el)) {
      //maybe a message?
      msgpack_unpacked_init(&msgunpack);
      if(msgpack_unpack_next(&msgunpack, (char *)el->str, el->len, NULL)) {
        msgpack_object o = msgunpack.data;
        if(o.type==MSGPACK_OBJECT_MAP && o.via.map.size != 0) {
          msgpack_object_kv* p;
          msgpack_object_kv* const pend = o.via.map.ptr + o.via.map.size;
          for(p = o.via.map.ptr; p < pend; ++p) {
            cur=&p->val;
            if(ngx_strncmp("time", p->key.via.raw.ptr, p->key.via.raw.size)==0) {
              switch(cur->type) {
                case MSGPACK_OBJECT_POSITIVE_INTEGER:
                  msg.message_time=(time_t) cur->via.u64;
                  break;
                case MSGPACK_OBJECT_NEGATIVE_INTEGER:
                  msg.message_time=(time_t) cur->via.i64;
                  break;
                case MSGPACK_OBJECT_RAW:
                  msg.message_time=ngx_atotm((u_char *)cur->via.raw.ptr, cur->via.raw.size);
                  break;
                default:
                  msg.message_time=0;
              }
            }
            else if(ngx_strncmp("tag", p->key.via.raw.ptr, p->key.via.raw.size)==0) {
              switch(cur->type) {
                case MSGPACK_OBJECT_POSITIVE_INTEGER:
                  msg.message_tag=(ngx_int_t) cur->via.u64;
                  break;
                case MSGPACK_OBJECT_NEGATIVE_INTEGER:
                  msg.message_tag=(ngx_int_t) cur->via.i64;
                  break;
                case MSGPACK_OBJECT_RAW:
                  msg.message_tag=ngx_atoi((u_char *)cur->via.raw.ptr, cur->via.raw.size);
                  break;
                default:
                  msg.message_tag=0;
              }
            }
            else if(ngx_strncmp("content_type", p->key.via.raw.ptr, p->key.via.raw.size)==0) {
              if(cur->type == MSGPACK_OBJECT_RAW) {
                msg.content_type.len=cur->via.raw.size;
                msg.content_type.data=(u_char *)cur->via.raw.ptr;
              }
            }
            else if(ngx_strncmp("data", p->key.via.raw.ptr, p->key.via.raw.size)==0) {
              msg.buf=&buf;
              if(cur->type == MSGPACK_OBJECT_RAW) {
                buf.start = buf.pos = (u_char *)cur->via.raw.ptr;
                buf.end = buf.last = (u_char *)(cur->via.raw.ptr + cur->via.raw.size);
                buf.memory = 1;
                buf.last_buf = 1;
                buf.last_in_chain = 1;
              }
            }
            else if(ngx_strncmp("channel", p->key.via.raw.ptr, p->key.via.raw.size)==0) {
              channel_id.len=cur->via.raw.size;
              channel_id.data=(u_char *)cur->via.raw.ptr;
            }
          }
        }
      }
      else {
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: invalid msgpack message from redis");
      }
      msgpack_unpacked_destroy(&msgunpack);
    
      ngx_http_push_store_publish_raw(&channel_id, &msg, 0, NULL);
    }
    else { //not a string
      redisEchoCallback(c, el, NULL);
    }
  }
  else if(CHECK_REPLY_ARRAY_MIN_SIZE(reply, 3)
    && CHECK_REPLY_STRVAL(reply->element[0], "subscribe")
    && CHECK_REPLY_STR(reply->element[1])
    && CHECK_REPLY_INT(reply->element[2])) {

    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "REDIS: subscribed to %s (%i total)", reply->element[1]->str, reply->element[1]->integer);
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
    ngx_snprintf(subscriber_channel, 255, "nginx_push_module:%s", subscriber_id);
  }
  if(c==NULL) {
    //init redis
    redis_nginx_open_context((const char *)"localhost", 8537, 1, &c);
    redisAsyncCommand(c, redis_subscriber_callback, NULL, "SUBSCRIBE %s", subscriber_channel);
  }
  return c;
}

static ngx_pool_t *chanhead_ensure_pool_exists(nhpm_channel_head_t *chanhead) {
  if(chanhead->pool==NULL) {
    if((chanhead->pool=ngx_create_pool(NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_POOL_SIZE, ngx_cycle->log))==NULL) {
      ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "can't allocate memory for channel subscriber pool");
    }
  }
  return chanhead->pool;
}

static void subscriber_publishing_cleanup_callback(nhpm_subscriber_cleanup_t *cln) {
  nhpm_subscriber_t            *sub = cln->sub;
  nhpm_channel_head_cleanup_t  *shared = cln->shared;
  //  ngx_http_push_loc_conf_t     *cf = ngx_http_get_module_loc_conf((ngx_http_request_t *)sub->subscriber, ngx_http_push_module);
  ngx_int_t                     done;
  
  
  done = sub->prev==NULL && sub->next==NULL;
  
  //remove subscriber from list
  if(sub->prev != NULL) {
    sub->prev->next=sub->next;
  }
  if(sub->next != NULL) {
    sub->next->prev=sub->prev;
  }
  
  sub->next = sub->prev = NULL;
  
  if(done) {
    //release pool
    assert(shared->sub_count != 0);
    //redisAsyncCommand(rds_ctx(), &redisCheckErrorCallback, NULL, "EVALSHA %s 0 %b %i %i %i", nhpm_rds_lua_hashes.subscriber_count, STR(&(shared->id)), -(shared->sub_count), cf->channel_timeout, 0); //this happened during publishing
    ngx_destroy_pool(shared->pool);
  }
}

static ngx_int_t chanhead_gc_add(nhpm_channel_head_t *head) {
  nhpm_llist_timed_t         *chanhead_cleanlink;
  if(head->cleanlink==NULL) {
    //add channel head to cleanup list
    if((chanhead_cleanlink=ngx_alloc(sizeof(*chanhead_cleanlink), ngx_cycle->log))==NULL) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "can't allocate memory for channel subscriber head cleanup list element");
      return NGX_ERROR;
    }
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
    head->cleanlink=chanhead_cleanlink;
  }
  
  //initialize cleanup timer
  if(!chanhead_cleanup_timer.timer_set) {
    ngx_add_timer(&chanhead_cleanup_timer, NGX_HTTP_PUSH_DEFAULT_CHANHEAD_CLEANUP_INTERVAL);
  }
  return NGX_OK;
}

static ngx_int_t chanhead_gc_withdraw(nhpm_channel_head_t *chanhead) {
  //remove from cleanup list if we're there
  nhpm_llist_timed_t    *cl;
  if(chanhead->cleanlink!=NULL) {
    cl=chanhead->cleanlink;
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
  return NGX_OK;
}

static ngx_int_t chanhead_ensure_cleanup_exists(nhpm_channel_head_t *head) {
  nhpm_channel_head_cleanup_t *hcln;
  if(head->shared_cleanup == NULL) {
    if((hcln=ngx_pcalloc(head->pool, sizeof(*hcln)))==NULL) {
      return NGX_ERROR;
    }
    head->shared_cleanup = hcln;
  }
  return NGX_OK;
}

static ngx_int_t ngx_http_push_store_publish_raw(ngx_str_t *channel_id, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line){
  nhpm_channel_head_t        *head;
  nhpm_subscriber_t          *sub, *next;
  ngx_buf_t                  *buffer=NULL;
  ngx_str_t                  *etag, *content_type;
  ngx_chain_t                *chain;
  nhpm_channel_head_cleanup_t *hcln;
  
  CHANNEL_HASH_FIND(channel_id, head);
  if(head==NULL) {
    return NGX_HTTP_PUSH_MESSAGE_QUEUED;
  }
  
  chanhead_ensure_pool_exists(head);
  chanhead_ensure_cleanup_exists(head);
  
  if(msg!=NULL) {
    etag = ngx_http_push_store_etag_from_message(msg, head->pool);
    content_type = ngx_http_push_store_content_type_from_message(msg, head->pool);
    chain = ngx_http_push_create_output_chain(msg->buf, head->pool, ngx_cycle->log);
    if(chain==NULL) {
      return NGX_ERROR;
    }
    buffer = chain->buf;
    buffer->recycled = 1;
    
  }
  
  //set some things the cleanup callback will need
  hcln = head->shared_cleanup;
  head->shared_cleanup = NULL;
  hcln->sub_count=head->sub_count;
  hcln->head=NULL;
  hcln->id.len = head->id.len;
  hcln->id.data = head->id.data;
  //ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "hcln->id.len == %i, head cleanup: %p", hcln->id.len, hcln);
  hcln->pool=head->pool;
  
  for(sub=head->sub; sub!=NULL; sub=next) {
    ngx_chain_t               *rchain;
    ngx_buf_t                 *rbuffer;
    ngx_http_request_t        *r=(ngx_http_request_t *)sub->subscriber;

    sub->cln->handler = (ngx_http_cleanup_pt) subscriber_publishing_cleanup_callback;
    next = sub->next; //becase the cleanup callback will dequeue this subscriber
    if(((nhpm_subscriber_cleanup_t *)sub->cln->data)->shared != hcln) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "wrong shared cleanup for subscriber %p: should be %p, is %p", sub, hcln, ((nhpm_subscriber_cleanup_t *)sub->cln->data)->shared);
    }
    
    if(msg!=NULL) {
      //each response needs its own chain and buffer, though the buffer contents can be shared
      rchain = ngx_pcalloc(sub->pool, sizeof(*rchain));
      rbuffer = ngx_pcalloc(sub->pool, sizeof(*rbuffer));
      rchain->next = NULL;
      rchain->buf = rbuffer;
      ngx_memcpy(rbuffer, buffer, sizeof(*buffer));
      
      ngx_http_finalize_request(r, ngx_http_push_prepare_response_to_subscriber_request(r, rchain, content_type, etag, msg->message_time));
    }
    else {
      ngx_http_finalize_request(r, ngx_http_push_respond_status_only(r, status_code, status_line));
    }
  }
  
  chanhead_gc_add(head);
  
  redisAsyncCommand(rds_ctx(), &redisCheckErrorCallback, NULL, "EVALSHA %s 0 %b %i", nhpm_rds_lua_hashes.subscriber_count, STR(channel_id), -(head->sub_count));
  
  head->sub_count=0;
  head->pool=NULL; //pool will be destroyed on cleanup
  head->sub=NULL;

  return NGX_HTTP_PUSH_MESSAGE_RECEIVED;
}

static void handle_chanhead_gc_queue(ngx_int_t force_delete) {
  nhpm_llist_timed_t    *cur, *next;
  nhpm_channel_head_t   *ch = NULL;
  
  //ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "ngx_http_push_store_chanhead_cleanup_timer_handler");
  for(cur=chanhead_cleanup_head; cur != NULL; cur=next) {
    next=cur->next;
    if(force_delete || ngx_time() - cur->time > NGX_HTTP_PUSH_CHANHEAD_EXPIRE_SEC) {
      ch = (nhpm_channel_head_t *)cur->data;
      if (ch->sub==NULL) { //still no subscribers here
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "channel_head is empty and expired. delete %p.", ch);
        CHANNEL_HASH_DEL(ch);
        if(ch->shared_cleanup != NULL) {
          
          ngx_str_t *str=&(ch->shared_cleanup->id);
          
          if(str->data == ch->id.data) {
            if((str->data=ngx_palloc(ch->shared_cleanup->pool, ch->id.len)) == NULL) {
              ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "can't allocate space for channel id for chanhead cleanup");
            }
            else {
              str->len = ch->id.len;
              ngx_memcpy(str->data, ch->id.data, str->len);
            }
          }
        }
        ngx_free(ch);
      }
      else {
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "channel_head is still being used.");
      }
      ngx_free(cur);
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

static void ngx_http_push_store_chanhead_cleanup_timer_handler(ngx_event_t *ev) {
  handle_chanhead_gc_queue(0);
  if (!(ngx_quit || ngx_terminate || ngx_exiting || chanhead_cleanup_head==NULL)) {
    ngx_add_timer(ev, NGX_HTTP_PUSH_DEFAULT_CHANHEAD_CLEANUP_INTERVAL);
  }
}

static ngx_int_t redis_array_to_channel(redisReply *r, ngx_http_push_channel_t *ch) {
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
    ch->message_queue = NULL;
    ch->workers_with_subscribers = NULL;
    
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
  ngx_str_t           *channel_id;
  callback_pt          callback;
  void                *privdata;
} redis_channel_callback_data_t;

static void redisChannelInfoCallback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply *reply=r;
  redis_channel_callback_data_t *d=(redis_channel_callback_data_t *)privdata;
  ngx_http_push_channel_t channel = {{0}};

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

static ngx_int_t ngx_http_push_store_delete_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  redis_channel_callback_data_t *d;
  if((d=ngx_alloc(sizeof(*d), ngx_cycle->log))==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "Failed to allocate memory for some callback data");
    return NGX_ERROR;
  }
  d->channel_id = channel_id;
  d->callback = callback;
  d->privdata = privdata;
  
  redisAsyncCommand(rds_ctx(), &redisChannelInfoCallback, d, "EVALSHA %s 0 %b", nhpm_rds_lua_hashes.delete, STR(channel_id));

  return NGX_OK;
}



static ngx_int_t ngx_http_push_store_find_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  redis_channel_callback_data_t *d;
  if((d=ngx_alloc(sizeof(*d), ngx_cycle->log))==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "Failed to allocate memory for some callback data");
    return NGX_ERROR;
  }
  d->channel_id = channel_id;
  d->callback = callback;
  d->privdata = privdata;
  
  redisAsyncCommand(rds_ctx(), &redisChannelInfoCallback, d, "EVALSHA %s 0 %b", nhpm_rds_lua_hashes.find_channel, STR(channel_id));
  
  return NGX_OK;
}



static void * ngx_nhpm_alloc(size_t size) {
  return ngx_alloc(size, ngx_cycle->log);
}

static ngx_http_push_msg_t * msg_from_redis_get_message_reply(redisReply *r, ngx_int_t offset, void *(*allocator)(size_t size)) {
  ngx_http_push_msg_t *msg=NULL;
  ngx_buf_t           *buf=NULL;
  redisReply         **els = r->element;
  size_t len = 0, content_type_len = 0;

  if(CHECK_REPLY_ARRAY_MIN_SIZE(r, offset + 4)
   && CHECK_REPLY_INT(els[offset])     //id - time
   && CHECK_REPLY_INT(els[offset+1])   //id - tag
   && CHECK_REPLY_STR(els[offset+2])   //message
   && CHECK_REPLY_STR(els[offset+3])){ //content-type
    len=els[offset+2]->len;
    content_type_len=els[offset+3]->len;
    if((msg=allocator(sizeof(*msg) + sizeof(ngx_buf_t) + len + content_type_len))==NULL) {
      ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "push module: can't allocate memory for message from redis reply");
      return NULL;
    }
    ngx_memzero(msg, sizeof(*msg)+sizeof(ngx_buf_t));
    //set up message buffer;
    msg->buf = (void *)(&msg[1]);
    buf = msg->buf;
    buf->start = buf->pos = (void *)(&buf[1]);
    buf->end = buf->last = buf->start + (u_char)len;
    ngx_memcpy(buf->start, els[3]->str, len);
    buf->memory = 1;
    buf->last_buf = 1;
    buf->last_in_chain = 1;
    
    if(content_type_len>0) {
      msg->content_type.len=content_type_len;
      msg->content_type.data=buf->end;
      ngx_memcpy(msg->content_type.data, els[4]->str, content_type_len);
    }
    
    msg->message_time = els[1]->integer;
    msg->message_tag = els[2]->integer;
    return msg;
  }
  else {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "push module: invalid message redis reply");
    return NULL;
  }
}

typedef struct {
  ngx_http_request_t  *r;
  ngx_str_t           *channel_id;
  ngx_http_push_msg_id_t *msg_id;
  callback_pt          callback;
  void                *privdata;
} redis_get_message_data_t;

static void redis_get_message_callback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply                *reply= r;
  redis_get_message_data_t  *d= (redis_get_message_data_t *)privdata;
  ngx_http_push_msg_t       *msg=NULL;
  
  
  //output: result_code, msg_time, msg_tag, message, content_type,  channel-subscriber-count
  // result_code can be: 200 - ok, 403 - channel not found, 404 - not found, 410 - gone, 418 - not yet available
  
  if ( !CHECK_REPLY_ARRAY_MIN_SIZE(reply, 1) || !CHECK_REPLY_INT(reply->element[0]) ) {
    //no good
    ngx_free(d);
    return;
  }
  
  switch(reply->element[0]->integer) {
    case 200: //ok
      if((msg=msg_from_redis_get_message_reply(reply, 1, &ngx_nhpm_alloc))) {
        d->callback(NGX_HTTP_PUSH_MESSAGE_FOUND, msg, d->privdata);
      }
      break;
    case 403: //channel not found
    case 404: //not found
      d->callback(NGX_HTTP_PUSH_MESSAGE_NOTFOUND, NULL, d->privdata);
      break;
    case 410: //gone
      d->callback(NGX_HTTP_PUSH_MESSAGE_EXPIRED, NULL, d->privdata);
      break;
    case 418: //not yet available
      d->callback(NGX_HTTP_PUSH_MESSAGE_EXPECTED, NULL, d->privdata);
      break;
  }
  
  ngx_free(d);
}

static ngx_int_t ngx_http_push_store_async_get_message(ngx_str_t *channel_id, ngx_http_push_msg_id_t *msg_id, callback_pt callback, void *privdata) {
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
  
  //input:  keys: [], values: [channel_id, msg_time, msg_tag, no_msgid_order, create_channel_ttl, subscriber_channel]
  //subscriber channel is not given, because we don't care to subscribe
  redisAsyncCommand(rds_ctx(), &redis_get_message_callback, (void *)d, "EVALSHA %s 0 %b %i %i %s", nhpm_rds_lua_hashes.get_message, STR(channel_id), msg_id->time, msg_id->tag, "FILO", 0);
  return NGX_OK; //async only now!
}

//initialization
static ngx_int_t ngx_http_push_store_init_module(ngx_cycle_t *cycle) {
  ngx_core_conf_t                *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  ngx_http_push_worker_processes = ccf->worker_processes;
  //initialize our little IPC
  return NGX_OK;
}

static ngx_int_t ngx_http_push_store_init_postconfig(ngx_conf_t *cf) {
  //nothing to do but be OK.
  return NGX_OK;
}

static void ngx_http_push_store_create_main_conf(ngx_conf_t *cf, ngx_http_push_main_conf_t *mcf) {
  mcf->shm_size=NGX_CONF_UNSET_SIZE;
}

static void ngx_http_push_store_exit_worker(ngx_cycle_t *cycle) {
  nhpm_channel_head_t *cur, *tmp;
  nhpm_subscriber_t *sub;
  redisAsyncContext *ctx;

  if((ctx=rds_ctx())!=NULL)
    redis_nginx_force_close_context(&ctx);
  if((ctx=rds_sub_ctx())!=NULL)
    redis_nginx_force_close_context(&ctx);
  
  handle_chanhead_gc_queue(1);
  
  HASH_ITER(hh, subhash, cur, tmp) {
    //any subscribers?
    sub = cur->sub;
    while (sub != NULL) {
      ngx_http_finalize_request((ngx_http_request_t *)sub->subscriber, NGX_HTTP_CLOSE);
      sub = sub->next;
    }
    if(cur->pool != NULL) {
      ngx_destroy_pool(cur->pool);
    }
    if(cur->cleanlink != NULL) {
      ngx_free(cur->cleanlink);
    }
    HASH_DEL(subhash, cur);
    ngx_free(cur);
  }
  
  ngx_del_timer(&chanhead_cleanup_timer);
}

static void ngx_http_push_store_exit_master(ngx_cycle_t *cycle) {
  //destroy channel tree in shared memory
  //ngx_http_push_walk_rbtree(ngx_http_push_movezig_channel_locked, ngx_http_push_shm_zone);
  //deinitialize IPC
  
}

static void subscriber_cleanup_callback(nhpm_subscriber_cleanup_t *cln) {
  nhpm_subscriber_t *sub = cln->sub;
  nhpm_channel_head_cleanup_t *shared = cln->shared;
  nhpm_channel_head_t *head = shared->head;
  ngx_int_t done;
  done = sub->prev==NULL && sub->next==NULL;
  
  //remove subscriber from list
  if(sub->prev != NULL) {
    sub->prev->next=sub->next;
  }
  if(sub->next != NULL) {
    sub->next->prev=sub->prev;
  }
  
  redisAsyncCommand(rds_ctx(), &redisEchoCallback, NULL, "EVALSHA %s 0 %b %i", nhpm_rds_lua_hashes.subscriber_count, STR(&(shared->id)), -1);
  head->sub_count--;
  
  if(done) {
    //add chanhead to gc list
    head->sub=NULL;
    chanhead_gc_add(head);
  }
}

static ngx_int_t ngx_http_push_store_set_subscriber_cleanup_callback(nhpm_channel_head_t *head, nhpm_subscriber_t *sub, ngx_http_cleanup_pt *cleanup_callback) {
  ngx_http_cleanup_t        *cln = sub->cln;
  nhpm_channel_head_cleanup_t *headcln;
  nhpm_subscriber_cleanup_t *subcln;
  ngx_http_request_t        *r = (ngx_http_request_t *)sub->subscriber;
  //ngx_http_push_loc_conf_t  *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  chanhead_ensure_cleanup_exists(head);
  headcln = head->shared_cleanup;
  headcln->head = head;
  
  headcln->id.len = head->id.len;
  headcln->id.data = head->id.data;
  
  headcln->pool = head->pool;
  headcln->sub_count = 0;
  
  if(cln == NULL) {
    if((cln=ngx_http_cleanup_add(r, sizeof(*subcln)))==NULL) {
      return NGX_ERROR;
    }
    subcln = cln->data;
    subcln->shared = head->shared_cleanup;
    subcln->sub = sub;
    sub->cln=cln;
  }
  cln->handler = (ngx_http_cleanup_pt) cleanup_callback;
  return NGX_OK;
}

static ngx_int_t ngx_http_push_store_subscribe_new(ngx_str_t *channel_id, ngx_http_request_t *r) {
  //this is the new shit
  ngx_http_push_loc_conf_t  *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  nhpm_channel_head_t       *chanhead = NULL;
  nhpm_subscriber_t         *nextsub;
  
  CHANNEL_HASH_FIND(channel_id, chanhead);
  if(chanhead==NULL) {
    chanhead=(nhpm_channel_head_t *)ngx_calloc(sizeof(*chanhead) + channel_id->len, ngx_cycle->log);
    if(chanhead==NULL) {
      ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "can't allocate memory for (new) channel subscriber head");
      return NGX_ERROR;
    }
    chanhead->id.len = channel_id->len;
    chanhead->id.data = (u_char *)&chanhead[1];
    ngx_memcpy(chanhead->id.data, channel_id->data, channel_id->len);

    chanhead->sub_count=0;
    CHANNEL_HASH_ADD(chanhead);
  }
  else {
    chanhead_gc_withdraw(chanhead);
  }
  if(chanhead_ensure_pool_exists(chanhead)==NULL) {
    return NGX_ERROR;
  }
  
  if((nextsub=ngx_pcalloc(chanhead->pool, sizeof(*nextsub)))==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "can't allocate memory for (new) subscriber in channel sub pool");
    return NGX_ERROR;
  }
  
  nextsub->subscriber= (void *)r;
  nextsub->type= LONGPOLL;
  nextsub->pool= r->pool;
  if(chanhead->sub != NULL) {
    chanhead->sub->prev = nextsub;
    nextsub->next = chanhead->sub;
  }
  else {
    //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "first subscriber for %V (%p): %p", &chanhead->id, chanhead, nextsub);
  }
  chanhead->sub = nextsub;
  
  chanhead->sub_count++;
  redisAsyncCommand(rds_ctx(), &redisCheckErrorCallback, NULL, "EVALSHA %s 0 %b %i %i %i", nhpm_rds_lua_hashes.subscriber_count, STR(channel_id), 1, cf->channel_timeout, 0);

  ngx_push_longpoll_subscriber_enqueue(nextsub->subscriber, cf->subscriber_timeout);

  //add timeout timer
  //TODO

  //add teardown callbacks
  if(ngx_http_push_store_set_subscriber_cleanup_callback(chanhead, nextsub, (ngx_http_cleanup_pt *)subscriber_cleanup_callback) != NGX_OK) {
    return NGX_ERROR;
  }
  
  return NGX_OK;
}

typedef struct {
  ngx_str_t              *channel_id;
  ngx_http_push_msg_id_t *msg_id;
  callback_pt           callback;
  void                 *privdata;
} redis_subscribe_data_t;

static void redis_getmessage_callback(redisAsyncContext *c, void *vr, void *privdata) {
  redis_subscribe_data_t    *d = (redis_subscribe_data_t *) privdata;
  redisReply                *reply = (redisReply *)vr;
  ngx_http_request_t        *r = (ngx_http_request_t *)d->privdata;
  ngx_http_push_loc_conf_t  *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  ngx_int_t                  status;
  ngx_http_push_msg_t        *msg=NULL;
  
  //output: result_code, msg_time, msg_tag, message, content_type, channel-subscriber-count
  // result_code can be: 200 - ok, 403 - channel not found, 404 - not found, 410 - gone, 418 - not yet available
  
  if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
    redisEchoCallback(c,reply,privdata);
    ngx_free(d);
    return;
  }
  
  if ( !CHECK_REPLY_ARRAY_MIN_SIZE(reply, 1) || !CHECK_REPLY_INT(reply->element[0]) ) {
    //no good
    ngx_free(d);
    return;
  }
  
  status = reply->element[0]->integer;
  
  switch(status) {
    ngx_str_t                  *etag;
    ngx_str_t                  *content_type;
    ngx_chain_t                *chain=NULL;
    time_t                      last_modified;
    ngx_int_t                   ret;
    
    case 200: //ok
      switch(cf->subscriber_concurrency) {
        case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_BROADCAST:
          if((msg=msg_from_redis_get_message_reply(reply, 1, &ngx_nhpm_alloc))) {
            ngx_http_push_alloc_for_subscriber_response(r->pool, 0, msg, &chain, &content_type, &etag, &last_modified);
            ret=ngx_http_push_prepare_response_to_subscriber_request(r, chain, content_type, etag, last_modified);
            d->callback(ret, msg, d->privdata);
          }
          break;

        case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_LASTIN:
          //kick everyone elese out, then subscribe
          redisAsyncCommand(rds_ctx(), &redisEchoCallback, NULL, "EVALSHA %s 0 %b %i", nhpm_rds_lua_hashes.publish_status, STR(d->channel_id), 409);
          if((msg=msg_from_redis_get_message_reply(reply, 1, &ngx_nhpm_alloc))) {
            ngx_http_push_alloc_for_subscriber_response(r->pool, 0, msg, &chain, &content_type, &etag, &last_modified);
            ret=ngx_http_push_prepare_response_to_subscriber_request(r, chain, content_type, etag, last_modified);
            d->callback(ret, NULL, d->privdata);
          }
          break;

        case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_FIRSTIN:
          if(CHECK_REPLY_ARRAY_MIN_SIZE(reply, 6) && CHECK_REPLY_INT(reply->element[5]) && reply->element[5]->integer > 0 ) {
            ngx_http_push_respond_status_only(r, NGX_HTTP_NOT_FOUND, &NGX_HTTP_PUSH_HTTP_STATUS_409);
          }
          break;

        default:
          ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "unexpected subscriber_concurrency config value");
      }
      break;
    case 418: //not yet available
    case 403: //channel not found
      // ♫ It's gonna be the future soon ♫
      if (ngx_http_push_store_subscribe_new(d->channel_id, r) == NGX_OK) {
        d->callback(NGX_DONE, NULL, d->privdata);
      }
      else {
        d->callback(NGX_ERROR, NULL, d->privdata);
      }
      break;
    case 404: //not found
      d->callback(NGX_HTTP_NOT_FOUND, NULL, d->privdata);
      break;
    case 410: //gone
      //subscriber wants an expired message
      //TODO: maybe respond with entity-identifiers for oldest available message?
      d->callback(NGX_HTTP_NO_CONTENT, NULL, d->privdata);
      break;
    default: //shouldn't be here!
      d->callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, d->privdata);
  }
  if(msg != NULL) {
    ngx_free(msg);
  }
  ngx_free(d);
}

static ngx_int_t ngx_http_push_store_subscribe(ngx_str_t *channel_id, ngx_http_push_msg_id_t *msg_id, ngx_http_push_loc_conf_t *cf, callback_pt callback, void *privdata) {
  redis_subscribe_data_t       *d=NULL;
  //static ngx_int_t                       subscribed = 0;
  
  assert(callback != NULL);
  
  if((d=ngx_calloc(sizeof(*d), ngx_cycle->log))==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "can't allocate redis get_message callback data");
    return callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, privdata);
  }

  d->channel_id=channel_id;
  d->msg_id=msg_id;
  d->callback=callback;
  d->privdata=privdata;
  
  //input:  keys: [], values: [channel_id, msg_time, msg_tag, no_msgid_order, create_channel_ttl, subscriber_channel]
  redisAsyncCommand(rds_ctx(), &redis_getmessage_callback, (void *)d, "EVALSHA %s 0 %b %i %i %s %i %s", nhpm_rds_lua_hashes.get_message, STR(channel_id), msg_id->time, msg_id->tag, "FILO", cf->channel_timeout, subscriber_channel);
  return NGX_OK;
}
 /* 
  ngx_http_push_msg_t            *msg;
  ngx_int_t                       msg_search_outcome;
  ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  if (cf->authorize_channel==1) {
    channel = ngx_http_push_store_find_channel(channel_id, cf->channel_timeout, NULL);
  }else{
    channel = ngx_http_push_store_get_channel(channel_id, cf->channel_timeout, NULL);
  }
  if (channel==NULL) {
    //unable to allocate channel OR channel not found
    if(cf->authorize_channel) {
      return callback(NGX_HTTP_FORBIDDEN, r);
    }
    else {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate shared memory for channel");
      return callback(NGX_HTTP_INTERNAL_SERVER_ERROR, r);
    }
  }
  
  switch(ngx_http_push_handle_subscriber_concurrency(channel, r, cf)) {
    case NGX_DECLINED: //this request was declined for some reason.
      //status codes and whatnot should have already been written. just get out of here quickly.
      return callback(NGX_OK, r);
    case NGX_ERROR:
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: error handling subscriber concurrency setting");
      return callback(NGX_ERROR, r);
  }
  
  
  msg = ngx_http_push_store_get_channel_message(channel, msg_id, &msg_search_outcome, cf);
  
  if (cf->ignore_queue_on_no_cache && !ngx_http_push_allow_caching(r)) {
    msg_search_outcome = NGX_HTTP_PUSH_MESSAGE_EXPECTED; 
    msg = NULL;
  }
  
  switch(msg_search_outcome) {
    //for message-found:
    ngx_str_t                  *etag;
    ngx_str_t                  *content_type;
    ngx_chain_t                *chain=NULL;
    time_t                      last_modified;

    case NGX_HTTP_PUSH_MESSAGE_EXPECTED:
      // ♫ It's gonna be the future soon ♫
      if (ngx_http_push_store_subscribe_new(channel, r) == NGX_OK) {

        redisAsyncCommand(rds_ctx(), NULL, NULL, "ECHO SUB channel:%b", STR(&(channel->id)));
        return callback(NGX_DONE, r);
      }
      else {
        return callback(NGX_ERROR, r);
      }

    case NGX_HTTP_PUSH_MESSAGE_EXPIRED:
      //subscriber wants an expired message
      //TODO: maybe respond with entity-identifiers for oldest available message?
      return callback(NGX_HTTP_NO_CONTENT, r);
      
    case NGX_HTTP_PUSH_MESSAGE_FOUND:
      ngx_http_push_alloc_for_subscriber_response(r->pool, 0, msg, &chain, &content_type, &etag, &last_modified);
      ngx_int_t ret=ngx_http_push_prepare_response_to_subscriber_request(r, chain, content_type, etag, last_modified);
      ngx_http_push_store->release_message(channel, msg);
      return callback(ret, r);
      
    default: //we shouldn't be here.
      return callback(NGX_HTTP_INTERNAL_SERVER_ERROR, r);
  }
*/

static ngx_str_t * ngx_http_push_store_etag_from_message(ngx_http_push_msg_t *msg, ngx_pool_t *pool){
  ngx_str_t *etag;
  if(pool!=NULL && (etag = ngx_palloc(pool, sizeof(*etag) + NGX_INT_T_LEN))==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate memory for Etag header in pool");
    return NULL;
  }
  else if(pool==NULL && (etag = ngx_alloc(sizeof(*etag) + NGX_INT_T_LEN, ngx_cycle->log))==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate memory for Etag header");
    return NULL;
  }
  etag->data = (u_char *)(etag+1);
  etag->len = ngx_sprintf(etag->data,"%ui", msg->message_tag)- etag->data;
  return etag;
}

static ngx_str_t * ngx_http_push_store_content_type_from_message(ngx_http_push_msg_t *msg, ngx_pool_t *pool){
  ngx_str_t *content_type;
  if(pool != NULL && (content_type = ngx_palloc(pool, sizeof(*content_type) + msg->content_type.len))==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate memory for Content Type header in pool");
    return NULL;
  }
  else if(pool == NULL && (content_type = ngx_alloc(sizeof(*content_type) + msg->content_type.len, ngx_cycle->log))==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate memory for Content Type header");
    return NULL;
  }
  content_type->data = (u_char *)(content_type+1);
  content_type->len = msg->content_type.len;
  ngx_memcpy(content_type->data, msg->content_type.data, content_type->len);
  return content_type;
}

typedef struct {
  ngx_str_t            *channel_id;
  time_t                msg_time;
  ngx_http_push_msg_t  *msg;
  callback_pt           callback;
  void                 *privdata;
} redis_publish_callback_data_t;

static void redisPublishCallback(redisAsyncContext *, void *, void *);

static ngx_int_t ngx_http_push_store_publish_message(ngx_str_t *channel_id, ngx_http_push_msg_t *msg, ngx_http_push_loc_conf_t *cf, callback_pt callback, void *privdata) {
  redis_publish_callback_data_t *d=NULL;
  
  assert(callback != NULL);
  
  //if(cf->max_messages > 0) { //channel buffers exist


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

  //ngx_http_push_store_publish_raw(channel_id, msg, 0, NULL);
  
  //input:  keys: [], values: [channel_id, time, message, content_type, msg_ttl]
  //output: message_tag, channel_hash
  redisAsyncCommand(rds_ctx(), &redisPublishCallback, (void *)d, "EVALSHA %s 0 %b %i %b %b %i", nhpm_rds_lua_hashes.publish, STR(channel_id), msg->message_time, BUF(msg->buf), STR(&(msg->content_type)), cf->buffer_timeout);
  return NGX_OK;
}

static void redisPublishCallback(redisAsyncContext *c, void *r, void *privdata) {
  redis_publish_callback_data_t *d=(redis_publish_callback_data_t *)privdata;
  redisReply *reply=r;
  redisReply *cur;
  ngx_http_push_msg_id_t msg_id;
  ngx_http_push_channel_t ch={{0}};
  
  if(CHECK_REPLY_ARRAY_MIN_SIZE(reply, 2)) {
    msg_id.time=d->msg_time;
    msg_id.tag=reply->element[0]->integer;

    cur=reply->element[1];
    switch(redis_array_to_channel(cur, &ch)) {
      case NGX_OK:
        d->callback(ch.subscribers > 0 ? NGX_HTTP_PUSH_MESSAGE_RECEIVED : NGX_HTTP_PUSH_MESSAGE_QUEUED, &ch, d->privdata);
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

ngx_http_push_store_t  ngx_http_push_store_redis = {
    //init
    &ngx_http_push_store_init_module,
    &ngx_http_push_store_init_worker,
    &ngx_http_push_store_init_postconfig,
    &ngx_http_push_store_create_main_conf,
    
    //shutdown
    &ngx_http_push_store_exit_worker,
    &ngx_http_push_store_exit_master,
    
    //async-friendly functions with callbacks
    &ngx_http_push_store_async_get_message, //+callback
    &ngx_http_push_store_subscribe, //+callback
    &ngx_http_push_store_publish_message, //+callback
    
    &ngx_http_push_store_delete_channel, //+callback
    &ngx_http_push_store_find_channel, //+callback
    
    //message stuff
    &ngx_http_push_store_etag_from_message,
    &ngx_http_push_store_content_type_from_message,
    
    //interprocess communication
    NULL,
    NULL
    

};
