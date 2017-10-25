#include <nchan_module.h>
#include <subscribers/common.h>

#include <store/memory/ipc.h>
#include <store/memory/store-private.h>
#include <store/memory/ipc-handlers.h>
#include <store/memory/store.h>

#include <store/redis/store.h>
#include <store/redis/store-private.h>

#include <util/nchan_msg.h>
#include "internal.h"
#include "memstore_redis.h"
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:MEM-REDIS:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:MEM-REDIS:" fmt, ##arg)


typedef struct msgexpected_callback_llist_s msgexpected_callback_llist_t;
struct msgexpected_callback_llist_s {
  void                         (*cb)(nchan_msg_status_t, void *);
  msgexpected_callback_llist_t  *next;
};

typedef struct sub_data_s sub_data_t;

struct sub_data_s {
  subscriber_t                 *sub;
  memstore_channel_head_t      *chanhead;
  ngx_str_t                    *chid;
  ngx_event_t                   timeout_ev;
  nchan_msg_status_t            last_msg_status;
  msgexpected_callback_llist_t *waiting_for_msg_expected;
  sub_data_t                  **onconnect_callback_pd;
}; //sub_data_t

/*
static ngx_int_t empty_callback(){
  return NGX_OK;
}
*/

static void respond_msgexpected_callbacks(sub_data_t *d, nchan_msg_status_t status) {
  msgexpected_callback_llist_t    *cur, *next;
  cur = d->waiting_for_msg_expected;
  d->waiting_for_msg_expected = NULL;
  for(; cur != NULL; cur = next) {
    next = cur->next;
    cur->cb(status, &cur[1]);
    ngx_free(cur);
  }
}

static ngx_int_t sub_enqueue(ngx_int_t status, void *ptr, sub_data_t *d) {
  DBG("%p memstore-redis subsriber enqueued ok", d->sub);
  
  d->chanhead->status = READY;
  d->chanhead->spooler.fn->handle_channel_status_change(&d->chanhead->spooler);
  
  return NGX_OK;
}

ngx_int_t memstore_redis_subscriber_destroy(subscriber_t *sub) {
  DBG("%p destroy", sub);
  return internal_subscriber_destroy(sub);
}

static ngx_int_t sub_dequeue(ngx_int_t status, void *ptr, sub_data_t* d) {
  DBG("%p dequeue", d->sub);
  respond_msgexpected_callbacks(d, MSG_NORESPONSE);
  return NGX_OK;
}

static ngx_int_t sub_respond_message(ngx_int_t status, void *ptr, sub_data_t* d) {
  nchan_msg_t       *msg = (nchan_msg_t *) ptr;
  nchan_loc_conf_t   cf;
  nchan_msg_id_t    *lastid;
  ngx_pool_t        *deflate_pool = NULL;
  DBG("%p memstore-redis subscriber respond with message", d->sub);

  cf.max_messages = d->chanhead->max_messages;
  cf.redis.enabled = 0;
  cf.message_timeout = msg->expires - ngx_time();
  cf.complex_max_messages = NULL;
  cf.complex_message_timeout = NULL;
  cf.message_compression = msg->compressed ? msg->compressed->compression : NCHAN_MSG_NO_COMPRESSION;
  
  if(cf.message_compression != NCHAN_MSG_NO_COMPRESSION) {
    deflate_pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE / 2, ngx_cycle->log);
    if(!deflate_pool) {
      ERR("unable to create deflatepool");
      return NGX_ERROR;
    }
    nchan_deflate_message_if_needed(msg, &cf, NULL, deflate_pool);
  }
  else {
    msg->compressed = NULL;
  }
  
  lastid = &d->chanhead->latest_msgid;
  
  respond_msgexpected_callbacks(d, MSG_NORESPONSE);
  
  assert(lastid->tagcount == 1 && msg->id.tagcount == 1);
  if(lastid->time < msg->id.time || 
    (lastid->time == msg->id.time && lastid->tag.fixed[0] < msg->id.tag.fixed[0])) {
    memstore_ensure_chanhead_is_ready(d->chanhead, 1);
    nchan_store_chanhead_publish_message_generic(d->chanhead, msg, 0, &cf, NULL, NULL);
  }
  else {
    //meh, this message has already been delivered probably hopefully
  }
  if(deflate_pool) {
    ngx_destroy_pool(deflate_pool);
  }
  return NGX_OK;
}

static ngx_int_t sub_destroy_handler(ngx_int_t status, void *d, sub_data_t *pd) {
  DBG("%p sub_destroy_handler", pd->sub);
  if(pd->onconnect_callback_pd)
    (*pd->onconnect_callback_pd) = NULL;
  return NGX_OK;
}

static ngx_int_t reconnect_callback(ngx_int_t status, void *d, void *pd) {
  sub_data_t *sd = *((sub_data_t **) pd);
  if(status != NGX_OK) {
    return NGX_ERROR;
  }
  if(sd) {
    DBG("%reconnect callback");
    assert(sd->chanhead->redis_sub == sd->sub);
    assert(&sd->chanhead->id == sd->chid);
    nchan_store_redis.subscribe(sd->chid, sd->chanhead->redis_sub);
    sd->onconnect_callback_pd = NULL;
    sd->sub->dequeue_after_response = 0;
    ((internal_subscriber_t *)sd->sub)->already_dequeued = 0;
    ngx_free(pd);
  }
  else {
    DBG("%reconnect callback skipped");
  }
  return NGX_OK;
}

static ngx_int_t sub_respond_status(ngx_int_t status, void *ptr, sub_data_t *d) {
  nchan_loc_conf_t  fake_cf;
  DBG("%p memstore-redis subscriber respond with status %i", d->sub, status);
  switch(status) {
    case NGX_HTTP_GONE: //delete
    case NGX_HTTP_CLOSE: //delete
      respond_msgexpected_callbacks(d, MSG_NORESPONSE);
      fake_cf = *d->sub->cf;
      fake_cf.redis.enabled = 0;
      d->sub->destroy_after_dequeue = 1;
      nchan_store_memory.delete_channel(d->chid, &fake_cf, NULL, NULL);
      //now the chanhead will be in the garbage collector
      d->chanhead->redis_sub = NULL;
      
      if(redis_connection_status(d->sub->cf) != CONNECTED && d->onconnect_callback_pd == NULL) {
        sub_data_t **dd = ngx_alloc(sizeof(*d), ngx_cycle->log);
        *dd = d;
        d->onconnect_callback_pd = dd;
        redis_store_callback_on_connected(d->sub->cf, 0, reconnect_callback, dd);
      }
      break;
      
    case NGX_HTTP_NO_CONTENT:
      if(d->last_msg_status != MSG_EXPECTED) {
        //the message buffer has just been walked start to finish
        nchan_memstore_publish_notice(d->chanhead, NCHAN_NOTICE_BUFFER_LOADED, NULL);
      }
      d->last_msg_status = MSG_EXPECTED;
      respond_msgexpected_callbacks(d, MSG_EXPECTED);
      //TODO: stuff about REDIS_MODE_BACKUP
      
      break;
      
    default:
      //meh, no big deal.
      break;
  }
  
  return NGX_OK;
}

static ngx_int_t sub_notify_handler(ngx_int_t code, void *data, sub_data_t *d) {
  if(code == NCHAN_NOTICE_REDIS_CHANNEL_MESSAGE_BUFFER_SIZE_CHANGE) {
    intptr_t   max_messages = (intptr_t )data;
    d->chanhead->max_messages = max_messages;
    memstore_chanhead_messages_gc(d->chanhead);
  }
  return NGX_OK;
}

ngx_int_t nchan_memstore_redis_subscriber_notify_on_MSG_EXPECTED(subscriber_t *sub, nchan_msg_id_t *id, void (*cb)(nchan_msg_status_t, void*), size_t pd_sz, void *pd) {
  sub_data_t      *d = internal_subscriber_get_privdata(sub);
  int8_t           cmpval = nchan_compare_msgids(id, &sub->last_msgid);
  if(cmpval < 0) {
    cb(MSG_NORESPONSE, pd);
  }
  else if(d->last_msg_status == MSG_EXPECTED) {
    cb(MSG_EXPECTED, pd);
  }
  else {
    msgexpected_callback_llist_t *cbl;
    void  *pd_copy;
    if((cbl = ngx_alloc(sizeof(*cbl) + pd_sz, ngx_cycle->log)) == NULL) {
      ERR("Unable to allocate memory for notify_on_MSG_EXPECTED callback llist");
      return NGX_ERROR;
    }
    pd_copy = &cbl[1];
    ngx_memcpy(pd_copy, pd, pd_sz);
    cbl->cb = cb;
    cbl->next = d->waiting_for_msg_expected;
    d->waiting_for_msg_expected = cbl;
  }
  return NGX_OK;
}
/*
static void reset_timer(sub_data_t *data) {
  if(data->timeout_ev.timer_set) {
    ngx_del_timer(&data->timeout_ev);
  }
  ngx_add_timer(&data->timeout_ev, MEMSTORE_REDIS_SUBSCRIBER_TIMEOUT * 1000);
}
*/
/*
static ngx_int_t keepalive_reply_handler(ngx_int_t renew, void *_, void* pd) {
  sub_data_t *d = (sub_data_t *)pd;
  if(d->sub->release(d->sub) == NGX_OK) {
    if(renew) {
      reset_timer(d);
    }
    else{
      d->sub->dequeue(d->sub);
    }
  }
  return NGX_OK;
}
*/

static ngx_str_t   sub_name = ngx_string("memstore-redis");

subscriber_t *memstore_redis_subscriber_create(memstore_channel_head_t *chanhead) {
  subscriber_t               *sub;
  sub_data_t                 *d;
  
  assert(chanhead->cf);
  
  sub = internal_subscriber_create_init(&sub_name, chanhead->cf, sizeof(*d), (void **)&d, (callback_pt )sub_enqueue, (callback_pt )sub_dequeue, (callback_pt )sub_respond_message, (callback_pt )sub_respond_status, (callback_pt )sub_notify_handler, (callback_pt )sub_destroy_handler);
  
  
  sub->destroy_after_dequeue = 0;
  sub->dequeue_after_response = 0;
  
  d->sub = sub;
  d->chanhead = chanhead;
  d->chid = &chanhead->id;
  d->last_msg_status = MSG_PENDING;
  d->waiting_for_msg_expected = NULL;
  d->onconnect_callback_pd = NULL;

  
  /*
  ngx_memzero(&d->timeout_ev, sizeof(d->timeout_ev));
  nchan_init_timer(&d->timeout_ev, timeout_ev_handler, d)
  reset_timer(d);
  */
  DBG("%p created memstore-redis subscriber with privdata %p", d->sub, d);
  return sub;
}
