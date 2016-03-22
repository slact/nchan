#include <nchan_module.h>
#include <subscribers/common.h>
#include <store/memory/shmem.h>
#include <store/memory/ipc.h>
#include <store/memory/store-private.h>
#include <store/memory/ipc-handlers.h>

#include <store/memory/store.h>
#include <store/redis/store.h>

#include "internal.h"
#include "memstore_redis.h"
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:MEM-REDIS:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:MEM-REDIS:" fmt, ##arg)


typedef struct sub_data_s sub_data_t;

struct sub_data_s {
  subscriber_t                 *sub;
  nchan_store_channel_head_t   *chanhead;
  ngx_str_t                    *chid;
  ngx_event_t                   timeout_ev;
}; //sub_data_t

/*
static ngx_int_t empty_callback(){
  return NGX_OK;
}
*/

static ngx_int_t sub_enqueue(ngx_int_t timeout, void *ptr, sub_data_t *d) {
  DBG("%p memstore-redis subsriber enqueued ok", d->sub);
  
  d->chanhead->status = READY;
  d->chanhead->spooler.fn->handle_channel_status_change(&d->chanhead->spooler);
  
  return NGX_OK;
}

ngx_int_t memstore_redis_subscriber_destroy(subscriber_t *sub) {
  return internal_subscriber_destroy(sub);
}

static ngx_int_t sub_dequeue(ngx_int_t status, void *ptr, sub_data_t* d) {
  ngx_free(d);
  return NGX_OK;
}

static ngx_int_t sub_respond_message(ngx_int_t status, void *ptr, sub_data_t* d) {
  nchan_msg_t       *msg = (nchan_msg_t *) ptr;
  nchan_loc_conf_t   cf;
  nchan_msg_id_t    *lastid;    
  DBG("%p memstore-redis subscriber respond with message", d->sub);
  
  cf.max_messages = d->chanhead->max_messages;
  cf.use_redis = 0;
  cf.buffer_timeout = msg->expires - ngx_time();
  
  //update_subscriber_last_msg_id(d->sub, msg);
  
  lastid = &d->chanhead->latest_msgid;
  
  assert(lastid->tagcount == 1 && lastid->tagcount == 1);
  if(lastid->time < msg->id.time || 
    (lastid->time == msg->id.time && lastid->tag.fixed[0] < msg->id.tag.fixed[0])) {
    memstore_ensure_chanhead_is_ready(d->chanhead, 1);
    nchan_store_chanhead_publish_message_generic(d->chanhead, msg, 0, &cf, NULL, NULL);
  }
  else {
    //meh, this message has already been delivered probably hopefully
  }
  
  //d->sub->last_msg_id = msg->id;
  
  return NGX_OK;
}

static ngx_int_t sub_respond_status(ngx_int_t status, void *ptr, sub_data_t *d) {
  DBG("%p memstore-redis subscriber respond with status", d->sub);
  switch(status) {
    case NGX_HTTP_GONE: //delete
    case NGX_HTTP_CLOSE: //delete
      nchan_store_memory.delete_channel(d->chid, NULL, NULL);
      break;
    
    default:
      //meh, no big deal.
      break;
  }
  
  return NGX_OK;
}

static ngx_int_t sub_notify_handler(ngx_int_t code, void *data, sub_data_t *d) {
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

subscriber_t *memstore_redis_subscriber_create(nchan_store_channel_head_t *chanhead) {
  subscriber_t               *sub;
  sub_data_t                 *d;
  d = ngx_alloc(sizeof(*d), ngx_cycle->log);
  sub = internal_subscriber_create_init(&sub_name, d, (callback_pt )sub_enqueue, (callback_pt )sub_dequeue, (callback_pt )sub_respond_message, (callback_pt )sub_respond_status, (callback_pt )sub_notify_handler);
  
  sub->destroy_after_dequeue = 0;
  sub->dequeue_after_response = 0;
  
  d->sub = sub;
  d->chanhead = chanhead;
  d->chid = &chanhead->id;
  /*
  ngx_memzero(&d->timeout_ev, sizeof(d->timeout_ev));
#if nginx_version >= 1008000
  d->timeout_ev.cancelable = 1;
#endif
  d->timeout_ev.handler = timeout_ev_handler;
  d->timeout_ev.data = d;
  d->timeout_ev.log = ngx_cycle->log;
  reset_timer(d);
  */
  DBG("%p created memstore-redis subscriber with privdata %p", d->sub, d);
  return sub;
}
