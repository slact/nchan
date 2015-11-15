#include <nchan_module.h>
#include "../store/memory/shmem.h"
#include "../store/memory/ipc.h"
#include "../store/memory/store-private.h"
#include "../store/memory/ipc-handlers.h"

#include "../store/memory/store.h"

#include "internal.h"
#include "memstore_multi.h"
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:MEM-MULTI:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:MEM-MULTI:" fmt, ##arg)


typedef struct {
  nchan_store_channel_head_t   *multi_chanhead;
  nchan_store_multi_t          *multi;
  ngx_int_t                     n;
  ngx_event_t                   timeout_ev;
} sub_data_t;

/*
static ngx_int_t empty_callback(){
  return NGX_OK;
}
*/

static ngx_int_t sub_enqueue(ngx_int_t timeout, void *ptr, sub_data_t *d) {
  DBG("%p enqueued (%p %V %i) %V", d->multi->sub, d->multi_chanhead, &d->multi_chanhead->id, d->n, &d->multi->id);
  assert(d->multi_chanhead->multi_waiting > 0);
  d->multi_chanhead->multi_waiting --;
  if(d->multi_chanhead->multi_waiting == 0) {
    memstore_ready_chanhead_unless_stub(d->multi_chanhead);
  }
  
  return NGX_OK;
}

static ngx_int_t sub_dequeue(ngx_int_t status, void *ptr, sub_data_t* d) {
  DBG("%p dequeued (%p %V %i) %V", d->multi->sub, d->multi_chanhead, &d->multi_chanhead->id, d->n, &d->multi->id);
  d->multi_chanhead->status = WAITING;
  d->multi_chanhead->multi_waiting++;
  d->multi->sub = NULL;
  return NGX_OK;
}

static ngx_int_t sub_respond_message(ngx_int_t status, nchan_msg_t *msg, sub_data_t* d) {
  nchan_msg_t       remsg;
  nchan_msg_id_t    *last_msgid;
  
  //remsg = ngx_alloc(sizeof(*remsg), ngx_cycle->log);
  //assert(remsg);
  
  ngx_memcpy(&remsg, msg, sizeof(*msg));
  remsg.shared = 0;
  remsg.temp_allocd = 0;
  
  remsg.id.tag = d->n + remsg.id.tag * (d->multi_chanhead->multi_count); //this is how we multiplex
  
  assert(d->multi_chanhead->stub == 0);
  memstore_ensure_chanhead_is_ready(d->multi_chanhead);
  
  last_msgid = &d->multi_chanhead->spooler.prev_msg_id;
  assert(last_msgid->time <= remsg.id.time 
    || (last_msgid->time == remsg.id.time && last_msgid->tag <= remsg.id.tag ));
  
  remsg.prev_id = *last_msgid;
  
  ERR("%p respond with transformed message %p %i:%i (%p %V %i) %V", d->multi->sub, &remsg, remsg.id.time, remsg.id.tag, d->multi_chanhead, &d->multi_chanhead->id, d->n, &d->multi->id);
  
  nchan_memstore_publish_generic(d->multi_chanhead, &remsg, 0, NULL);
  
  return NGX_OK;
}

static ngx_int_t sub_respond_status(ngx_int_t status, void *ptr, sub_data_t *d) {
  DBG("%p subscriber respond with status (%p %V %i) %V", d->multi->sub, d->multi_chanhead, &d->multi_chanhead->id, d->n, &d->multi->id);
  switch(status) {
    case NGX_HTTP_GONE: //delete
    case NGX_HTTP_CLOSE: //delete
      //nchan_store_memory.delete_channel(d->chid, NULL, NULL);
      //TODO: change status to NOTREADY and whatnot
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
static void timeout_ev_handler(ngx_event_t *ev) {
  sub_data_t *d = (sub_data_t *)ev->data;
  
#if FAKESHARD
  memstore_fakeprocess_push(d->owner);
#endif
  DBG("%p timeout event. Ping originator to see if still needed.", d->sub);
  d->sub->reserve(d->sub);
  memstore_ipc_send_memstore_subscriber_keepalive(d->originator, d->chid, d->sub, d->foreign_chanhead, keepalive_reply_handler, d);
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}
*/

subscriber_t *memstore_multi_subscriber_create(nchan_store_channel_head_t *chanhead, uint8_t n) {
  sub_data_t                  *d;
  nchan_store_channel_head_t  *target_ch;
  
  nchan_loc_conf_t cf;
  
  cf.use_redis = chanhead->use_redis;
  
  d = ngx_alloc(sizeof(*d), ngx_cycle->log);
  if(d == NULL) {
    ERR("couldn't allocate memstore-redis subscriber data");
    return NULL;
  }
  
  subscriber_t *sub = internal_subscriber_create("memstore-redis", d);
  internal_subscriber_set_enqueue_handler(sub, (callback_pt )sub_enqueue);
  internal_subscriber_set_dequeue_handler(sub, (callback_pt )sub_dequeue);
  internal_subscriber_set_respond_message_handler(sub, (callback_pt )sub_respond_message);
  internal_subscriber_set_respond_status_handler(sub, (callback_pt )sub_respond_status);
  internal_subscriber_set_notify_handler(sub, (callback_pt )sub_notify_handler);
  
  sub->destroy_after_dequeue = 1;
  sub->dequeue_after_response = 0;

  d->multi = &chanhead->multi[n];
  d->multi->sub = sub;
  d->multi_chanhead = chanhead;
  d->n = n;
  chanhead->multi_waiting++;
  
  target_ch = nchan_memstore_get_chanhead(&d->multi->id, &cf);
  assert(target_ch);
  target_ch->spooler.fn->add(&target_ch->spooler, sub);
  
  DBG("%p created with privdata %p", d->multi->sub, d);
  return sub;
}
