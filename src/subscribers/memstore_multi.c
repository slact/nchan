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
  subscriber_t                 *sub;
  nchan_store_channel_head_t   *multi_chanhead;
  ngx_str_t                    *chid;
  ngx_int_t                     n;
  ngx_event_t                   timeout_ev;
} sub_data_t;

/*
static ngx_int_t empty_callback(){
  return NGX_OK;
}
*/

static ngx_int_t sub_enqueue(ngx_int_t timeout, void *ptr, sub_data_t *d) {
  DBG("%p subsriber enqueued ok", d->sub);
  assert(d->multi_chanhead->multi_waiting > 0);
  d->multi_chanhead->multi_waiting --;
  if(d->multi_chanhead->multi_waiting == 0) {
    d->multi_chanhead->status = READY;
    d->multi_chanhead->spooler.fn->handle_channel_status_change(&d->multi_chanhead->spooler);
  }
  
  return NGX_OK;
}

static ngx_int_t sub_dequeue(ngx_int_t status, void *ptr, sub_data_t* d) {
  return NGX_OK;
}

static ngx_int_t sub_respond_message(ngx_int_t status, void *ptr, sub_data_t* d) {
  //nchan_msg_t       *msg = (nchan_msg_t *) ptr;
  //nchan_loc_conf_t   cf;
  //nchan_msg_id_t    *lastid;    
  //DBG("%p subscriber respond with message", d->sub);
  
  //this is going to be a little complicated
  
  //d->sub->last_msg_id = msg->id;
  
  return NGX_OK;
}

static ngx_int_t sub_respond_status(ngx_int_t status, void *ptr, sub_data_t *d) {
  DBG("%p subscriber respond with status", d->sub);
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
  
  sub_data_t                 *d;
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
  
  d->sub = sub;
  d->multi_chanhead = chanhead;
  d->n = n;
  
  d->chid = &chanhead->multi[n].id;;
  
  
  DBG("%p created with privdata %p", d->sub, d);
  return sub;
}
