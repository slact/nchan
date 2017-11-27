#include <nchan_module.h>
#include <subscribers/common.h>
#include <store/memory/ipc.h>
#include <store/memory/store-private.h>
#include <store/memory/ipc-handlers.h>
#include "internal.h"
#include "memstore_ipc.h"
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:MEM-IPC:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:MEM-IPC:" fmt, ##arg)


typedef struct sub_data_s sub_data_t;

struct sub_data_s {
  subscriber_t                 *sub;
  ngx_str_t                    *chid;
  ngx_int_t                     originator;
  ngx_int_t                     unhooked;
  ngx_int_t                     owner;
  void                         *foreign_chanhead;
  ngx_event_t                   timeout_ev;
}; //sub_data_t

static ngx_int_t empty_callback(){
  return NGX_OK;
}

static ngx_int_t sub_enqueue(ngx_int_t status, void *ptr, sub_data_t *d) {
  DBG("%p (%V) memstore subsriber enqueued ok", d->sub, d->chid);
  return NGX_OK;
}

static ngx_int_t sub_dequeue(ngx_int_t status, void *ptr, sub_data_t* d) {
  ngx_int_t           ret;
  internal_subscriber_t  *fsub = (internal_subscriber_t  *)d->sub;
  DBG("%p (%V) memstore subscriber dequeue: notify owner", d->sub, d->chid);
  if(d->timeout_ev.timer_set) {
    ngx_del_timer(&d->timeout_ev);
  }
  if(!d->unhooked) {
    ret = memstore_ipc_send_unsubscribed(d->originator, d->chid, NULL);
  }
  else {
    ret = NGX_OK;
  }
  
  if(fsub->sub.reserved > 0) {
    DBG("%p (%V) not ready to destroy (reserved for %i)", fsub, d->chid, fsub->sub.reserved);
    fsub->awaiting_destruction = 1;
  }
  else {
    DBG("%p (%V) destroy", fsub, d->chid);
  }
  
  return ret;
}

static ngx_int_t sub_respond_message(ngx_int_t status, void *ptr, sub_data_t* d) {
  ngx_int_t               rc;
  nchan_msg_t            *msg = (nchan_msg_t *) ptr;
  internal_subscriber_t  *fsub = (internal_subscriber_t  *)d->sub;
  
  //DBG("%p (%V) memstore subscriber (lastid %V) respond with message %V (lastid %V)", d->sub, d->chid, msgid_to_str(&d->sub->last_msg_id), msgid_to_str(&msg->id), msgid_to_str(&msg->prev_id));
  
  //update_subscriber_last_msg_id(d->sub, msg);
  if(!d->unhooked) {
    rc = memstore_ipc_send_publish_message(d->originator, d->chid, msg, d->sub->cf, empty_callback, NULL);
  }
  else {
    rc = NGX_OK;
  }
  //no multi-ids allowed here
  assert(msg->id.tagcount == 1);
    
  fsub->sub.last_msgid = msg->id;
  
  return rc;
}

static ngx_int_t sub_respond_status(ngx_int_t status, void *ptr, sub_data_t *d) {
  const ngx_str_t   *status_line = (const ngx_str_t *)ptr;
  if((status == NGX_HTTP_NO_CONTENT || status == NGX_HTTP_NOT_MODIFIED) && !status_line) {
    //don't care, ignore
    return NGX_OK;
  }
  
  if(!ptr) {
    switch(status) {
      case NGX_HTTP_GONE: //delete
        status_line = &NCHAN_HTTP_STATUS_410;
        break;
      case NGX_HTTP_CONFLICT:
        status_line = &NCHAN_HTTP_STATUS_409;
        break;
      case NGX_HTTP_NO_CONTENT:
        assert(0);
        break;
      case NGX_HTTP_CLOSE: //delete
        break;
      case NGX_HTTP_NOT_MODIFIED: //timeout?
        break;
      case NGX_HTTP_FORBIDDEN:
        break;
      default:
        ERR("unknown status %i", status);
    }
  }
  if(!d->unhooked) {
    return memstore_ipc_send_publish_status(d->originator, d->chid, status, status_line, empty_callback, NULL);
  }
  else {
    return NGX_OK;
  }
}
static void reset_timer(sub_data_t *data) {
  if(data->timeout_ev.timer_set) {
    ngx_del_timer(&data->timeout_ev);
  }
  ngx_add_timer(&data->timeout_ev, MEMSTORE_IPC_SUBSCRIBER_TIMEOUT * 1000);
}

ngx_int_t memstore_ipc_subscriber_keepalive_renew(subscriber_t *sub) {
  sub_data_t *d = internal_subscriber_get_privdata(sub);
  reset_timer(d);
  return NGX_OK;
}

static void timeout_ev_handler(ngx_event_t *ev) {
  sub_data_t *d = (sub_data_t *)ev->data;
  
  if(!ev->timedout) {
    return;
  }
  ev->timedout = 0;
  
#if FAKESHARD
  memstore_fakeprocess_push(d->owner);
#endif
  DBG("%p (%V), timeout event. Ping originator to see if still needed.", d->sub, d->chid);
  if(!d->unhooked) {
    memstore_ipc_send_memstore_subscriber_keepalive(d->originator, d->chid, d->sub, d->foreign_chanhead);
  }
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}

static ngx_str_t  sub_name = ngx_string("memstore-ipc");

subscriber_t *memstore_ipc_subscriber_create(ngx_int_t originator_slot, ngx_str_t *chid, nchan_loc_conf_t *cf, void* foreign_chanhead) { //, nchan_channel_head_t *local_chanhead) {
  static  nchan_msg_id_t      newest_msgid = NCHAN_NEWEST_MSGID;
  sub_data_t                 *d;
  subscriber_t               *sub;
  
  assert(originator_slot != memstore_slot());
  sub = internal_subscriber_create_init(&sub_name, cf, sizeof(*d), (void **)&d, (callback_pt )sub_enqueue, (callback_pt )sub_dequeue, (callback_pt )sub_respond_message, (callback_pt )sub_respond_status, NULL, NULL);
  
  sub->last_msgid = newest_msgid;
  sub->destroy_after_dequeue = 1;
  d->sub = sub;
  d->chid = chid;
  d->originator = originator_slot;
  d->unhooked = 0;
  assert(foreign_chanhead != NULL);
  d->foreign_chanhead = foreign_chanhead;
  //d->chanhead = local_chanhead;
  d->owner = memstore_slot();

  ngx_memzero(&d->timeout_ev, sizeof(d->timeout_ev));
  nchan_init_timer(&d->timeout_ev, timeout_ev_handler, d);

  reset_timer(d);
  DBG("%p (%V) memstore-ipc subscriber created with privdata %p", d->sub, d->chid, d);
  return sub;
}

ngx_int_t memstore_ipc_subscriber_unhook(subscriber_t *sub) {
  sub_data_t *d = internal_subscriber_get_privdata(sub);
  d->unhooked = 1;
  return NGX_OK;
}
