#include <nchan_module.h>
#include <subscribers/common.h>
#include "internal.h"
#include "getmsg_proxy.h"
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:PROXY:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:PROXY:" fmt, ##arg)


typedef struct sub_data_s sub_data_t;

struct sub_data_s {
  subscriber_t                 *sub;
  ngx_str_t                    *chid;
  ngx_event_t                   timeout_ev;
  callback_pt                   cb;
  void                         *pd;
}; //sub_data_t

static ngx_int_t sub_enqueue(ngx_int_t status, void *ptr, sub_data_t *d) {
  DBG("%p enqueued ok", d->sub);
  return NGX_OK;
}

static ngx_int_t sub_dequeue(ngx_int_t status, void *ptr, sub_data_t* d) {
  internal_subscriber_t  *fsub = (internal_subscriber_t  *)d->sub;
  DBG("%p dequeue:", d->sub);

  //TODO: make sure proxy target is already taken care of
  
  if(fsub->sub.reserved > 0) {
    DBG("%p  not ready to destroy (reserved for %i)", fsub, fsub->sub.reserved);
    fsub->awaiting_destruction = 1;
  }
  else {
    DBG("%p destroy", fsub);
  }
  
  return NGX_OK;
}

static ngx_int_t sub_respond_message(ngx_int_t status, void *ptr, sub_data_t* d) {
  DBG("%p forwarding msg", d->sub);
  nchan_msg_t            *msg = (nchan_msg_t *) ptr;
  d->cb(MSG_FOUND, msg, d->pd);
  d->cb = NULL;
  //TODO : dequeue
  return NGX_OK;
}

static ngx_int_t sub_respond_status(ngx_int_t status, void *ptr, sub_data_t *d) {
  assert(d->cb);
  if(!ptr) {
    switch(status) {
      case NGX_HTTP_GONE: //delete
        DBG("%p forwarding MSG_EXPIRED", d->sub);
        d->cb(MSG_EXPIRED, NULL, d->pd);
        d->cb = NULL;
        break;
      case NGX_HTTP_CONFLICT:
      case NGX_HTTP_CLOSE: //delete
      case NGX_HTTP_FORBIDDEN:
      case NGX_HTTP_NOT_FOUND:
      case NGX_HTTP_REQUEST_TIMEOUT:
        DBG("%p forwarding MSG_NOTFOUND", d->sub);
        d->cb(MSG_NOTFOUND, NULL, d->pd);
        d->cb = NULL;
        break;
      case NGX_HTTP_NO_CONTENT:
        //translate back to MSG request status code
        DBG("%p forwarding MSG_EXPECTED", d->sub);
        d->cb(MSG_EXPECTED, NULL, d->pd);
        d->cb = NULL;
        break;
      case NGX_HTTP_NOT_MODIFIED: //does this even ever happen? I don't think so.
        assert(0);
        break;
      default:
        ERR("unknown status %i", status);
    }
  }
  return NGX_OK;
}

static ngx_str_t sub_name = ngx_string("getmsg-proxy");

subscriber_t *getmsg_proxy_subscriber_create(nchan_msg_id_t *msgid, callback_pt cb, void *pd) {
  sub_data_t                 *d;
  subscriber_t               *sub;
  
  sub = internal_subscriber_create_init(&sub_name, NULL /*no config*/, sizeof(*d), (void **)&d, (callback_pt )sub_enqueue, (callback_pt )sub_dequeue, (callback_pt )sub_respond_message, (callback_pt )sub_respond_status, NULL, NULL);
  
  
  DBG("created new getmsg_proxy sub %p", sub);
  nchan_copy_new_msg_id(&sub->last_msgid, msgid);
  sub->destroy_after_dequeue = 1;
  sub->dequeue_after_response = 1;
  d->sub = sub;
  d->cb = cb;
  d->pd = pd;
  return sub;
}
