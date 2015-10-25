#include <nchan_module.h>
//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:LONGPOLL:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:LONGPOLL:" fmt, ##arg)
#include <assert.h>
#include "longpoll-private.h"

void memstore_fakeprocess_push(ngx_int_t slot);
void memstore_fakeprocess_push_random(void);
void memstore_fakeprocess_pop();
ngx_int_t memstore_slot();

static const subscriber_t new_longpoll_sub;

static void empty_handler() { }

static void sudden_abort_handler(subscriber_t *sub) {
#if FAKESHARD
  full_subscriber_t  *fsub = (full_subscriber_t  *)sub;
  memstore_fakeprocess_push(fsub->data.owner);
#endif
  sub->dequeue(sub);
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}

subscriber_t *longpoll_subscriber_create(ngx_http_request_t *r) {
  DBG("create for req %p", r);
  full_subscriber_t  *fsub;
  //TODO: allocate from pool (but not the request's pool)
  if((fsub = ngx_alloc(sizeof(*fsub), ngx_cycle->log)) == NULL) {
    ERR("Unable to allocate");
    return NULL;
  }
  ngx_memcpy(&fsub->sub, &new_longpoll_sub, sizeof(new_longpoll_sub));
  fsub->sub.data = &fsub->data;
  fsub->data.request = r;
  fsub->data.cln = NULL;
  fsub->data.finalize_request = 1;
  fsub->data.holding = 0;
  fsub->sub.cf = ngx_http_get_module_loc_conf(r, nchan_module);
  ngx_memzero(&fsub->data.timeout_ev, sizeof(fsub->data.timeout_ev));
  fsub->data.timeout_handler = empty_handler;
  fsub->data.timeout_handler_data = NULL;
  fsub->data.dequeue_handler = empty_handler;
  fsub->data.dequeue_handler_data = NULL;
  fsub->data.already_enqueued = 0;
  fsub->data.already_responded = 0;
  fsub->data.awaiting_destruction = 0;
  fsub->data.reserved = 0;
  
  fsub->data.owner = memstore_slot();
  
  //http request sudden close cleanup
  if((fsub->data.cln = ngx_http_cleanup_add(r, 0)) == NULL) {
    ERR("Unable to add request cleanup for longpoll subscriber");
    return NULL;
  }
  fsub->data.cln->data = fsub;
  fsub->data.cln->handler = (ngx_http_cleanup_pt )sudden_abort_handler;
  DBG("%p created for request %p", &fsub->sub, r);
  return &fsub->sub;
}

ngx_int_t longpoll_subscriber_destroy(subscriber_t *sub) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)sub;
  if(fsub->data.reserved > 0) {
    DBG("%p not ready to destroy (reserved for %i) for req %p", sub, fsub->data.reserved, fsub->data.request);
    fsub->data.awaiting_destruction = 1;
  }
  else {
    DBG("%p destroy for req %p", sub, fsub->data.request);
    ngx_free(fsub);
  }
  return NGX_OK;
}

static void ensure_request_hold(full_subscriber_t *fsub) {
  if(fsub->data.holding == 0) {
    DBG("hodl request %p", fsub->data.request);
    fsub->data.holding = 1;
    fsub->data.request->read_event_handler = ngx_http_test_reading;
    fsub->data.request->write_event_handler = ngx_http_request_empty_handler;
    fsub->data.request->main->count++; //this is the right way to hold and finalize the request... maybe
  }
}

static ngx_int_t longpoll_reserve(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  DBG("%p reserve for req %p", self, fsub->data.request);
  ensure_request_hold(fsub);
  fsub->data.reserved++;
  return NGX_OK;
}
static ngx_int_t longpoll_release(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  DBG("%p release for req %p", self, fsub->data.request);
  assert(fsub->data.reserved > 0);
  fsub->data.reserved--;
  if(fsub->data.awaiting_destruction == 1 && fsub->data.reserved == 0) {
    ngx_free(fsub);
    return NGX_ABORT;
  }
  else {
    return NGX_OK;
  }
}

static void timeout_ev_handler(ngx_event_t *ev) {
  full_subscriber_t *fsub = (full_subscriber_t *)ev->data;
#if FAKESHARD
  memstore_fakeprocess_push(fsub->data.owner);
#endif
  fsub->data.timeout_handler(&fsub->sub, fsub->data.timeout_handler_data);
  fsub->sub.dequeue_after_response = 1;
  fsub->sub.respond_status(&fsub->sub, NGX_HTTP_NOT_MODIFIED, NULL);
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}

ngx_int_t longpoll_enqueue(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  assert(fsub->data.already_enqueued == 0);
  DBG("%p enqueue", self);
  
  fsub->data.finalize_request = 1;
  
  fsub->data.already_enqueued = 1;
  ensure_request_hold(fsub);
  if(self->cf->subscriber_timeout > 0) {
    //add timeout timer
    //nextsub->ev should be zeroed;
    fsub->data.timeout_ev.handler = timeout_ev_handler;
    fsub->data.timeout_ev.data = fsub;
    fsub->data.timeout_ev.log = ngx_cycle->log;
    ngx_add_timer(&fsub->data.timeout_ev, self->cf->subscriber_timeout * 1000);
  }
  return NGX_OK;
}

static ngx_int_t longpoll_dequeue(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  if(fsub->data.timeout_ev.timer_set) {
    ngx_del_timer(&fsub->data.timeout_ev);
  }
  DBG("%p dequeue", self);
  fsub->data.dequeue_handler(&fsub->sub, fsub->data.dequeue_handler_data);
  if(self->destroy_after_dequeue) {
    longpoll_subscriber_destroy(self);
  }
  return NGX_OK;
}

static ngx_int_t dequeue_maybe(subscriber_t *self) {
  if(self->dequeue_after_response) {
    self->dequeue(self);
  }
  return NGX_OK;
}

static ngx_int_t finalize_maybe(subscriber_t *self, ngx_int_t rc) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  if(((subscriber_data_t *)self->data)->finalize_request) {
    DBG("finalize request %p", fsub->data.request);
    ngx_http_finalize_request(fsub->data.request, rc);
  }
  return NGX_OK;
}
static ngx_int_t abort_response(subscriber_t *sub, char *errmsg) {
  ERR("abort! %s", errmsg);
  finalize_maybe(sub, NGX_ERROR);
  dequeue_maybe(sub);
  return NGX_ERROR;
}

static ngx_int_t longpoll_respond_message(subscriber_t *self, nchan_msg_t *msg) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  ngx_int_t                  rc;
  char                      *err = NULL;
  DBG("%p respond req %p msg %p", self, fsub->data.request, msg);
  
  assert(fsub->data.already_responded != 1);
  
  fsub->data.already_responded = 1;
  
  //disable abort handler
  fsub->data.cln->handler = empty_handler;
  
  if((rc = nchan_respond_msg(fsub->data.request, msg, 0, &err)) == NGX_OK) {
    finalize_maybe(self, rc);
    dequeue_maybe(self);
    return rc;
  }
  else {
    return abort_response(self, err);
  }
}

static ngx_int_t longpoll_respond_status(subscriber_t *self, ngx_int_t status_code, const ngx_str_t *status_line) {
  ngx_http_request_t    *r = ((full_subscriber_t *)self)->data.request;
  DBG("%p respond req %p status %i", self, r, status_code);
  
  //disable abort handler
  ((full_subscriber_t *)self)->data.cln->handler = empty_handler;
  
  nchan_respond_status(r, status_code, status_line, 0);
  
  finalize_maybe(self, NGX_OK);
  dequeue_maybe(self);
  return NGX_OK;
}

static ngx_int_t longpoll_set_timeout_callback(subscriber_t *self, subscriber_callback_pt cb, void *privdata) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  fsub->data.timeout_handler = cb;
  fsub->data.timeout_handler_data = privdata;
  return NGX_OK;
}

static void request_cleanup_handler(subscriber_t *sub) {

}


static ngx_int_t longpoll_set_dequeue_callback(subscriber_t *self, subscriber_callback_pt cb, void *privdata) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  if(fsub->data.cln == NULL) {
    fsub->data.cln = ngx_http_cleanup_add(fsub->data.request, 0);
    fsub->data.cln->data = self;
    fsub->data.cln->handler = (ngx_http_cleanup_pt )request_cleanup_handler;
  }
  fsub->data.dequeue_handler = cb;
  fsub->data.dequeue_handler_data = privdata;
  return NGX_OK;
}

static const subscriber_t new_longpoll_sub = {
  &longpoll_enqueue,
  &longpoll_dequeue,
  &longpoll_respond_message,
  &longpoll_respond_status,
  &longpoll_set_timeout_callback,
  &longpoll_set_dequeue_callback,
  &longpoll_reserve,
  &longpoll_release,
  "longpoll",
  LONGPOLL,
  1, //deque after response
  1, //destroy after dequeue
  NULL
};