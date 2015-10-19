#include <nchan_module.h>
#include "internal.h"
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:INTERNAL:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:INTERNAL:" fmt, ##arg)

void memstore_fakeprocess_push(ngx_int_t slot);
void memstore_fakeprocess_push_random(void);
void memstore_fakeprocess_pop();
ngx_int_t memstore_slot();

static const subscriber_t new_internal_sub;

static ngx_int_t empty_callback(ngx_int_t code, void *ptr, void *d) {
  return NGX_OK;
}
static void sub_empty_callback(){ }

ngx_int_t internal_subscriber_set_enqueue_handler(subscriber_t *sub, callback_pt handler) {
  ((internal_subscriber_t *)sub)->enqueue = handler;
  return NGX_OK;
}
ngx_int_t internal_subscriber_set_dequeue_handler(subscriber_t *sub, callback_pt handler) {
  ((internal_subscriber_t *)sub)->dequeue = handler;
  return NGX_OK;
}
ngx_int_t internal_subscriber_set_respond_message_handler(subscriber_t *sub, callback_pt handler) {
  ((internal_subscriber_t *)sub)->respond_message = handler;
  return NGX_OK;
}
ngx_int_t internal_subscriber_set_respond_status_handler(subscriber_t *sub, callback_pt handler) {
  ((internal_subscriber_t *)sub)->respond_status = handler;
  return NGX_OK;
}

subscriber_t *internal_subscriber_create(const char* name, void *privdata) {
  internal_subscriber_t               *fsub;
  static nchan_loc_conf_t  dummy_config = {0};
  dummy_config.buffer_timeout = 0;
  dummy_config.max_messages = -1;
  
  if((fsub = ngx_alloc(sizeof(*fsub), ngx_cycle->log)) == NULL) {
    ERR("Unable to allocate");
    return NULL;
  }
  
  fsub->enqueue = empty_callback;
  fsub->dequeue = empty_callback;
  fsub->respond_message = empty_callback;
  fsub->respond_status = empty_callback;
  ngx_memcpy(&fsub->sub, &new_internal_sub, sizeof(new_internal_sub));
  fsub->sub.name= (name == NULL ? "internal" : name);
  DBG("%p create %s with privdata %p", fsub, fsub->sub.name, privdata);
  fsub->privdata = privdata;
  
  fsub->sub.cf = &dummy_config;
  fsub->already_dequeued = 0;
  fsub->awaiting_destruction = 0;
  fsub->reserved = 0;
  
  fsub->timeout_handler = sub_empty_callback;
  fsub->dequeue_handler = sub_empty_callback;
  fsub->dequeue_handler_data = NULL;
  
  fsub->owner = memstore_slot();
  return &fsub->sub;
}

ngx_int_t internal_subscriber_destroy(subscriber_t *sub) {
  internal_subscriber_t  *fsub = (internal_subscriber_t  *)sub;
  if(fsub->reserved > 0) {
    DBG("%p not ready to destroy (reserved for %i)", sub, fsub->reserved);
    fsub->awaiting_destruction = 1;
  }
  else {
    DBG("%p (%s) destroy", sub, fsub->sub.name);
    ngx_free(fsub);
  }
  return NGX_OK;
}

static ngx_int_t internal_reserve(subscriber_t *self) {
  internal_subscriber_t  *fsub = (internal_subscriber_t  *)self;
  DBG("%p )%s) reserve", self, fsub->sub.name);
  fsub->reserved++;
  return NGX_OK;
}
static ngx_int_t internal_release(subscriber_t *self) {
  internal_subscriber_t  *fsub = (internal_subscriber_t  *)self;
  DBG("%p (%s) release", self, fsub->sub.name);
  fsub->reserved--;
  if(fsub->awaiting_destruction == 1 && fsub->reserved == 0) {
    DBG("%p (%s) free", self, fsub->sub.name);
    ngx_free(fsub);
    return NGX_ABORT;
  }
  else {
    return NGX_OK;
  }
}

void *internal_subscriber_get_privdata(subscriber_t *sub) {
  internal_subscriber_t               *fsub = (internal_subscriber_t *)sub;
  return fsub->privdata;
}

static void reset_timer(internal_subscriber_t *f) {
  if(f->sub.cf->subscriber_timeout > 0) {
    if(f->timeout_ev.timer_set) {
      ngx_del_timer(&f->timeout_ev);
    }
    ngx_add_timer(&f->timeout_ev, f->sub.cf->subscriber_timeout * 1000);
  }
}

static void timeout_ev_handler(ngx_event_t *ev) {
  internal_subscriber_t *fsub = (internal_subscriber_t *)ev->data;
#if FAKESHARD
  memstore_fakeprocess_push(fsub->owner);
#endif
  DBG("%p (%s) timeout", fsub, fsub->sub.name);
  fsub->timeout_handler(&fsub->sub, fsub->timeout_handler_data);
  fsub->sub.dequeue_after_response = 1;
  fsub->sub.respond_status(&fsub->sub, NGX_HTTP_NOT_MODIFIED, NULL);
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}

static ngx_int_t internal_enqueue(subscriber_t *self) {
  internal_subscriber_t   *fsub = (internal_subscriber_t *)self;
  DBG("%p (%s) enqueue", self, fsub->sub.name);
  if(self->cf->subscriber_timeout > 0 && !fsub->timeout_ev.timer_set) {
    //add timeout timer
    //nextsub->ev should be zeroed;
    fsub->timeout_ev.handler = timeout_ev_handler;
    fsub->timeout_ev.data = fsub;
    fsub->timeout_ev.log = ngx_cycle->log;
    reset_timer(fsub);
  }
  fsub->enqueue(fsub->sub.cf->buffer_timeout, NULL, fsub->privdata);
  return NGX_OK;
}

static ngx_int_t internal_dequeue(subscriber_t *self) {
  internal_subscriber_t   *f = (internal_subscriber_t *)self;
  assert(!f->already_dequeued);
  f->already_dequeued = 1;
  DBG("%p (%s) dequeue sub", self, f->sub.name);
  f->dequeue(NGX_OK, NULL, f->privdata);
  f->dequeue_handler(self, f->dequeue_handler_data);
  if(self->cf->subscriber_timeout > 0 && f->timeout_ev.timer_set) {
    ngx_del_timer(&f->timeout_ev);
  }
  if(self->destroy_after_dequeue) {
    internal_subscriber_destroy(self);
  }
  return NGX_OK;
}

static ngx_int_t dequeue_maybe(subscriber_t *self) {
  if(self->dequeue_after_response) {
    self->dequeue(self);
  }
  return NGX_OK;
}

static ngx_int_t internal_respond_message(subscriber_t *self, NCHAN_msg_t *msg) {
  internal_subscriber_t   *f = (internal_subscriber_t *)self;
  DBG("%p (%s) respond msg %p", self, f->sub.name, msg);
  f->respond_message(NGX_OK, msg, f->privdata);
  reset_timer(f);
  return dequeue_maybe(self);
}

static ngx_int_t internal_respond_status(subscriber_t *self, ngx_int_t status_code, const ngx_str_t *status_line) {
  internal_subscriber_t   *f = (internal_subscriber_t *)self;
  DBG("%p status %i", self, status_code);
  if(status_code == NGX_HTTP_GONE) {
    self->dequeue_after_response = 1;
  }
  f->respond_status(status_code, (void *)status_line, f->privdata);
  reset_timer(f);
  return dequeue_maybe(self);
}

static ngx_int_t internal_set_timeout_callback(subscriber_t *self, subscriber_callback_pt cb, void *privdata) {

  internal_subscriber_t   *f = (internal_subscriber_t *)self;
  if(cb != NULL) {
    DBG("%p set timeout handler to %p", self, cb);
    f->timeout_handler = cb;
  }
  if(privdata != NULL) {
    DBG("%p set timeout handler data to %p", self, privdata);
    f->timeout_handler_data = privdata;
  }
  return NGX_OK;
}

static ngx_int_t internal_set_dequeue_callback(subscriber_t *self, subscriber_callback_pt cb, void *privdata) {
  internal_subscriber_t   *f = (internal_subscriber_t *)self;
  if(cb != NULL) {
    DBG("%p set dequeue handler to %p", self, cb);
    f->dequeue_handler = cb;
  }
  if(privdata != NULL) {
    DBG("%p set dequeue handler data to %p", self, cb);
    f->dequeue_handler_data = privdata;
  }
  return NGX_OK;
}

ngx_int_t internal_subscriber_set_name(subscriber_t *self, const char *name) {
  self->name = name;
  return NGX_OK;
}

static const subscriber_t new_internal_sub = {
  &internal_enqueue,
  &internal_dequeue,
  &internal_respond_message,
  &internal_respond_status,
  &internal_set_timeout_callback,
  &internal_set_dequeue_callback,
  &internal_reserve,
  &internal_release,
  "internal",
  INTERNAL,
  0, //stick around after response
  1, //destroy after dequeue
  NULL
};
