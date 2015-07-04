#include <ngx_http_push_module.h>
#include "internal.h"
#include <assert.h>

#define DEBUG_LEVEL NGX_LOG_INFO
#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:INTERNAL:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:INTERNAL:" fmt, ##arg)


static const subscriber_t new_internal_sub;

static ngx_int_t empty_callback(ngx_int_t code, void *ptr, void *d) {
  return NGX_OK;
}
static void sub_empty_callback(){ }

typedef struct {
  subscriber_t            sub;
  callback_pt             enqueue;
  callback_pt             dequeue;
  callback_pt             respond_message;
  callback_pt             respond_status;
  ngx_event_t             timeout_ev;
  subscriber_callback_pt  timeout_handler;
  void                   *timeout_handler_data;
  subscriber_callback_pt  dequeue_handler;
  void                   *dequeue_handler_data;
  void                   *privdata;
  unsigned                already_dequeued:1;
} full_subscriber_t;

ngx_int_t internal_subscriber_set_enqueue_handler(subscriber_t *sub, callback_pt handler) {
  ((full_subscriber_t *)sub)->enqueue = handler;
  return NGX_OK;
}
ngx_int_t internal_subscriber_set_dequeue_handler(subscriber_t *sub, callback_pt handler) {
  ((full_subscriber_t *)sub)->dequeue = handler;
  return NGX_OK;
}
ngx_int_t internal_subscriber_set_respond_message_handler(subscriber_t *sub, callback_pt handler) {
  ((full_subscriber_t *)sub)->respond_message = handler;
  return NGX_OK;
}
ngx_int_t internal_subscriber_set_respond_status_handler(subscriber_t *sub, callback_pt handler) {
  ((full_subscriber_t *)sub)->respond_status = handler;
  return NGX_OK;
}

subscriber_t *internal_subscriber_create(void *privdata) {
  DBG("create with privdata %p");
  full_subscriber_t               *fsub;
  static ngx_http_push_loc_conf_t  dummy_config = {0};
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
  fsub->privdata = privdata;
  
  fsub->sub.pool = ngx_create_pool(NGX_HTTP_PUSH_DEFAULT_INTERNAL_SUBSCRIBER_POOL_SIZE, ngx_cycle->log);
  fsub->sub.cf = &dummy_config;
  fsub->already_dequeued = 0;
  
  fsub->timeout_handler = sub_empty_callback;
  fsub->dequeue_handler = sub_empty_callback;
  fsub->dequeue_handler_data = NULL;
  return &fsub->sub;
}

ngx_int_t internal_subscriber_destroy(subscriber_t *sub) {
  DBG("destroy %p", sub);
  ngx_free(sub);
  return NGX_OK;
}


static void reset_timer(full_subscriber_t *f) {
  if(f->sub.cf->subscriber_timeout > 0) {
    if(f->timeout_ev.timer_set) {
      ngx_del_timer(&f->timeout_ev);
    }
    ngx_add_timer(&f->timeout_ev, f->sub.cf->subscriber_timeout * 1000);
  }
}

static void timeout_ev_handler(ngx_event_t *ev) {
  full_subscriber_t *fsub = (full_subscriber_t *)ev->data;
  DBG("%p timeout", fsub);
  fsub->timeout_handler(&fsub->sub, fsub->timeout_handler_data);
  fsub->sub.dequeue_after_response = 1;
  fsub->sub.respond_status(&fsub->sub, NGX_HTTP_NOT_MODIFIED, NULL);
}

static ngx_int_t internal_enqueue(subscriber_t *self) {
  full_subscriber_t   *fsub = (full_subscriber_t *)self;
  DBG("%p enqueue", self);
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
  full_subscriber_t   *f = (full_subscriber_t *)self;
  assert(!f->already_dequeued);
  f->already_dequeued = 1;
  DBG("%p dequeue sub", self);
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

static ngx_int_t internal_respond_message(subscriber_t *self, ngx_http_push_msg_t *msg) {
  full_subscriber_t   *f = (full_subscriber_t *)self;
  DBG("%p respond msg %p", self, msg);
  f->respond_message(NGX_OK, msg, f->privdata);
  reset_timer(f);
  return dequeue_maybe(self);
}

static ngx_int_t internal_respond_status(subscriber_t *self, ngx_int_t status_code, const ngx_str_t *status_line) {
  full_subscriber_t   *f = (full_subscriber_t *)self;
  DBG("%p status %i", self, status_code);
  switch(status_code) {
    case NGX_HTTP_NO_CONTENT: //message expired
    case NGX_HTTP_GONE: //delete
    case NGX_HTTP_CLOSE: //delete
    case NGX_HTTP_NOT_MODIFIED: //timeout?
    case NGX_HTTP_NOT_FOUND: //not found triggers a dequeue, too?
    case NGX_HTTP_FORBIDDEN:
    case NGX_HTTP_INTERNAL_SERVER_ERROR:
      self->dequeue_after_response = 1;
      break;
  }
  
  f->respond_status(status_code, (void *)status_line, f->privdata);
  reset_timer(f);
  return dequeue_maybe(self);
}

static ngx_int_t internal_set_timeout_callback(subscriber_t *self, subscriber_callback_pt cb, void *privdata) {

  full_subscriber_t   *f = (full_subscriber_t *)self;
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
  full_subscriber_t   *f = (full_subscriber_t *)self;
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
  "internal",
  INTERNAL,
  0, //stick around after response
  1, //destroy after dequeue
  NULL
};
