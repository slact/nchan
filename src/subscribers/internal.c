#include <ngx_http_push_module.h>
#define DEBUG_LEVEL NGX_LOG_INFO
#define DBG(...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, __VA_ARGS__)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)


static const subscriber_t new_internal_sub;

static ngx_int_t empty_callback(ngx_int_t code, void *ptr, void *d) {
  return NGX_OK;
}

typedef struct {
  subscriber_t        sub;
  callback_pt         enqueue;
  callback_pt         dequeue;
  callback_pt         respond_message;
  callback_pt         respond_status;
  ngx_http_cleanup_t  cln;
  void               *privdata;
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
  DBG("internal subscriver create with privdata %p");
  full_subscriber_t  *fsub;
  if((fsub = ngx_alloc(sizeof(*fsub), ngx_cycle->log)) == NULL) {
    ERR("Unable to allocate internal subscriber");
    return NULL;
  }
  fsub->enqueue = empty_callback;
  fsub->dequeue = empty_callback;
  fsub->respond_message = empty_callback;
  fsub->respond_status = empty_callback;
  ngx_memcpy(&fsub->sub, &new_internal_sub, sizeof(new_internal_sub));
  fsub->privdata = privdata;
  return &fsub->sub;
}

ngx_int_t internal_subscriber_destroy(subscriber_t *sub) {
  DBG("internal subscriver destroy %p for req %p", sub, sub->request);
  full_subscriber_t  *f = (full_subscriber_t *) sub;
  if(f->cln.data != NULL) {
    ngx_free(f->cln.data);
  }
  ngx_free(sub);
  return NGX_OK;
}
static ngx_int_t internal_enqueue(subscriber_t *self, ngx_int_t timeout) {
  full_subscriber_t   *f = (full_subscriber_t *)self;
  DBG("internal subscriber enqueue sub %p req", self);
  return f->enqueue(timeout, NULL, f->privdata);
  return NGX_OK;
}

static ngx_int_t internal_dequeue(subscriber_t *self) {
  full_subscriber_t   *f = (full_subscriber_t *)self;
  DBG("longpoll dequeue sub %p", self);
  f->dequeue(NGX_OK, NULL, f->privdata);
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
  DBG("internal subscriber respond sub %p msg %p", self, msg);
  f->respond_message(NGX_OK, msg, f->privdata);
  return dequeue_maybe(self);
}

static ngx_int_t internal_respond_status(subscriber_t *self, ngx_int_t status_code, const ngx_str_t *status_line) {
  full_subscriber_t   *f = (full_subscriber_t *)self;
  DBG("longpoll respond sub %p req %p status %i", self, self->request, status_code);
  f->respond_message(status_code, (void *)status_line, f->privdata);
  if(f->cln.handler) {
    f->cln.handler(f->cln.data);
    //don't run cln.next
  }
  return dequeue_maybe(self);
}

static ngx_http_cleanup_t *internal_add_next_response_cleanup(subscriber_t *self, size_t privdata_size) {
  full_subscriber_t   *f = (full_subscriber_t *)self;
  DBG("longpoll %p req %p add_response_cleanup", self, self->request);
  if(privdata_size > 0) {
    f->cln.data = ngx_alloc(privdata_size, ngx_cycle->log);
  }
  if(f->cln.handler) {
    f->cln.handler(f->cln.data);
    //don't run cln.next
  }
  return &f->cln;
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
  &internal_add_next_response_cleanup,
  "internal",
  INTERNAL,
  0, //stick around after response
  1, //destroy after dequeue
  NULL
};
