#include <ngx_http_push_module.h>
#include "../store/memory/shmem.h"
#include "../store/memory/ipc.h"
#include "../store/memory/store-private.h"
#include "../store/memory/ipc-handlers.h"
#include "internal.h"
#define DEBUG_LEVEL NGX_LOG_INFO
#define DBG(...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, __VA_ARGS__)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)

typedef struct {
  ngx_str_t   *chid;
  ngx_int_t    originator;
  void        *foreign_chanhead;
} sub_data_t;

static const subscriber_t new_memstore_sub;

static ngx_int_t empty_callback(){
  return NGX_OK;
}

static ngx_int_t sub_enqueue(ngx_int_t timeout, void *ptr, sub_data_t *d) {
  DBG("memstore subsriber enqueued ok");
  return NGX_OK;
}

static ngx_int_t sub_dequeue(ngx_int_t status, void *ptr, sub_data_t* d) {
  DBG("memstore subscriber dequeue: notify owner");
  return memstore_ipc_send_publish_status(d->originator, d->chid, NGX_HTTP_GONE, NULL);
}

static ngx_int_t sub_respond_message(ngx_int_t status, void *ptr, sub_data_t* d) {
  DBG("memstore subscriber respond with message");
  ngx_http_push_msg_t     *msg = (ngx_http_push_msg_t *) ptr;
  return memstore_ipc_send_publish_message(d->originator, d->chid, msg, 50, 0, 0, empty_callback, NULL);
}

static ngx_int_t sub_respond_status(ngx_int_t status, void *ptr, sub_data_t *d) {
  DBG("memstore subscriber respond with status");
  switch(status) {
    case NGX_HTTP_NO_CONTENT: //message expired
    case NGX_HTTP_GONE: //delete
    case NGX_HTTP_CLOSE: //delete
    case NGX_HTTP_NOT_MODIFIED: //timeout?
    case NGX_HTTP_FORBIDDEN:
      //do nothing, will be dequeued automatically
      break;
    default:
      ERR("unknown status %i", status);
  }
  return NGX_OK;
}

subscriber_t *memstore_subscriber_create(ngx_int_t originator_slot, ngx_str_t *chid, void* foreign_chanhead) {
  DBG("memstore subscriver create with privdata %p");
  sub_data_t    *d = ngx_alloc(sizeof(*d), ngx_cycle->log);
  if(d == NULL) {
    ERR("couldn't allocate memstore subscriber data");
    return NULL;
  }
  subscriber_t *sub = internal_subscriber_create(d);
  internal_subscriber_set_name(sub, "memstore-ipc");
  internal_subscriber_set_enqueue_handler(sub, (callback_pt )sub_enqueue);
  internal_subscriber_set_dequeue_handler(sub, (callback_pt )sub_dequeue);
  internal_subscriber_set_respond_message_handler(sub, (callback_pt )sub_respond_message);
  internal_subscriber_set_respond_status_handler(sub, (callback_pt )sub_respond_status);

  return sub;
}
