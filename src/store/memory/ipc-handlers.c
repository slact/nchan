#include <ngx_http_push_module.h>
#include <ngx_channel.h>
#include "shmem.h"
#include "ipc.h"
#include "ipc-handlers.h"
#include "store-private.h"
#include "../../subscribers/memstore.h"

#define IPC_SUBSCRIBE               1
#define IPC_SUBSCRIBE_REPLY         2
#define IPC_UNSUBSCRIBE             3
#define IPC_UNSUBSCRIBED            4
#define IPC_PUBLISH_MESSAGE         5
#define IPC_PUBLISH_MESSAGE_REPLY   6
#define IPC_PUBLISH_STATUS          7
#define IPC_GET_MESSAGE             8
#define IPC_GET_MESSAGE_REPLY       9
#define IPC_DELETE                  10
#define IPC_DELETE_REPLY            11

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, __VA_ARGS__)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)

static ngx_int_t empty_callback() {
  return NGX_OK;
}

static ngx_str_t *str_shm_copy(ngx_str_t *str){
  return shm_copy_string(ngx_http_push_memstore_get_shm(), str);
}

static void str_shm_free(ngx_str_t *str) {
  shm_free(ngx_http_push_memstore_get_shm(), str);
}

////////// SUBSCRIBE ////////////////
typedef struct {
  ngx_str_t    *shm_chid;
  ngx_atomic_t *shm_sub_count;
  void         *subscriber;
  void         *privdata;
} subscribe_data_t;
ngx_int_t memstore_ipc_send_subscribe(ngx_int_t dst, ngx_str_t *chid, void* privdata) {
  subscribe_data_t        data = {str_shm_copy(chid), NULL, NULL, privdata};
  return ipc_alert(ngx_http_push_memstore_get_ipc(), dst, IPC_SUBSCRIBE, &data, sizeof(data));
}
static void receive_subscribe(ngx_int_t sender, void *data) {
  nhpm_channel_head_t     *head;
  subscribe_data_t        *d = (subscribe_data_t *)data;
  subscriber_t            *sub;
  
  DBG("received subscribe request for channel %V pridata", d->shm_chid, d->privdata);
  head = ngx_http_push_memstore_get_chanhead(d->shm_chid);
  
  if(head == NULL) {
    ERR("couldn't get chanhead while receiving subscribe ipc msg");
    d->shm_sub_count = 0;
    d->subscriber = NULL;
  }
  else {
    sub = memstore_subscriber_create(sender, &head->id, d->subscriber);
    nhpm_memstore_subscriber_create(head, sub);
    d->subscriber = sub;
    d->shm_sub_count = 0;
  }

  d->shm_sub_count = &head->sub_count; //fake for now
  ipc_alert(ngx_http_push_memstore_get_ipc(), sender, IPC_SUBSCRIBE_REPLY, d, sizeof(*d));
}
static void receive_subscribe_reply(ngx_int_t sender, void *data) {
  subscribe_data_t *d = (subscribe_data_t *)data;
  DBG("received subscribe reply for channel %V pridata", d->shm_chid, d->privdata);
  //we have the chanhead address, but are too afraid to use it.
  nhpm_channel_head_t   *head = ngx_http_push_memstore_get_chanhead(d->shm_chid);
  if(head == NULL) {
    ERR("Error regarding an aspect of life or maybe freshly fallen cookie crumbles");
  }
  head->ipc_sub = d->subscriber;
  //TODO: shared counts
  head->status = READY;
  str_shm_free(d->shm_chid);
}



////////// UNSUBSCRIBE ////////////////
typedef struct {
  ngx_str_t    *shm_chid;
  void         *subscriber;
  void         *privdata;
} unsubscribe_data_t;
ngx_int_t memstore_ipc_send_unsubscribe(ngx_int_t dst, ngx_str_t *chid, void *subscriber, void* privdata) {
  unsubscribe_data_t        data = {str_shm_copy(chid), subscriber, privdata};
  return ipc_alert(ngx_http_push_memstore_get_ipc(), dst, IPC_UNSUBSCRIBE, &data, sizeof(data));
}
static void receive_unsubscribe(ngx_int_t sender, void *data) {
  unsubscribe_data_t    *d = (unsubscribe_data_t *)data;
  subscriber_t          *sub;
  DBG("received subscribe request for channel %V pridata", d->shm_chid, d->privdata);
  sub = (subscriber_t *)d->subscriber;
  //hope this pointer we got is still good...
  sub->dequeue(sub);
}





/////////// PUBLISH STATUS ///////////
typedef struct {
  ngx_str_t    *shm_chid;
  ngx_int_t     status;
  void         *privdata;
} publish_status_data_t;
ngx_int_t memstore_ipc_send_publish_status(ngx_int_t dst, ngx_str_t *chid, ngx_int_t status,  void* privdata) {
  publish_status_data_t        data = {str_shm_copy(chid), status, privdata};
  ipc_alert(ngx_http_push_memstore_get_ipc(), dst, IPC_PUBLISH_STATUS, &data, sizeof(data));
  return NGX_OK;
}
static void receive_publish_status(ngx_int_t sender, void *data) {
  publish_status_data_t *d = (publish_status_data_t *)data;
  DBG("received publish status reply for channel %V", d->shm_chid);
  switch (d->status) {
    case NGX_HTTP_NO_CONTENT: //message expired
      ERR("It's... not... possible!!!!");
      break;
    case NGX_HTTP_GONE: //delete
    case NGX_HTTP_CLOSE: //delete
    case NGX_HTTP_NOT_MODIFIED: //timeout?
    case NGX_HTTP_FORBIDDEN:
      ngx_http_push_memstore_force_delete_channel(d->shm_chid, empty_callback, NULL);
      break;
    default:
      ERR("Nothing grittier than a mouthful of grit.");
      break;
  }
  str_shm_free(d->shm_chid);
}



////////// PUBLISH  ////////////////
typedef struct {
  ngx_str_t                 *shm_chid;
  ngx_http_push_msg_t       *shm_msg;
  ngx_int_t                  msg_timeout;
  ngx_int_t                  max_msgs;
  ngx_int_t                  min_msgs;
  void                      *privdata;
} publish_data_t;

typedef struct {
  callback_pt    cb;
  void          *pd;
} publish_extradata_t;

ngx_int_t memstore_ipc_send_publish_message(ngx_int_t dst, ngx_str_t *chid, ngx_http_push_msg_t *shm_msg, ngx_int_t msg_timeout, ngx_int_t max_msgs, ngx_int_t min_msgs, callback_pt callback, void *privdata) {
  //nhpm_channel_head_t *head;
  publish_extradata_t    *ed = ngx_alloc(sizeof(*ed), ngx_cycle->log);
  ed->cb = callback;
  ed->pd = privdata;
  publish_data_t  data = {str_shm_copy(chid), shm_msg, msg_timeout, max_msgs, min_msgs, ed};
  return ipc_alert(ngx_http_push_memstore_get_ipc(), dst, IPC_PUBLISH_MESSAGE, &data, sizeof(data));
}

typedef struct {
  ngx_int_t        sender;
  publish_data_t  *d;
} publish_callback_data;

static ngx_int_t publish_message_generic_callback(ngx_int_t, void *, void *);

static void receive_publish_message(ngx_int_t sender, void *data) {
  publish_data_t         *d = (publish_data_t *)data;
  publish_callback_data   cd;
  nhpm_channel_head_t    *head;
  cd.d = d;
  cd.sender = sender;
  
  DBG("received publish request for channel %V  msg %p pridata %p", d->shm_chid, d->shm_msg, d->privdata);
  if(memstore_channel_owner(d->shm_chid) == ngx_process_slot) {
    ngx_http_push_store_publish_message_generic(d->shm_chid, d->shm_msg, 1, d->msg_timeout, d->max_msgs, d->min_msgs, publish_message_generic_callback, &cd); //so long as callback is not evented, we're okay with that privdata
    str_shm_free(d->shm_chid);
  }
  else {
    head = ngx_http_push_memstore_get_chanhead(d->shm_chid);
    ngx_http_push_memstore_publish_generic(head, d->shm_msg, 0, NULL);
    //don't deallocate shm_msg
  }
}

typedef struct {
  ngx_str_t   *shm_chid;
  ngx_int_t    status;// NGX_HTTP_PUSH_MESSAGE_RECEIVED or NGX_HTTP_PUSH_MESSAGE_QUEUED;
  time_t       last_seen;
  ngx_uint_t   subscribers;
  ngx_uint_t   messages;
  void        *privdata;
} publish_response_data;

static ngx_int_t publish_message_generic_callback(ngx_int_t status, void *rptr, void *privdata) {
  publish_callback_data   *cd = (publish_callback_data *)privdata;
  publish_response_data    rd;
  ngx_http_push_channel_t *ch = (ngx_http_push_channel_t *)rptr;
  rd.shm_chid = cd->d->shm_chid;
  rd.status = status;
  rd.privdata = cd->d->privdata;
  if(ch != NULL) {
    rd.last_seen = ch->last_seen;
    rd.subscribers = ch->subscribers;
    rd.messages = ch->messages;
  }
  ipc_alert(ngx_http_push_memstore_get_ipc(), cd->sender, IPC_PUBLISH_MESSAGE_REPLY, &rd, sizeof(rd));
  return NGX_OK;
}
static void receive_publish_message_reply(ngx_int_t sender, void *data) {
  ngx_http_push_channel_t   ch;
  publish_response_data    *d = (publish_response_data *)data;
  publish_extradata_t      *ed = (publish_extradata_t *)d->privdata;
  DBG("received publish reply for channel %V  pridata %p", d->shm_chid, d->privdata);
  
  ch.last_seen = d->last_seen;
  ch.subscribers = d->subscribers;
  ch.messages = d->messages;
  ed->cb(d->status, &ch, ed->pd);
  
  str_shm_free(d->shm_chid);
  ngx_free(ed);
}


////////// GET MESSAGE ////////////////
typedef struct {
  ngx_str_t              *shm_chid;
  ngx_http_push_msg_id_t  msgid;
  void                   *privdata;
} getmessage_data_t;

typedef struct {
  ngx_str_t           *shm_chid;
  ngx_http_push_msg_t *shm_msg;
  ngx_int_t            getmsg_code;
  void                *privdata;
} getmessage_reply_data_t;

//incidentally these two are the same size. if they weren't there'd be a bug.
ngx_int_t memstore_ipc_send_get_message(ngx_int_t dst, ngx_str_t *chid, ngx_http_push_msg_id_t *msgid, void *privdata) {
  getmessage_data_t      data = {str_shm_copy(chid), {0}, privdata};
  data.msgid.time = msgid->time;
  data.msgid.tag = msgid->tag;
  
  return ipc_alert(ngx_http_push_memstore_get_ipc(), dst, IPC_GET_MESSAGE, &data, sizeof(data));
}
static void receive_get_message(ngx_int_t sender, void *data) {
  nhpm_channel_head_t *head;
  nhpm_message_t *msg;
  ngx_int_t       status;
  getmessage_data_t *d = (getmessage_data_t *)data;
  getmessage_reply_data_t *rd = (getmessage_reply_data_t *)data;
  DBG("received get_message request for channel %V  msg %p pridata %p", d->shm_chid, d->privdata);
  
  head = ngx_http_push_memstore_find_chanhead(d->shm_chid);
  if(head == NULL) {
    //no such thing here. reply.
    rd->getmsg_code = NGX_HTTP_PUSH_MESSAGE_NOTFOUND;
    rd->shm_msg = NULL;
  }
  else {
    msg = chanhead_find_next_message(head, &d->msgid, &status);
    rd->getmsg_code = status;
    rd->shm_msg = msg == NULL ? NULL : msg->msg;
  }
  ipc_alert(ngx_http_push_memstore_get_ipc(), sender, IPC_GET_MESSAGE_REPLY, rd, sizeof(*rd));
}

static void receive_get_message_reply(ngx_int_t sender, void *data) {
  getmessage_reply_data_t *d = (getmessage_reply_data_t *)data;
  DBG("received get_message reply for channel %V  msg %p pridata %p", d->shm_chid, d->privdata);
  ngx_http_push_memstore_handle_get_message_reply(d->shm_msg, d->getmsg_code, d->privdata);
  str_shm_free(d->shm_chid);
}




////////// DELETE ////////////////
typedef struct {
  ngx_str_t           *shm_chid;
  void                *privdata;
} delete_data_t;
ngx_int_t memstore_ipc_send_delete(ngx_int_t dst, ngx_str_t *chid, void *privdata) {
  delete_data_t  data = {str_shm_copy(chid), privdata};
  return ipc_alert(ngx_http_push_memstore_get_ipc(), dst, IPC_DELETE, &data, sizeof(data));
}
static void receive_delete(ngx_int_t sender, void *data) {
  delete_data_t *d = (delete_data_t *)data;
  DBG("received delete request for channel %V  msg %p pridata %p", d->shm_chid, d->privdata);
  ipc_alert(ngx_http_push_memstore_get_ipc(), sender, IPC_DELETE_REPLY, d, sizeof(*d));
}
static void receive_delete_reply(ngx_int_t sender, void *data) {
  delete_data_t *d = (delete_data_t *)data;
  DBG("received delete reply for channel %V  msg %p pridata %p", d->shm_chid, d->privdata);
  str_shm_free(d->shm_chid);
}

static void (*ipc_alert_handler[])(ngx_int_t, void *) = {
  [IPC_SUBSCRIBE] =             receive_subscribe,
  [IPC_SUBSCRIBE_REPLY] =       receive_subscribe_reply,
  [IPC_UNSUBSCRIBE] =           receive_unsubscribe,
  [IPC_PUBLISH_MESSAGE] =       receive_publish_message,
  [IPC_PUBLISH_MESSAGE_REPLY] = receive_publish_message_reply,
  [IPC_PUBLISH_STATUS] =        receive_publish_status,
  [IPC_GET_MESSAGE] =           receive_get_message,
  [IPC_GET_MESSAGE_REPLY] =     receive_get_message_reply,
  [IPC_DELETE] =                receive_delete,
  [IPC_DELETE_REPLY] =          receive_delete_reply
};

void memstore_ipc_alert_handler(ngx_int_t sender, ngx_uint_t code, void *data) {
  ipc_alert_handler[code](sender, data);
}