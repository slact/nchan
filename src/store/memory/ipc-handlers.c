#include <ngx_http_push_module.h>
#include <ngx_channel.h>
#include "shmem.h"
#include "ipc.h"
#include "ipc-handlers.h"
#include "store-private.h"

#define IPC_SUBSCRIBE               1
#define IPC_SUBSCRIBE_REPLY         2
#define IPC_PUBLISH_MESSAGE         3
#define IPC_PUBLISH_MESSAGE_REPLY   4
#define IPC_GET_MESSAGE             5
#define IPC_GET_MESSAGE_REPLY       6
#define IPC_DELETE                  7
#define IPC_DELETE_REPLY            8

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, __VA_ARGS__)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)

ngx_int_t get_pid(ngx_int_t owner_slot) {
  return 1;
}

typedef struct {
  ngx_str_t *shm_chid;
  void      *privdata;
} subscribe_data_t;
ngx_int_t memstore_ipc_send_subscribe(ipc_t *ipc, ngx_int_t owner, ngx_str_t *shm_chid, void* privdata) {
  subscribe_data_t        data = {shm_chid, privdata};
  return ipc_alert(ipc, owner, IPC_SUBSCRIBE, &data);
}
static void receive_subscribe(ngx_int_t sender, void *data) {
  subscribe_data_t *d = (subscribe_data_t *)data;
  DBG("received subscribe request for channel %V pridata", d->shm_chid, d->privdata);
}
static void receive_subscribe_reply(ngx_int_t sender, void *data) {
  subscribe_data_t *d = (subscribe_data_t *)data;
  DBG("received subscribe reply for channel %V pridata", d->shm_chid, d->privdata);
}

typedef struct {
  ngx_str_t           *shm_chid;
  ngx_http_push_msg_t *shm_msg;
  void                *privdata;
} publish_data_t;
ngx_int_t memstore_ipc_send_publish_message(ipc_t *ipc, ngx_int_t owner, ngx_str_t *shm_chid, ngx_http_push_msg_t *shm_msg, void *privdata) {
  publish_data_t  data = {shm_chid, shm_msg, privdata};
  return ipc_alert(ipc, owner, IPC_PUBLISH_MESSAGE, &data);
}
static void receive_publish(ngx_int_t sender, void *data) {
  publish_data_t *d = (publish_data_t *)data;
  DBG("received publish request for channel %V  msg %p pridata %p", d->shm_chid, d->shm_msg, d->privdata);
}
static void receive_publish_reply(ngx_int_t sender, void *data) {
  publish_data_t *d = (publish_data_t *)data;
  DBG("received publish reply for channel %V  msg %p pridata %p", d->shm_chid, d->shm_msg, d->privdata);
}



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
ngx_int_t memstore_ipc_send_get_message(ipc_t *ipc, ngx_int_t owner, ngx_str_t *shm_chid, ngx_http_push_msg_id_t *msgid, void *privdata) {
  getmessage_data_t      data = {shm_chid, {0}, privdata};
  data.msgid.time = msgid->time;
  data.msgid.tag = msgid->tag;
  
  return ipc_alert(ipc, owner, IPC_GET_MESSAGE, &data);
}
static void receive_get_message(ngx_int_t sender, void *data) {
  nhpm_channel_head_t *head;
  nhpm_message_t *msg;
  ngx_int_t       status;
  getmessage_data_t *d = (getmessage_data_t *)data;
  getmessage_reply_data_t *rd = (getmessage_reply_data_t *)data;
  DBG("received get_message request for channel %V  msg %p pridata %p", d->shm_chid, d->privdata);
  
  head = ngx_http_push_store_find_chanhead(d->shm_chid);
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
  ipc_alert(ngx_http_push_memstore_get_ipc(), sender, IPC_GET_MESSAGE_REPLY, rd);
}

static void receive_get_message_reply(ngx_int_t sender, void *data) {
  getmessage_reply_data_t *d = (getmessage_reply_data_t *)data;
  DBG("received get_message reply for channel %V  msg %p pridata %p", d->shm_chid, d->privdata);
  ngx_http_push_memstore_handle_get_message_reply(d->shm_msg, d->getmsg_code, d->privdata);
}

typedef struct {
  ngx_str_t           *shm_chid;
  void                *privdata;
} delete_data_t;
ngx_int_t memstore_ipc_send_delete(ipc_t *ipc, ngx_int_t owner, ngx_str_t *shm_chid, void *privdata) {
  delete_data_t  data = {shm_chid, privdata};
  return ipc_alert(ipc, owner, IPC_DELETE, &data);
}
static void receive_delete(ngx_int_t sender, void *data) {
  delete_data_t *d = (delete_data_t *)data;
  DBG("received delete request for channel %V  msg %p pridata %p", d->shm_chid, d->privdata);
}
static void receive_delete_reply(ngx_int_t sender, void *data) {
  delete_data_t *d = (delete_data_t *)data;
  DBG("received delete reply for channel %V  msg %p pridata %p", d->shm_chid, d->privdata);
}

static void (*ipc_alert_handler[])(ngx_int_t, void *) = {
  [IPC_SUBSCRIBE] =             receive_subscribe,
  [IPC_SUBSCRIBE_REPLY] =       receive_subscribe_reply,
  [IPC_PUBLISH_MESSAGE] =       receive_publish,
  [IPC_PUBLISH_MESSAGE_REPLY] = receive_publish_reply,
  [IPC_GET_MESSAGE] =           receive_get_message,
  [IPC_GET_MESSAGE_REPLY] =     receive_get_message_reply,
  [IPC_DELETE] =                receive_delete,
  [IPC_DELETE_REPLY] =          receive_delete_reply
};

void memstore_ipc_alert_handler(ngx_int_t sender, ngx_uint_t code, void *data) {
  ipc_alert_handler[code](sender, data);
}