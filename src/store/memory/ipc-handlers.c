#include <ngx_http_push_module.h>
#include <ngx_channel.h>
#include "shmem.h"
#include "ipc.h"
#include "ipc-handlers.h"
#include "store-private.h"
#include <assert.h>
#include "../../subscribers/memstore.h"

#define IPC_SUBSCRIBE               1
#define IPC_SUBSCRIBE_REPLY         2
#define IPC_UNSUBSCRIBE             3  //NOT USED
#define IPC_UNSUBSCRIBED            4
#define IPC_PUBLISH_MESSAGE         5
#define IPC_PUBLISH_MESSAGE_REPLY   6
#define IPC_PUBLISH_STATUS          7
#define IPC_GET_MESSAGE             8
#define IPC_GET_MESSAGE_REPLY       9
#define IPC_DELETE                  10
#define IPC_DELETE_REPLY            11
#define IPC_GET_CHANNEL_INFO        12
#define IPC_GET_CHANNEL_INFO_REPLY  13
#define IPC_SUBSCRIBER_KEEPALIVE    14
#define IPC_SUBSCRIBER_KEEPALIVE_REPLY 15


#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "IPC-HANDLERS: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "IPC-HANDLERS: " fmt, ##args)
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
  ngx_str_t               *shm_chid;
  nhpm_channel_head_shm_t *shared_channel_data;
  nhpm_channel_head_t     *origin_chanhead;
  subscriber_t            *subscriber;
} subscribe_data_t;
ngx_int_t memstore_ipc_send_subscribe(ngx_int_t dst, ngx_str_t *chid, nhpm_channel_head_t *origin_chanhead) {
  DBG("send subscribe to %i, %V", dst, chid);
  subscribe_data_t        data = {str_shm_copy(chid), NULL, origin_chanhead, NULL};
  return ipc_alert(ngx_http_push_memstore_get_ipc(), dst, IPC_SUBSCRIBE, &data, sizeof(data));
}
static void receive_subscribe(ngx_int_t sender, void *data) {
  nhpm_channel_head_t     *head;
  subscribe_data_t        *d = (subscribe_data_t *)data;
  subscriber_t            *sub;
  
  DBG("received subscribe request for channel %V", d->shm_chid);
  head = ngx_http_push_memstore_get_chanhead(d->shm_chid);
  
  if(head == NULL) {
    ERR("couldn't get chanhead while receiving subscribe ipc msg");
    d->shared_channel_data = NULL;
    d->subscriber = NULL;
  }
  else {
    sub = memstore_subscriber_create(sender, &head->id, d->origin_chanhead);
    sub->enqueue(sub);
    head->spooler.add(&head->spooler, sub);
    d->subscriber = sub;
    d->shared_channel_data = head->shared;
  }

  ipc_alert(ngx_http_push_memstore_get_ipc(), sender, IPC_SUBSCRIBE_REPLY, d, sizeof(*d));
}
static void receive_subscribe_reply(ngx_int_t sender, void *data) {
  subscribe_data_t *d = (subscribe_data_t *)data;
  DBG("received subscribe reply for channel %V", d->shm_chid);
  //we have the chanhead address, but are too afraid to use it.
  nhpm_channel_head_t   *head = ngx_http_push_memstore_get_chanhead(d->shm_chid);
  if(head == NULL) {
    ERR("Error regarding an aspect of life or maybe freshly fallen cookie crumbles");
  }
  if(head->shared) {
    assert(head->shared == d->shared_channel_data);
  }
  head->shared = d->shared_channel_data;
  assert(head->shared != NULL);
  assert(head->ipc_sub == NULL);
  head->ipc_sub = d->subscriber;
  //TODO: shared counts
  head->status = READY;
  str_shm_free(d->shm_chid);
}



////////// UNSUBSCRIBED ////////////////
typedef struct {
  ngx_str_t    *shm_chid;
  void         *privdata;
} unsubscribed_data_t;
ngx_int_t memstore_ipc_send_unsubscribed(ngx_int_t dst, ngx_str_t *chid, void* privdata) {
  DBG("send unsubscribed to %i %V", dst, chid);
  unsubscribed_data_t        data = {str_shm_copy(chid), privdata};
  return ipc_alert(ngx_http_push_memstore_get_ipc(), dst, IPC_UNSUBSCRIBED, &data, sizeof(data));
}
static void receive_unsubscribed(ngx_int_t sender, void *data) {
  unsubscribed_data_t    *d = (unsubscribed_data_t *)data;
  DBG("received unsubscribed request for channel %V pridata", d->shm_chid, d->privdata);
  if(memstore_channel_owner(d->shm_chid) != memstore_slot()) {
    nhpm_channel_head_t    *head;
    //find channel
    head = ngx_http_push_memstore_find_chanhead(d->shm_chid);
    if(head == NULL) {
      //already deleted maybe?
      DBG("already unsubscribed...");
      return;
    }
    //gc if no subscribers
    if(head->sub_count == 0) {
      DBG("add %p to GC", head);
      head->ipc_sub = NULL;
      chanhead_gc_add(head, "received UNSUBPSCRIVED over ipc, sub_count == 0");
    }
    else {
      //subscribe again?...
      DBG("maybe subscribe again?...");
    }
  }
  else {
    ERR("makes no sense...");
  }
  str_shm_free(d->shm_chid);
}


/////////// PUBLISH STATUS ///////////
typedef struct {
  ngx_str_t    *shm_chid;
  ngx_int_t     status;
  void         *privdata;
} publish_status_data_t;
ngx_int_t memstore_ipc_send_publish_status(ngx_int_t dst, ngx_str_t *chid, ngx_int_t status,  void* privdata) {
  publish_status_data_t        data = {str_shm_copy(chid), status, privdata};
  DBG("send publish status to %i %V", dst, chid);
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
  callback_pt                callback;
  void                      *callback_privdata;
} publish_data_t;

ngx_int_t memstore_ipc_send_publish_message(ngx_int_t dst, ngx_str_t *chid, ngx_http_push_msg_t *shm_msg, ngx_int_t msg_timeout, ngx_int_t max_msgs, ngx_int_t min_msgs, callback_pt callback, void *privdata) {
  DBG("IPC: send publish message to %i ch %V", dst, chid);
  publish_data_t  data = {str_shm_copy(chid), shm_msg, msg_timeout, max_msgs, min_msgs, callback, privdata};
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
  
  DBG("IPC: received publish request for channel %V  msg %p", d->shm_chid, d->shm_msg);
  if(memstore_channel_owner(d->shm_chid) == memstore_slot()) {
    ngx_http_push_store_publish_message_generic(d->shm_chid, d->shm_msg, 1, d->msg_timeout, d->max_msgs, d->min_msgs, publish_message_generic_callback, &cd); //so long as callback is not evented, we're okay with that privdata
    //string will be freed on publish response
  }
  else {
    head = ngx_http_push_memstore_get_chanhead(d->shm_chid);
    ngx_http_push_memstore_publish_generic(head, d->shm_msg, 0, NULL);
    //don't deallocate shm_msg
  }
  
  str_shm_free(d->shm_chid);
  d->shm_chid=NULL;
}

typedef struct {
  ngx_int_t    status;// NGX_HTTP_PUSH_MESSAGE_RECEIVED or NGX_HTTP_PUSH_MESSAGE_QUEUED;
  time_t       last_seen;
  ngx_uint_t   subscribers;
  ngx_uint_t   messages;
  callback_pt  callback;
  void        *callback_privdata;
} publish_response_data;

static ngx_int_t publish_message_generic_callback(ngx_int_t status, void *rptr, void *privdata) {
  DBG("IPC: publish message generic callback");
  publish_callback_data   *cd = (publish_callback_data *)privdata;
  publish_response_data    rd;
  ngx_http_push_channel_t *ch = (ngx_http_push_channel_t *)rptr;
  rd.status = status;
  rd.callback = cd->d->callback;
  rd.callback_privdata = cd->d->callback_privdata;
  if(ch != NULL) {
    rd.last_seen = ch->last_seen;
    rd.subscribers = ch->subscribers;
    rd.messages = ch->messages;
  }
  DBG("IPC: publish message reply to %i", cd->sender);
  ipc_alert(ngx_http_push_memstore_get_ipc(), cd->sender, IPC_PUBLISH_MESSAGE_REPLY, &rd, sizeof(rd));
  return NGX_OK;
}
static void receive_publish_message_reply(ngx_int_t sender, void *data) {
  ngx_http_push_channel_t   ch;
  publish_response_data    *d = (publish_response_data *)data;
  DBG("IPC: received publish reply");
  
  ch.last_seen = d->last_seen;
  ch.subscribers = d->subscribers;
  ch.messages = d->messages;
  d->callback(d->status, &ch, d->callback_privdata);
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
  DBG("IPC: send get message from %i ch %V", dst, chid);
  return ipc_alert(ngx_http_push_memstore_get_ipc(), dst, IPC_GET_MESSAGE, &data, sizeof(data));
}
static void receive_get_message(ngx_int_t sender, void *data) {
  nhpm_channel_head_t *head;
  nhpm_message_t *msg = NULL;
  ngx_int_t       status = NGX_ERROR;
  getmessage_data_t *d = (getmessage_data_t *)data;
  getmessage_reply_data_t *rd = (getmessage_reply_data_t *)data;
  DBG("IPC: received get_message request for channel %V privdata %p", d->shm_chid, d->privdata);
  
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
  DBG("IPC: send get_message_reply for channel %V  msg %p, privdata: %p", d->shm_chid, msg, d->privdata);
  ipc_alert(ngx_http_push_memstore_get_ipc(), sender, IPC_GET_MESSAGE_REPLY, rd, sizeof(*rd));
}

static void receive_get_message_reply(ngx_int_t sender, void *data) {
  getmessage_reply_data_t *d = (getmessage_reply_data_t *)data;
  DBG("IPC: received get_message reply for channel %V  msg %p pridata %p", d->shm_chid, d->shm_msg, d->privdata);
  ngx_http_push_memstore_handle_get_message_reply(d->shm_msg, d->getmsg_code, d->privdata);
  str_shm_free(d->shm_chid);
}




////////// DELETE ////////////////
typedef struct {
  ngx_str_t           *shm_chid;
  ngx_int_t            return_code;
  ngx_int_t            sender;
  callback_pt          callback;
  void                *privdata;
} delete_data_t;
ngx_int_t memstore_ipc_send_delete(ngx_int_t dst, ngx_str_t *chid, callback_pt callback,void *privdata) {
  delete_data_t  data = {str_shm_copy(chid), 0, 0, callback, privdata};
  DBG("IPC: send delete to %i ch %V", dst, chid);
  return ipc_alert(ngx_http_push_memstore_get_ipc(), dst, IPC_DELETE, &data, sizeof(data));
}
static ngx_int_t delete_callback_handler(ngx_int_t, void *, void*);
static void receive_delete(ngx_int_t sender, void *data) {
  delete_data_t *d = (delete_data_t *)data;
  d->sender = sender;
  DBG("IPC received delete request for channel %V pridata %p", d->shm_chid, d->privdata);
  ngx_http_push_memstore_force_delete_channel(d->shm_chid, delete_callback_handler, d);
}
static ngx_int_t delete_callback_handler(ngx_int_t code, void *nothing, void* privdata) {
  delete_data_t *d = (delete_data_t *)privdata;
  d->return_code = code;
  ipc_alert(ngx_http_push_memstore_get_ipc(), d->sender, IPC_DELETE_REPLY, d, sizeof(*d));
  return NGX_OK;
}
static void receive_delete_reply(ngx_int_t sender, void *data) {
  delete_data_t *d = (delete_data_t *)data;
  DBG("IPC received delete reply for channel %V  msg %p pridata %p", d->shm_chid, d->privdata);
  d->callback(d->return_code, NULL, d->privdata); 
  str_shm_free(d->shm_chid);
}




////////// GET CHANNEL INFO ////////////////
typedef struct {
  ngx_str_t               *shm_chid;
  nhpm_channel_head_shm_t *channel_info;
  callback_pt              callback;
  void                    *privdata;
} channel_info_data_t;
ngx_int_t memstore_ipc_send_get_channel_info(ngx_int_t dst, ngx_str_t *chid, callback_pt callback, void* privdata) {
  DBG("send get_channel_info to %i %V", dst, chid);
  channel_info_data_t        data = {str_shm_copy(chid), NULL, callback, privdata};
  return ipc_alert(ngx_http_push_memstore_get_ipc(), dst, IPC_GET_CHANNEL_INFO, &data, sizeof(data));
}
static void receive_get_channel_info(ngx_int_t sender, void *data) {
  channel_info_data_t    *d = (channel_info_data_t *)data;
  nhpm_channel_head_t    *head;
  DBG("received get_channel_info request for channel %V pridata", d->shm_chid, d->privdata);
  head = ngx_http_push_memstore_find_chanhead(d->shm_chid);
  if(head == NULL) {
    //already deleted maybe?
    DBG("channel not for for get_channel_info");
    return;
    d->channel_info = NULL;
  }
  else {
    d->channel_info = head->shared;
  }
  ipc_alert(ngx_http_push_memstore_get_ipc(), sender, IPC_GET_CHANNEL_INFO_REPLY, d, sizeof(*d));
}

static void receive_get_channel_info_reply(ngx_int_t sender, void *data) {
  channel_info_data_t      *d = (channel_info_data_t *)data;
  ngx_http_push_channel_t  chan;
  nhpm_channel_head_shm_t  *chinfo = d->channel_info;
  
  //construct channel
  chan.subscribers = chinfo->sub_count;
  chan.last_seen = chinfo->last_seen;
  chan.id.data = d->shm_chid->data;
  chan.id.len = d->shm_chid->len;
  chan.messages = chinfo->stored_message_count;
  d->callback(NGX_OK, &chan, d->privdata);
}



/////////// SUBSCRIBER KEEPALIVE ///////////
typedef struct {
  ngx_str_t             *shm_chid;
  subscriber_t          *ipc_sub;
  nhpm_channel_head_t   *originator;
  unsigned               renew:1;
  callback_pt            callback;
  void                  *privdata;
} sub_keepalive_data_t;

ngx_int_t memstore_ipc_send_memstore_subscriber_keepalive(ngx_int_t dst, ngx_str_t *chid, subscriber_t *sub, nhpm_channel_head_t *ch, callback_pt callback, void *privdata) {
  sub_keepalive_data_t        data = {str_shm_copy(chid), sub, ch, 0, callback, privdata};
  DBG("send SUBBSCRIBER KEEPALIVE to %i %V", dst, chid);
  ipc_alert(ngx_http_push_memstore_get_ipc(), dst, IPC_SUBSCRIBER_KEEPALIVE, &data, sizeof(data));
  return NGX_OK;
}
static void receive_subscriber_keepalive(ngx_int_t sender, void *data) {
  sub_keepalive_data_t   *d = (sub_keepalive_data_t *)data;
  nhpm_channel_head_t    *head;
  DBG("received subscriber keepalive for channel %V", d->shm_chid);
  head = ngx_http_push_memstore_find_chanhead(d->shm_chid);
  if(head == NULL) {
    DBG("not subscribed anymore");
    d->renew = 0;
  }
  else {
    assert(head == d->originator);
    assert(head->ipc_sub == d->ipc_sub);
    if(head->sub_count == 0) {
      if(ngx_time() - head->last_subscribed > MEMSTORE_IPC_SUBSCRIBER_TIMEOUT) {
        d->renew = 0;
      }
      else {
        DBG("No subscribers, but there was one %i sec ago. don't unsubscribe.", ngx_time() - head->last_subscribed);
        d->renew = 1;
      }
    }
    else {
      d->renew = 1;
    }
  }
  ipc_alert(ngx_http_push_memstore_get_ipc(), sender, IPC_SUBSCRIBER_KEEPALIVE_REPLY, d, sizeof(*d));
}

static void receive_subscriber_keepalive_reply(ngx_int_t sender, void *data) {
  sub_keepalive_data_t *d = (sub_keepalive_data_t *)data;
  d->callback(d->renew, NULL, d->privdata);
  str_shm_free(d->shm_chid);
}

static void (*ipc_alert_handler[])(ngx_int_t, void *) = {
  [IPC_SUBSCRIBE] =             receive_subscribe,
  [IPC_SUBSCRIBE_REPLY] =       receive_subscribe_reply,
  [IPC_UNSUBSCRIBED] =          receive_unsubscribed,
  [IPC_PUBLISH_MESSAGE] =       receive_publish_message,
  [IPC_PUBLISH_MESSAGE_REPLY] = receive_publish_message_reply,
  [IPC_PUBLISH_STATUS] =        receive_publish_status,
  [IPC_GET_MESSAGE] =           receive_get_message,
  [IPC_GET_MESSAGE_REPLY] =     receive_get_message_reply,
  [IPC_DELETE] =                receive_delete,
  [IPC_DELETE_REPLY] =          receive_delete_reply,
  [IPC_GET_CHANNEL_INFO] =      receive_get_channel_info,
  [IPC_GET_CHANNEL_INFO_REPLY]= receive_get_channel_info_reply,
  [IPC_SUBSCRIBER_KEEPALIVE] =  receive_subscriber_keepalive,
  [IPC_SUBSCRIBER_KEEPALIVE_REPLY] = receive_subscriber_keepalive_reply
};

void memstore_ipc_alert_handler(ngx_int_t sender, ngx_uint_t code, void *data) {
  ipc_alert_handler[code](sender, data);
}