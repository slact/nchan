#include <nchan_module.h>
#include <ngx_channel.h>
#include "shmem.h"
#include "ipc.h"
#include "ipc-handlers.h"
#include "store-private.h"
#include <assert.h>
#include "../../subscribers/memstore_ipc.h"

#define IPC_SUBSCRIBE               1
#define IPC_SUBSCRIBE_REPLY         2
#define IPC_UNSUBSCRIBE             3  //NOT USED
#define IPC_UNSUBSCRIBED            4
#define IPC_PUBLISH_MESSAGE         5
#define IPC_PUBLISH_MESSAGE_REPLY   6
#define IPC_PUBLISH_STATUS          7
#define IPC_PUBLISH_STATUS_reply    8
#define IPC_GET_MESSAGE             9
#define IPC_GET_MESSAGE_REPLY       10
#define IPC_DELETE                  11
#define IPC_DELETE_REPLY            12
#define IPC_GET_CHANNEL_INFO        13
#define IPC_GET_CHANNEL_INFO_REPLY  14
#define IPC_DOES_CHANNEL_EXIST      15
#define IPC_DOES_CHANNEL_EXIST_REPLY 16
#define IPC_SUBSCRIBER_KEEPALIVE    17
#define IPC_SUBSCRIBER_KEEPALIVE_REPLY 18


//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "IPC-HANDLERS(%i):" fmt, memstore_slot(), ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "IPC-HANDLERS(%i):" fmt, memstore_slot(), ##args)


//lots of copypasta here, but it's the fastest way for me to write these IPC handlers
//maybe TODO: simplify this stuff, but probably not as it's not a performance penalty and the code is simple


//static ngx_int_t empty_callback() {
//  return NGX_OK;
//}

static ngx_str_t *str_shm_copy(ngx_str_t *str){
  ngx_str_t *out;
  out = shm_copy_immutable_string(nchan_memstore_get_shm(), str);
  DBG("create shm_str %p (data@ %p) %V", out, out->data, out);
  return out;
}

static void str_shm_free(ngx_str_t *str) {
  DBG("free shm_str %V @ %p", str, str->data);
  shm_free_immutable_string(nchan_memstore_get_shm(), str);
}

static void str_shm_verify(ngx_str_t *str) {
  shm_verify_immutable_string(nchan_memstore_get_shm(), str);
}

////////// SUBSCRIBE ////////////////
typedef struct {
  ngx_str_t                   *shm_chid;
  store_channel_head_shm_t    *shared_channel_data;
  nchan_store_channel_head_t  *origin_chanhead;
  subscriber_t                *subscriber;
  unsigned                     use_redis:1;
} subscribe_data_t;

ngx_int_t memstore_ipc_send_subscribe(ngx_int_t dst, ngx_str_t *chid, nchan_store_channel_head_t *origin_chanhead, nchan_loc_conf_t *cf) {
  DBG("send subscribe to %i, %V", dst, chid);
  //origin_chanhead->use_redis
  subscribe_data_t   data;
  ngx_memzero(&data, sizeof(data));
  data.shm_chid = str_shm_copy(chid);
  data.shared_channel_data = NULL;
  data.origin_chanhead = origin_chanhead;
  data.subscriber = NULL;
  data.use_redis = cf->use_redis;
  
  return ipc_alert(nchan_memstore_get_ipc(), dst, IPC_SUBSCRIBE, &data, sizeof(data));
}
static void receive_subscribe(ngx_int_t sender, void *data) {
  nchan_store_channel_head_t *head;
  subscribe_data_t           *d = (subscribe_data_t *)data;
  subscriber_t               *ipc_sub = NULL;
  
  nchan_loc_conf_t            fake_conf;
  //ngx_memzero(&fake_conf, sizeof(fake_conf));
  fake_conf.use_redis = d->use_redis;
  
  str_shm_verify(d->shm_chid);
  DBG("received subscribe request for channel %V", d->shm_chid);
  head = nchan_memstore_get_chanhead(d->shm_chid, &fake_conf);
  
  if(head == NULL) {
    ERR("couldn't get chanhead while receiving subscribe ipc msg");
    d->shared_channel_data = NULL;
    d->subscriber = NULL;
  }
  else {
    ipc_sub = memstore_ipc_subscriber_create(sender, &head->id, head->use_redis, d->origin_chanhead);
    d->subscriber = ipc_sub;
    d->shared_channel_data = head->shared;
  }
  
  DBG("send subscribe reply for channel %V", d->shm_chid);
  ipc_alert(nchan_memstore_get_ipc(), sender, IPC_SUBSCRIBE_REPLY, d, sizeof(*d));
  
  if(ipc_sub) {
    head->spooler.fn->add(&head->spooler, ipc_sub);
  }
}
static void receive_subscribe_reply(ngx_int_t sender, void *data) {
  subscribe_data_t             *d = (subscribe_data_t *)data;
  nchan_store_channel_head_t   *head;
  nchan_loc_conf_t              fake_conf;
  store_channel_head_shm_t     *old_shared;
  DBG("received subscribe reply for channel %V", d->shm_chid);
  //we have the chanhead address, but are too afraid to use it.
  
  //ngx_memzero(&fake_conf, sizeof(fake_conf));
  fake_conf.use_redis = d->use_redis;
  
  str_shm_verify(d->shm_chid);
  
  head = nchan_memstore_get_chanhead(d->shm_chid, &fake_conf);
  if(head == NULL) {
    ERR("Error regarding an aspect of life or maybe freshly fallen cookie crumbles");
    assert(0);
  }
  old_shared = head->shared;
  if(old_shared) {
    assert(old_shared == d->shared_channel_data);
  }
  DBG("receive subscribe proceed to do ipc_sub stuff");
  head->shared = d->shared_channel_data;
  
  if(old_shared == NULL) {
    ERR("%V local sub_count %i, internal_sub_count %i", &head->id,  head->sub_count, head->internal_sub_count);
    //ngx_atomic_fetch_add(&head->shared->sub_count, head->sub_count);
    ngx_atomic_fetch_add(&head->shared->internal_sub_count, head->internal_sub_count);
  }
  else {
    ERR("%V sub count already shared, don't update", &head->id);
  }
  
  assert(head->shared != NULL);
  if(head->foreign_owner_ipc_sub) {
    assert(head->foreign_owner_ipc_sub == d->subscriber);
  }
  else {
    head->foreign_owner_ipc_sub = d->subscriber;
  }
  
  memstore_ready_chanhead_unless_stub(head);
  
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
  return ipc_alert(nchan_memstore_get_ipc(), dst, IPC_UNSUBSCRIBED, &data, sizeof(data));
}
static void receive_unsubscribed(ngx_int_t sender, void *data) {
  unsubscribed_data_t    *d = (unsubscribed_data_t *)data;
  DBG("received unsubscribed request for channel %V pridata", d->shm_chid, d->privdata);
  if(memstore_channel_owner(d->shm_chid) != memstore_slot()) {
    nchan_store_channel_head_t    *head;
    //find channel
    head = nchan_memstore_find_chanhead(d->shm_chid);
    if(head == NULL) {
      //already deleted maybe?
      DBG("already unsubscribed...");
      return;
    }
    //gc if no subscribers
    if(head->sub_count == 0) {
      DBG("add %p to GC", head);
      head->foreign_owner_ipc_sub = NULL;
      chanhead_gc_add(head, "received UNSUBSCRIVED over ipc, sub_count == 0");
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

////////// PUBLISH STATUS ////////////////
typedef struct {
  ngx_str_t                 *shm_chid;
  ngx_int_t                  status_code;
  const ngx_str_t           *status_line;
  callback_pt                callback;
  void                      *callback_privdata;
} publish_status_data_t;

ngx_int_t memstore_ipc_send_publish_status(ngx_int_t dst, ngx_str_t *chid, ngx_int_t status_code, const ngx_str_t *status_line, callback_pt callback, void *privdata) {
  DBG("IPC: send publish status to %i ch %V", dst, chid);
  publish_status_data_t  data = {str_shm_copy(chid), status_code, status_line, callback, privdata};
  return ipc_alert(nchan_memstore_get_ipc(), dst, IPC_PUBLISH_STATUS, &data, sizeof(data));
}

static void receive_publish_status(ngx_int_t sender, void *data) {
  
  publish_status_data_t         *d = (publish_status_data_t *)data;
  nchan_store_channel_head_t    *chead;
  
  str_shm_verify(d->shm_chid);
  
  if((chead = nchan_memstore_find_chanhead(d->shm_chid)) == NULL) {
    ERR("can't find chanhead for id %V", d->shm_chid);
    assert(0);
    return;
  }
  
  DBG("IPC: received publish status for channel %V status %i %s", d->shm_chid, d->status_code, d->status_line);
  
  nchan_memstore_publish_generic(chead, NULL, d->status_code, d->status_line);
  
  str_shm_free(d->shm_chid);
  d->shm_chid=NULL;
}

////////// PUBLISH  ////////////////
typedef struct {
  ngx_str_t                 *shm_chid;
  nchan_msg_t               *shm_msg;
  ngx_int_t                  msg_timeout;
  ngx_int_t                  max_msgs;
  ngx_int_t                  min_msgs;
  uint8_t                    use_redis;
  callback_pt                callback;
  void                      *callback_privdata;
} publish_data_t;

ngx_int_t memstore_ipc_send_publish_message(ngx_int_t dst, ngx_str_t *chid, nchan_msg_t *shm_msg, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  ngx_int_t         ret;
  publish_data_t    data;
  DBG("IPC: send publish message to %i ch %V", dst, chid);
  assert(shm_msg->shared == 1);
  assert(shm_msg->temp_allocd == 0);
  assert(chid->data != NULL);
  //stop valgrind's complainting
  ngx_memzero(&data, sizeof(data));
  data.shm_chid = str_shm_copy(chid);
  data.shm_msg = shm_msg;
  data.msg_timeout = cf->buffer_timeout;
  data.max_msgs = cf->max_messages;
  data.min_msgs = cf->min_messages;
  data.use_redis = cf->use_redis;
  data.callback = callback;
  data.callback_privdata = privdata;
  
  assert(data.shm_chid->data != NULL);

  str_shm_verify(data.shm_chid);
  
  msg_reserve(shm_msg, "publish_message");
  
  ret= ipc_alert(nchan_memstore_get_ipc(), dst, IPC_PUBLISH_MESSAGE, &data, sizeof(data));
  return ret;
}

typedef struct {
  ngx_int_t        sender;
  publish_data_t  *d;
} publish_callback_data;

static ngx_int_t publish_message_generic_callback(ngx_int_t, void *, void *);

static void receive_publish_message(ngx_int_t sender, void *data) {
  publish_data_t               *d = (publish_data_t *)data;
  nchan_loc_conf_t              cf;
  publish_callback_data         cd;
  nchan_store_channel_head_t   *head;
  cd.d = d;
  cd.sender = sender;
  
  cf.buffer_timeout = d->msg_timeout;
  cf.min_messages = d->min_msgs;
  cf.max_messages = d->max_msgs;
  cf.use_redis = d->use_redis;
  
  str_shm_verify(d->shm_chid);
  assert(d->shm_chid->data != NULL);
  
  DBG("IPC: received publish request for channel %V  msg %p", d->shm_chid, d->shm_msg);
  
  if(memstore_channel_owner(d->shm_chid) == memstore_slot()) {
    nchan_store_publish_message_generic(d->shm_chid, d->shm_msg, 1, &cf, publish_message_generic_callback, &cd); //so long as callback is not evented, we're okay with that privdata
    //string will be freed on publish response
  }
  else {
    head = nchan_memstore_get_chanhead(d->shm_chid, &cf);
    nchan_memstore_publish_generic(head, d->shm_msg, 0, NULL);
    //don't deallocate shm_msg
  }
  
  msg_release(d->shm_msg, "publish_message");
  str_shm_free(d->shm_chid);
  d->shm_chid=NULL;
}

typedef struct {
  ngx_int_t    status;// NCHAN_MESSAGE_RECEIVED or NCHAN_MESSAGE_QUEUED;
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
  nchan_channel_t *ch = (nchan_channel_t *)rptr;
  rd.status = status;
  rd.callback = cd->d->callback;
  rd.callback_privdata = cd->d->callback_privdata;
  if(ch != NULL) {
    rd.last_seen = ch->last_seen;
    rd.subscribers = ch->subscribers;
    rd.messages = ch->messages;
  }
  DBG("IPC: publish message reply to %i", cd->sender);
  ipc_alert(nchan_memstore_get_ipc(), cd->sender, IPC_PUBLISH_MESSAGE_REPLY, &rd, sizeof(rd));
  return NGX_OK;
}
static void receive_publish_message_reply(ngx_int_t sender, void *data) {
  nchan_channel_t         ch;
  publish_response_data  *d = (publish_response_data *)data;
  DBG("IPC: received publish reply");
  
  ch.last_seen = d->last_seen;
  ch.subscribers = d->subscribers;
  ch.messages = d->messages;
  d->callback(d->status, &ch, d->callback_privdata);
}


////////// GET MESSAGE ////////////////
typedef struct {
  ngx_str_t              *shm_chid;
  nchan_msg_id_t          msgid;
  void                   *privdata;
} getmessage_data_t;

typedef struct {
  ngx_str_t           *shm_chid;
  nchan_msg_t         *shm_msg;
  nchan_msg_status_t   getmsg_code;
  void                *privdata;
} getmessage_reply_data_t;

ngx_int_t memstore_ipc_send_get_message(ngx_int_t dst, ngx_str_t *chid, nchan_msg_id_t *msgid, void *privdata) {
  getmessage_data_t      data;
  
  data.shm_chid= str_shm_copy(chid);
  ngx_memcpy(&data.msgid, msgid, sizeof(*msgid));
  data.privdata = privdata;
  
  
  DBG("IPC: send get message from %i ch %V", dst, chid);
  return ipc_alert(nchan_memstore_get_ipc(), dst, IPC_GET_MESSAGE, &data, sizeof(data));
}
static void receive_get_message(ngx_int_t sender, void *data) {
  nchan_store_channel_head_t  *head;
  store_message_t             *msg = NULL;
  nchan_msg_status_t           status;
  getmessage_data_t           *d = (getmessage_data_t *)data;
  
  str_shm_verify(d->shm_chid);
  assert(d->shm_chid->len>1);
  assert(d->shm_chid->data!=NULL);
  getmessage_reply_data_t *rd = (getmessage_reply_data_t *)data;
  assert(rd->shm_chid->len>1);
  assert(rd->shm_chid->data!=NULL);
  DBG("IPC: received get_message request for channel %V privdata %p", d->shm_chid, d->privdata);
  
  head = nchan_memstore_find_chanhead(d->shm_chid);
  if(head == NULL) {
    //no such thing here. reply.
    rd->getmsg_code = MSG_NOTFOUND;
    rd->shm_msg = NULL;
  }
  else {
    msg = chanhead_find_next_message(head, &d->msgid, &status);
    rd->getmsg_code = status;
    rd->shm_msg = msg == NULL ? NULL : msg->msg;
  }
  DBG("IPC: send get_message_reply for channel %V  msg %p, privdata: %p", d->shm_chid, msg, d->privdata);
  if(rd->shm_msg) {
    msg_reserve(rd->shm_msg, "get_message_reply");
  }
  ipc_alert(nchan_memstore_get_ipc(), sender, IPC_GET_MESSAGE_REPLY, rd, sizeof(*rd));
}

static void receive_get_message_reply(ngx_int_t sender, void *data) {
  getmessage_reply_data_t *d = (getmessage_reply_data_t *)data;
  
  str_shm_verify(d->shm_chid);
  assert(d->shm_chid->len>1);
  assert(d->shm_chid->data!=NULL);
  DBG("IPC: received get_message reply for channel %V  msg %p pridata %p", d->shm_chid, d->shm_msg, d->privdata);
  nchan_memstore_handle_get_message_reply(d->shm_msg, d->getmsg_code, d->privdata);
  if(d->shm_msg) {
    assert(d->shm_msg->refcount > 0);
    msg_release(d->shm_msg, "get_message_reply");
  }
  str_shm_free(d->shm_chid);
}




////////// DELETE ////////////////
typedef struct {
  ngx_str_t           *shm_chid;
  ngx_int_t            sender;
  nchan_channel_t     *shm_channel_info;
  ngx_int_t            code;
  callback_pt          callback;
  void                *privdata;
} delete_data_t;

ngx_int_t memstore_ipc_send_delete(ngx_int_t dst, ngx_str_t *chid, callback_pt callback,void *privdata) {
  delete_data_t  data = {str_shm_copy(chid), 0, NULL, 0, callback, privdata};
  DBG("IPC: send delete to %i ch %V", dst, chid);
  return ipc_alert(nchan_memstore_get_ipc(), dst, IPC_DELETE, &data, sizeof(data));
}

static ngx_int_t delete_callback_handler(ngx_int_t, void *, void*);

static void receive_delete(ngx_int_t sender, void *data) {
  delete_data_t *d = (delete_data_t *)data;
  d->sender = sender;
  DBG("IPC received delete request for channel %V pridata %p", d->shm_chid, d->privdata);
  nchan_memstore_force_delete_channel(d->shm_chid, delete_callback_handler, d);
}

static ngx_int_t delete_callback_handler(ngx_int_t code, void *ch, void* privdata) {
  nchan_channel_t      *chan = (nchan_channel_t *)ch;
  nchan_channel_t      *chan_info;
  delete_data_t        *d = (delete_data_t *)privdata;
  
  d->code = code;
  if (ch) {
    if((chan_info = shm_alloc(nchan_memstore_get_shm(), sizeof(*chan_info), "channel info for delete IPC response")) == NULL) {
      d->shm_channel_info = NULL;
      //yeah
      ERR("unable to allocate chan_info");
    }
    else {
      d->shm_channel_info= chan_info;
      chan_info->messages = chan->messages;
      chan_info->subscribers = chan->subscribers;
      chan_info->last_seen = chan->last_seen;
    }
  }
  else {
    d->shm_channel_info = NULL;
  }
  ipc_alert(nchan_memstore_get_ipc(), d->sender, IPC_DELETE_REPLY, d, sizeof(*d));
  return NGX_OK;
}
static void receive_delete_reply(ngx_int_t sender, void *data) {
  delete_data_t       *d = (delete_data_t *)data;

  str_shm_verify(d->shm_chid);
  
  DBG("IPC received delete reply for channel %V privdata %p", d->shm_chid, d->privdata);
  d->callback(d->code, d->shm_channel_info, d->privdata);
  
  if(d->shm_channel_info != NULL) {
    shm_free(nchan_memstore_get_shm(), d->shm_channel_info);
  }
  str_shm_free(d->shm_chid);
}




////////// GET CHANNEL INFO ////////////////
typedef struct {
  ngx_str_t                 *shm_chid;
  store_channel_head_shm_t  *channel_info;
  callback_pt                callback;
  void                      *privdata;
} channel_info_data_t;

ngx_int_t memstore_ipc_send_get_channel_info(ngx_int_t dst, ngx_str_t *chid, callback_pt callback, void* privdata) {
  DBG("send get_channel_info to %i %V", dst, chid);
  channel_info_data_t        data = {str_shm_copy(chid), NULL, callback, privdata};
  return ipc_alert(nchan_memstore_get_ipc(), dst, IPC_GET_CHANNEL_INFO, &data, sizeof(data));
}
static void receive_get_channel_info(ngx_int_t sender, void *data) {
  channel_info_data_t           *d = (channel_info_data_t *)data;
  nchan_store_channel_head_t    *head;
  
  DBG("received get_channel_info request for channel %V pridata", d->shm_chid, d->privdata);
  head = nchan_memstore_find_chanhead(d->shm_chid);
  assert(memstore_slot() == memstore_channel_owner(d->shm_chid));
  if(head == NULL) {
    //already deleted maybe?
    DBG("channel not for for get_channel_info");
    d->channel_info = NULL;
  }
  else {
    d->channel_info = head->shared;
  }
  ipc_alert(nchan_memstore_get_ipc(), sender, IPC_GET_CHANNEL_INFO_REPLY, d, sizeof(*d));
}

static void receive_get_channel_info_reply(ngx_int_t sender, void *data) {
  channel_info_data_t       *d = (channel_info_data_t *)data;
  nchan_channel_t            chan;
  store_channel_head_shm_t  *chinfo = d->channel_info;
  
  if(chinfo) {
    //construct channel
    chan.subscribers = chinfo->sub_count;
    chan.last_seen = chinfo->last_seen;
    chan.id.data = d->shm_chid->data;
    chan.id.len = d->shm_chid->len;
    chan.messages = chinfo->stored_message_count;
    d->callback(NGX_OK, &chan, d->privdata);
  }
  else {
    d->callback(NGX_OK, NULL, d->privdata);
  }
  str_shm_free(d->shm_chid);
}


////////// DOES CHANNEL EXIST? ////////////////
typedef struct {
  ngx_str_t               *shm_chid;
  ngx_int_t                channel_exists;
  callback_pt              callback;
  void                    *privdata;
} channel_existence_data_t;

ngx_int_t memstore_ipc_send_does_channel_exist(ngx_int_t dst, ngx_str_t *chid, callback_pt callback, void* privdata) {
  DBG("send does_channel_exist to %i %V", dst, chid);
  channel_existence_data_t        data = {str_shm_copy(chid), 0, callback, privdata};
  
  return ipc_alert(nchan_memstore_get_ipc(), dst, IPC_DOES_CHANNEL_EXIST, &data, sizeof(data));
}

static void receive_does_channel_exist(ngx_int_t sender, void *data) {
  channel_existence_data_t      *d = (channel_existence_data_t *)data;
  nchan_store_channel_head_t    *head;
  
  DBG("received does_channel_exist request for channel %V pridata", d->shm_chid, d->privdata);
  
  head = nchan_memstore_find_chanhead(d->shm_chid);
  
  assert(memstore_slot() == memstore_channel_owner(d->shm_chid));
  
  d->channel_exists = (head != NULL);
  ipc_alert(nchan_memstore_get_ipc(), sender, IPC_DOES_CHANNEL_EXIST_REPLY, d, sizeof(*d));
}

static void receive_does_channel_exist_reply(ngx_int_t sender, void *data) {
  channel_existence_data_t      *d = (channel_existence_data_t *)data;
  
  d->callback(d->channel_exists, NULL, d->privdata);
  str_shm_free(d->shm_chid);
}

/////////// SUBSCRIBER KEEPALIVE ///////////
typedef struct {
  ngx_str_t                   *shm_chid;
  subscriber_t                *ipc_sub;
  nchan_store_channel_head_t  *originator;
  ngx_uint_t                   renew;
  callback_pt                  callback;
  void                        *privdata;
} sub_keepalive_data_t;

ngx_int_t memstore_ipc_send_memstore_subscriber_keepalive(ngx_int_t dst, ngx_str_t *chid, subscriber_t *sub, nchan_store_channel_head_t *ch, callback_pt callback, void *privdata) {
  sub_keepalive_data_t        data = {str_shm_copy(chid), sub, ch, 0, callback, privdata};
  DBG("send SUBBSCRIBER KEEPALIVE to %i %V", dst, chid);
  ipc_alert(nchan_memstore_get_ipc(), dst, IPC_SUBSCRIBER_KEEPALIVE, &data, sizeof(data));
  return NGX_OK;
}
static void receive_subscriber_keepalive(ngx_int_t sender, void *data) {
  sub_keepalive_data_t          *d = (sub_keepalive_data_t *)data;
  nchan_store_channel_head_t    *head;
  DBG("received subscriber keepalive for channel %V", d->shm_chid);
  head = nchan_memstore_find_chanhead(d->shm_chid);
  if(head == NULL) {
    DBG("not subscribed anymore");
    d->renew = 0;
  }
  else {
    assert(head == d->originator);
    assert(head->status == READY || head->status == STUBBED);
    assert(head->foreign_owner_ipc_sub == d->ipc_sub);
    if(head->sub_count == 0) {
      if(ngx_time() - head->last_subscribed > MEMSTORE_IPC_SUBSCRIBER_TIMEOUT) {
        d->renew = 0;
        DBG("No subscribers lately. Time... to die.");
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
  ipc_alert(nchan_memstore_get_ipc(), sender, IPC_SUBSCRIBER_KEEPALIVE_REPLY, d, sizeof(*d));
}

static void receive_subscriber_keepalive_reply(ngx_int_t sender, void *data) {
  sub_keepalive_data_t        *d = (sub_keepalive_data_t *)data;
  d->callback(d->renew, NULL, d->privdata);
  str_shm_free(d->shm_chid);
}

static void (*ipc_alert_handler[])(ngx_int_t, void *) = {
  [IPC_SUBSCRIBE] =                   receive_subscribe,
  [IPC_SUBSCRIBE_REPLY] =             receive_subscribe_reply,
  [IPC_UNSUBSCRIBED] =                receive_unsubscribed,
  [IPC_PUBLISH_MESSAGE] =             receive_publish_message,
  [IPC_PUBLISH_MESSAGE_REPLY] =       receive_publish_message_reply,
  [IPC_PUBLISH_STATUS] =              receive_publish_status,
  [IPC_GET_MESSAGE] =                 receive_get_message,
  [IPC_GET_MESSAGE_REPLY] =           receive_get_message_reply,
  [IPC_DELETE] =                      receive_delete,
  [IPC_DELETE_REPLY] =                receive_delete_reply,
  [IPC_GET_CHANNEL_INFO] =            receive_get_channel_info,
  [IPC_GET_CHANNEL_INFO_REPLY]=       receive_get_channel_info_reply,
  [IPC_DOES_CHANNEL_EXIST] =          receive_does_channel_exist,
  [IPC_DOES_CHANNEL_EXIST_REPLY]=     receive_does_channel_exist_reply,
  [IPC_SUBSCRIBER_KEEPALIVE] =        receive_subscriber_keepalive,
  [IPC_SUBSCRIBER_KEEPALIVE_REPLY] =  receive_subscriber_keepalive_reply
};

void memstore_ipc_alert_handler(ngx_int_t sender, ngx_uint_t code, void *data) {
  ipc_alert_handler[code](sender, data);
}