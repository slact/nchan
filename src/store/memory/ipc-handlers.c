#include <nchan_module.h>
#include <ngx_channel.h>
#include "shmem.h"
#include "ipc.h"
#include "ipc-handlers.h"
#include "store-private.h"
#include "store.h"
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

#define IPC_TEST_FLOOD                 30


//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "IPC-HANDLERS(%i):" fmt, memstore_slot(), ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "IPC-HANDLERS(%i):" fmt, memstore_slot(), ##args)

//#define DEBUG_MEMZERO(var) ngx_memzero(var, sizeof(*(var)))
#define DEBUG_MEMZERO(var) /*nothing*/

//lots of copypasta here, but it's the fastest way for me to write these IPC handlers
//maybe TODO: simplify this stuff, but probably not as it's not a performance penalty and the code is simple


//static ngx_int_t empty_callback() {
//  return NGX_OK;
//}

static nchan_msg_id_t zero_msgid = NCHAN_ZERO_MSGID;

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

////////// SUBSCRIBE ////////////////
union subdata_u {
  nchan_store_channel_head_t  *origin_chanhead;
  subscriber_t                *subscriber;
};

typedef struct {
  ngx_str_t                   *shm_chid;
  store_channel_head_shm_t    *shared_channel_data;
  union subdata_u              d;
  uint8_t                      use_redis;
} subscribe_data_t;

ngx_int_t memstore_ipc_send_subscribe(ngx_int_t dst, ngx_str_t *chid, nchan_store_channel_head_t *origin_chanhead, nchan_loc_conf_t *cf) {
  DBG("send subscribe to %i, %V", dst, chid);
  //origin_chanhead->use_redis
  subscribe_data_t   data; 
  DEBUG_MEMZERO(&data);
  
  data.shm_chid = str_shm_copy(chid);
  data.shared_channel_data = NULL;
  data.d.origin_chanhead = origin_chanhead;
  data.use_redis = cf->use_redis;
  
  return ipc_alert(nchan_memstore_get_ipc(), dst, IPC_SUBSCRIBE, &data, sizeof(data));
}
static void receive_subscribe(ngx_int_t sender, subscribe_data_t *d) {
  nchan_store_channel_head_t *head;
  subscriber_t               *ipc_sub = NULL;
  
  nchan_loc_conf_t            fake_conf;
  //ngx_memzero(&fake_conf, sizeof(fake_conf));
  fake_conf.use_redis = d->use_redis;
  
  DBG("received subscribe request for channel %V", d->shm_chid);
  head = nchan_memstore_get_chanhead(d->shm_chid, &fake_conf);
  
  if(head == NULL) {
    ERR("couldn't get chanhead while receiving subscribe ipc msg");
    d->shared_channel_data = NULL;
    d->d.subscriber = NULL;
  }
  else {
    ipc_sub = memstore_ipc_subscriber_create(sender, &head->id, head->use_redis, d->d.origin_chanhead);
    d->d.subscriber = ipc_sub;
    d->shared_channel_data = head->shared;
  }
  
  ipc_alert(nchan_memstore_get_ipc(), sender, IPC_SUBSCRIBE_REPLY, d, sizeof(*d));
  DBG("sent subscribe reply for channel %V to %i", d->shm_chid, sender);
  
  if(ipc_sub) {
    head->spooler.fn->add(&head->spooler, ipc_sub);
  }
}
static void receive_subscribe_reply(ngx_int_t sender, subscribe_data_t *d) {
  nchan_store_channel_head_t   *head;
  nchan_loc_conf_t              fake_conf;
  store_channel_head_shm_t     *old_shared;
  DBG("received subscribe reply for channel %V", d->shm_chid);
  //we have the chanhead address, but are too afraid to use it.
  
  //ngx_memzero(&fake_conf, sizeof(fake_conf));
  fake_conf.use_redis = d->use_redis;
  
  head = nchan_memstore_get_chanhead_no_ipc_sub(d->shm_chid, &fake_conf);
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
    //ERR("%V local sub_count %i, internal_sub_count %i", &head->id,  head->sub_count, head->internal_sub_count);
    assert(head->sub_count >= head->internal_sub_count);
    ngx_atomic_fetch_add(&head->shared->sub_count, head->sub_count - head->internal_sub_count);
    ngx_atomic_fetch_add(&head->shared->internal_sub_count, head->internal_sub_count);
  }
  else {
    ERR("%V sub count already shared, don't update", &head->id);
  }
  
  assert(head->shared != NULL);
  if(head->foreign_owner_ipc_sub) {
    assert(head->foreign_owner_ipc_sub == d->d.subscriber);
  }
  else {
    head->foreign_owner_ipc_sub = d->d.subscriber;
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
static void receive_unsubscribed(ngx_int_t sender, unsubscribed_data_t *d) {
  DBG("received unsubscribed request for channel %V privdata %p", d->shm_chid, d->privdata);
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



static void receive_publish_status(ngx_int_t sender, publish_status_data_t *d) {
  static ngx_str_t               nullstring = ngx_null_string;
  nchan_store_channel_head_t    *chead;
  
  if((chead = nchan_memstore_find_chanhead(d->shm_chid)) == NULL) {
    if(ngx_exiting) {
      ERR("can't find chanhead for id %V, but it's okay.", d->shm_chid);
    }
    else {
      ERR("can't find chanhead for id %V", d->shm_chid);
      assert(0);
    }
    return;
  }
  
  DBG("IPC: received publish status for channel %V status %i %s", d->shm_chid, d->status_code, d->status_line == NULL ? &nullstring : d->status_line);
  
  nchan_memstore_publish_generic(chead, NULL, d->status_code, d->status_line);
  
  str_shm_free(d->shm_chid);
  d->shm_chid=NULL;
}

////////// PUBLISH  ////////////////
typedef struct {
  ngx_str_t                 *shm_chid;
  nchan_msg_t               *shm_msg;
  uint16_t                   msg_timeout;
  uint16_t                   max_msgs;
  callback_pt                callback;
  void                      *callback_privdata;
  unsigned                   use_redis:1;
  
} publish_data_t;

ngx_int_t memstore_ipc_send_publish_message(ngx_int_t dst, ngx_str_t *chid, nchan_msg_t *shm_msg, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  ngx_int_t         ret;
  publish_data_t    data; 
  DEBUG_MEMZERO(&data);
  
  DBG("IPC: send publish message to %i ch %V", dst, chid);
  assert(shm_msg->shared == 1);
  assert(shm_msg->temp_allocd == 0);
  assert(chid->data != NULL);
  data.shm_chid = str_shm_copy(chid);
  data.shm_msg = shm_msg;
  data.msg_timeout = cf->buffer_timeout;
  data.max_msgs = cf->max_messages;
  data.use_redis = cf->use_redis;
  data.callback = callback;
  data.callback_privdata = privdata;
  
  assert(data.shm_chid->data != NULL);
  assert(msg_reserve(shm_msg, "publish_message") == NGX_OK);
  
  ret= ipc_alert(nchan_memstore_get_ipc(), dst, IPC_PUBLISH_MESSAGE, &data, sizeof(data));
  return ret;
}

typedef struct {
  ngx_int_t        sender;
  publish_data_t  *d;
  unsigned         allocd:1;
} publish_callback_data;

static ngx_int_t publish_message_generic_callback(ngx_int_t, void *, void *);

static void receive_publish_message(ngx_int_t sender, publish_data_t *d) {
  nchan_loc_conf_t              cf;
  
  publish_callback_data         cd_data;
  publish_callback_data        *cd;
  nchan_store_channel_head_t   *head;
  
  cf.buffer_timeout = d->msg_timeout;
  cf.max_messages = d->max_msgs;
  cf.use_redis = d->use_redis;

  assert(d->shm_chid->data != NULL);
  
  DBG("IPC: received publish request for channel %V  msg %p", d->shm_chid, d->shm_msg);
  
  if(memstore_channel_owner(d->shm_chid) == memstore_slot()) {
    if(cf.use_redis) {
      cd = ngx_alloc(sizeof(*cd) + sizeof(*d), ngx_cycle->log);
      cd->allocd=1;
      cd->d = (publish_data_t *)&cd[1];
      *cd->d = *d;
    }
    else {
      cd = &cd_data;
      cd->allocd=0;
      cd->d = d;
    }
    
    cd->sender = sender;
    
    nchan_store_publish_message_generic(d->shm_chid, d->shm_msg, 1, &cf, publish_message_generic_callback, cd);
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
  uint16_t        status;// NCHAN_MESSAGE_RECEIVED or NCHAN_MESSAGE_QUEUED;
  uint32_t        subscribers;
  uint16_t        messages;
  time_t          last_seen;
  time_t          msg_time;
  uint16_t        msg_tag;
  callback_pt     callback;
  void           *callback_privdata;
} publish_response_data;

static ngx_int_t publish_message_generic_callback(ngx_int_t status, void *rptr, void *privdata) {
  DBG("IPC: publish message generic callback");
  publish_callback_data   *cd = (publish_callback_data *)privdata;
  publish_response_data    rd; 
  DEBUG_MEMZERO(&rd);
  
  nchan_channel_t *ch = (nchan_channel_t *)rptr;
  rd.status = status;
  rd.callback = cd->d->callback;
  rd.callback_privdata = cd->d->callback_privdata;
  if(ch != NULL) {
    rd.last_seen = ch->last_seen;
    rd.subscribers = ch->subscribers;
    rd.messages = ch->messages;
    assert(ch->last_published_msg_id.tagcount == 1);
    rd.msg_time = ch->last_published_msg_id.time;
    rd.msg_tag = ch->last_published_msg_id.tag.fixed[0];
  }
  ipc_alert(nchan_memstore_get_ipc(), cd->sender, IPC_PUBLISH_MESSAGE_REPLY, &rd, sizeof(rd));
  if(cd->allocd) {
    ngx_free(cd);
  }
  return NGX_OK;
}
static void receive_publish_message_reply(ngx_int_t sender, publish_response_data *d) {
  nchan_channel_t         ch;
  DBG("IPC: received publish reply");
  
  ch.last_seen = d->last_seen;
  ch.subscribers = d->subscribers;
  ch.messages = d->messages;
  ch.last_published_msg_id.time = d->msg_time;
  ch.last_published_msg_id.tag.fixed[0] = d->msg_tag;
  ch.last_published_msg_id.tagcount = 1;
  ch.last_published_msg_id.tagactive = 0;
  d->callback(d->status, &ch, d->callback_privdata);
}


////////// GET MESSAGE ////////////////
union getmsg_u {
  struct req_s {
    nchan_msg_id_t          msgid;
  } req;
  
  struct resp_s {
    nchan_msg_status_t      getmsg_code;
    nchan_msg_t            *shm_msg;
  } resp; 
};

typedef struct {
  ngx_str_t              *shm_chid;
  void                   *privdata;
  union getmsg_u          d;
} getmessage_data_t;

ngx_int_t memstore_ipc_send_get_message(ngx_int_t dst, ngx_str_t *chid, nchan_msg_id_t *msgid, void *privdata) {
  getmessage_data_t      data;
  
  data.shm_chid= str_shm_copy(chid);
  data.privdata = privdata;
  data.d.req.msgid = *msgid;
  
  DBG("IPC: send get message from %i ch %V", dst, chid);
  return ipc_alert(nchan_memstore_get_ipc(), dst, IPC_GET_MESSAGE, &data, sizeof(data));
}
static void receive_get_message(ngx_int_t sender, getmessage_data_t *d) {
  nchan_store_channel_head_t  *head;
  store_message_t             *msg = NULL;
  nchan_msg_status_t           status;
  
  assert(d->shm_chid->len>1);
  assert(d->shm_chid->data!=NULL);
  DBG("IPC: received get_message request for channel %V privdata %p", d->shm_chid, d->privdata);
  
  head = nchan_memstore_find_chanhead(d->shm_chid);
  if(head == NULL) {
    //no such thing here. reply.
    d->d.resp.getmsg_code = MSG_NOTFOUND;
    d->d.resp.shm_msg = NULL;
  }
  else {
    msg = chanhead_find_next_message(head, &d->d.req.msgid, &status);
    d->d.resp.getmsg_code = status;
    d->d.resp.shm_msg = msg == NULL ? NULL : msg->msg;
  }
  DBG("IPC: send get_message_reply for channel %V  msg %p, privdata: %p", d->shm_chid, msg, d->privdata);
  if(d->d.resp.shm_msg) {
    assert(msg_reserve(d->d.resp.shm_msg, "get_message_reply") == NGX_OK);
  }
  ipc_alert(nchan_memstore_get_ipc(), sender, IPC_GET_MESSAGE_REPLY, d, sizeof(*d));
}

static void receive_get_message_reply(ngx_int_t sender, getmessage_data_t *d) {
  
  assert(d->shm_chid->len>1);
  assert(d->shm_chid->data!=NULL);
  DBG("IPC: received get_message reply for channel %V msg %p privdata %p", d->shm_chid, d->d.resp.shm_msg, d->privdata);
  nchan_memstore_handle_get_message_reply(d->d.resp.shm_msg, d->d.resp.getmsg_code, d->privdata);
  if(d->d.resp.shm_msg) {
    msg_release(d->d.resp.shm_msg, "get_message_reply");
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

static ngx_int_t delete_callback_handler(ngx_int_t, nchan_channel_t *, delete_data_t *);

static void receive_delete(ngx_int_t sender, delete_data_t *d) {
  d->sender = sender;
  DBG("IPC received delete request for channel %V privdata %p", d->shm_chid, d->privdata);
  nchan_memstore_force_delete_channel(d->shm_chid, (callback_pt )delete_callback_handler, d);
}

static ngx_int_t delete_callback_handler(ngx_int_t code, nchan_channel_t *chan, delete_data_t *d) {
  nchan_channel_t      *chan_info;
  
  d->code = code;
  if (chan) {
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
      
      if(chan->last_published_msg_id.tagcount > NCHAN_FIXED_MULTITAG_MAX) {
        //meh, this can't be triggered... can it?...
        nchan_msg_id_t           zeroid = NCHAN_ZERO_MSGID;
        chan_info->last_published_msg_id = zeroid;
      }
      else {
        chan_info->last_published_msg_id = chan->last_published_msg_id;
      }
    }
  }
  else {
    d->shm_channel_info = NULL;
  }
  ipc_alert(nchan_memstore_get_ipc(), d->sender, IPC_DELETE_REPLY, d, sizeof(*d));
  return NGX_OK;
}
static void receive_delete_reply(ngx_int_t sender, delete_data_t *d) {
  
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
  nchan_msg_id_t             last_msgid;
  callback_pt                callback;
  void                      *privdata;
} channel_info_data_t;

ngx_int_t memstore_ipc_send_get_channel_info(ngx_int_t dst, ngx_str_t *chid, callback_pt callback, void* privdata) {
  DBG("send get_channel_info to %i %V", dst, chid);
  channel_info_data_t        data;
  DEBUG_MEMZERO(&data);
  data.shm_chid = str_shm_copy(chid);
  data.channel_info = NULL;
  data.last_msgid = zero_msgid;
  data.callback = callback;
  data.privdata = privdata;
  return ipc_alert(nchan_memstore_get_ipc(), dst, IPC_GET_CHANNEL_INFO, &data, sizeof(data));
}
static void receive_get_channel_info(ngx_int_t sender, channel_info_data_t *d) {
  nchan_store_channel_head_t    *head;
  
  DBG("received get_channel_info request for channel %V privdata %p", d->shm_chid, d->privdata);
  head = nchan_memstore_find_chanhead(d->shm_chid);
  assert(memstore_slot() == memstore_channel_owner(d->shm_chid));
  if(head == NULL) {
    //already deleted maybe?
    DBG("channel not for for get_channel_info");
    d->channel_info = NULL;
  }
  else {
    d->channel_info = head->shared;
    assert(head->channel.last_published_msg_id.tagcount <= 1);
    d->last_msgid = head->channel.last_published_msg_id;
  }
  ipc_alert(nchan_memstore_get_ipc(), sender, IPC_GET_CHANNEL_INFO_REPLY, d, sizeof(*d));
}

static void receive_get_channel_info_reply(ngx_int_t sender, channel_info_data_t *d) {
  nchan_channel_t            chan;
  store_channel_head_shm_t  *chinfo = d->channel_info;
  
  if(chinfo) {
    //construct channel
    chan.subscribers = chinfo->sub_count;
    chan.last_seen = chinfo->last_seen;
    chan.id.data = d->shm_chid->data;
    chan.id.len = d->shm_chid->len;
    chan.messages = chinfo->stored_message_count;
    chan.last_published_msg_id = d->last_msgid;
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

static void receive_does_channel_exist(ngx_int_t sender, channel_existence_data_t *d) {
  nchan_store_channel_head_t    *head;
  
  DBG("received does_channel_exist request for channel %V privdata %p", d->shm_chid, d->privdata);
  
  head = nchan_memstore_find_chanhead(d->shm_chid);
  
  assert(memstore_slot() == memstore_channel_owner(d->shm_chid));
  
  d->channel_exists = (head != NULL);
  ipc_alert(nchan_memstore_get_ipc(), sender, IPC_DOES_CHANNEL_EXIST_REPLY, d, sizeof(*d));
}

static void receive_does_channel_exist_reply(ngx_int_t sender, channel_existence_data_t *d) {
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
  DBG("send SUBSCRIBER KEEPALIVE to %i %V", dst, chid);
  ipc_alert(nchan_memstore_get_ipc(), dst, IPC_SUBSCRIBER_KEEPALIVE, &data, sizeof(data));
  return NGX_OK;
}
static void receive_subscriber_keepalive(ngx_int_t sender, sub_keepalive_data_t *d) {
  nchan_store_channel_head_t    *head;
  DBG("received SUBSCRIBER KEEPALIVE from %i for channel %V", sender, d->shm_chid);
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
      if(ngx_time() - head->last_subscribed_local > MEMSTORE_IPC_SUBSCRIBER_TIMEOUT) {
        d->renew = 0;
        DBG("No subscribers lately. Time... to die.");
      }
      else {
        DBG("No subscribers, but there was one %i sec ago. don't unsubscribe.", ngx_time() - head->last_subscribed_local);
        d->renew = 1;
      }
    }
    else {
      d->renew = 1;
    }
  }
  ipc_alert(nchan_memstore_get_ipc(), sender, IPC_SUBSCRIBER_KEEPALIVE_REPLY, d, sizeof(*d));
}

static void receive_subscriber_keepalive_reply(ngx_int_t sender, sub_keepalive_data_t *d) {
  d->callback(d->renew, NULL, d->privdata);
  str_shm_free(d->shm_chid);
}

/////////// FLOOD TEST ///////////
typedef struct {
  uint64_t          n;
} flood_data_t;


static int  flood_seq = 0;
ngx_int_t memstore_ipc_send_flood_test(ngx_int_t dst) {
  flood_data_t        data = {flood_seq++};
  ipc_alert(nchan_memstore_get_ipc(), dst, IPC_TEST_FLOOD, &data, sizeof(data));
  return NGX_OK;
}


static void receive_flood_test(ngx_int_t sender, flood_data_t *d) {
  struct timespec  tv;
  tv.tv_sec=0;
  tv.tv_nsec=8000000;
  ERR("      received FLOOD TEST from %i seq %l", sender, d->n);
  nanosleep(&tv, NULL);
}

static ipc_handler_pt ipc_alert_handler[] = {
  [IPC_SUBSCRIBE] =                   (ipc_handler_pt )receive_subscribe,
  [IPC_SUBSCRIBE_REPLY] =             (ipc_handler_pt )receive_subscribe_reply,
  [IPC_UNSUBSCRIBED] =                (ipc_handler_pt )receive_unsubscribed,
  [IPC_PUBLISH_MESSAGE] =             (ipc_handler_pt )receive_publish_message,
  [IPC_PUBLISH_MESSAGE_REPLY] =       (ipc_handler_pt )receive_publish_message_reply,
  [IPC_PUBLISH_STATUS] =              (ipc_handler_pt )receive_publish_status,
  [IPC_GET_MESSAGE] =                 (ipc_handler_pt )receive_get_message,
  [IPC_GET_MESSAGE_REPLY] =           (ipc_handler_pt )receive_get_message_reply,
  [IPC_DELETE] =                      (ipc_handler_pt )receive_delete,
  [IPC_DELETE_REPLY] =                (ipc_handler_pt )receive_delete_reply,
  [IPC_GET_CHANNEL_INFO] =            (ipc_handler_pt )receive_get_channel_info,
  [IPC_GET_CHANNEL_INFO_REPLY]=       (ipc_handler_pt )receive_get_channel_info_reply,
  [IPC_DOES_CHANNEL_EXIST] =          (ipc_handler_pt )receive_does_channel_exist,
  [IPC_DOES_CHANNEL_EXIST_REPLY]=     (ipc_handler_pt )receive_does_channel_exist_reply,
  [IPC_SUBSCRIBER_KEEPALIVE] =        (ipc_handler_pt )receive_subscriber_keepalive,
  [IPC_SUBSCRIBER_KEEPALIVE_REPLY] =  (ipc_handler_pt )receive_subscriber_keepalive_reply,
  [IPC_TEST_FLOOD]                 =  (ipc_handler_pt )receive_flood_test
};

void memstore_ipc_alert_handler(ngx_int_t sender, ngx_uint_t code, void *data) {
  ipc_alert_handler[code](sender, data);
}
