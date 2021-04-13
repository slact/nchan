#include <nchan_module.h>
#include <ngx_channel.h>
#include "ipc.h"
#include "ipc-handlers.h"
#include "store-private.h"
#include "store.h"
#include <assert.h>
#include <store/redis/store.h>
#include <subscribers/memstore_ipc.h>
#include <subscribers/getmsg_proxy.h>
#include <subscribers/memstore_redis.h>
#include <util/nchan_msg.h>
#include <util/nchan_benchmark.h>


//macro black magic, AKA X-Macros
#define LIST_IPC_COMMANDS(L) \
  L(subscribe) \
  L(subscribe_reply) \
  L(subscribe_chanhead_release) \
  L(subscribe_chanhead_nevermind_release) \
  L(unsubscribed) \
  L(publish_message) \
  L(publish_message_reply) \
  L(publish_status) \
  L(publish_notice) \
  L(get_message) \
  L(get_message_reply) \
  L(delete) \
  L(delete_reply) \
  L(get_channel_info) \
  L(get_channel_info_reply) \
  L(channel_auth_check) \
  L(channel_auth_check_reply) \
  L(subscriber_keepalive) \
  L(subscriber_keepalive_reply) \
  L(get_group) \
  L(group) \
  L(group_delete) \
  L(flood_test) \
  L(benchmark_initialize) \
  L(benchmark_start) \
  L(benchmark_stop) \
  L(benchmark_abort) \
  L(benchmark_finish) \
  L(benchmark_finish_reply) \



#define MAKE_ipc_handlers_t(val) ipc_handler_pt val;
typedef struct {
  LIST_IPC_COMMANDS(MAKE_ipc_handlers_t)
} ipc_handlers_t;

#define MAKE_ipc_command_codes_t(val) int val;
typedef struct {
  LIST_IPC_COMMANDS(MAKE_ipc_command_codes_t);
} ipc_command_codes_t;

#define MAKE_ipc_cmd(val) offsetof(ipc_handlers_t, val)/sizeof(ipc_handler_pt),
static ipc_command_codes_t ipc_cmd = {
  LIST_IPC_COMMANDS(MAKE_ipc_cmd)
};

#define IPC_CMDS (sizeof(ipc_handlers_t)/sizeof(ipc_handler_pt))

#define ipc_cmd(cmd, dst, data) ipc_alert(nchan_memstore_get_ipc(), dst, ipc_cmd.cmd, data, sizeof(*(data)))
#define ipc_broadcast_cmd(cmd, data) ipc_broadcast_alert(nchan_memstore_get_ipc(), ipc_cmd.cmd, data, sizeof(*(data)))

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
  out = shm_copy_immutable_string(nchan_store_memory_shmem, str);
  if(out) {
    DBG("create shm_str %p (data@ %p) %V", out, out->data, out);
  }
  return out;
}

static void str_shm_free(ngx_str_t *str) {
  DBG("free shm_str %V @ %p", str, str->data);
  shm_free_immutable_string(nchan_store_memory_shmem, str);
}

////////// SUBSCRIBE ////////////////
typedef struct {
  ngx_str_t                   *shm_chid;
  store_channel_head_shm_t    *shared_channel_data;
  nchan_loc_conf_t            *cf;
  memstore_channel_head_t     *origin_chanhead;
  memstore_channel_head_t     *owner_chanhead;
  subscriber_t                *subscriber;
  ngx_int_t                    rc;
} subscribe_data_t;

ngx_int_t memstore_ipc_send_subscribe(ngx_int_t dst, ngx_str_t *chid, memstore_channel_head_t *origin_chanhead, nchan_loc_conf_t *cf) {
  DBG("send subscribe to %i, %V", dst, chid);

  subscribe_data_t   data; 
  DEBUG_MEMZERO(&data);
  
  if((data.shm_chid = str_shm_copy(chid)) == NULL) {
    nchan_log_ooshm_error("sending IPC subscribe alert for channel %V", chid);
    return NGX_DECLINED;
  }
  data.shared_channel_data = NULL;
  data.origin_chanhead = origin_chanhead;
  data.owner_chanhead = NULL;
  data.cf = cf;
  
  assert(memstore_str_owner(data.shm_chid) == dst);
  
  return ipc_cmd(subscribe, dst, &data);
}
static void receive_subscribe(ngx_int_t sender, subscribe_data_t *d) {
  memstore_channel_head_t    *head;
  subscriber_t               *ipc_sub = NULL;
  ngx_int_t                   rc;
  
  DBG("received subscribe request for channel %V", d->shm_chid);
  head = nchan_memstore_get_chanhead(d->shm_chid, d->cf);
  
  if(head == NULL) {
    //ERR("couldn't get chanhead while receiving subscribe ipc msg");
    d->shared_channel_data = NULL;
    d->subscriber = NULL;
  }
  else {
    ipc_sub = memstore_ipc_subscriber_create(sender, &head->id, d->cf, d->origin_chanhead);
    d->subscriber = ipc_sub;
    d->shared_channel_data = head->shared;
    d->owner_chanhead = head;
    memstore_chanhead_reserve(head, "interprocess subscribe");
    
    ngx_atomic_fetch_add(&head->shared->gc.outside_refcount, 1); //it's awkward to put this refcount here, but necessary.
    
    assert(d->shared_channel_data);
  }
  
  if(ipc_sub) {
    rc = head->spooler.fn->add(&head->spooler, ipc_sub);
    d->rc = rc;
  }
  else {
    d->rc = NGX_ERROR;
  }
  
  ipc_cmd(subscribe_reply, sender, d);
  DBG("sent subscribe reply for channel %V to %i", d->shm_chid, sender);
}
static void receive_subscribe_reply(ngx_int_t sender, subscribe_data_t *d) {
  memstore_channel_head_t      *head;
  store_channel_head_shm_t     *old_shared;
  DBG("received subscribe reply for channel %V", d->shm_chid);
  //we have the chanhead address, but are too afraid to use it.
  
  if((head = nchan_memstore_get_chanhead_no_ipc_sub(d->shm_chid, d->cf)) == NULL) {
    ERR("Error regarding an aspect of life or maybe freshly fallen cookie crumbles");
    str_shm_free(d->shm_chid);
    return;
  }
  
  if(head != d->origin_chanhead) {    
    assert(d->owner_chanhead);
    ipc_cmd(subscribe_chanhead_nevermind_release, sender, d);
    return;
  }
  
  if(!d->shared_channel_data && !d->subscriber) {
    //ERR("failed to subscribe");
    nchan_memstore_publish_generic(head, NULL, NGX_HTTP_INSUFFICIENT_STORAGE, NULL);
    head->status = NOTREADY;
    chanhead_gc_add(head, "failed to subscribe to channel owner worker");
  }
  else {
    old_shared = head->shared;
    if(old_shared) {
      assert(old_shared == d->shared_channel_data);
    }
    DBG("receive subscribe proceed to do ipc_sub stuff");
    head->shared = d->shared_channel_data;
    
    if(old_shared == NULL) {
      //ERR("%V local total_sub_count %i, internal_sub_count %i", &head->id,  head->sub_count, head->internal_sub_count);
      assert(head->total_sub_count >= head->internal_sub_count);
      ngx_atomic_fetch_add(&head->shared->sub_count, head->total_sub_count - head->internal_sub_count);
      ngx_atomic_fetch_add(&head->shared->internal_sub_count, head->internal_sub_count);
    }
    else {
      ERR("%V sub count already shared, don't update", &head->id);
    }
    
    assert(head->shared != NULL);
    if(head->foreign_owner_ipc_sub && head->foreign_owner_ipc_sub != d->subscriber) {
      // we got another subscriber for this chanhead, probably due to some very heavy delays
      // discard it, keeping the old one.
      // (It might be nice to discard the _old_ subscriber, but the owner worker may have already deleted it
      // or may have changed altogether due to a previous worker crash)
      ERR("Got ipc-subscriber for an already subscribed channel %V", &head->id);
      memstore_ready_chanhead_unless_stub(head);
      ipc_cmd(subscribe_chanhead_nevermind_release, sender, d);
      return;
    }
    else {
      head->foreign_owner_ipc_sub = d->subscriber;
    }
    
    memstore_ready_chanhead_unless_stub(head);
  }
  
  str_shm_free(d->shm_chid);
  if(d->owner_chanhead) {
    ipc_cmd(subscribe_chanhead_release, sender, d);
  }
}

static void receive_subscribe_chanhead_release(ngx_int_t sender, subscribe_data_t *d) {
  DBG("release the %V", &d->owner_chanhead->id);
  memstore_chanhead_release(d->owner_chanhead, "interprocess subscribe");
}
static void receive_subscribe_chanhead_nevermind_release(ngx_int_t sender, subscribe_data_t *d) {
  ERR("release & nevermind the %V", &d->owner_chanhead->id);
  memstore_channel_head_t      *head;
  head = nchan_memstore_find_chanhead(d->shm_chid);
  if(head == NULL || head != d->owner_chanhead) {
    ERR("wrong chanhead on receive_subscribe_chanhead_nevermind_release ( expected %p, got %p)", d->owner_chanhead, head);
    return;
  }
  
  memstore_ipc_subscriber_unhook(d->subscriber);
  d->subscriber->fn->respond_status(d->subscriber, NGX_HTTP_GONE, NULL, NULL);
  
  memstore_chanhead_release(d->owner_chanhead, "interprocess subscribe");
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
  if(data.shm_chid == NULL) {
    nchan_log_ooshm_error("sending IPC unsubscribe alert for channel %V", chid);
    return NGX_DECLINED;
  }
  return ipc_cmd(unsubscribed, dst, &data);
}
static void receive_unsubscribed(ngx_int_t sender, unsubscribed_data_t *d) {
  DBG("received unsubscribed request for channel %V privdata %p", d->shm_chid, d->privdata);
  if(memstore_channel_owner(d->shm_chid) != memstore_slot()) {
    memstore_channel_head_t    *head;
    //find channel
    head = nchan_memstore_find_chanhead(d->shm_chid);
    if(head == NULL) {
      //already deleted maybe?
      DBG("already unsubscribed...");
      return;
    }
    //gc if no subscribers
    if(head->total_sub_count == 0) {
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
  ngx_int_t                  code;
  const ngx_str_t           *data;
  callback_pt                callback;
  void                      *callback_privdata;
} publish_code_data_t;

ngx_int_t memstore_ipc_send_publish_status(ngx_int_t dst, ngx_str_t *chid, ngx_int_t status_code, const ngx_str_t *status_line, callback_pt callback, void *privdata) {
  DBG("IPC: send publish status to %i ch %V", dst, chid);
  publish_code_data_t  data = {str_shm_copy(chid), status_code, status_line, callback, privdata};
  if(data.shm_chid == NULL) {
    nchan_log_ooshm_error("sending IPC status alert for channel %V", chid);
    return NGX_DECLINED;
  }
  return ipc_cmd(publish_status, dst, &data);
}

static void receive_publish_status(ngx_int_t sender, publish_code_data_t *d) {
  memstore_channel_head_t       *chead;
  
  if((chead = nchan_memstore_find_chanhead(d->shm_chid)) == NULL) {
    if(ngx_exiting || ngx_quit) {
      ERR("can't find chanhead for id %V, but it's okay.", d->shm_chid);
    }
    else {
      ERR("Can't find chanhead for id %V while publishing status %i. This is not a big deal if you just reloaded Nchan.", d->shm_chid, d->code);
    }
    str_shm_free(d->shm_chid);
    return;
  }
  
  DBG("IPC: received publish status for channel %V status %i", d->shm_chid, d->code);
  
  nchan_memstore_publish_generic(chead, NULL, d->code, d->data);
  
  str_shm_free(d->shm_chid);
  d->shm_chid=NULL;
}

////////// PUBLISH_NOTICE ////////////////
ngx_int_t memstore_ipc_send_publish_notice(ngx_int_t dst, ngx_str_t *chid, ngx_int_t notice_code, void *notice_data) {
  DBG("IPC: send publish notice to %i ch %V", dst, chid);
  publish_code_data_t  data = {str_shm_copy(chid), notice_code, notice_data, NULL, NULL};
  if(data.shm_chid == NULL) {
    nchan_log_ooshm_error("sending IPC notice alert for channel %V", chid);
    return NGX_DECLINED;
  }
  return ipc_cmd(publish_notice, dst, &data);
}

static void receive_publish_notice(ngx_int_t sender, publish_code_data_t *d) {
  memstore_channel_head_t       *chead;
  if((chead = nchan_memstore_find_chanhead(d->shm_chid)) == NULL) {
    if(ngx_exiting || ngx_quit) {
      ERR("can't find chanhead for id %V, but it's okay.", d->shm_chid);
    }
    else {
      ERR("Can't find chanhead for id %V while publishing status %i. This is not a big deal if you just reloaded Nchan.", d->shm_chid, d->code);
    }
    str_shm_free(d->shm_chid);
    return;
  }
  
  DBG("IPC: received publish notice for channel %V notice %i", d->shm_chid, d->code);
  
  nchan_memstore_publish_notice(chead, d->code, d->data);
  
  str_shm_free(d->shm_chid);
}

////////// PUBLISH  ////////////////
typedef struct {
  ngx_str_t                 *shm_chid;
  nchan_msg_t               *shm_msg;
  nchan_loc_conf_t          *cf;
  callback_pt                callback;
  void                      *callback_privdata;
  
} publish_data_t;

ngx_int_t memstore_ipc_send_publish_message(ngx_int_t dst, ngx_str_t *chid, nchan_msg_t *shm_msg, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  publish_data_t    data; 
  DEBUG_MEMZERO(&data);
  
  DBG("IPC: send publish message to %i ch %V", dst, chid);
  assert(shm_msg->storage == NCHAN_MSG_SHARED);
  assert(chid->data != NULL);
  data.shm_chid = str_shm_copy(chid);
  if(data.shm_chid == NULL) {
    nchan_log_ooshm_error("sending IPC publish-message alert for channel %V", chid);
    return NGX_DECLINED;
  }
  data.shm_msg = shm_msg;
  data.cf = cf;
  data.callback = callback;
  data.callback_privdata = privdata;
  
  assert(data.shm_chid->data != NULL);
  assert(msg_reserve(shm_msg, "publish_message") == NGX_OK);
  
  return ipc_cmd(publish_message, dst, &data);
}

typedef struct {
  ngx_int_t        sender;
  publish_data_t  *d;
  unsigned         allocd:1;
} publish_callback_data;

static ngx_int_t publish_message_generic_callback(ngx_int_t, void *, void *);

static void receive_publish_message(ngx_int_t sender, publish_data_t *d) {
  
  publish_callback_data         cd_data;
  publish_callback_data        *cd;
  memstore_channel_head_t      *head;

  assert(d->shm_chid->data != NULL);
  
  DBG("IPC: received publish request for channel %V  msg %p", d->shm_chid, d->shm_msg);
  
  if(memstore_channel_owner(d->shm_chid) == memstore_slot()) {
    if(d->cf->redis.enabled) {
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
    
    nchan_store_publish_message_generic(d->shm_chid, d->shm_msg, 1, d->cf, publish_message_generic_callback, cd);
    //string will be freed on publish response
  }
  else {
    if((head = nchan_memstore_get_chanhead(d->shm_chid, d->cf))) {
      nchan_memstore_publish_generic(head, d->shm_msg, 0, NULL);
    }
    else {
      ERR("Unable to get chanhead for publishing");
    }
    
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
  ipc_cmd(publish_message_reply, cd->sender, &rd);
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
  
  if((data.shm_chid = str_shm_copy(chid)) == NULL) {
    nchan_log_ooshm_error("sending IPC get-message alert for channel %V", chid);
    return NGX_DECLINED;
  }
  data.privdata = privdata;
  data.d.req.msgid = *msgid;
  
  DBG("IPC: send get message from %i ch %V", dst, chid);
  assert(data.shm_chid->len >= 1);
  return ipc_cmd(get_message, dst, &data);
}


typedef struct {
  ngx_int_t            sender;
  getmessage_data_t    data;
} getmessage_data_proxy_pd_t;

//for retrieving messages from unready buffers
static ngx_int_t ipc_getmsg_proxy_callback(ngx_int_t code, nchan_msg_t *msg, getmessage_data_proxy_pd_t *ppd) {
  ppd->data.d.resp.getmsg_code = code;
  ppd->data.d.resp.shm_msg = msg;
  if(msg) {
    assert(msg_reserve(msg, "get_message_reply") == NGX_OK);
  }
  ipc_cmd(get_message_reply, ppd->sender, &ppd->data);
  ngx_free(ppd);
  return NGX_OK;
}

static void receive_get_message(ngx_int_t sender, getmessage_data_t *d) {
  memstore_channel_head_t     *head;
  store_message_t             *msg = NULL;
  
  
  assert(d->shm_chid->len >= 1);
  assert(d->shm_chid->data!=NULL);
  DBG("IPC: received get_message request for channel %V privdata %p", d->shm_chid, d->privdata);
  
  head = nchan_memstore_find_chanhead(d->shm_chid);
  if(head == NULL) {
    //no such thing here. reply.
    d->d.resp.getmsg_code = MSG_NOTFOUND;
    d->d.resp.shm_msg = NULL;
  }
  else if(head->msg_buffer_complete) {
    msg = chanhead_find_next_message(head, &d->d.req.msgid, &d->d.resp.getmsg_code);
    d->d.resp.shm_msg = msg == NULL ? NULL : msg->msg;
  }
  else {
    //buffer is not ready. reply when it's ready
    getmessage_data_proxy_pd_t *ppd = ngx_alloc(sizeof(*ppd), ngx_cycle->log);
    if(ppd == NULL) {
      ERR("couldn't allocate getmessage proxy data for ipc get_message");
      goto err;
    }
    ppd->data = *d;
    ppd->sender = sender;
    
    subscriber_t *getmsg_sub = getmsg_proxy_subscriber_create(&d->d.req.msgid, (callback_pt )ipc_getmsg_proxy_callback, ppd);
    if(!getmsg_sub) {
      ERR("couldn't allocate getmessage proxy subscriber for ipc get_message");
      goto err;
    }
    if(head->spooler.fn->add(&head->spooler, getmsg_sub) != NGX_OK) {
      ERR("couldn't enqueue getmsg proxy subscriber for ipc get_message");
      goto err;
    }
    return;
  }
  if(d->d.resp.shm_msg) {
    assert(msg_reserve(d->d.resp.shm_msg, "get_message_reply") == NGX_OK);
  }
  DBG("IPC: send get_message_reply for channel %V  msg %p, privdata: %p", d->shm_chid, msg, d->privdata);
  ipc_cmd(get_message_reply, sender, d);
  return;

err:
  d->d.resp.getmsg_code = MSG_ERROR;
  d->d.resp.shm_msg = NULL;
  ipc_cmd(get_message_reply, sender, d);
}

static void receive_get_message_reply(ngx_int_t sender, getmessage_data_t *d) {
  
  assert(d->shm_chid->len >= 1);
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
  if(data.shm_chid == NULL) {
    nchan_log_ooshm_error("sending IPC send-delete alert for channel %V", chid);
    return NGX_DECLINED;
  }
  DBG("IPC: send delete to %i ch %V", dst, chid);
  return ipc_cmd(delete, dst, &data);
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
    if((chan_info = shm_alloc(nchan_store_memory_shmem, sizeof(*chan_info), "channel info for delete IPC response")) == NULL) {
      d->shm_channel_info = NULL;
      d->code = NGX_HTTP_INSUFFICIENT_STORAGE;
      nchan_log_ooshm_error("sending IPC delete-reply alert for channel %V", d->shm_chid);
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
  ipc_cmd(delete_reply, d->sender, d);
  return NGX_OK;
}
static void receive_delete_reply(ngx_int_t sender, delete_data_t *d) {
  
  DBG("IPC received delete reply for channel %V privdata %p", d->shm_chid, d->privdata);
  d->callback(d->code, d->shm_channel_info, d->privdata);
  
  if(d->shm_channel_info != NULL) {
    shm_free(nchan_store_memory_shmem, d->shm_channel_info);
  }
  str_shm_free(d->shm_chid);
}




////////// GET CHANNEL INFO ////////////////
typedef struct {
  ngx_str_t                 *shm_chid;
  nchan_loc_conf_t          *cf;
  store_channel_head_shm_t  *channel_info;
  nchan_msg_id_t             last_msgid;
  callback_pt                callback;
  void                      *privdata;
} channel_info_data_t;

ngx_int_t memstore_ipc_send_get_channel_info(ngx_int_t dst, ngx_str_t *chid, nchan_loc_conf_t *cf, callback_pt callback, void* privdata) {
  DBG("send get_channel_info to %i %V", dst, chid);
  channel_info_data_t        data;
  DEBUG_MEMZERO(&data);
  if((data.shm_chid = str_shm_copy(chid)) == NULL) {
    nchan_log_ooshm_error("sending IPC get-channel-info alert for channel %V", chid);
    return NGX_DECLINED;
  }

  data.channel_info = NULL;
  data.last_msgid = zero_msgid;
  data.cf = cf;
  data.callback = callback;
  data.privdata = privdata;
  return ipc_cmd(get_channel_info, dst, &data);
}

typedef struct {
  channel_info_data_t   d;
  ngx_int_t             sender;
} channel_info_find_chanhead_backup_data_t;

static void receive_get_channel_info_continued(ngx_int_t sender, channel_info_data_t *d, memstore_channel_head_t *head) {
  assert(memstore_slot() == memstore_channel_owner(d->shm_chid));
  if(head == NULL) {
    //already deleted maybe?
    DBG("channel not for for get_channel_info");
    d->channel_info = NULL;
  }
  else {
    d->channel_info = head->shared;
    assert(head->latest_msgid.tagcount <= 1);
    d->last_msgid = head->latest_msgid;
  }
  ipc_cmd(get_channel_info_reply, sender, d);
}

static ngx_int_t find_chanhead_w_backup_callback(ngx_int_t rc, void *vd, void *pd) {
  channel_info_find_chanhead_backup_data_t *d = pd;
  memstore_channel_head_t                  *head = vd;
  
  receive_get_channel_info_continued(d->sender, &d->d, head);
  
  ngx_free(d);
  return NGX_OK;
}

static void receive_get_channel_info(ngx_int_t sender, channel_info_data_t *d) {
  memstore_channel_head_t    *head;
  
  DBG("received get_channel_info request for channel %V privdata %p", d->shm_chid, d->privdata);
  if(d->cf->redis.enabled && d->cf->redis.storage_mode == REDIS_MODE_BACKUP) {
    channel_info_find_chanhead_backup_data_t *dd = ngx_alloc(sizeof(*dd), ngx_cycle->log);
    dd->d = *d;
    dd->sender = sender;
    nchan_memstore_find_chanhead_with_backup(d->shm_chid, d->cf, find_chanhead_w_backup_callback, dd);
  }
  else {
    head = nchan_memstore_find_chanhead(d->shm_chid);
    receive_get_channel_info_continued(sender, d, head);
  }
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


////////// CHANNEL AUTHORIZATION DATA ////////////////
typedef struct {
  ngx_str_t               *shm_chid;
  unsigned                 auth_ok:1;
  unsigned                 channel_must_exist:1;
  nchan_loc_conf_t        *cf;
  ngx_int_t                max_subscribers;
  callback_pt              callback;
  void                    *privdata;
} channel_authcheck_data_t;

ngx_int_t memstore_ipc_send_channel_existence_check(ngx_int_t dst, ngx_str_t *chid, nchan_loc_conf_t *cf, callback_pt callback, void* privdata) {
  DBG("send channel_auth_check to %i %V", dst, chid);
  channel_authcheck_data_t        data;
  DEBUG_MEMZERO(&data);
  if((data.shm_chid = str_shm_copy(chid)) == NULL) {
    nchan_log_ooshm_error("sending IPC channel-existence-check alert for channel %V", chid);
    return NGX_DECLINED;
  }
  data.auth_ok = 0;
  data.channel_must_exist = cf->subscribe_only_existing_channel;
  data.cf = cf;
  data.max_subscribers = cf->max_channel_subscribers;
  data.callback = callback;
  data.privdata = privdata;
  
  return ipc_cmd(channel_auth_check, dst, &data);
}

typedef struct {
  ngx_int_t                 sender;
  channel_authcheck_data_t  d;
} channel_authcheck_data_callback_t;

static ngx_int_t redis_receive_channel_auth_check_callback(ngx_int_t status, void *ch, void *d) {
  nchan_channel_t                     *channel = (nchan_channel_t *)ch;
  channel_authcheck_data_callback_t   *data = (channel_authcheck_data_callback_t *)d;
  if(channel == NULL) {
    data->d.auth_ok = !data->d.channel_must_exist;
  }
  else if(data->d.max_subscribers == 0) {
    data->d.auth_ok = 1;
  }
  else {
    data->d.auth_ok = channel->subscribers < data->d.max_subscribers;
  }
  ipc_cmd(channel_auth_check_reply, data->sender, &data->d);
  ngx_free(d);
  return NGX_OK;
}

static void receive_channel_auth_check(ngx_int_t sender, channel_authcheck_data_t *d) {
  memstore_channel_head_t    *head;
  
  DBG("received channel_auth_check request for channel %V privdata %p", d->shm_chid, d->privdata);
  
  assert(memstore_slot() == memstore_channel_owner(d->shm_chid));
  if(!d->cf->redis.enabled) {
    head = nchan_memstore_find_chanhead(d->shm_chid);
    if(head == NULL) {
      d->auth_ok = !d->channel_must_exist;
    }
    else if (d->max_subscribers == 0) {
      d->auth_ok = 1;
    }
    else {
      assert(head->shared);
      d->auth_ok = head->shared->sub_count < (ngx_uint_t )d->max_subscribers;
    }
    ipc_cmd(channel_auth_check_reply, sender, d);
  }
  else {
    channel_authcheck_data_callback_t    *dd = ngx_alloc(sizeof(*dd), ngx_cycle->log);
    dd->d = *d;
    dd->sender = sender;
    nchan_store_redis.find_channel(d->shm_chid, d->cf, redis_receive_channel_auth_check_callback, dd);
  }
}

static void receive_channel_auth_check_reply(ngx_int_t sender, channel_authcheck_data_t *d) {
  d->callback(d->auth_ok, NULL, d->privdata);
  str_shm_free(d->shm_chid);
}

/////////// SUBSCRIBER KEEPALIVE ///////////
typedef enum {
  KA_REPLY_NORENEW = 0, 
  KA_REPLY_RENEW,
  KA_REPLY_UNHOOK_NORENEW,
} keepalive_reply_action_t;

typedef struct {
  ngx_str_t                   *shm_chid;
  subscriber_t                *ipc_sub;
  memstore_channel_head_t     *originator;
  keepalive_reply_action_t     reply_action;  
} sub_keepalive_data_t;

ngx_int_t memstore_ipc_send_memstore_subscriber_keepalive(ngx_int_t dst, ngx_str_t *chid, subscriber_t *sub, memstore_channel_head_t *ch) {
  sub_keepalive_data_t        data;
  DEBUG_MEMZERO(&data);
  if((data.shm_chid = str_shm_copy(chid)) == NULL) {
    nchan_log_ooshm_error("sending IPC keepalive alert for channel %V", chid);
    return NGX_DECLINED;
  }
  data.ipc_sub = sub;
  data.originator = ch;
  
  data.reply_action = KA_REPLY_NORENEW;
  
  sub->fn->reserve(sub);
  
  DBG("send SUBSCRIBER KEEPALIVE to %i %V", dst, chid);
  
  ipc_cmd(subscriber_keepalive, dst, &data);
  return NGX_OK;
}
static void receive_subscriber_keepalive(ngx_int_t sender, sub_keepalive_data_t *d) {
  memstore_channel_head_t    *head;
  DBG("received SUBSCRIBER KEEPALIVE from %i for channel %V", sender, d->shm_chid);
  head = nchan_memstore_find_chanhead(d->shm_chid);
  str_shm_free(d->shm_chid);
  
  if(head == NULL) {
    DBG("not subscribed anymore");
    d->reply_action = KA_REPLY_NORENEW;
  }
  else {
    if(head != d->originator) {
      ERR("Got keepalive for expired channel %V", &head->id);
      d->reply_action = KA_REPLY_UNHOOK_NORENEW;
    }
    else if(head->status != READY && head->status != STUBBED) {
      if(head->status == WAITING && head->foreign_owner_ipc_sub == NULL) {
        //wrong-status head. don't renew, get rid of this chanhead
        nchan_memstore_publish_generic(head, NULL, NGX_HTTP_SERVICE_UNAVAILABLE, NULL);
        nchan_memstore_force_delete_channel(d->shm_chid, NULL, NULL);
        d->reply_action = KA_REPLY_UNHOOK_NORENEW;
      }
      else {
        //something's gone extra weird. 
        //kick out these subs, delete chanhead... just in case
        nchan_memstore_publish_generic(head, NULL, NGX_HTTP_SERVICE_UNAVAILABLE, NULL);
        nchan_memstore_force_delete_channel(d->shm_chid, NULL, NULL);
        d->reply_action = KA_REPLY_UNHOOK_NORENEW;
      }
    }
    else if(head->foreign_owner_ipc_sub != d->ipc_sub) {
      // we got another subscriber for this chanhead, probably due to some very heavy delays
      // discard it, keeping the old one.
      // (It might be nice to discard the _old_ subscriber, but the owner worker may have already deleted it
      ERR("Got ipc-subscriber during keepalive for an already subscribed channel %V", &head->id);
      d->reply_action = KA_REPLY_UNHOOK_NORENEW;
    }
    else if(head->total_sub_count == 0) {
      if(ngx_time() - head->last_subscribed_local > MEMSTORE_IPC_SUBSCRIBER_TIMEOUT) {
        d->reply_action = KA_REPLY_NORENEW;
        DBG("No subscribers lately. Time... to die.");
      }
      else {
        DBG("No subscribers, but there was one %i sec ago. don't unsubscribe.", ngx_time() - head->last_subscribed_local);
        d->reply_action = KA_REPLY_RENEW;
      }
    }
    else {
      d->reply_action = KA_REPLY_RENEW;
    }
  }
  ipc_cmd(subscriber_keepalive_reply, sender, d);
}

static void receive_subscriber_keepalive_reply(ngx_int_t sender, sub_keepalive_data_t *d) {
  subscriber_t     *sub = d->ipc_sub;
  switch(d->reply_action) {
    case KA_REPLY_NORENEW:
      sub->fn->dequeue(sub);
      sub->fn->release(sub, 0);
      break;
    case KA_REPLY_RENEW:
      memstore_ipc_subscriber_keepalive_renew(sub);
      sub->fn->release(sub, 0);
      break;
    case KA_REPLY_UNHOOK_NORENEW:
      memstore_ipc_subscriber_unhook(sub);
      sub->fn->release(sub, 0);
      break;
    default:
      raise(SIGABRT);
  }
}


/////////// GROUPS ///////////

ngx_int_t memstore_ipc_send_get_group(ngx_int_t dst, ngx_str_t *group_id) {
  ngx_str_t         *shm_id = str_shm_copy(group_id);
  if(shm_id == NULL) {
    nchan_log_ooshm_error("sending IPC get-group alert for group %V", group_id);
    return NGX_DECLINED;
  }
  DBG("send GET GROUP to %i %p %V", dst, shm_id, shm_id);
  ipc_cmd(get_group, dst, &shm_id);
  return NGX_OK;
}

static void receive_get_group(ngx_int_t sender, ngx_str_t **group_id) {
  nchan_group_t  *shared_group;
  int             new_group;
  DBG("received GET GROUP from %i %p %V", sender, *group_id, *group_id);
  
  shared_group = memstore_group_owner_find(nchan_memstore_get_groups(), *group_id, &new_group);
  if(!new_group) { //new group is automatically broadcast to everyone, this one already exists.
    ipc_cmd(group, sender, &shared_group);
  }
  
  str_shm_free(*group_id);
}

ngx_int_t memstore_ipc_broadcast_group(nchan_group_t *shared_group) {
  DBG("broadcast GROUP %V to everyone but me", &shared_group->name);
  ipc_broadcast_cmd(group, &shared_group);
  
  return NGX_OK;
}

static void receive_group(ngx_int_t sender, nchan_group_t **shared_group) {
  DBG("receive GROUP %V", &(*shared_group)->name);
  
  memstore_group_receive(nchan_memstore_get_groups(), *shared_group);
}

ngx_int_t memstore_ipc_broadcast_group_delete(nchan_group_t *shared_group) {
  DBG("send DELETE GROUP");
  ipc_broadcast_cmd(group_delete, &shared_group);
  return NGX_OK;
}

static void receive_group_delete(ngx_int_t sender, nchan_group_t **shared_group) {
  DBG("receive GROUP DELETE %V", &(*shared_group)->name);
  
  memstore_group_receive_delete(nchan_memstore_get_groups(), *shared_group);
}

/////////// FLOOD TEST ///////////
typedef struct {
  uint64_t          n;
} flood_data_t;


static int  flood_seq = 0;
ngx_int_t memstore_ipc_send_flood_test(ngx_int_t dst) {
  flood_data_t        data = {flood_seq++};
  ipc_cmd(flood_test, dst, &data);
  return NGX_OK;
}


static void receive_flood_test(ngx_int_t sender, flood_data_t *d) {
  struct timespec  tv;
  tv.tv_sec=0;
  tv.tv_nsec=8000000;
  ERR("      received FLOOD TEST from %i seq %l", sender, d->n);
  nanosleep(&tv, NULL);
}

////////// BENCHMARK INITIALIZE //////////////
typedef struct {
  nchan_loc_conf_t *loc_conf;
  time_t        time;
  uint32_t      id;
  nchan_benchmark_shared_t shared;
} benchmark_initialize_data_t;

ngx_int_t memstore_ipc_broadcast_benchmark_initialize(void *bench_pd) {
  nchan_benchmark_t            *bench = bench_pd;
  benchmark_initialize_data_t   data;
  DEBUG_MEMZERO(&data);
  data.loc_conf = bench->loc_conf;
  data.id = bench->id;
  data.time = bench->time.init;
  data.shared = bench->shared;
  
  return ipc_broadcast_cmd(benchmark_initialize, &data);
}
static void receive_benchmark_initialize(ngx_int_t sender, benchmark_initialize_data_t *d) {
  nchan_benchmark_initialize_from_ipc(sender, d->loc_conf, d->time, d->id, &d->shared);
}

////////// BENCHMARK RUN ////////////////
typedef struct {
  nchan_loc_conf_t *loc_conf;
} benchmark_run_data_t;

ngx_int_t memstore_ipc_broadcast_benchmark_run(void) {
  benchmark_run_data_t        data;
  DEBUG_MEMZERO(&data);
  return ipc_broadcast_cmd(benchmark_start, &data);
}
static void receive_benchmark_start(ngx_int_t sender, benchmark_run_data_t *d) {
  nchan_benchmark_run();
}

////////// BENCHMARK STOP ////////////////
typedef struct {
  nchan_loc_conf_t *loc_conf;
} benchmark_stop_data_t;

ngx_int_t memstore_ipc_broadcast_benchmark_stop(void) {
  benchmark_stop_data_t        data;
  DEBUG_MEMZERO(&data);
  return ipc_broadcast_cmd(benchmark_stop, &data);
}
static void receive_benchmark_stop(ngx_int_t sender, benchmark_stop_data_t *d) {
  nchan_benchmark_stop();
}

////////// BENCHMARK FINISH ////////////////
typedef struct {
  nchan_benchmark_data_t data;
} benchmark_finish_data_t;

ngx_int_t memstore_ipc_broadcast_benchmark_finish(void) {
  benchmark_finish_data_t        data;
  DEBUG_MEMZERO(&data);
  return ipc_broadcast_cmd(benchmark_finish, &data);
}
static void receive_benchmark_finish(ngx_int_t sender, benchmark_finish_data_t *d) {
  nchan_benchmark_t        *bench = nchan_benchmark_get_active();
  benchmark_finish_data_t   data;
  DEBUG_MEMZERO(&data);
  nchan_benchmark_dequeue_subscribers();
  data.data = bench->data;
  ipc_cmd(benchmark_finish_reply, sender, &data);
  nchan_benchmark_cleanup();
}

static void receive_benchmark_finish_reply(ngx_int_t sender, benchmark_finish_data_t *d) {
  nchan_benchmark_receive_finished_data(&d->data);
}

////////// BENCHMARK ABORT ////////////////
typedef struct {
  nchan_benchmark_data_t data;
} benchmark_abort_data_t;

ngx_int_t memstore_ipc_broadcast_benchmark_abort(void) {
  benchmark_abort_data_t        data;
  DEBUG_MEMZERO(&data);
  return ipc_broadcast_cmd(benchmark_abort, &data);
}
static void receive_benchmark_abort(ngx_int_t sender, benchmark_abort_data_t *d) {
  nchan_benchmark_abort();
}

#define MAKE_ipc_cmd_handler(val) [offsetof(ipc_handlers_t, val)/sizeof(ipc_handler_pt)] = (ipc_handler_pt )receive_ ## val,
static ipc_handler_pt ipc_cmd_handler[] = {
  LIST_IPC_COMMANDS(MAKE_ipc_cmd_handler)
};

void memstore_ipc_alert_handler(ngx_int_t sender, ngx_uint_t code, void *data) {
  if(code >= IPC_CMDS) {
    ERR("received invalid code %ui from sender %i", code, sender);
    return;
  }
  ipc_cmd_handler[code](sender, data);
}
