#include <nchan_module.h>
#include <subscribers/common.h>
#include <store/memory/ipc.h>
#include <store/memory/store-private.h>
#include <store/memory/ipc-handlers.h>

#include <store/memory/store.h>
#include <store/redis/store.h>

#include "internal.h"
#include "memstore_multi.h"
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:MEM-MULTI:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:MEM-MULTI:" fmt, ##arg)


typedef struct {
  memstore_channel_head_t      *target_chanhead;
  memstore_channel_head_t      *multi_chanhead;
  memstore_multi_t             *multi;
  ngx_int_t                     n;
} sub_data_t;

/*
static ngx_int_t empty_callback(){
  return NGX_OK;
}
*/

static void change_sub_count(memstore_channel_head_t *ch, ngx_int_t n) {
  ch->total_sub_count += n;
  ch->channel.subscribers += n;
  if(ch->shared) {
    ngx_atomic_fetch_add(&ch->shared->sub_count, n);
  }
  if(ch->cf->redis.enabled && ch->cf->redis.storage_mode >= REDIS_MODE_DISTRIBUTED) {
    memstore_fakesub_add(ch, n);
  }
}

static ngx_int_t sub_enqueue(ngx_int_t status, void *ptr, sub_data_t *d) {
  DBG("%p enqueued (%p %V %i) %V", d->multi->sub, d->multi_chanhead, &d->multi_chanhead->id, d->n, &d->multi->id);
  assert(d->multi_chanhead->multi_subscribers_pending > 0);
  d->multi_chanhead->multi_subscribers_pending --;
  if(d->multi_chanhead->multi_subscribers_pending == 0) {
    memstore_ready_chanhead_unless_stub(d->multi_chanhead);
  }
  
  return NGX_OK;
}

static ngx_int_t sub_dequeue(ngx_int_t status, void *ptr, sub_data_t* d) {
  DBG("%p dequeued (%p %V %i) %V", d->multi->sub, d->multi_chanhead, &d->multi_chanhead->id, d->n, &d->multi->id);
  d->multi_chanhead->status = WAITING;
  d->multi_chanhead->multi_subscribers_pending++;
  d->multi->sub = NULL;
  
  return NGX_OK;
}

static ngx_int_t sub_respond_message(ngx_int_t status, nchan_msg_t *msg, sub_data_t* d) {
  nchan_msg_t       remsg;
  ngx_int_t         mcount;
  
  int16_t          tags[NCHAN_MULTITAG_MAX], prevtags[NCHAN_MULTITAG_MAX];
  
  //remsg = ngx_alloc(sizeof(*remsg), ngx_cycle->log);
  //assert(remsg);
  
  assert( msg->id.tagcount == 1 );
  assert( msg->prev_id.tagcount == 1 );
  nchan_msg_derive_stack(msg, &remsg, tags);
  
  mcount = d->multi_chanhead->multi_count;
  
  remsg.prev_id.tagcount = mcount;
  remsg.prev_id.tagactive = d->n;
  
  remsg.id.tagcount = mcount;
  remsg.id.tagactive = d->n;
  
  if(mcount > NCHAN_FIXED_MULTITAG_MAX) {
    remsg.id.tag.allocd = tags;
    tags[0]=msg->id.tag.fixed[0];
    remsg.prev_id.tag.allocd = prevtags;
    prevtags[0]=msg->prev_id.tag.fixed[0];
  }
  
  nchan_expand_msg_id_multi_tag(&remsg.prev_id, 0, d->n, -1);
  nchan_expand_msg_id_multi_tag(&remsg.id, 0, d->n, -1);
  
  memstore_ensure_chanhead_is_ready(d->multi_chanhead, 1);
  
  DBG("%p respond with transformed message %p %V (%p %V %i) %V", d->multi->sub, &remsg, msgid_to_str(&remsg.id), d->multi_chanhead, &d->multi_chanhead->id, d->n, &d->multi->id);
  
  nchan_memstore_publish_generic(d->multi_chanhead, &remsg, 0, NULL);
  
  return NGX_OK;
}

static ngx_int_t sub_respond_status(ngx_int_t status, void *ptr, sub_data_t *d) {
  DBG("%p subscriber respond with status %i (%p %V %i) %V", d->multi->sub, status, d->multi_chanhead, &d->multi_chanhead->id, d->n, &d->multi->id);
  switch(status) {
    case NGX_HTTP_GONE: //delete
    case NGX_HTTP_CLOSE: //delete
      nchan_memstore_publish_generic(d->multi_chanhead, NULL, NGX_HTTP_GONE, &NCHAN_HTTP_STATUS_410);
      //nchan_store_memory.delete_channel(d->chid, NULL, NULL);
      break;
    
    case NGX_HTTP_CONFLICT:
      nchan_memstore_publish_generic(d->multi_chanhead, NULL, NGX_HTTP_CONFLICT, &NCHAN_HTTP_STATUS_410);
      break;
    
    default:
      //meh, no big deal.
      break;
  }
  
  return NGX_OK;
}

static ngx_int_t sub_notify_handler(ngx_int_t code, void *data, sub_data_t *d) {
  if(code == NCHAN_SUB_MULTI_NOTIFY_ADDSUB) {
    change_sub_count(d->target_chanhead, (ngx_int_t )data);
  }
  else if(code == NCHAN_NOTICE_SUBSCRIBER_INFO_REQUEST) {
    nchan_memstore_publish_notice(d->multi_chanhead, code, data);
  }
  return NGX_OK;
}

static ngx_str_t  sub_name = ngx_string("memstore-multi");

subscriber_t *memstore_multi_subscriber_create(memstore_channel_head_t *chanhead, uint8_t n) {
  static nchan_msg_id_t        latest_msgid = NCHAN_NEWEST_MSGID;
  sub_data_t                  *d;
  memstore_channel_head_t     *target_ch;
  ngx_int_t                    multi_subs;
  subscriber_t                *sub;
  
  if((target_ch = nchan_memstore_get_chanhead(&chanhead->multi[n].id, chanhead->cf))==NULL) {
    return NULL;
  }
  
  assert(chanhead->cf);
  
  sub = internal_subscriber_create_init(&sub_name, chanhead->cf, sizeof(*d), (void **)&d, (callback_pt )sub_enqueue, (callback_pt )sub_dequeue, (callback_pt )sub_respond_message, (callback_pt )sub_respond_status, (callback_pt )sub_notify_handler, NULL);
  
  sub->last_msgid = latest_msgid;
  
  sub->destroy_after_dequeue = 1;
  sub->dequeue_after_response = 0;

  //DBG("create multi sub for %V (n=%i) pending=%i", &chanhead->multi[n].id, n, chanhead->multi_subscribers_pending);
  d->multi = &chanhead->multi[n];
  d->multi->sub = sub;
  d->multi_chanhead = chanhead;
  d->n = n;
  d->target_chanhead = target_ch;
  
  assert(chanhead->multi_subscribers_pending > 0);

  target_ch->spooler.fn->add(&target_ch->spooler, sub);
  
  multi_subs = chanhead->shared->sub_count;

  change_sub_count(target_ch, multi_subs);
  
  DBG("%p created with privdata %p", d->multi->sub, d);
  return sub;
}
