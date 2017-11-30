#include <nchan_module.h>
#include "spool.h"
#include <assert.h>

#define DEBUG_LEVEL NGX_LOG_DEBUG
//#define DEBUG_LEVEL NGX_LOG_WARN

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SPOOL:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SPOOL:" fmt, ##arg)

#define NCHAN_MSG_NORESPONSE_RETRY_TIME 200
#define NCHAN_SPOOL_FETCHMSG_MAX_TIMES 20

//////// SPOOLs -- Subscriber Pools  /////////

static ngx_int_t spool_remove_subscriber(subscriber_pool_t *, spooled_subscriber_t *);
static void spool_bubbleup_dequeue_handler(subscriber_pool_t *spool, subscriber_t *sub, channel_spooler_t *spl);
//static void spool_bubbleup_bulk_dequeue_handler(subscriber_pool_t *spool, subscriber_type_t type, ngx_int_t count, channel_spooler_t *spl);
static ngx_int_t spool_respond_general(subscriber_pool_t *self, nchan_msg_t *msg, ngx_int_t status_code, void *code_data, unsigned notice);
static ngx_int_t spool_transfer_subscribers(subscriber_pool_t *spool, subscriber_pool_t *newspool, uint8_t update_subscriber_last_msgid);
static ngx_int_t destroy_spool(subscriber_pool_t *spool);
static ngx_int_t remove_spool(subscriber_pool_t *spool);
static ngx_int_t spool_fetch_msg(subscriber_pool_t *spool);

static nchan_msg_id_t     latest_msg_id = NCHAN_NEWEST_MSGID;
static nchan_msg_id_t     oldest_msg_id = NCHAN_OLDEST_MSGID;


static subscriber_pool_t *find_spool(channel_spooler_t *spl, nchan_msg_id_t *id) {
  rbtree_seed_t      *seed = &spl->spoolseed;
  ngx_rbtree_node_t  *node;
  
  if((node = rbtree_find_node(seed, id)) != NULL) {
    return (subscriber_pool_t *)rbtree_data_from_node(node);
  }
  else {
    return NULL;
  }
}
/*
typedef struct {
  int                  n;
  subscriber_pool_t   *msg_expected_spool;
  
  int                  err;
} spool_verify_data_t;


static void log_spool_err(spool_verify_data_t *d, subscriber_pool_t *spool, char *str) {
  ERR("%p %s [%s]", spool, msg_status_to_chr(spool->msg_status), str);
  d->err = 1;
}

static int validate_spooler_walker(rbtree_seed_t *seed, subscriber_pool_t *spool, spool_verify_data_t *d) {
  if(spool->msg_status == MSG_EXPECTED) {
    if(d->msg_expected_spool) {
      log_spool_err(d, spool, "Found more than 1 spool with MSG_EXPECTED status");
    }
    else {
      d->msg_expected_spool = spool;
    }
  }
  else {
    if(spool->sub_count == 0) {
      log_spool_err(d, spool, "empty spool (not MSG_EXPECTED)");
    }
  }
  d->n++;

  return NGX_OK;
}

static int validate_spooler(channel_spooler_t *spl, char *str) {
  spool_verify_data_t  d;
  ngx_memzero(&d, sizeof(d));
  rbtree_walk(&spl->spoolseed, (rbtree_walk_callback_pt )validate_spooler_walker, &d);
  if(d.err == 0) {
    ERR("%s: validated %i spools in channel spooler %V", str, d.n, spl->chid);
  }
  else {
    ERR("%s: validating %i spools  FAILED in channel spooler %V; %i error(s).", str, d.n, spl->chid, d.err);
  }
  return d.err == 0;
}
*/

static int msg_ids_equal(nchan_msg_id_t *id1, nchan_msg_id_t *id2) {
  int           i, max;
  int16_t      *tags1, *tags2;
  
  if(id1->time != id2->time || id1->tagcount != id2->tagcount) return 0;
  max = id1->tagcount;
  if(max <= NCHAN_FIXED_MULTITAG_MAX) {
    tags1 = id1->tag.fixed;
    tags2 = id2->tag.fixed;
  }
  else {
    tags1 = id1->tag.allocd;
    tags2 = id2->tag.allocd;
  }
  
  
  for(i=0; i < max; i++) {
    if(tags1[i] != tags2[i]) return 0;
  }
  return 1;
}

static void spooler_timer_handler(ngx_event_t *ev) {
  spooler_event_ll_t *spl_ev = container_of(ev, spooler_event_ll_t, ev);
  spl_ev->callback(ev->data);
  if(spl_ev->prev) {
    spl_ev->prev->next = spl_ev->next;
  }
  if(spl_ev->next) {
    spl_ev->next->prev = spl_ev->prev;
  }
  if(spl_ev->spooler->spooler_dependent_events == spl_ev) {
    spl_ev->spooler->spooler_dependent_events = spl_ev->next;
  }
  ngx_free(spl_ev);
}

ngx_event_t *spooler_add_timer(channel_spooler_t *spl, ngx_msec_t timeout, void (*cb)(void *), void (*cancel)(void *), void *pd) {
  spooler_event_ll_t  *spl_ev = ngx_alloc(sizeof(*spl_ev), ngx_cycle->log);
  ngx_memzero(&spl_ev->ev, sizeof(spl_ev->ev));
  nchan_init_timer(&spl_ev->ev, spooler_timer_handler, pd);
  
  spl_ev->callback = cb;
  spl_ev->cancel = cancel;
  
  spl_ev->spooler = spl;
  spl_ev->next = spl->spooler_dependent_events;
  spl_ev->prev = NULL;
  if(spl->spooler_dependent_events) {
    spl->spooler_dependent_events->prev = spl_ev;
  }
  spl->spooler_dependent_events = spl_ev;
  
  ngx_add_timer(&spl_ev->ev, timeout);
  return &spl_ev->ev;
}

static void fetchmsg_ev_handler(ngx_event_t *ev) {
  subscriber_pool_t *spool = (subscriber_pool_t *)ev->data;
  DBG("stack-overflow-buster fetchmsg event for spool %p", spool);
  if(spool->msg_status == MSG_INVALID) {
    spool_fetch_msg(spool);
  }
}

static ngx_inline void init_spool(channel_spooler_t *spl, subscriber_pool_t *spool, nchan_msg_id_t *id) {
  nchan_copy_new_msg_id(&spool->id, id);
  spool->msg = NULL;
  spool->msg_status = MSG_INVALID;
  
  spool->first = NULL;
  spool->pool = NULL;
  spool->sub_count = 0;
  spool->non_internal_sub_count = 0;
  spool->generation = 0;
  spool->responded_count = 0;
  ngx_memzero(&spool->fetchmsg_ev, sizeof(spool->fetchmsg_ev));
  nchan_init_timer(&spool->fetchmsg_ev, fetchmsg_ev_handler, spool);
  spool->fetchmsg_current_count=0;
  spool->fetchmsg_prev_msec=0;
  
  spool->spooler = spl;
}

static subscriber_pool_t *get_spool(channel_spooler_t *spl, nchan_msg_id_t *id) {
  rbtree_seed_t      *seed = &spl->spoolseed;
  ngx_rbtree_node_t  *node;
  subscriber_pool_t *spool;
  
  if(id->time == NCHAN_NEWEST_MSGID_TIME) {
    spool = &spl->current_msg_spool;
    spool->msg_status = MSG_EXPECTED;
    return &spl->current_msg_spool;
  }
  
  if((node = rbtree_find_node(seed, id)) == NULL) {
    
    if((node = rbtree_create_node(seed, sizeof(*spool))) == NULL) {
      ERR("can't create rbtree node for spool");
      return NULL;
    }
    
   // DBG("CREATED spool node %p for msgid %V", node, msgid_to_str(id));
    spool = (subscriber_pool_t *)rbtree_data_from_node(node);
    
    init_spool(spl, spool, id);
    
    if(rbtree_insert_node(seed, node) != NGX_OK) {
      ERR("couldn't insert spool node");
      rbtree_destroy_node(seed, node);
      return NULL;
    }
  }
  else {
    spool = (subscriber_pool_t *)rbtree_data_from_node(node);
    DBG("found spool node %p with msgid %V", node, msgid_to_str(id));
    assert(spool->id.time == id->time);
  }
  return spool;
}

static ngx_int_t spool_nextmsg(subscriber_pool_t *spool, nchan_msg_id_t *new_last_id) {
  subscriber_pool_t      *newspool;
  channel_spooler_t      *spl = spool->spooler;
  
  ngx_int_t               immortal_spool = spool->id.time == NCHAN_NEWEST_MSGID_TIME;
  int16_t                 largetags[NCHAN_MULTITAG_MAX];
  nchan_msg_id_t          new_id = NCHAN_ZERO_MSGID;
  
  nchan_copy_msg_id(&new_id, &spool->id, largetags);
  nchan_update_multi_msgid(&new_id, new_last_id, largetags);
  
  //ERR("spool %p nextmsg (%V) --", spool, msgid_to_str(&spool->id));
  //ERR(" --  update with               (%V) --", msgid_to_str(new_last_id));
  //ERR(" -- newid                       %V", msgid_to_str(&new_id));
  
  if(msg_ids_equal(&spool->id, &new_id)) {
    ERR("nextmsg id same as curmsg (%V)", msgid_to_str(&spool->id));
    assert(0);
  }
  else {
    newspool = !immortal_spool ? find_spool(spl, &new_id) : get_spool(spl, &new_id);
    
    if(newspool != NULL) {
      assert(spool != newspool);
      spool_transfer_subscribers(spool, newspool, 0);
      if(!immortal_spool) destroy_spool(spool);
    }
    else {
      ngx_rbtree_node_t       *node;
      assert(!immortal_spool);
      node = rbtree_node_from_data(spool);
      rbtree_remove_node(&spl->spoolseed, node);
      nchan_copy_msg_id(&spool->id, &new_id, NULL);
      rbtree_insert_node(&spl->spoolseed, node);
      spool->msg_status = MSG_INVALID;
      spool->msg = NULL;
      newspool = spool;
      
      /*
      newspool = get_spool(spl, &new_id);
      assert(spool != newspool);
      spool_transfer_subscribers(spool, newspool, 0);
      destroy_spool(spool);
      */
    }

    
    if(newspool->non_internal_sub_count > 0 && spl->handlers->use != NULL) {
      spl->handlers->use(spl, spl->handlers_privdata);
    }
    
    if(newspool->sub_count > 0) {
      switch(newspool->msg_status) {
        case MSG_CHANNEL_NOTREADY:
          newspool->msg_status = MSG_INVALID;
          /*fallthrough*/
        case MSG_INVALID:
          spool_fetch_msg(newspool);
          break;
        case MSG_EXPECTED:
          spool_respond_general(newspool, NULL, NGX_HTTP_NO_CONTENT, NULL, 0);
          break;
        default:
          break;
      }
    }
  }
  
  return NGX_OK;
}

typedef struct {
  nchan_msg_id_t     msg_id;
  channel_spooler_t *spooler;
} nomsg_retry_data_t;

static void spool_fetch_msg_noresponse_retry_cancel(void *pd) {
  nomsg_retry_data_t *d = pd;
  nchan_free_msg_id(&d->msg_id);
  ngx_free(d);
}

static void spool_fetch_msg_noresponse_retry_callback(void *pd) {
  nomsg_retry_data_t *d = pd;
  subscriber_pool_t *spool = get_spool(d->spooler, &d->msg_id);
  if(spool && spool->msg_status == MSG_INVALID) {
    spool_fetch_msg(spool);
  }
  else if(!spool) {
    DBG("spool not found for spool_fetch_msg_noresponse_retry_callback");
  }
  
  spool_fetch_msg_noresponse_retry_cancel(pd);
}

static ngx_int_t spool_fetch_msg_callback(nchan_msg_status_t findmsg_status, nchan_msg_t *msg, fetchmsg_data_t *data) {
  nchan_msg_status_t    prev_status;
  subscriber_pool_t    *spool, *nuspool;
  channel_spooler_t    *spl = data->spooler;
  int                   free_msg_id = 1;
  
  if(spl && data == spl->fetchmsg_cb_data_list) {
    spl->fetchmsg_cb_data_list = data->next;
  }
  if(data->next) {
    data->next->prev = data->prev;
  }
  if(data->prev) {
    data->prev->next = data->next;
  }
  
  if(spl == NULL) { //channel already deleted
    nchan_free_msg_id(&data->msgid);
    ngx_free(data);
    return NGX_OK;
  }
  
  if(spl->handlers->get_message_finish) {
    spl->handlers->get_message_finish(spl, spl->handlers_privdata);
  }
  
  if((spool = find_spool(spl, &data->msgid)) == NULL) {
    DBG("spool for msgid %V not found. discarding getmsg callback response.", msgid_to_str(&data->msgid));
    nchan_free_msg_id(&data->msgid);
    ngx_free(data);
    return NGX_ERROR;
  }
  
  prev_status = spool->msg_status;
  
  switch(findmsg_status) {
    case MSG_FOUND:
      spool->msg_status = findmsg_status;
      DBG("fetchmsg callback for spool %p msg FOUND %p %V", spool, msg, msgid_to_str(&msg->id));
      assert(msg != NULL);
      spool->msg = msg;
      spool_respond_general(spool, spool->msg, 0, NULL, 0);
      
      spool_nextmsg(spool, &msg->id);      
      break;
    
    case MSG_EXPECTED:
      // ♫ It's gonna be the future soon ♫
      if(spool->id.time == NCHAN_NTH_MSGID_TIME) {
        //wait for message in the NEWEST_ID spool
        nchan_msg_id_t  newest_id = NCHAN_NEWEST_MSGID;
        spool_nextmsg(spool, &newest_id); 
      }
      else {
        spool->msg_status = findmsg_status;
        DBG("fetchmsg callback for spool %p msg EXPECTED", spool);
        spool_respond_general(spool, NULL, NGX_HTTP_NO_CONTENT, NULL, 0);
        assert(msg == NULL);
        spool->msg = NULL;
      }
      break;
    
    case MSG_CHANNEL_NOTREADY:
      //just wait it out
      spool->msg = NULL;
      spool->msg_status = findmsg_status;
      break;
    
    case MSG_NORESPONSE:
      if(prev_status == MSG_PENDING) {
        spool->msg_status = MSG_INVALID;
        if(spool->sub_count > 0) {
          nomsg_retry_data_t *retry_data = ngx_alloc(sizeof(*retry_data), ngx_cycle->log);
          
          retry_data->spooler = spl;
          
          free_msg_id = 0;
          retry_data->msg_id = data->msgid;
          
          spooler_add_timer(spl, NCHAN_MSG_NORESPONSE_RETRY_TIME, spool_fetch_msg_noresponse_retry_callback, spool_fetch_msg_noresponse_retry_cancel, retry_data);
        }
      }
      break;
      
    case MSG_NOTFOUND:
      if(spl->fetching_strategy == FETCH_IGNORE_MSG_NOTFOUND) {
        spool->msg_status = prev_status;
        break;
      }
      /*fallthrough*/
    case MSG_EXPIRED:
      //is this right?
      //TODO: maybe message-expired notification
      spool->msg_status = findmsg_status;
      spool_respond_general(spool, NULL, NGX_HTTP_NO_CONTENT, NULL, 0);
      nuspool = get_spool(spool->spooler, &oldest_msg_id);
      if(spool != nuspool) {
        spool_transfer_subscribers(spool, nuspool, 1);
        destroy_spool(spool);
      }
      else if(spool->id.tagcount == 1 && nchan_compare_msgids(&spool->id, &oldest_msg_id) == 0) {
        // oldest msgid not found or expired. that means there are no messages in this channel, 
        // so move these subscribers over to the current_msg_spool
        nuspool = get_spool(spool->spooler, &latest_msg_id);
        assert(spool != nuspool);
        spool_transfer_subscribers(spool, nuspool, 1);
        destroy_spool(spool);
      }
      else if(spool == &spool->spooler->current_msg_spool) {
        //sit there and wait, i guess
        spool->msg_status = MSG_EXPECTED;
      }
      else {
        ERR("Unexpected spool == nuspool during spool fetch_msg_callback. This is weird, please report this to the developers. findmsg_status: %i", findmsg_status);
        assert(0);
      }
      break;
    
    case MSG_PENDING:
      ERR("spool %p set status to MSG_PENDING", spool);
      break;
      
    default:
      assert(0);
      break;
  }
  
  if(free_msg_id) {
    nchan_free_msg_id(&data->msgid);
  }
  ngx_free(data);
  return NGX_OK;
}

static ngx_int_t spool_fetch_msg(subscriber_pool_t *spool) {
  fetchmsg_data_t        *data;
  channel_spooler_t      *spl = spool->spooler;
  
  
  //stack overflow protector
  //ERR("spool->fetchmsg_prev_msec %i (ngx_timeofday())->msec %i spool->fetchmsg_current_count %i", spool->fetchmsg_prev_msec, (ngx_timeofday())->msec, spool->fetchmsg_current_count);
  if(spool->fetchmsg_prev_msec == (ngx_timeofday())->msec) {
    if(spool->fetchmsg_current_count > NCHAN_SPOOL_FETCHMSG_MAX_TIMES) {
      ngx_add_timer(&spool->fetchmsg_ev, 0);
      spool->fetchmsg_current_count = 0;
      return NGX_DONE;
    }
    else {
      spool->fetchmsg_current_count++;
    }
  }
  else {
    spool->fetchmsg_current_count = 0;
    spool->fetchmsg_prev_msec = (ngx_timeofday())->msec;
  }
  
  if(*spl->channel_status != READY) {
    DBG("%p wanted to fetch msg %V, but channel %V not ready", spool, msgid_to_str(&spool->id), spl->chid);
    spool->msg_status = MSG_CHANNEL_NOTREADY;
    return NGX_DECLINED;
  }
  DBG("%p fetch msg %V for channel %V", spool, msgid_to_str(&spool->id), spl->chid);
  data = ngx_alloc(sizeof(*data), ngx_cycle->log); //correctness over efficiency (at first).
  //TODO: optimize this alloc away
  
  assert(data);
  
  data->next = spl->fetchmsg_cb_data_list;
  if(data->next) {
    data->next->prev = data;
  }
  spl->fetchmsg_cb_data_list = data;
  data->prev = NULL;
  
  nchan_copy_new_msg_id(&data->msgid, &spool->id);
  data->spooler = spool->spooler;
  
  assert(spool->msg == NULL);
  assert(spool->msg_status == MSG_INVALID);
  spool->msg_status = MSG_PENDING;
  if(spl->handlers->get_message_start) {
    spl->handlers->get_message_start(spl, spl->handlers_privdata);
  }
  switch(spl->fetching_strategy) {
    case FETCH:
    case FETCH_IGNORE_MSG_NOTFOUND:
      spool->spooler->store->get_message(spool->spooler->chid, &spool->id, spool->spooler->cf, (callback_pt )spool_fetch_msg_callback, data);
      break;
    case NO_FETCH:
      //do nothing
      break;
  }
  return NGX_OK;
}

static void spool_sub_dequeue_callback(subscriber_t *sub, void *data) {
  spooled_subscriber_cleanup_t  *d = (spooled_subscriber_cleanup_t *)data;
  subscriber_pool_t             *spool = d->spool;
  
  DBG("sub %p dequeue callback", sub);
  
  assert(sub == d->ssub->sub);
  spool_remove_subscriber(spool, d->ssub);
  spool_bubbleup_dequeue_handler(spool, sub, spool->spooler);
  
  if(sub->type != INTERNAL && spool->spooler->publish_events) {
    nchan_maybe_send_channel_event_message(sub->request, SUB_DEQUEUE);
  }
}

static ngx_int_t spool_add_subscriber(subscriber_pool_t *self, subscriber_t *sub, uint8_t enqueue) {
  spooled_subscriber_t       *ssub;
  ngx_int_t                   rc;
  ngx_int_t                   internal_sub = sub->type == INTERNAL;
  
  ssub = ngx_calloc(sizeof(*ssub), ngx_cycle->log);
  //DBG("add sub %p to spool %p", sub, self);
  
  if(ssub == NULL) {
    ERR("failed to allocate new sub for spool");
    return NGX_ERROR;
  }
  
  ssub->next = self->first;
  ssub->prev = NULL;
  if(self->first != NULL) {
    self->first->prev = ssub;
  }
  self->first = ssub;
  self->sub_count++;
  if(!internal_sub) {
    self->non_internal_sub_count++;
  }
  ssub->dequeue_callback_data.ssub = ssub;
  ssub->dequeue_callback_data.spool = self;
  
  if(enqueue) {
    if((rc = sub->fn->enqueue(sub)) != NGX_OK) {
      //enqueue failed. undo everything and get out!
      self->sub_count --;
      self->first = ssub->next;
      if(self->first) {
        assert(self->first->prev == ssub);
        self->first->prev = NULL;
      }
      if(!internal_sub) {
        self->non_internal_sub_count--;
      }
      ngx_free(ssub);
      return rc;
    }
    else if(sub->type != INTERNAL && self->spooler->publish_events) {
      nchan_maybe_send_channel_event_message(sub->request, SUB_ENQUEUE);
    }
  }
  
  sub->fn->set_dequeue_callback(sub, spool_sub_dequeue_callback, &ssub->dequeue_callback_data);
  ssub->sub = sub;
  
  return NGX_OK;
}

static ngx_int_t spool_remove_subscriber(subscriber_pool_t *self, spooled_subscriber_t *ssub) {
  assert(ssub->next != ssub);
  assert(ssub->prev != ssub);
  spooled_subscriber_t   *prev, *next;
  prev = ssub->prev;
  next = ssub->next;
  if(next) {
    next->prev = prev;
  }
  if(prev) {
    prev->next = next;
  }
  if(self->first == ssub) {
    self->first = next;
  }
  
  if(ssub->sub->type != INTERNAL) {
    self->non_internal_sub_count--;
  }
  
  ngx_free(ssub);

  assert(self->sub_count > 0);
  self->sub_count--;
  return NGX_OK;
}

static ngx_int_t spool_respond_general(subscriber_pool_t *self, nchan_msg_t *msg, ngx_int_t code, void *code_data, unsigned notice) {
  ngx_uint_t                  numsubs[SUBSCRIBER_TYPES];
  spooled_subscriber_t       *nsub, *nnext;
  subscriber_t               *sub;
  
  //channel_spooler_t          *spl = self->spooler;
  //validate_spooler(spl, "before respond_general");
  //nchan_msg_id_t             unid;
  //nchan_msg_id_t             unprevid;
  //int8_t                     i, max;
  
  ngx_memzero(numsubs, sizeof(numsubs));
  self->generation++;
  
  DBG("spool %p (%V) (subs: %i) respond with msg %p or code %i", self, msgid_to_str(&self->id), self->sub_count, msg, code);
  if(msg) {
    DBG("msgid: %V", msgid_to_str(&msg->id));
    DBG("prev: %V", msgid_to_str(&msg->prev_id));
  }
  
  /*
  if(msg && msg->prev_id.time > 0 && msg->id.tagcount > 1) {
    assert(msg->shared == 0);
    max = msg->id.tagcount;
    for(i=0; i< max; i++) {
      unid.tag[i] =     msg->id.tag[i];
      unprevid.tag[i] = msg->prev_id.tag[i];
      if(unid.tag[i] == -1)     msg->id.tag[i]   =    self->id.tag[i];
      if(unprevid.tag[i] == -1) msg->prev_id.tag[i] = self->id.tag[i];
    }
  }
  */
  
  //uint8_t publish_events = self->spooler->publish_events;
  
  for(nsub = self->first; nsub != NULL; nsub = nnext) {
    sub = nsub->sub;
    nnext = nsub->next;
    
    if(msg) {
      self->responded_count++;
      sub->fn->respond_message(sub, msg);
    }
    else if(!notice) {
      self->responded_count++;
      sub->fn->respond_status(sub, code, code_data, NULL);
    }
    else {
      sub->fn->notify(sub, code, code_data);
    }
  }
  
  if(!notice && code != NGX_HTTP_NO_CONTENT) self->responded_count++;
  //assert(validate_spooler(spl, "after respond_general"));
  return NGX_OK;
}

/////////// SPOOLER - container of several spools //////////

channel_spooler_t *create_spooler() {
  channel_spooler_t  *spooler;
  if((spooler = ngx_alloc(sizeof(*spooler), ngx_cycle->log))==NULL) {
    ERR("Can't allocate spooler");
    return NULL;
  }
  return spooler;
}

static void spool_bubbleup_dequeue_handler(subscriber_pool_t *spool, subscriber_t *sub, channel_spooler_t *spl) {
  //bubble on up, yeah
  channel_spooler_handlers_t *h = spl->handlers;
  if(h->dequeue) {
    h->dequeue(spl, sub, spl->handlers_privdata);
  }
  else if (h->bulk_dequeue){
    h->bulk_dequeue(spl, sub->type, 1, spl->handlers_privdata);
  }
  else {
    ERR("Neither dequeue_handler not bulk_dequeue_handler present in spooler for spool sub dequeue");
  }
}

/*
static void spool_bubbleup_bulk_dequeue_handler(subscriber_pool_t *spool, subscriber_type_t type, ngx_int_t count, channel_spooler_t *spl) {
  //bubble on up, yeah
  if(spl->handlers->bulk_dequeue) {
    spl->handlers->bulk_dequeue(spl, type, count, spl->handlers_privdata);
  }
}
*/

static ngx_int_t spooler_add_subscriber(channel_spooler_t *self, subscriber_t *sub) {
  nchan_msg_id_t          *msgid = &sub->last_msgid;
  subscriber_pool_t       *spool;
  subscriber_type_t        subtype;
  ngx_int_t                rc;
  
  if(self->want_to_stop) {
    ERR("Not accepting new subscribers right now. want to stop.");
    return NGX_ERROR;
  }
  
  //validate_spooler(self, "before add_subscriber");
  
  spool = get_spool(self, msgid);
  
  assert(spool->id.time == msgid->time);
  
  
  if(spool == NULL) {
    return NGX_ERROR;
  }

  subtype = sub->type;
  
  if((rc = spool_add_subscriber(spool, sub, 1)) != NGX_OK) {
    DBG("couldn't add subscriber to spool %p", spool);
    return rc;
  }
  self->handlers->add(self, sub, self->handlers_privdata);
  
  switch(spool->msg_status) {
    case MSG_FOUND:
      assert(spool->msg);
      spool_respond_general(spool, spool->msg, 0, NULL, 0);
      break;
    
    case MSG_INVALID:
      assert(spool->msg == NULL);
      spool_fetch_msg(spool);
      break;
    
    case MSG_CHANNEL_NOTREADY:
    case MSG_PENDING:
      //nothing to do
      break;
      
    case MSG_EXPECTED:
      //notify subscriber
      sub->fn->respond_status(sub, NGX_HTTP_NO_CONTENT, NULL, NULL);
      break;
      
    case MSG_EXPIRED:
    case MSG_NOTFOUND:
    case MSG_NORESPONSE:
      //shouldn't happen
      assert(0);
  }
  
  if(self->handlers->use != NULL && subtype != INTERNAL) {
    self->handlers->use(self, self->handlers_privdata);
  }
  
  //validate_spooler(self, "after add_subscriber");
  
  return NGX_OK;
}


static ngx_int_t spool_transfer_subscribers(subscriber_pool_t *spool, subscriber_pool_t *newspool, uint8_t update_subscriber_last_msgid) {
  ngx_int_t               count = 0;
  subscriber_t           *sub;
  spooled_subscriber_t   *cur;
  channel_spooler_t      *spl = spool->spooler;
  
  assert(spl == newspool->spooler);
  
  if(spool == NULL || newspool == NULL) {
    ERR("failed to transfer spool subscribers");
    return 0;
  }
  for(cur = spool->first; cur != NULL; cur = spool->first) {
    sub = cur->sub;
    spool_remove_subscriber(spool, cur);
    if(update_subscriber_last_msgid) {
      sub->last_msgid=newspool->id;
    }
    if(spool_add_subscriber(newspool, sub, 0) == NGX_OK) {
      count++;
    }
  }
  
  return count;
}

typedef struct spool_collect_overflow_s spool_collect_overflow_t;
struct spool_collect_overflow_s {
  subscriber_pool_t                *spool;
  struct spool_collect_overflow_s  *next;
};// spool_collect_overflow_t;

#define SPOOLER_RESPOND_SPOOLARRAY_SIZE 32

typedef struct {
  nchan_msg_id_t             min;
  nchan_msg_id_t             max;
  uint8_t                    multi;
  ngx_int_t                  n;
  nchan_msg_t               *msg;
  subscriber_pool_t         *spools[SPOOLER_RESPOND_SPOOLARRAY_SIZE];
  spool_collect_overflow_t  *overflow;
} spooler_respond_data_t;


static rbtree_walk_direction_t compare_msgid_onetag_range(nchan_msg_id_t *min, nchan_msg_id_t *max, nchan_msg_id_t *id) {
  
  assert(min->tagcount == max->tagcount);
  assert(max->tagcount == id->tagcount);
  assert(id->tagcount == 1);
  
  if(min->time < id->time || (min->time == id->time && min->tag.fixed[0] <= id->tag.fixed[0])) {
    if(max->time > id->time || (max->time == id->time && max->tag.fixed[0] > id->tag.fixed[0])) {
      //inrange
      return RBTREE_WALK_LEFT_RIGHT;
    }
    else {
      //too large
      return RBTREE_WALK_LEFT;
    }
  }
  else {
    //too small
    return RBTREE_WALK_RIGHT;
  } 
}

static int8_t compare_msgid_time(nchan_msg_id_t *min, nchan_msg_id_t *max, nchan_msg_id_t *cur) {
  if(min->time <= cur->time) {
    if(max->time >= cur->time) {
      return 0;
    }
    else {
      return 1;
    }
  }
  else {
    return -1;
  }
}


static void spoolcollector_addspool(spooler_respond_data_t *data, subscriber_pool_t *spool) {
  spool_collect_overflow_t  *overflow;
  if(data->n < SPOOLER_RESPOND_SPOOLARRAY_SIZE) {
    data->spools[data->n] = spool;
  }
  else {
    if((overflow = ngx_alloc(sizeof(*overflow), ngx_cycle->log)) == NULL) {
      ERR("can't allocate spoolcollector overflow");
      return;
    }
    overflow->next = data->overflow;
    overflow->spool = spool;
    data->overflow = overflow;
  }
  data->n++;
}

static subscriber_pool_t *spoolcollector_unwind_nextspool(spooler_respond_data_t *data) {
  spool_collect_overflow_t  *overflow;
  subscriber_pool_t         *spool;
  if(data->n > SPOOLER_RESPOND_SPOOLARRAY_SIZE) {
    overflow = data->overflow;
    spool = overflow->spool;
    data->overflow = overflow->next;
    ngx_free(overflow);
    data->n--;
    return spool;
  }
  else if(data->n > 0) {
    return data->spools[--data->n];
  }
  else {
    return NULL;
  }
}


static rbtree_walk_direction_t collect_spool_range_callback(rbtree_seed_t *seed, subscriber_pool_t *spool, spooler_respond_data_t *data) {
  rbtree_walk_direction_t  dir;
  uint8_t multi_count = data->multi;
  
  if(multi_count <= 1) {
    dir = compare_msgid_onetag_range(&data->min, &data->max, &spool->id);
    if(dir == RBTREE_WALK_LEFT_RIGHT) {
      spoolcollector_addspool(data, spool);
    }
    return dir;
  }
  else {
    int tc = compare_msgid_time(&data->min, &data->max, &spool->id);
    if(tc < 0) {
      return RBTREE_WALK_RIGHT;
    }
    else if(tc > 0) {
      return RBTREE_WALK_LEFT;
    }
    else {
      time_t      timmin = data->min.time, timmax = data->max.time, timcur = spool->id.time;
      
      int         max_cmp = -1, min_cmp = -1;
      
      if( timcur > timmin && timcur < timmax) {
        spoolcollector_addspool(data, spool);
      }
      else if(timcur == timmax && timcur == timmin) {
        if( nchan_compare_msgid_tags(&spool->id, &data->max) < 0
         && nchan_compare_msgid_tags(&spool->id, &data->min) >= 0 ) 
        {
          spoolcollector_addspool(data, spool);
        } 
      }
      else if((timcur == timmax && (max_cmp = nchan_compare_msgid_tags(&spool->id, &data->max)) < 0) 
           || (timcur == timmin && (min_cmp = nchan_compare_msgid_tags(&spool->id, &data->min)) >= 0))
      {
        spoolcollector_addspool(data, spool);
      }
      else if( timcur > timmin && timcur < timmax) {
        spoolcollector_addspool(data, spool);
      }
      /*
      else {
        ERR("time_min: %i, time_cur: %i, time_max: %i", timmin, timcur, timmax);
      }
      */
      
      return RBTREE_WALK_LEFT_RIGHT;
    }
  }
}

static ngx_int_t spooler_respond_status(channel_spooler_t *self, nchan_msg_id_t *id, ngx_int_t status_code, ngx_str_t *status_line) {
  subscriber_pool_t         *spool = find_spool(self, id);
  //validate_spooler(self, "before respond_status");
  if(spool) {
    if(status_code == NGX_HTTP_NO_CONTENT) {
      spool->msg_status = MSG_EXPECTED;
    }
    spool_respond_general(spool, NULL, status_code, status_line, 0);
    destroy_spool(spool);
  }
  //validate_spooler(self, "after respond_status");
  return NGX_OK;
}

static ngx_int_t spooler_respond_message(channel_spooler_t *self, nchan_msg_t *msg) {
  spooler_respond_data_t     srdata;
  subscriber_pool_t         *spool;
  ngx_int_t                  responded_subs = 0;
  
  srdata.min = msg->prev_id;
  srdata.max = msg->id;
  srdata.multi = msg->id.tagcount;
  srdata.overflow = NULL;
  srdata.msg = msg;
  srdata.n = 0;
  
  //spooler_print_contents(self);
  
  //find all spools between msg->prev_id and msg->id
  rbtree_conditional_walk(&self->spoolseed, (rbtree_walk_conditional_callback_pt )collect_spool_range_callback, &srdata);
  /*
  if(srdata.n == 0) {
    DBG("no spools in range %V -- ", msgid_to_str(&msg->prev_id));
    DBG(" -- %V", msgid_to_str(&msg->id));
  }
  */
  while((spool = spoolcollector_unwind_nextspool(&srdata)) != NULL) {
    responded_subs += spool->sub_count;
    if(msg->id.tagcount > NCHAN_FIXED_MULTITAG_MAX) {
      assert(spool->id.tag.allocd != msg->id.tag.allocd);
    }
    spool_respond_general(spool, msg, 0, NULL, 0);
    if(msg->id.tagcount > NCHAN_FIXED_MULTITAG_MAX) {
      assert(spool->id.tag.allocd != msg->id.tag.allocd);
    }
    spool_nextmsg(spool, &msg->id);
  }
  
  spool = get_spool(self, &latest_msg_id);
  if(spool->sub_count > 0 && *self->channel_buffer_complete) {
#if NCHAN_BENCHMARK
    responded_subs += spool->sub_count;
#endif
    spool_respond_general(spool, msg, 0, NULL, 0);
    spool_nextmsg(spool, &msg->id);
  }

  nchan_copy_msg_id(&self->prev_msg_id, &msg->id, NULL);
  
#if NCHAN_BENCHMARK
  self->last_responded_subscriber_count = responded_subs;
#endif
  return NGX_OK;
}

typedef struct {
  channel_spooler_t *spl;
  nchan_msg_t       *msg;
  ngx_int_t          code;
  void              *code_data;
  unsigned           notice:1;
} spooler_respond_generic_data_t;

static ngx_int_t spooler_respond_rbtree_node_spool(rbtree_seed_t *seed, subscriber_pool_t *spool, void *data) {
  spooler_respond_generic_data_t  *d = data;
  
  return spool_respond_general(spool, d->msg, d->code, d->code_data, d->notice);
}

static ngx_int_t spooler_respond_generic(channel_spooler_t *self, nchan_msg_t *msg, ngx_int_t code, void *code_data, unsigned notice) {
  spooler_respond_generic_data_t  data = {self, msg, code, code_data, notice};
  rbtree_walk(&self->spoolseed, (rbtree_walk_callback_pt )spooler_respond_rbtree_node_spool, &data);
  spool_respond_general(&self->current_msg_spool, data.msg, data.code, data.code_data, notice);
  return NGX_OK;
}

static ngx_int_t spooler_broadcast_status(channel_spooler_t *self, ngx_int_t code, const ngx_str_t *line) {
  return spooler_respond_generic(self, NULL, code, (void *)line, 0);
}

static ngx_int_t spooler_broadcast_notice(channel_spooler_t *self, ngx_int_t code, void *data) {
  return spooler_respond_generic(self, NULL, code, data, 1);
}

static ngx_int_t spooler_spool_dequeue_all(rbtree_seed_t *seed, subscriber_pool_t *spool, void *data) {
  spooled_subscriber_t *cur;
  
  for(cur = spool->first; cur != NULL; cur = cur->next) {
    cur->sub->dequeue_after_response = 1;  
  }
  
  return NGX_OK;
}

static ngx_int_t spooler_prepare_to_stop(channel_spooler_t *spl) {
  rbtree_walk(&spl->spoolseed, (rbtree_walk_callback_pt )spooler_spool_dequeue_all, (void *)spl);
  spl->want_to_stop = 1;
  return NGX_OK;
}



static void *spool_rbtree_node_id(void *data) {
  return &((subscriber_pool_t *)data)->id;
}

static uint32_t spool_rbtree_bucketer(void *vid) {
  nchan_msg_id_t   *id = (nchan_msg_id_t *)vid;
  return (uint32_t )id->time;
}

static ngx_int_t spool_rbtree_compare(void *v1, void *v2) {
  nchan_msg_id_t   *id1 = (nchan_msg_id_t *)v1;
  nchan_msg_id_t   *id2 = (nchan_msg_id_t *)v2;
  time_t            t1 = id1->time;
  time_t            t2 = id2->time;
  ngx_int_t         tag1;
  ngx_int_t         tag2;
  
  if(t1 > t2) {
    return 1;
  }
  else if (t1 < t2) {
    return -1;
  }
  else {
    uint16_t   i, max1 = id1->tagcount, max2 = id2->tagcount;
    uint16_t   max = max1 > max2 ? max1 : max2;
    int16_t   *tags1, *tags2;
    
    tags1 = max1 <= NCHAN_FIXED_MULTITAG_MAX ? id1->tag.fixed : id1->tag.allocd;
    tags2 = max2 <= NCHAN_FIXED_MULTITAG_MAX ? id2->tag.fixed : id2->tag.allocd;
    
    for(i=0; i < max; i++) {
      tag1 = i < max1 ? tags1[i] : -1;
      tag2 = i < max2 ? tags2[i] : -1;
      if(tag1 > tag2) {
        return 1;
      }
      else if(tag1 < tag2) {
        return -1;
      }
    }
    return 0;
  }
}

static int its_time_for_a_spooling_filter(void *data) {
  return ((subscriber_pool_t *)data)->msg_status == MSG_CHANNEL_NOTREADY;
}

static ngx_int_t its_time_for_a_spooling(rbtree_seed_t *seed, subscriber_pool_t *spool, void *data) {
  ngx_int_t       rc;
  //validate_spool(spool);
  assert(spool->msg_status == MSG_CHANNEL_NOTREADY || spool->msg_status == MSG_INVALID);
  spool->msg_status = MSG_INVALID;
  rc = spool_fetch_msg(spool);
  assert(rc == NGX_OK || rc == NGX_DONE);
  return rc;
}

static ngx_int_t spooler_channel_status_changed(channel_spooler_t *self) {
  switch(*self->channel_status) {
    case READY:
      rbtree_walk_writesafe(&self->spoolseed, its_time_for_a_spooling_filter, (rbtree_walk_callback_pt )its_time_for_a_spooling, NULL); 
      break;
      
    default:
      //do nothing
      break;
  };
  return NGX_OK;
}


static ngx_int_t spooler_print_contents_callback(rbtree_seed_t *seed, subscriber_pool_t *spool, channel_spooler_t *spl) {
  spooled_subscriber_t       *cur;
  
  ERR("  spool %p id %V", spool, msgid_to_str(&spool->id));
  for(cur = spool->first; cur != NULL; cur = cur->next) {
    ERR("    %V", cur->sub->name);
  }
  
  return NGX_OK;
}

ngx_int_t spooler_print_contents(channel_spooler_t *spl) {
  ERR("spooler for channel %V", spl->chid);
  spooler_print_contents_callback(NULL, &spl->current_msg_spool, spl);
  rbtree_walk_incr(&spl->spoolseed, (rbtree_walk_callback_pt )spooler_print_contents_callback, spl);
  return NGX_OK;
}



static int spooler_catch_up_filter(void *data) {
  nchan_msg_status_t  status =  ((subscriber_pool_t *)data)->msg_status;
  return status == MSG_EXPECTED || status == MSG_PENDING;
}

static ngx_int_t spooler_catch_up_callback(rbtree_seed_t *seed, subscriber_pool_t *spool, void *data) {
  spool->msg_status = MSG_INVALID;
  spool_fetch_msg(spool);
  return NGX_OK;
}

ngx_int_t spooler_catch_up(channel_spooler_t *spl) {
  rbtree_walk_writesafe(&spl->spoolseed, spooler_catch_up_filter, (rbtree_walk_callback_pt )spooler_catch_up_callback, NULL); 
  return NGX_OK;
}


static channel_spooler_fn_t  spooler_fn = {
  spooler_add_subscriber,
  spooler_channel_status_changed,
  spooler_respond_message,
  spooler_respond_status,
  spooler_broadcast_status,
  spooler_broadcast_notice,
  spooler_prepare_to_stop
};

channel_spooler_t *start_spooler(channel_spooler_t *spl, ngx_str_t *chid, chanhead_pubsub_status_t *channel_status, uint8_t *channel_buffer_complete, nchan_store_t *store, nchan_loc_conf_t *cf, spooler_fetching_strategy_t fetching_strategy, channel_spooler_handlers_t *handlers, void *handlers_privdata) {
  if(!spl->running) {
    ngx_memzero(spl, sizeof(*spl));
    rbtree_init(&spl->spoolseed, "spooler msg_id tree", spool_rbtree_node_id, spool_rbtree_bucketer, spool_rbtree_compare);
    
    spl->fn=&spooler_fn;
    //spl->prev_msg_id.time=0;
    //spl->prev_msg_id.tag=0;
    
    DBG("start SPOOLER %p", *spl);
    
    spl->chid = chid;
    spl->store = store;
    
    spl->channel_status = channel_status;
    spl->channel_buffer_complete = channel_buffer_complete;
    
    spl->running = 1;
    //spl->want_to_stop = 0;
    spl->publish_events = 1;
    spl->fetching_strategy = fetching_strategy;
    
    init_spool(spl, &spl->current_msg_spool, &latest_msg_id);
    spl->current_msg_spool.msg_status = MSG_EXPECTED;
    
    spl->handlers = handlers;
    spl->handlers_privdata = handlers_privdata;
    
    spl->cf = cf;
    
    return spl;
  }
  else {
    ERR("looks like spooler is already running. make sure spooler->running=0 before starting.");
    assert(0);
    return NULL;
  }
}

static ngx_int_t remove_spool(subscriber_pool_t *spool) {
  channel_spooler_t    *spl = spool->spooler;
  ngx_rbtree_node_t    *node = rbtree_node_from_data(spool);
  
  DBG("remove spool node %p", node);
  
  assert(spool->spooler->running);
  
  if(spool->fetchmsg_ev.timer_set) {
    ngx_del_timer(&spool->fetchmsg_ev);
  }
  
  nchan_free_msg_id(&spool->id);
  rbtree_remove_node(&spl->spoolseed, rbtree_node_from_data(spool));
  
  //assert((node = rbtree_find_node(&spl->spoolseed, &spool->id)) == NULL);
  //double-check that it's gone 
  
  return NGX_OK;
}

static ngx_int_t destroy_spool(subscriber_pool_t *spool) {
  rbtree_seed_t         *seed = &spool->spooler->spoolseed;
  spooled_subscriber_t  *ssub, *ssub_next;
  subscriber_t          *sub;
  ngx_rbtree_node_t     *node = rbtree_node_from_data(spool);
  
  remove_spool(spool);
  
  DBG("destroy spool node %p", node);
  
  for(ssub = spool->first; ssub!=NULL; ssub = ssub_next) {
    sub = ssub->sub;
    ssub_next = ssub->next;
    //DBG("dequeue sub %p in spool %p", sub, spool);
    sub->fn->dequeue(sub);
  }
  assert(spool->sub_count == 0);
  assert(spool->first == NULL);
  
  ngx_memset(spool, 0x42, sizeof(*spool)); //debug
  
  rbtree_destroy_node(seed, node);
  return NGX_OK;
}

ngx_int_t stop_spooler(channel_spooler_t *spl, uint8_t dequeue_subscribers) {
  ngx_rbtree_node_t    *cur, *sentinel;
  spooler_event_ll_t   *ecur, *ecur_next;
  subscriber_pool_t    *spool;
  rbtree_seed_t        *seed = &spl->spoolseed;
  ngx_rbtree_t         *tree = &seed->tree;
  ngx_int_t             n=0;
  sentinel = tree->sentinel;
  
  fetchmsg_data_t      *dcur;
#if NCHAN_RBTREE_DBG
  ngx_int_t active_before = seed->active_nodes, allocd_before = seed->active_nodes;
#endif
  if(spl->running) {
    
    for(ecur = spl->spooler_dependent_events; ecur != NULL; ecur = ecur_next) {
      ecur_next = ecur->next;
      if(ecur->cancel) {
        ecur->cancel(ecur->ev.data);
      }
      ngx_event_del_timer(&ecur->ev);
      
      ngx_free(ecur);
    }
    
    for(cur = tree->root; cur != NULL && cur != sentinel; cur = tree->root) {
      spool = (subscriber_pool_t *)rbtree_data_from_node(cur);
      if(dequeue_subscribers) {
        destroy_spool(spool);
      }
      else {
        remove_spool(spool);
        rbtree_destroy_node(seed, cur);
      }
      n++;
    }
    
    for(dcur = spl->fetchmsg_cb_data_list; dcur != NULL; dcur = dcur->next) {
      dcur->spooler = NULL;
    }
    
    DBG("stopped %i spools in SPOOLER %p", n, *spl);
  }
  else {
    DBG("SPOOLER %p not running", *spl);
  }
#if NCHAN_RBTREE_DBG
  assert(active_before - n == 0);
  assert(allocd_before - n == 0);
  assert(seed->active_nodes == 0);
  assert(seed->allocd_nodes == 0);
#endif
  nchan_free_msg_id(&spl->prev_msg_id);
  spl->running = 0;
  return NGX_OK;
}
