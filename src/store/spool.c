#include <nchan_module.h>
#include "rbtree_util.h"
#include "spool.h"
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DEBUG_LEVEL NGX_LOG_WARN

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SPOOL:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SPOOL:" fmt, ##arg)
#define COMMAND_SPOOL(spool, fn_name, arg...) ((spool)->fn_name((spool), ##arg))

//////// SPOOLs -- Subscriber Pools  /////////

static ngx_int_t spool_remove_subscriber(subscriber_pool_t *, spooled_subscriber_t *);
static void spool_bubbleup_dequeue_handler(subscriber_pool_t *spool, subscriber_t *sub, channel_spooler_t *spl);
//static void spool_bubbleup_bulk_dequeue_handler(subscriber_pool_t *spool, subscriber_type_t type, ngx_int_t count, channel_spooler_t *spl);
static ngx_int_t spool_respond_general(subscriber_pool_t *self, nchan_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line);
static ngx_int_t spool_transfer_subscribers(subscriber_pool_t *spool, subscriber_pool_t *newspool, uint8_t update_subscriber_last_msgid);
static ngx_int_t destroy_spool(subscriber_pool_t *spool);
static ngx_int_t remove_spool(subscriber_pool_t *spool);
static ngx_int_t spool_fetch_msg(subscriber_pool_t *spool);

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

static subscriber_pool_t *get_spool(channel_spooler_t *spl, nchan_msg_id_t *id) {
  rbtree_seed_t      *seed = &spl->spoolseed;
  ngx_rbtree_node_t  *node;
  subscriber_pool_t *spool;
  
  if((node = rbtree_find_node(seed, id)) == NULL) {
    
    if((node = rbtree_create_node(seed, sizeof(*spool))) == NULL) {
      ERR("can't create rbtree node for spool");
      return NULL;
    }
    
   // DBG("CREATED spool node %p for msgid %i:%i", node, id->time, id->tag);
    spool = (subscriber_pool_t *)rbtree_data_from_node(node);
    
    ngx_memzero(spool, sizeof(*spool));
    spool->spooler = spl;
    spool->id = *id;
    spool->spooler = spl;
    spool->msg_status = MSG_INVALID;
    spool->msg = NULL;
    
    if(rbtree_insert_node(seed, node) != NGX_OK) {
      ERR("couldn't insert spool node");
      rbtree_destroy_node(seed, node);
      return NULL;
    }
  }
  else {
    spool = (subscriber_pool_t *)rbtree_data_from_node(node);
    //DBG("found spool node %p with msgid %i:%i", node, id->time, id->tag);
    assert(spool->id.time == id->time);
  }
  return spool;
}

static ngx_int_t spool_nextmsg(subscriber_pool_t *spool, nchan_msg_id_t *new_last_id) {
  subscriber_pool_t      *newspool;
  channel_spooler_t      *spl = spool->spooler;
  
  DBG("spool nextmsg %p (%i:%i) newid %i:%i", spool, spool->id.time, spool->id.tag, new_last_id->time, new_last_id->tag);
  
  assert(spool->id.time != new_last_id->time || spool->id.tag != new_last_id->tag);
  
  if((newspool = find_spool(spl, new_last_id)) != NULL) {
    assert(spool != newspool);
    spool_transfer_subscribers(spool, newspool, 0);
    destroy_spool(spool);
  }
  else {
    ngx_rbtree_node_t       *node;
    node = rbtree_node_from_data(spool);
    rbtree_remove_node(&spl->spoolseed, node);
    spool->id = *new_last_id;
    rbtree_insert_node(&spl->spoolseed, node);
    spool->msg_status = MSG_INVALID;
    spool->msg = NULL;
    newspool = spool;
    
    /*
    newspool = get_spool(spl, new_last_id);
    assert(spool != newspool);
   spool_transfer_subscribers(spool, newspool, 0);
    destroy_spool(spool);
    */
  }
  if(newspool->sub_count > 0 && newspool->msg_status == MSG_INVALID) {
    spool_fetch_msg(newspool);
  }
  return NGX_OK;
}
typedef struct {
  channel_spooler_t   *spooler;
  nchan_msg_id_t       msgid;
} fetchmsg_data_t;

static ngx_int_t spool_fetch_msg_callback(nchan_msg_status_t findmsg_status, nchan_msg_t *msg, fetchmsg_data_t *data) {
  nchan_msg_id_t        anymsg = {0,0};
  subscriber_pool_t    *spool, *nuspool;
  
  if((spool = find_spool(data->spooler, &data->msgid)) == NULL) {
    ERR("spool for msgid %i:%i not found. discarding getmsg callback response.", data->msgid.time, data->msgid.tag);
    ngx_free(data);
    return NGX_ERROR;
  }
  
  ngx_free(data);
  
  spool->msg_status = findmsg_status;
  
  switch(findmsg_status) {
    case MSG_FOUND:
      DBG("fetchmsg callback for spool %p msg FOUND %p %i:%i", spool, msg, msg->id.time, msg->id.tag);
      assert(msg != NULL);
      spool->msg = msg;
      spool_respond_general(spool, spool->msg, 0, NULL);
      
      spool_nextmsg(spool, &msg->id);      
      break;
      
    case MSG_EXPECTED:
      // ♫ It's gonna be the future soon ♫
      DBG("fetchmsg callback for spool %p msg EXPECTED", spool);
      assert(msg == NULL);
      spool->msg = NULL;
      break;
      
    case MSG_NOTFOUND:
    case MSG_EXPIRED:
      //is this right?
      //TODO: maybe message-expired notification
      //spool_respond_general(spool, NULL, NGX_HTTP_NO_CONTENT, NULL);
      nuspool = get_spool(spool->spooler, &anymsg);
      assert(spool != nuspool);
      spool_transfer_subscribers(spool, nuspool, 1);
      destroy_spool(spool);
      break;
      
    default:
      assert(0);
      break;
  }
  
  return NGX_OK;
}

static ngx_int_t spool_fetch_msg(subscriber_pool_t *spool) {
  fetchmsg_data_t        *data;
  if(*spool->spooler->channel_status != READY) {
    DBG("%p wanted to fetch msg %i:%i, but channel %V not ready", spool, spool->id.time, spool->id.tag, spool->spooler->chid);
    spool->msg_status = MSG_CHANNEL_NOTREADY;
    return NGX_DECLINED;
  }
  DBG("%p fetch msg %i:%i for channel %V", spool, spool->id.time, spool->id.tag, spool->spooler->chid);
  data = ngx_alloc(sizeof(*data), ngx_cycle->log); //correctness over efficiency (at first).
  //TODO: optimize this alloc away
  
  assert(data);
  
  data->msgid = spool->id;
  data->spooler = spool->spooler;
  
  assert(spool->msg == NULL);
  assert(spool->msg_status == MSG_INVALID);
  spool->msg_status = MSG_PENDING;
  spool->spooler->store->get_message(spool->spooler->chid, &spool->id, (callback_pt )spool_fetch_msg_callback, data);
  return NGX_OK;
}



static void spool_sub_dequeue_callback(subscriber_t *sub, void *data) {
  spooled_subscriber_cleanup_t  *d = (spooled_subscriber_cleanup_t *)data;
  subscriber_pool_t             *spool = d->spool;
  
  assert(sub == d->ssub->sub);
  spool_remove_subscriber(spool, d->ssub);
  spool_bubbleup_dequeue_handler(spool, sub, spool->spooler);
  
/*
  if(spool->bulk_dequeue_handler) {
    void         *pd = spool->bulk_dequeue_handler_privdata;
    if(spool->type == SHORTLIVED) {
      if(spool->generation == 0) {
        //just some random aborted subscriber
        spool->bulk_dequeue_handler(spool, sub->type, 1, pd);
      }
      else {
        //first response. pretend to dequeue everything right away
        if(spool->responded_shortlived_subs == 1) {
          //assumes all SHORTLIVED subs are the same type. This is okay for now, but may lead to bugs.
          spool->bulk_dequeue_handler(spool, sub->type, spool->shortlived_sub_count + 1, pd);
        }
      }
    }
    else {
      spool->bulk_dequeue_handler(spool, sub->type, 1, pd);
    }
  }
*/

}

static ngx_int_t spool_add_subscriber(subscriber_pool_t *self, subscriber_t *sub, uint8_t enqueue) {
  spooled_subscriber_t       *ssub;
  
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
  ssub->dequeue_callback_data.ssub = ssub;
  ssub->dequeue_callback_data.spool = self;
  
  if(enqueue) {
    sub->fn->enqueue(sub);
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
  
  ngx_free(ssub);
  
  self->sub_count--;
  assert(self->sub_count >= 0);

  return NGX_OK;
}

static ngx_int_t spool_respond_general(subscriber_pool_t *self, nchan_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line) {
  ngx_uint_t                  numsubs[SUBSCRIBER_TYPES];
  spooled_subscriber_t       *nsub, *nnext;
  subscriber_t               *sub;
  
  ngx_memzero(numsubs, sizeof(numsubs));
  self->generation++;
  
  //DBG("spool %p respond with msg %p or code %i", self, msg, status_code);
  
  for(nsub = self->first; nsub != NULL; nsub = nnext) {
    sub = nsub->sub;
    self->responded_count++;
    nnext = nsub->next;
    
    if(msg) {
      sub->fn->respond_message(sub, msg);
    }
    else {
      sub->fn->respond_status(sub, status_code, status_line);
    }
  }
  self->responded_count++;
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
  if(spl->dequeue_handler) {
    spl->dequeue_handler(spl, sub, spl->dequeue_handler_privdata);
  }
  else if (spl->bulk_dequeue_handler){
    spl->bulk_dequeue_handler(spl, sub->type, 1, spl->bulk_dequeue_handler_privdata);
  }
  else {
    ERR("Neither dequeue_handler not bulk_dequeue_handler present in spooler for spool sub dequeue");
  }
}

/*
static void spool_bubbleup_bulk_dequeue_handler(subscriber_pool_t *spool, subscriber_type_t type, ngx_int_t count, channel_spooler_t *spl) {
  //bubble on up, yeah
  if(spl->bulk_dequeue_handler) {
    spl->bulk_dequeue_handler(spl, type, count, spl->bulk_dequeue_handler_privdata);
  }
}
*/

static ngx_int_t spooler_add_subscriber(channel_spooler_t *self, subscriber_t *sub) {
  nchan_msg_id_t          *msgid = &sub->last_msg_id;
  subscriber_pool_t       *spool;
  
  if(self->want_to_stop) {
    ERR("Not accepting new subscribers right now. want to stop.");
    return NGX_ERROR;
  }
  
  spool = get_spool(self, msgid);
  
  assert(spool->id.time == msgid->time);
  
  if(spool == NULL) {
    return NGX_ERROR;
  }

  if(spool_add_subscriber(spool, sub, 1) != NGX_OK) {
    ERR("couldn't add subscriber to spool %p", spool);
    return NGX_ERROR;
  }
  
  if(self->add_handler != NULL) {
    self->add_handler(self, sub, self->add_handler_privdata);
  }
  else {
    ERR("SPOOLER %p has no add_handler, couldn't add subscriber %p", self, sub);
  }
  
  switch(spool->msg_status) {
    case MSG_FOUND:
      assert(spool->msg);
      assert(spool->sub_count == 1);
      spool_respond_general(spool, spool->msg, 0, NULL);
      break;
    
    case MSG_INVALID:
      assert(spool->msg == NULL);
      spool_fetch_msg(spool);
      break;
    
    case MSG_CHANNEL_NOTREADY:
    case MSG_PENDING:
    case MSG_EXPECTED:
      //nothing to do but wait
      break;
      
    case MSG_EXPIRED:
    case MSG_NOTFOUND:
      //shouldn't happen
      assert(0);
  }

  return NGX_OK;
}


static ngx_int_t spool_transfer_subscribers(subscriber_pool_t *spool, subscriber_pool_t *newspool, uint8_t update_subscriber_last_msgid) {
  ngx_int_t               count = 0;
  subscriber_t           *sub;
  spooled_subscriber_t   *cur;
  
  assert(spool->spooler == newspool->spooler);
  
  if(spool == NULL || newspool == NULL) {
    ERR("failed to transfer spool subscribers");
    return 0;
  }
  for(cur = spool->first; cur != NULL; cur = spool->first) {
    sub = cur->sub;
    spool_remove_subscriber(spool, cur);
    if(update_subscriber_last_msgid) {
      sub->last_msg_id=newspool->id;
    }
    spool_add_subscriber(newspool, sub, 0);
    count++;
  }
  
  return count;
}

static ngx_int_t spooler_respond_message(channel_spooler_t *self, nchan_msg_t *msg) {
  static nchan_msg_id_t      any_msg = {0,0};
  int                        i, max=2;
  subscriber_pool_t         *spools[] = { find_spool(self, &msg->prev_id), find_spool(self, &any_msg) };
  //DBG("%p respond msg %i:%i", self, msg->id.time, msg->id.tag);
  
  if(msg->prev_id.time == 0 && msg->prev_id.tag == 0) {
    max = 1;
  }
  
  for(i=0; i < max; i++) { 
    //respond to prev_id spool and the {0,0} spool, as it's meant to accept any message;
    if(spools[i]) {
      spool_respond_general(spools[i], msg, 0, NULL);
      spool_nextmsg(spools[i], &msg->id);
    }
  }

  self->prev_msg_id.time = msg->id.time;
  self->prev_msg_id.tag = msg->id.tag;
  
  return NGX_OK;
}

typedef struct {
  channel_spooler_t *spl;
  nchan_msg_t       *msg;
  ngx_int_t          code;
  const ngx_str_t   *line;
} spooler_respond_generic_data_t;

static ngx_int_t spooler_respond_rbtree_node_spool(rbtree_seed_t *seed, subscriber_pool_t *spool, void *data) {
  spooler_respond_generic_data_t  *d = data;
  
  return spool_respond_general(spool, d->msg, d->code, d->line);
}

static ngx_int_t spooler_respond_generic(channel_spooler_t *self, nchan_msg_t *msg, ngx_int_t code, const ngx_str_t *line) {
  spooler_respond_generic_data_t  data = {self, msg, code, line};
  rbtree_walk(&self->spoolseed, (rbtree_walk_callback_pt )spooler_respond_rbtree_node_spool, &data);
  return NGX_OK;
}

static ngx_int_t spooler_respond_status(channel_spooler_t *self, ngx_int_t code, const ngx_str_t *line) {
  return spooler_respond_generic(self, NULL, code, line);
}

static ngx_int_t spooler_set_dequeue_handler(channel_spooler_t *self, void (*handler)(channel_spooler_t *, subscriber_t *, void*), void *privdata) {
  if(handler != NULL) {
    self->dequeue_handler = handler;
  }
  if(privdata != NULL) {
    self->dequeue_handler_privdata = privdata;
  }
  return NGX_OK;
}
static ngx_int_t spooler_set_bulk_dequeue_handler(channel_spooler_t *self, void (*handler)(channel_spooler_t *, subscriber_type_t, ngx_int_t, void *), void *privdata) {
  if(handler != NULL) {
    self->bulk_dequeue_handler = handler;
  }
  if(privdata != NULL) {
    self->bulk_dequeue_handler_privdata = privdata;
  }
  return NGX_OK;
}

static ngx_int_t spooler_set_add_handler(channel_spooler_t *self, void (*handler)(channel_spooler_t *, subscriber_t *, void *), void *privdata) {
  if(handler != NULL) {
    self->add_handler = handler;
  }
  if(privdata != NULL) {
    self->add_handler_privdata = privdata;
  }
  return NGX_OK;
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
    tag1 = id1->tag;
    tag2 = id2->tag;
    if(tag1 > tag2) {
      return 1;
    }
    else if(tag1 < tag2) {
      return -1;
    }
    else {
      return 0;
    }
  }
}

static ngx_int_t its_time_for_a_spooling(rbtree_seed_t *seed, subscriber_pool_t *spool, void *data) {
  ngx_int_t       rc;
  
  if(spool->msg_status == MSG_CHANNEL_NOTREADY) {
    spool->msg_status = MSG_INVALID;
    rc = spool_fetch_msg(spool);
  }
  else {
    rc = NGX_OK;
  }
  
  assert(rc == NGX_OK);
  return rc;
}

static ngx_int_t spooler_channel_status_changed(channel_spooler_t *self) {
  rbtree_walk_callback_pt callback = NULL;
  switch(*self->channel_status) {
    case READY:
      callback = (rbtree_walk_callback_pt )its_time_for_a_spooling;
      break;
      
    default:
      //do nothing
      break;
  };
  
  if(callback) {
    rbtree_walk(&self->spoolseed, callback, NULL); 
  }
  return NGX_OK;
}

static channel_spooler_fn_t  spooler_fn = {
  spooler_add_subscriber,
  spooler_channel_status_changed,
  spooler_respond_message,
  spooler_respond_status,
  spooler_prepare_to_stop,
  spooler_set_add_handler,
  spooler_set_dequeue_handler,
  spooler_set_bulk_dequeue_handler
};

channel_spooler_t *start_spooler(channel_spooler_t *spl, ngx_str_t *chid, chanhead_pubsub_status_t *channel_status, nchan_store_t *store) {
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
    
    spl->running = 1;
    //spl->want_to_stop = 0;
    return spl;
  }
  else {
    ERR("looks like spooler is already running. make sure spooler->running=0 befire starting.");
    assert(0);
    return NULL;
  }
  
  
  /*
  nchan_msg_id_t        id = {0,0};
  subscriber_pool_t     *spl;
  
  spl = get_spool(spl, &id);
  
  */
  
  
  
  
  
  
  
  
  
  
  
  
}

static ngx_int_t remove_spool(subscriber_pool_t *spool) {
  channel_spooler_t    *spl = spool->spooler;
  ngx_rbtree_node_t    *node = rbtree_node_from_data(spool);
  
  DBG("remove spool node %p", node);
  
  assert(spool->spooler->running);
  
  rbtree_remove_node(&spl->spoolseed, rbtree_node_from_data(spool));

  assert((node = rbtree_find_node(&spl->spoolseed, &spool->id)) == NULL);
  //double-check that it's gone 
  
  return NGX_OK;
}

static ngx_int_t destroy_spool(subscriber_pool_t *spool) {
  rbtree_seed_t         *seed = &spool->spooler->spoolseed;
  spooled_subscriber_t  *ssub;
  subscriber_t          *sub;
  ngx_rbtree_node_t     *node = rbtree_node_from_data(spool);
  
  remove_spool(spool);
  
  DBG("destroy spool node %p", node);
  
  for(ssub = spool->first; ssub!=NULL; ssub=ssub->next) {
    sub = ssub->sub;
    //DBG("dequeue sub %p in spool %p", sub, spool);
    sub->fn->dequeue(sub);
  }
  assert(spool->sub_count == 0);
  assert(spool->first == NULL);
  
  ngx_memset(spool, 0x42, sizeof(*spool)); //debug
  
  rbtree_destroy_node(seed, node);
  return NGX_OK;
}

ngx_int_t stop_spooler(channel_spooler_t *spl) {
  ngx_rbtree_node_t    *cur, *sentinel;
  rbtree_seed_t        *seed = &spl->spoolseed;
  ngx_rbtree_t         *tree = &seed->tree;
  ngx_int_t             n=0;
  sentinel = tree->sentinel;
#if NCHAN_RBTREE_DBG
  ngx_int_t active_before = seed->active_nodes, allocd_before = seed->active_nodes;
#endif
  if(spl->running) {
    
    for(cur = tree->root; cur != NULL && cur != sentinel; cur = tree->root) {
      rbtree_remove_node(seed, cur);
      rbtree_destroy_node(seed, cur);
      n++;
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
  spl->running = 0;
  return NGX_OK;
}