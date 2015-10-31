#include <nchan_module.h>
#include "rbtree_util.h"
#include "spool.h"
#include <assert.h>

#define DEBUG_LEVEL NGX_LOG_DEBUG
//#define DEBUG_LEVEL NGX_LOG_WARN

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SPOOL:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SPOOL:" fmt, ##arg)
#define COMMAND_SPOOL(spool, fn_name, arg...) ((spool)->fn_name((spool), ##arg))

//////// SPOOLs -- Subscriber Pools  /////////

static ngx_int_t spool_remove(subscriber_pool_t *, spooled_subscriber_t *);
static ngx_int_t safely_destroy_spool(subscriber_pool_t *spool);
static ngx_int_t init_spool(channel_spooler_t *spl, subscriber_pool_t *spool);
static void spool_bubbleup_dequeue_handler(subscriber_pool_t *spool, subscriber_t *sub, channel_spooler_t *spl);
//static void spool_bubbleup_bulk_dequeue_handler(subscriber_pool_t *spool, subscriber_type_t type, ngx_int_t count, channel_spooler_t *spl);
static ngx_int_t spooler_msgleaf_respond_generic(channel_spooler_t *self, spooler_msg_leaf_t *leaf, nchan_msg_t *msg, ngx_int_t code, const ngx_str_t *line);
static ngx_int_t spool_respond_general(subscriber_pool_t *self, nchan_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line);
static ngx_int_t spooler_msgleaf_transfer_subscribers(channel_spooler_t *self, spooler_msg_leaf_t *leaf, spooler_msg_leaf_t *newleaf);
static ngx_int_t destroy_msgleaf(spooler_msg_leaf_t *leaf);
static ngx_int_t msgleaf_fetch_msg(spooler_msg_leaf_t *leaf);

static void msgid_to_str(nchan_msg_id_t *id, ngx_str_t *str) {
  str->len=sizeof(*id);
  str->data=(u_char *)id;
}

static spooler_msg_leaf_t *find_msgleaf(channel_spooler_t *spl, nchan_msg_id_t *id) {
  rbtree_seed_t      *seed = &spl->spoolseed;
  ngx_str_t           pseudoid;
  ngx_rbtree_node_t  *node;
  
  msgid_to_str(id, &pseudoid);
  
  if((node = rbtree_find_node(seed, &pseudoid)) != NULL) {
    return (spooler_msg_leaf_t *)rbtree_data_from_node(node);
  }
  else {
    return NULL;
  }
}

static ngx_int_t msgleaf_fetch_msg_callback(nchan_msg_status_t findmsg_status, nchan_msg_t *msg, spooler_msg_leaf_t *leaf) {
  spooler_msg_leaf_t      *newleaf;
  channel_spooler_t       *spl = leaf->spool.spooler;
  ngx_rbtree_node_t       *node;
  
  leaf->msg_status = findmsg_status;
  
  switch(findmsg_status) {
    case MSG_FOUND:
      assert(msg != NULL);
      leaf->msg = msg;
      spool_respond_general(&leaf->spool, leaf->msg, 0, NULL);
      
      newleaf = find_msgleaf(spl, &msg->id);
      assert(leaf != newleaf);
      if(newleaf) {
        spooler_msgleaf_transfer_subscribers(spl, leaf, newleaf);
        destroy_msgleaf(leaf);
      }
      else {
        node = rbtree_node_from_data(leaf);
        rbtree_remove_node(&spl->spoolseed, node);
        leaf->id = msg->id;
        rbtree_insert_node(&spl->spoolseed, node);
        leaf->msg_status = MSG_INVALID;
        leaf->msg = NULL;
        newleaf = leaf;
      }
      
      if(newleaf->spool.sub_count > 0 && newleaf->msg_status == MSG_INVALID) {
        msgleaf_fetch_msg(leaf);
      }
      
      break;
      
    case MSG_EXPECTED:
      // ♫ It's gonna be the future soon ♫
      assert(msg == NULL);
      leaf->msg = NULL;
      break;
      
    case MSG_NOTFOUND:
      //is this right?
      spooler_msgleaf_respond_generic(leaf->spl, leaf, NULL, NGX_HTTP_FORBIDDEN, NULL);
      break;
      
    case MSG_EXPIRED:
      leaf->msg = NULL;
      spooler_msgleaf_respond_generic(leaf->spl, leaf, NULL, NGX_HTTP_NO_CONTENT, NULL);
      break;
      
    default:
      assert(0);
      break;
  }
  
  return NGX_OK;
}

static ngx_int_t msgleaf_fetch_msg(spooler_msg_leaf_t *leaf) {
  assert(leaf->msg == NULL);
  assert(leaf->msg_status == MSG_INVALID);
  leaf->msg_status = MSG_PENDING;
  leaf->spl->store->get_message(leaf->spl->chid, &leaf->id, (callback_pt )msgleaf_fetch_msg_callback, leaf);
  return NGX_OK;
}

static spooler_msg_leaf_t *get_msgleaf(channel_spooler_t *spl, nchan_msg_id_t *id) {
  rbtree_seed_t      *seed = &spl->spoolseed;
  ngx_str_t           pseudoid;
  ngx_rbtree_node_t  *node;
  spooler_msg_leaf_t *leaf;
  
  msgid_to_str(id, &pseudoid);
  
  if((node = rbtree_find_node(seed, &pseudoid)) == NULL) {
    if((node = rbtree_create_node(seed, sizeof(*leaf))) == NULL) {
      ERR("can't create rbtree spooler_msgleaf");
      return NULL;
    }
    DBG("created node %p for msgid %i:%i", node, id->time, id->tag);
    leaf = (spooler_msg_leaf_t *)rbtree_data_from_node(node);
    leaf->id = *id;
    leaf->spl = spl;
    leaf->msg_status = MSG_INVALID;
    leaf->msg = NULL;
    
    msgid_to_str(&leaf->id, &leaf->pseudoid);
    
    init_spool(spl, &leaf->spool);
    if(rbtree_insert_node(seed, node) != NGX_OK) {
      ERR("couldn't insert msgleaf node");
      rbtree_destroy_node(seed, node);
      return NULL;
    }
  }
  else {
    leaf = (spooler_msg_leaf_t *)rbtree_data_from_node(node);
    DBG("found node %p with msgid %i:%i", node, id->time, id->tag);
    assert(leaf->id.time == id->time);
  }
  return leaf;
}

static void spool_sub_dequeue_callback(subscriber_t *sub, void *data) {
  spooled_subscriber_cleanup_t  *d = (spooled_subscriber_cleanup_t *)data;
  subscriber_pool_t             *spool = d->spool;
  
  assert(sub == d->ssub->sub);
  spool_remove(spool, d->ssub);
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

static ngx_int_t spool_add(subscriber_pool_t *self, subscriber_t *sub) {
  spooled_subscriber_t       *ssub;
  
  ssub = ngx_alloc(sizeof(*ssub), ngx_cycle->log);
  DBG("add sub %p to spool %p", sub, self);
  
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
  
  sub->enqueue(sub);
  
  sub->set_dequeue_callback(sub, spool_sub_dequeue_callback, &ssub->dequeue_callback_data);
  ssub->sub = sub;
  
  return NGX_OK;
}

static ngx_int_t spool_remove(subscriber_pool_t *self, spooled_subscriber_t *ssub) {
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
  
  DBG("spool %p respond with msg %p or code %i", self, msg, status_code);
  
  for(nsub = self->first; nsub != NULL; nsub = nnext) {
    sub = nsub->sub;
    self->responded_count++;
    nnext = nsub->next;
    
    if(msg) {
      sub->respond_message(sub, msg);
    }
    else {
      sub->respond_status(sub, status_code, status_line);
    }
  }
  return NGX_OK;
}

static ngx_int_t init_spool(channel_spooler_t *spl, subscriber_pool_t *spool) {
  ngx_memzero(spool, sizeof(*spool));
  
  spool->spooler = spl;
  return NGX_OK;
}

static ngx_int_t destroy_that_spool_right_NOW(subscriber_pool_t *spool) {
  if(spool->pool) {
    ngx_destroy_pool(spool->pool);
  }
  
  ngx_memset(spool, 0xFC, sizeof(*spool)); //debug

  return NGX_OK;
}

ngx_int_t  safely_destroy_spool(subscriber_pool_t *spool) {
  spooled_subscriber_t      *ssub;
  subscriber_t              *sub;
  ERR("Want to destroy spool %p", spool);
  if(spool->first == NULL) {
    assert(spool->sub_count == 0);
    destroy_that_spool_right_NOW(spool);
  }
  else{
    ERR("Can't destroy spool %p yet", spool);
    spool_respond_general(spool, NULL, NGX_HTTP_GONE, &NCHAN_HTTP_STATUS_410);
    for(ssub = spool->first; ssub!=NULL; ssub=ssub->next) {
      sub = ssub->sub;
      DBG("dequeue sub %p in spool %p", sub, spool);
      sub->dequeue(sub);
    }
    assert(spool->sub_count == 0);
    assert(spool->first == NULL);
    
    destroy_that_spool_right_NOW(spool);
  }
  return NGX_OK;
}

static subscriber_pool_t *get_msgleaf_spool(channel_spooler_t *self, spooler_msg_leaf_t *leaf) {
  return &leaf->spool;
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
  spooler_msg_leaf_t      *leaf;
  subscriber_pool_t       *spool;
  
  if(self->want_to_stop) {
    ERR("Not accepting new subscribers right now. want to stop.");
    return NGX_ERROR;
  }
  
  leaf = get_msgleaf(self, msgid);
  
  assert(leaf->id.time == msgid->time);
  
  spool = get_msgleaf_spool(self, leaf);
  
  if(spool == NULL) {
    return NGX_ERROR;
  }

  if(spool_add(spool, sub) != NGX_OK) {
    ERR("couldn't add subscriber to spool %p", spool);
    return NGX_ERROR;
  }
  
  if(self->add_handler != NULL) {
    self->add_handler(self, sub, self->add_handler_privdata);
  }
  else {
    ERR("SPOOLER %p has no add_handler, couldn't add subscriber %p", self, sub);
  }
  
  switch(leaf->msg_status) {
    case MSG_FOUND:
      assert(leaf->msg);
      assert(leaf->spool.sub_count == 1);
      spool_respond_general(&leaf->spool, leaf->msg, 0, NULL);
      break;
    
    case MSG_INVALID:
      assert(leaf->msg == NULL);
      msgleaf_fetch_msg(leaf);
      break;
      
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


static ngx_int_t spooler_msgleaf_respond_generic(channel_spooler_t *self, spooler_msg_leaf_t *leaf, nchan_msg_t *msg, ngx_int_t code, const ngx_str_t *line) {
  return spool_respond_general(&leaf->spool, msg, code, line);
  self->responded_count++;
  return NGX_OK;
}

static ngx_int_t spooler_msgleaf_transfer_subscribers(channel_spooler_t *self, spooler_msg_leaf_t *leaf, spooler_msg_leaf_t *newleaf) {
  ngx_int_t               count = 0;
  subscriber_t           *sub;
  spooled_subscriber_t   *cur;
  subscriber_pool_t      *spool, *nuspool;
  
  spool   = get_msgleaf_spool(self, leaf);
  nuspool = get_msgleaf_spool(self, newleaf);
  if(spool == NULL || nuspool == NULL) {
    ERR("failed to transfer spool subscribers");
    return 0;
  }
  for(cur = spool->first; cur != NULL; cur = spool->first) {
    sub = cur->sub;
    spool_remove(spool, cur);
    spool_add(nuspool, sub);
    count++;
  }
  
  return count;
}

static ngx_int_t spooler_respond_message(channel_spooler_t *self, nchan_msg_t *msg) {
  static nchan_msg_id_t      any_msg = {0,0};
  static nchan_msg_id_t     *findall[] = {NULL, &any_msg};
  int                        i;
  spooler_msg_leaf_t        *leaf, *newleaf;
  ngx_rbtree_node_t         *node;
  
  findall[0]=&msg->prev_id;
  
  for(i=0; i < 2; i++) { 
    //respond to prev_id spool and the {0,0} spool, as it's meant to accept any message;
    newleaf = find_msgleaf(self, &msg->id);
    
    leaf = find_msgleaf(self, findall[i]);
    if(leaf) {
      spooler_msgleaf_respond_generic(self, leaf, msg, 0, NULL);
      if(newleaf) {
        spooler_msgleaf_transfer_subscribers(self, leaf, newleaf);
        destroy_msgleaf(leaf);
      }
      else {
        node = rbtree_node_from_data(leaf);
        rbtree_remove_node(&self->spoolseed, node);
        leaf->id = msg->id;
        rbtree_insert_node(&self->spoolseed, node);
        newleaf = leaf;
      }
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

static ngx_int_t spooler_respond_rbtree_node_msgleaf(rbtree_seed_t *seed, spooler_msg_leaf_t *leaf, void *data) {
  spooler_respond_generic_data_t  *d = data;
  
  return spooler_msgleaf_respond_generic(d->spl, leaf, d->msg, d->code, d->line);
}

static ngx_int_t spooler_all_msgleaves_respond_generic(channel_spooler_t *self, nchan_msg_t *msg, ngx_int_t code, const ngx_str_t *line) {
  spooler_respond_generic_data_t  data = {self, msg, code, line};
  rbtree_walk(&self->spoolseed, (rbtree_walk_callback_pt )spooler_respond_rbtree_node_msgleaf, &data);
  return NGX_OK;
}

static ngx_int_t spooler_respond_status(channel_spooler_t *self, ngx_int_t code, const ngx_str_t *line) {
  return spooler_all_msgleaves_respond_generic(self, NULL, code, line);
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

static ngx_int_t spooler_msgleaf_dequeue_all(rbtree_seed_t *seed, spooler_msg_leaf_t *leaf, void *data) {
  spooled_subscriber_t *cur;
  
  for(cur = leaf->spool.first; cur != NULL; cur = cur->next) {
    cur->sub->dequeue_after_response = 1;  
  }
  
  return NGX_OK;
}

static ngx_int_t spooler_prepare_to_stop(channel_spooler_t *spl) {
  rbtree_walk(&spl->spoolseed, (rbtree_walk_callback_pt )spooler_msgleaf_dequeue_all, (void *)spl);
  spl->want_to_stop = 1;
  return NGX_OK;
}



static ngx_str_t *spooler_msgleaf_id(void *data) {
  return &((spooler_msg_leaf_t *)data)->pseudoid;
}


static channel_spooler_fn_t  spooler_fn = {
  spooler_add_subscriber,
  spooler_respond_message,
  spooler_respond_status,
  spooler_prepare_to_stop,
  spooler_set_add_handler,
  spooler_set_dequeue_handler,
  spooler_set_bulk_dequeue_handler
};

channel_spooler_t *start_spooler(channel_spooler_t *spl, ngx_str_t *chid, nchan_store_t *store) {
  if(!spl->running) {
    ngx_memzero(spl, sizeof(*spl));
    rbtree_init(&spl->spoolseed, "spooler msg_id tree", spooler_msgleaf_id, NULL, NULL);
    
    spl->fn=&spooler_fn;
    //spl->prev_msg_id.time=0;
    //spl->prev_msg_id.tag=0;
    
    DBG("start SPOOLER %p", *spl);
    
    spl->chid = chid;
    spl->store = store;
    
    spl->running = 1;
    //spl->want_to_stop = 0;
    return spl;
  }
  else {
    ERR("looks like spooler is already running. make sure spooler->running=0 befire starting.");
    return NULL;
  }
}

static ngx_int_t destroy_msgleaf(spooler_msg_leaf_t *leaf) {
  channel_spooler_t    *spl = leaf->spool.spooler;
  if(spl->running) {
    safely_destroy_spool(&leaf->spool);
  }
  rbtree_remove_node(&spl->spoolseed, rbtree_node_from_data(leaf));
  
  ngx_memset(leaf, 0x42, sizeof(*leaf)); //debug
  
  rbtree_destroy_node(&spl->spoolseed, rbtree_node_from_data(leaf));
  return NGX_OK;
}

ngx_int_t stop_spooler(channel_spooler_t *spl) {
  ngx_rbtree_node_t    *cur, *sentinel;
  rbtree_seed_t        *seed = &spl->spoolseed;
  ngx_rbtree_t         *tree = &seed->tree;
  sentinel = tree->sentinel;
  if(spl->running) {
    //rbtree_walk(&spl->spoolseed, (rbtree_walk_callback_pt )spooler_msgleaf_destroyer, (void *)spl);
    
    for(cur = tree->root; cur != NULL && cur != sentinel; cur = tree->root) {
      rbtree_remove_node(seed, cur);
      rbtree_destroy_node(seed, cur);
    }
    
    DBG("stopped SPOOLER %p", *spl);
  }
#if NCHAN_RBTREE_DBG
  assert(spl->spoolseed.active_nodes == 0);
  assert(spl->spoolseed.allocd_nodes == 0);
#endif
  spl->running = 0;
  return NGX_OK;
}