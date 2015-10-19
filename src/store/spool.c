#include <nchan_module.h>
#include "spool.h"
#include <assert.h>


#define NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_POOL_SIZE (2 * 1024)

#define DEBUG_LEVEL NGX_LOG_DEBUG
//#define DEBUG_LEVEL NGX_LOG_WARN

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SPOOL:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SPOOL:" fmt, ##arg)
#define COMMAND_SPOOL(spool, fn_name, arg...) ((spool)->fn_name((spool), ##arg))

//////// SPOOLs -- Subscriber Pools  /////////

static ngx_int_t spool_remove(subscriber_pool_t *, spooled_subscriber_t *);
static ngx_int_t safely_destroy_spool(subscriber_pool_t *spool);

static void spool_sub_dequeue_callback(subscriber_t *sub, void *data) {
  spooled_subscriber_cleanup_t  *d = (spooled_subscriber_cleanup_t *)data;
  subscriber_pool_t             *spool = d->spool;
  
  assert(sub == d->ssub->sub);
  spool_remove(spool, d->ssub);
  if(spool->dequeue_handler) {
    spool->dequeue_handler(spool, sub, spool->dequeue_handler_privdata);
  }

  if(spool->bulk_dequeue_handler) {
    void         *pd = spool->bulk_dequeue_handler_privdata;
    if(spool->type == SHORTLIVED) {
      if(spool->generation == 0) {
        //just some random aborted subscriber
        spool->bulk_dequeue_handler(spool, sub->type, 1, pd);
      }

      else {
        //first response. pretend to dequeue everything right away
        if(spool->responded_subs == 1) {
          //assumes all SHORTLIVED subs are the same type. This is okay for now, but may lead to bugs.
          spool->bulk_dequeue_handler(spool, sub->type, spool->sub_count + 1, pd);
        }
      }
    }
    else {
      spool->bulk_dequeue_handler(spool, sub->type, 1, pd);
    }
  }
}

static void spool_sub_empty_callback() {}

static ngx_int_t spool_add(subscriber_pool_t *self, subscriber_t *sub) {
  spooled_subscriber_t       *ssub;
  if(self->type == SHORTLIVED) {
    assert(self->pool);
    if(!sub->dequeue_after_response) {
      return NGX_ERROR;
    }
    ssub = ngx_palloc(self->pool, sizeof(*ssub));
    DBG("add sub %p to shortlived spool %p", sub, self);
  }
  else if(self->type == PERSISTENT) {
    assert(self->pool == NULL);
    if(sub->dequeue_after_response) {
      return NGX_ERROR;
    }
    ssub = ngx_alloc(sizeof(*ssub), ngx_cycle->log);
    DBG("add sub %p to persistent spool %p", sub, self);
  }
  else {
    ERR("Weird spool type. it sounds benign, but it's actually CATASTROPHIC.");
    assert(0); //we're not supposed to be here.
  }
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
  sub->set_dequeue_callback(sub, spool_sub_dequeue_callback, &ssub->dequeue_callback_data);
  ssub->sub = sub;
  //TODO: set timeout callback maybe? nah...
  
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
  if(self->pool) {
    assert(self->type == SHORTLIVED);
    //ngx_pfree(self->pool, ssub);
    DBG("remove spooled_sub %p from shortlived spool %p", ssub, self);
    ssub->sub->set_dequeue_callback(ssub->sub, spool_sub_empty_callback, NULL);
  }
  else {
    int                      i=0;
    spooled_subscriber_t    *scur;
    assert(self->type == PERSISTENT);
    DBG("remove spooled_sub %p from persistent spool %p. first sub is now %p", ssub, self, self->first);
    ngx_free(ssub);
    for(scur = self->first; scur != NULL; scur = scur->next) {
      i++;
      DBG("sub #%i is %p", i, scur);
    }
  }
  self->sub_count--;
  assert(self->sub_count >= 0);

  return NGX_OK;
}

static ngx_int_t spool_respond_general(subscriber_pool_t *self, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line) {
  spooled_subscriber_t       *nsub, *nnext;
  subscriber_t               *sub;
  self->generation++;
  
  DBG("spool %p respond with msg %p or code %i", self, msg, status_code);
  
  for(nsub = self->first; nsub != NULL; nsub = nnext) {
    sub = nsub->sub;
    self->responded_subs++;
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

static ngx_int_t spool_respond_status(subscriber_pool_t *self, ngx_int_t status_code, const ngx_str_t *status_line) {
  return spool_respond_general(self, NULL, status_code, status_line);
}

static ngx_int_t spool_respond_message(subscriber_pool_t *self, ngx_http_push_msg_t *msg) {
  return spool_respond_general(self, msg, 0, NULL);
}

static ngx_int_t spool_set_dequeue_handler(subscriber_pool_t *self, void (*handler)(subscriber_pool_t *, subscriber_t *, void*), void *privdata) {
  if(handler != NULL) {
    self->dequeue_handler = handler;
  }
  if(privdata != NULL) {
    self->dequeue_handler_privdata = privdata;
  }
  return NGX_OK;
}

static ngx_int_t spool_set_bulk_dequeue_handler(subscriber_pool_t *self, void (*handler)(subscriber_pool_t *, subscriber_type_t, ngx_int_t, void*), void *privdata) {
  if(handler != NULL) {
    self->bulk_dequeue_handler = handler;
  }
  if(privdata != NULL) {
    self->bulk_dequeue_handler_privdata = privdata;
  }
  return NGX_OK;
}

static subscriber_pool_t *create_spool(spool_type_t type) {
  subscriber_pool_t *spool = NULL;
  if(type == SHORTLIVED) {
    ngx_pool_t      *pool = ngx_create_pool(NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_POOL_SIZE, ngx_cycle->log);
    if(pool == NULL) {
      ERR("couldn't create pool for ONESHOT spool");
      return NULL;
    }
    if((spool = ngx_palloc(pool, sizeof(*spool))) == NULL) {
      ERR("couldn't allocate spool for ONESHOT spool");
      return NULL;
    }
    spool->pool = pool;
    spool->type = SHORTLIVED;
  }
  else if(type == PERSISTENT) {
    if((spool = ngx_alloc(sizeof(*spool), ngx_cycle->log)) == NULL) {
      ERR("couldn't allocate PERSIST spool");
      return NULL;
    }
    spool->pool = NULL;
    spool->type = PERSISTENT;
  }
  else {
    ERR("Unable to allocate unknown spool type");
    return NULL;
  }
  spool->first = NULL;
  spool->sub_count = 0;
  spool->generation = 0;
  spool->responded_subs = 0;
  spool->add = spool_add;
  spool->respond_message = spool_respond_message;
  spool->respond_status = spool_respond_status;
  spool->dequeue_handler = NULL;
  spool->dequeue_handler_privdata = NULL;
  spool->bulk_dequeue_handler = NULL;
  spool->bulk_dequeue_handler_privdata = NULL;
  spool->set_dequeue_handler = spool_set_dequeue_handler;
  spool->set_bulk_dequeue_handler = spool_set_bulk_dequeue_handler;
  return spool;
}

static subscriber_pool_t *recreate_spool(subscriber_pool_t *old_spool) {
  subscriber_pool_t *nuspool;
  if((nuspool = create_spool(old_spool->type)) == NULL) {
    ERR("unable to recreate spool %p", old_spool);
    return NULL;
  }
  COMMAND_SPOOL(nuspool, set_dequeue_handler, old_spool->dequeue_handler, old_spool->dequeue_handler_privdata);
  COMMAND_SPOOL(nuspool, set_bulk_dequeue_handler, old_spool->bulk_dequeue_handler, old_spool->bulk_dequeue_handler_privdata);
  return nuspool;
}

static ngx_int_t destroy_that_spool_right_NOW(subscriber_pool_t *spool) {
  if(spool->pool) {
    assert(spool->type == SHORTLIVED);
    //ngx_pfree(spool->pool, spool); //don't need to.
    ngx_destroy_pool(spool->pool);
  }
  else {
    assert(spool->type == PERSISTENT);
    ngx_free(spool);
  }
  return NGX_OK;
}

ngx_int_t  safely_destroy_spool(subscriber_pool_t *spool) {
  spooled_subscriber_t      *ssub;
  subscriber_t              *sub;
  if(spool->first == NULL) {
    assert(spool->sub_count == 0);
    destroy_that_spool_right_NOW(spool);
  }
  else{
    spool->respond_status(spool, NGX_HTTP_GONE, &NCHAN_HTTP_STATUS_410);
    switch(spool->type) {
      case SHORTLIVED:
        assert(spool->sub_count == 0);
        assert(spool->first == NULL);
        assert(spool->pool != NULL);
        //will be deleted when subsribers are done responding
        break;
      
      case PERSISTENT:  
        for(ssub = spool->first; ssub!=NULL; ssub=ssub->next) {
          sub = ssub->sub;
          DBG("dequeue sub %p in PERSISTENT spool %p", sub, spool);
          sub->dequeue(sub);
        }
        assert(spool->sub_count == 0);
        assert(spool->first == NULL);
        DBG("destroy PERSISTENT spool %p right now", spool);
        destroy_that_spool_right_NOW(spool);
        break;
      
      default:
        ERR("unknown spool type");
        return NGX_ERROR;
        break;
    }
  }
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

static ngx_int_t spooler_add_subscriber(channel_spooler_t *self, subscriber_t *sub) {
  if(self->want_to_stop) {
    ERR("Not accepting new subscribers right now. want to stop.");
    return NGX_ERROR;
  }
  
  if(COMMAND_SPOOL(self->shortlived, add, sub) != NGX_OK) {
    if(COMMAND_SPOOL(self->persistent, add, sub) != NGX_OK) {
      ERR("couldn't add subscriber to any spool");
      return NGX_ERROR;
    }
  }

  if(self->add_handler != NULL) {
    self->add_handler(self, sub, self->add_handler_privdata);
  }
  else {
    ERR("SPOOLER %p has no add_handler, couldn't add subscriber %p", self, sub);
  }

  return NGX_OK;
}


static ngx_int_t spooler_respond_generic(channel_spooler_t *self, ngx_http_push_msg_t *msg, ngx_int_t code, const ngx_str_t *line) {
  subscriber_pool_t       *old = self->shortlived;
  
  /*
  if(msg) {
    DBG("SPOOLER %p respond msg %p", self, msg);
  }
  else {
    DBG("SPOOLER %p respond code %i", self, code);
  }
  */
  
  //replacement spool
  self->shortlived = recreate_spool(old);
  if(msg) {
    COMMAND_SPOOL(old, respond_message, msg);
    COMMAND_SPOOL(self->persistent, respond_message, msg);
  }
  else {
    COMMAND_SPOOL(old, respond_status, code, line);
    COMMAND_SPOOL(self->persistent, respond_status, code, line);
  }
  safely_destroy_spool(old);
  self->responded_count++;
  return NGX_OK;
}

static ngx_int_t spooler_respond_message(channel_spooler_t *self, ngx_http_push_msg_t *msg) {
  return spooler_respond_generic(self, msg, 0, NULL);
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

static void bubbleup_dequeue_handler(subscriber_pool_t *spool, subscriber_t *sub, void *privdata) {
  channel_spooler_t *spl = (channel_spooler_t *)privdata;
  //bubble on up, yeah
  if(spl->dequeue_handler) {
    spl->dequeue_handler(spl, sub, spl->dequeue_handler_privdata);
  }
}

static void bubbleup_bulk_dequeue_handler(subscriber_pool_t *spool, subscriber_type_t type, ngx_int_t count, void *privdata) {
  channel_spooler_t *spl = (channel_spooler_t *)privdata;
  //bubble on up, yeah
  if(spl->bulk_dequeue_handler) {
    spl->bulk_dequeue_handler(spl, type, count, spl->bulk_dequeue_handler_privdata);
  }
}

static ngx_int_t spooler_prepare_to_stop(channel_spooler_t *spl) {
  spooled_subscriber_t  *ssub;
  for(ssub = spl->persistent->first; ssub != NULL; ssub = ssub->next) {
    ssub->sub->dequeue_after_response = 1;
  }
  spl->want_to_stop = 1;
  return NGX_OK;
}


channel_spooler_t *start_spooler(channel_spooler_t *spl) {
  if(!spl->running) {
    spl->shortlived = create_spool(SHORTLIVED);
    spl->persistent = create_spool(PERSISTENT);
    spl->add = spooler_add_subscriber;
    spl->respond_message = spooler_respond_message;
    spl->respond_status = spooler_respond_status;
    spl->set_dequeue_handler = spooler_set_dequeue_handler;
    spl->set_bulk_dequeue_handler = spooler_set_bulk_dequeue_handler;
    spl->set_add_handler = spooler_set_add_handler;
    spl->prepare_to_stop = spooler_prepare_to_stop;
    spl->add_handler = NULL;
    spl->add_handler_privdata = NULL;
    spl->dequeue_handler = NULL;
    spl->dequeue_handler_privdata = NULL;
    spl->bulk_dequeue_handler = NULL;
    spl->bulk_dequeue_handler_privdata = NULL;
    
    DBG("start SPOOLER %p", *spl);
    
    COMMAND_SPOOL(spl->shortlived, set_dequeue_handler, bubbleup_dequeue_handler, spl);
    COMMAND_SPOOL(spl->persistent, set_dequeue_handler, bubbleup_dequeue_handler, spl);
    COMMAND_SPOOL(spl->shortlived, set_bulk_dequeue_handler, bubbleup_bulk_dequeue_handler, spl);
    COMMAND_SPOOL(spl->persistent, set_bulk_dequeue_handler, bubbleup_bulk_dequeue_handler, spl);
    spl->running = 1;
    spl->want_to_stop = 0;
    return spl;
  }
  else {
    ERR("looks like spooler is already running. make sure spooler->running=0 befire starting.");
    return NULL;
  }
}

ngx_int_t stop_spooler(channel_spooler_t *spl) {
  if(spl->running) {
    safely_destroy_spool(spl->shortlived);
    safely_destroy_spool(spl->persistent);
    DBG("stopped SPOOLER %p", *spl);
  }
  spl->running = 0;
  return NGX_OK;
}