#include <ngx_http_push_module.h>
#include "spool.h"
#include <assert.h>


#define NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_POOL_SIZE (2 * 1024)
#define DEBUG_LEVEL NGX_LOG_WARN
#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SPOOL:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SPOOL:" fmt, ##arg)
#define COMMAND_SPOOL(spool, fn_name, arg...) ((spool)->fn_name((spool), ##arg))

//////// SPOOLs -- Subscriber Pools  /////////

static ngx_int_t spool_remove(subscriber_pool_t *, spooled_subscriber_t *);
static ngx_int_t safely_destroy_spool(subscriber_pool_t *spool);

static void spool_sub_dequeue_callback(subscriber_t *sub, void *data) {
  spooled_subscriber_cleanup_t  *d = (spooled_subscriber_cleanup_t *)data;
  subscriber_pool_t             *spool = d->spool;
  void                          *pd = spool->dequeue_handler_privdata;
  
  spool_remove(spool, d->ssub);
  
  if(spool->dequeue_handler) {
    if(spool->type == SHORTLIVED) {
      if(spool->generation == 0) {
        //just some random aborted subscriber
        spool->dequeue_handler(spool, 1, pd);
      }
      else {
        //first response. pretend to dequeue everything right away
        if(spool->responded_subs == 1) {
          spool->dequeue_handler(spool, spool->sub_count + 1, pd);
        }
      }
    }
    else {
      spool->dequeue_handler(spool, 1, pd);
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
  }
  else if(self->type == PERSISTENT) {
    assert(self->pool == NULL);
    if(sub->dequeue_after_response) {
      return NGX_ERROR;
    }
    ssub = ngx_alloc(sizeof(*ssub), ngx_cycle->log);
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
    ssub->sub->set_dequeue_callback(ssub->sub, spool_sub_empty_callback, NULL);
  }
  else {
    int                      i=0;
    spooled_subscriber_t    *scur;
    assert(self->type == PERSISTENT);
    ngx_free(ssub);
    DBG("Just freed %p. first sub is now %p", ssub, self->first);
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
  return spool_respond_general(self, msg, NULL, NULL);
}

static ngx_int_t spool_set_dequeue_handler(subscriber_pool_t *self, void (*handler)(subscriber_pool_t *, ngx_int_t, void*), void *privdata) {
  if(handler != NULL) {
    self->dequeue_handler = handler;
  }
  if(privdata != NULL) {
    self->dequeue_handler_privdata = privdata;
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
    }
    spool->pool = NULL;
    spool->type = PERSISTENT;
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
  spool->set_dequeue_handler = spool_set_dequeue_handler;
  return spool;
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
    spool->respond_status(spool, NGX_HTTP_GONE, &NGX_HTTP_PUSH_HTTP_STATUS_410);
    if(spool->type == SHORTLIVED) {
      assert(spool->sub_count == 0);
      assert(spool->first == NULL);
      assert(spool->pool != NULL);
      //will be deleted when subsribers are done responding
      return NGX_OK;
    }
    else {
      for(ssub = spool->first; ssub!=NULL; ssub=ssub->next) {
        sub = ssub->sub;
        sub->dequeue(sub);
      }
      assert(spool->sub_count == 0);
      assert(spool->first == NULL);
      destroy_that_spool_right_NOW(spool);
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

  return NGX_OK;
}


static void terribly_named_dequeue_handler(subscriber_pool_t *spool, ngx_int_t count, void *privdata);


static ngx_int_t spooler_respond_generic(channel_spooler_t *self, ngx_http_push_msg_t *msg, ngx_int_t code, const ngx_str_t *line) {
  subscriber_pool_t       *old = self->shortlived;

  //replacement shpool
  self->shortlived = create_spool(SHORTLIVED);
  COMMAND_SPOOL(self->shortlived, set_dequeue_handler, terribly_named_dequeue_handler, self);
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

static ngx_int_t spooler_set_dequeue_handler(channel_spooler_t *self, void (*handler)(channel_spooler_t *, ngx_int_t, void*), void *privdata) {
  if(handler != NULL) {
    self->dequeue_handler = handler;
  }
  if(privdata != NULL) {
    self->dequeue_handler_privdata = privdata;
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

static void terribly_named_dequeue_handler(subscriber_pool_t *spool, ngx_int_t count, void *privdata) {
  channel_spooler_t *spl = (channel_spooler_t *)privdata;
  //bubble on up, yeah
  spl->dequeue_handler(spl, count, spl->dequeue_handler_privdata);
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
    spl->set_add_handler = spooler_set_add_handler;
    spl->prepare_to_stop = spooler_prepare_to_stop;
    spl->dequeue_handler = NULL;
    spl->dequeue_handler_privdata = NULL;
    spl->add_handler = NULL;
    spl->add_handler_privdata = NULL;
    COMMAND_SPOOL(spl->shortlived, set_dequeue_handler, terribly_named_dequeue_handler, spl);
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
  }
  spl->running = 0;
  return NGX_OK;
}