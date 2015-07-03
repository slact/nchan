#include <ngx_http_push_module.h>
#include "spool.h"
#include "spool.h"
#include <assert.h>

#define NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_POOL_SIZE (2 * 1024)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)

#define COMMAND_SPOOL(spool, fn_name, arg...) ((spool)->fn_name((spool), ##arg))

//////// SPOOLs -- Subscriber Pools  /////////

static ngx_int_t spool_remove(subscriber_pool_t *, spooled_subscriber_t *);
static ngx_int_t safely_destroy_spool(subscriber_pool_t *spool);

static void spool_sub_dequeue_callback(subscriber_t *sub, void *data) {
  spooled_subscriber_cleanup_t  *d = (spooled_subscriber_cleanup_t *)data;
  subscriber_pool_t             *spool = d->spool;
  spool->sub_count--;
  assert(spool->sub_count >= 0);
  spool_remove(spool, d->ssub);
  if(spool->dequeue_handler != NULL) {
    spool->dequeue_handler(spool, 1, spool->dequeue_handler_privdata);
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

  ssub->dequeue_callback_data.ssub = ssub;
  ssub->dequeue_callback_data.spool = self;
  sub->set_dequeue_callback(sub, spool_sub_dequeue_callback, &ssub->dequeue_callback_data);
  //TODO: set timeout callback maybe? nah...
  
  return NGX_OK;
}

static ngx_int_t spool_remove(subscriber_pool_t *self, spooled_subscriber_t *ssub) {
  if(ssub->next) {
    ssub->next->prev = ssub->prev;
  }
  if(ssub->prev) {
    ssub->prev->next = ssub->next;
  }
  if(ssub == self->first) {
    self->first = ssub->next;
  }
  if(self->pool) {
    assert(self->type == SHORTLIVED);
    ngx_pfree(self->pool, ssub);
  }
  else {
    assert(self->type == PERSISTENT);
    ngx_free(ssub);
  }
  self->sub_count--;
  assert(self->sub_count >= 0);
  ssub->sub->set_dequeue_callback(ssub->sub, spool_sub_empty_callback, NULL);
  if(self->sub_count == 0 && self->type == SHORTLIVED) {
    //terminate!
    safely_destroy_spool(self);
  }

  return NGX_OK;
}

static ngx_int_t spool_respond_message(subscriber_pool_t *self, ngx_http_push_msg_t *msg) {
  spooled_subscriber_t       *nsub;
  subscriber_t               *sub;
  for(nsub = self->first; nsub != NULL; nsub = nsub->next) {
    sub = nsub->sub;
    sub->respond_message(sub, msg);
  }
  return NGX_OK;
}

static ngx_int_t spool_respond_status(subscriber_pool_t *self, ngx_int_t status_code, const ngx_str_t *status_line) {
  spooled_subscriber_t       *nsub;
  subscriber_t               *sub;
  for(nsub = self->first; nsub != NULL; nsub = nsub->next) {
    sub = nsub->sub;
    sub->respond_status(sub, status_code, status_line);
  }
  return NGX_OK;
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
  spool->add = spool_add;
  spool->respond_message = spool_respond_message;
  spool->respond_status = spool_respond_status;
  
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
  if(COMMAND_SPOOL(self->shortlived, add, sub) != NGX_OK) {
    if(COMMAND_SPOOL(self->persistent, add, sub) != NGX_OK) {
      ERR("couldn't add subscriber to any spool");
    }
  }

  //TODO: callbacks?

  return NGX_OK;
}


static ngx_int_t spooler_respond_message(channel_spooler_t *self, ngx_http_push_msg_t *msg) {
  COMMAND_SPOOL(self->shortlived, respond_message, msg);
  self->shortlived = create_spool(SHORTLIVED);
  COMMAND_SPOOL(self->persistent, respond_message, msg);
  self->responded_count++;
  return NGX_OK;
}

static ngx_int_t spooler_respond_status(channel_spooler_t *self, ngx_int_t code, const ngx_str_t *line) {
  COMMAND_SPOOL(self->shortlived, respond_status, code, line);
  self->shortlived = create_spool(SHORTLIVED);
  COMMAND_SPOOL(self->persistent, respond_status, code, line);
  self->responded_count++;
  return NGX_OK;
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

static void terribly_named_dequeue_handler(subscriber_pool_t *spool, ngx_int_t count, void *privdata) {
  channel_spooler_t *spl = (channel_spooler_t *)privdata;
  if(spool->type == SHORTLIVED) {
    //say we've dequeued everyone right away
    spl->dequeue_handler(spl, count + spool->sub_count, spl->dequeue_handler_privdata);
    //and disable the handler. 
    spl->set_dequeue_handler(spl, spool_sub_empty_callback, NULL);
    //TODO: timeoutable recycling collector
  }
  else {
    //bubble on up, yeah
    spl->dequeue_handler(spl, count, spl->dequeue_handler_privdata);
  }
}

channel_spooler_t *start_spooler(channel_spooler_t *spl) {
  spl->shortlived = create_spool(SHORTLIVED);
  spl->persistent = create_spool(PERSISTENT);
  spl->add = spooler_add_subscriber;
  spl->respond_message = spooler_respond_message;
  spl->respond_status = spooler_respond_status;
  spl->set_dequeue_handler=spooler_set_dequeue_handler;

  COMMAND_SPOOL(spl->shortlived, set_dequeue_handler, terribly_named_dequeue_handler, spl);
  return spl;
}

ngx_int_t stop_spooler(channel_spooler_t *spl) {
  safely_destroy_spool(spl->shortlived);
  safely_destroy_spool(spl->persistent);
  return NGX_OK;
}