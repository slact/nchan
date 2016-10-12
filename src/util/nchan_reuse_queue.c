#include <nchan_module.h>
#include "nchan_reuse_queue.h"
#include <assert.h>
#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "REUSEQUEUE:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REUSEQUEUE:" fmt, ##arg)

ngx_int_t nchan_reuse_queue_init(nchan_reuse_queue_t *rq, int prev, int next, void *(*alloc)(void *), ngx_int_t (*free)(void *, void*), void *privdata) {
  rq->size = 0;
  rq->prev_ptr_offset = prev;
    rq->next_ptr_offset = next;
  rq->first = NULL;
  rq->last = NULL;
  rq->reserve = NULL;
  rq->alloc = alloc;
  rq->free = free;
  rq->pd = privdata;
  
  return NGX_OK;
}

static ngx_inline void **thing_next_ptr(nchan_reuse_queue_t *rq, void *thing) {
  return (void *)((char *)thing + rq->next_ptr_offset);
}
static ngx_inline void *thing_next(nchan_reuse_queue_t *rq, void *thing) {
  return *thing_next_ptr(rq, thing);
}

static ngx_inline void **thing_prev_ptr(nchan_reuse_queue_t *rq, void *thing) {
  return (void *)((char *)thing + rq->prev_ptr_offset);
}

/*static ngx_inline void *thing_prev(nchan_reuse_queue_t *rq, void *thing) {
  return *thing_prev_ptr(rq, thing);
}


static void check_queue(nchan_reuse_queue_t *rq) {
  void *cur;
  for(cur = rq->first; cur != NULL; cur = thing_next(rq, cur)) {
    if(thing_prev_ptr(rq, rq->first)) {
      assert(thing_prev(rq, rq->first) != rq->first);
    }
    if(thing_next_ptr(rq, rq->first)) {
      assert(thing_next(rq, rq->first) != rq->first);
    }
  }
}
*/

ngx_int_t nchan_reuse_queue_flush(nchan_reuse_queue_t *rq) {
  void *pd = rq->pd;
  void *thing, *next;
  ngx_int_t     i=0;
  next = rq->first;
  while((thing = next) != NULL) {
    i++;
    next = thing_next(rq, thing);
    if(rq->free) rq->free(pd, thing);
  }
  rq->reserve = rq->first;
  rq->first = NULL;
  rq->last = NULL;
  return i;
}

ngx_int_t nchan_reuse_queue_shutdown(nchan_reuse_queue_t *rq) {
  if(rq->free) {
    void *pd = rq->pd;
    void *thing, *next;
    next = rq->first;
    while((thing = next) != NULL) {
      next = thing_next(rq, thing);
      rq->free(pd, thing);
    }
    next = rq->reserve;
    while((thing = next) != NULL) {
      next = thing_next(rq, thing);
      rq->free(pd, thing);
    }
  }
  
  if(rq->last) {
    *thing_next_ptr(rq, rq->last) = rq->reserve;
  }
  rq->first = NULL;
  rq->last = NULL;
  
  if(rq->first) {
    rq->reserve = rq->first;
  }
  return NGX_OK;
}

void *nchan_reuse_queue_push(nchan_reuse_queue_t *rq) {
  void     *q;
  //check_queue(rq);
  if(rq->reserve) {
    q = rq->reserve;
    rq->reserve = thing_next(rq, q);
  }
  else {
    q = rq->alloc(rq->pd);
    rq->size++;
  }
  *thing_next_ptr(rq, q) = NULL;
  *thing_prev_ptr(rq, q) = rq->last;
  if(rq->last) {
    *thing_next_ptr(rq, rq->last) = q;
  }
  rq->last = q;
  if(!rq->first) {
    rq->first = q;
  }
  //check_queue(rq);
  return q;
}



void *nchan_reuse_queue_first(nchan_reuse_queue_t *rq) {
  return rq->first;
}

void nchan_reuse_queue_each(nchan_reuse_queue_t *rq, void (*callback)(void *)) {
  void      *cur;
  for(cur = rq->first; cur != NULL; cur = thing_next(rq, cur)) {
    callback(cur);
  }
}

ngx_int_t nchan_reuse_queue_pop(nchan_reuse_queue_t *rq) {
  void      *q = rq->first;
  //check_queue(rq);
  if(q) {
    rq->first = thing_next(rq, q);
    if(rq->first) {
      *thing_prev_ptr(rq, rq->first) = NULL;
    }
    if(rq->last == q) {
      rq->last = NULL;
    }
    *thing_prev_ptr(rq, q) = NULL;
    *thing_next_ptr(rq, q) = rq->reserve;
    rq->reserve = q;
  }
  //check_queue(rq);
  return NGX_OK;
}
