#include <nchan_module.h>
#include <nchan_reaper.h>

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "REAPER: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "REAPER: " fmt, ##args)

static void reaper_timer_handler(ngx_event_t *ev);

ngx_int_t nchan_reaper_start(nchan_reaper_t *rp, char *name, int prev, int next, ngx_int_t (*ready)(void *), void (*reap)(void *), int tick_sec) {
  rp->name = name;
  rp->count = 0;
  rp->next_ptr_offset = next;
  rp->prev_ptr_offset = prev;
  rp->last = NULL;
  rp->first = NULL;
  rp->ready = ready;
  rp->reap = reap;
  ngx_memzero(&rp->timer, sizeof(rp->timer));
  rp->timer.handler = reaper_timer_handler;
  rp->timer.log = ngx_cycle->log;
  rp->timer.data = rp;
  rp->tick_usec = tick_sec * 1000;
  
  DBG("start reaper %s with tick time of %i sec", name, tick_sec);
  
  return NGX_OK;
}

static void its_reaping_time(nchan_reaper_t *rp, uint8_t force);

ngx_int_t nchan_reaper_stop(nchan_reaper_t *rp) {
  its_reaping_time(rp, 1);
  if(rp->timer.timer_set) {
    ngx_del_timer(&rp->timer);
  }
  DBG("stopped reaper %s", rp->name);
  return NGX_OK;
}

static ngx_inline void **thing_next_ptr(nchan_reaper_t *rp, void *thing) {
  return thing + rp->next_ptr_offset;
}
static ngx_inline void *thing_next(nchan_reaper_t *rp, void *thing) {
  return *thing_next_ptr(rp, thing);
}

static ngx_inline void **thing_prev_ptr(nchan_reaper_t *rp, void *thing) {
  return thing + rp->prev_ptr_offset;
}
static ngx_inline void *thing_prev(nchan_reaper_t *rp, void *thing) {
  return *thing_prev_ptr(rp, thing);
}


static ngx_inline void reaper_reset_timer(nchan_reaper_t *rp) {
  if (!(ngx_quit || ngx_terminate || ngx_exiting || rp->count == 0)) {
    ngx_add_timer(&rp->timer, rp->tick_usec);
  }
}

ngx_int_t nchan_reaper_add(nchan_reaper_t *rp, void *thing) {
  if(rp->ready(thing) == NGX_OK) {
    rp->reap(thing);
    return NGX_OK;
  }
  
  void    **next = thing_next_ptr(rp, thing);
  void    **prev = thing_prev_ptr(rp, thing);
  
  if(rp->last) {
    *thing_next_ptr(rp, rp->last) = thing;
  }
  *prev = rp->last;
  *next = NULL;
  rp->last = thing;
  
  if(rp->first == NULL) {
    rp->first = thing;
  }
  
  DBG("reap %p later (waiting to be reaped: %i)", thing, rp->count);
  rp->count++;
  reaper_reset_timer(rp);
  return NGX_OK;
}

static void its_reaping_time(nchan_reaper_t *rp, uint8_t force) {
  void                *cur = rp->first, *next, *prev;
  void                *first = rp->first, *last = rp->last;
  while(cur != NULL) {
    next = thing_next(rp, cur);
    if(force || rp->ready(cur) == NGX_OK) {
      prev = thing_prev(rp, cur);
      rp->reap(cur);
      if(prev) *thing_next_ptr(rp, prev) = next;
      if(next) *thing_prev_ptr(rp, next) = prev;
      if(cur == first) rp->first = NULL;
      if(cur == last) rp->last = NULL;
      rp->count--;
      DBG("reaped %p (waiting to be reaped: %i)", cur, rp->count);
    }
    cur = next;
  } 
}

static void reaper_timer_handler(ngx_event_t *ev) {
  nchan_reaper_t      *rp = ev->data;
  its_reaping_time(rp, 0);
  reaper_reset_timer(rp);
}