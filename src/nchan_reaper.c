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
  rp->rotate = 0;
  rp->max_notready = 0;
  
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
  if(rp->first == NULL) assert(rp->count == 0);
  if (rp->count > 0) {
    ERR("reap %s again later (remaining: %i)", rp->name, rp->count);
    ngx_add_timer(&rp->timer, rp->tick_usec);
  }
  
}

ngx_int_t nchan_reaper_add(nchan_reaper_t *rp, void *thing) {
  if(rp->ready(thing) == NGX_OK) {
    rp->reap(thing);
    return NGX_OK;
  }
  
  void *cur;
  int i=0;
  for(cur = rp->first; cur != NULL; cur = thing_next(rp, cur)) {
    i++;
    assert(cur != thing);
  }
  
  assert(i == rp->count);
  
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
  
  assert(rp->count >= 0 && rp->count < 1000);
  DBG("reap %s %p later (waiting to be reaped: %i)", rp->name, thing, rp->count);
  rp->count++;
  reaper_reset_timer(rp);
  return NGX_OK;
}


//this is not a safe function -- it doesn't check if the thing is actually being reaped.
ngx_int_t nchan_reaper_withdraw(nchan_reaper_t *rp, void *thing) {
  void *prev, *next;
  
  void *cur;
  
  prev = thing_prev(rp, thing);
  next = thing_next(rp, thing);
  
  if(prev) *thing_next_ptr(rp, prev) = next;
  
  if(next) *thing_prev_ptr(rp, next) = prev;
  
  if(rp->last == thing)  rp->last  = prev;
  if(rp->first == thing) rp->first = next;
  
  assert(rp->count > 0);
  rp->count--;
  
  if(rp->last == NULL) {
    assert(rp->count == 0);
  }
  
  //debugging stuff
  *thing_next_ptr(rp, thing) = NULL;
  *thing_prev_ptr(rp, thing) = NULL;
  
  int i=0;
  for(cur = rp->first; cur != NULL; cur = thing_next(rp, cur)) {
    i++;
    assert(cur != thing);
  }
  assert(i == rp->count);
  
  DBG("withdraw %s %p", rp->name, thing);
  return NGX_OK;
}

static ngx_inline void reap_ready_thing(nchan_reaper_t *rp, void *cur, void *next) {
  void   *prev;
  prev = thing_prev(rp, cur);
  if(prev != NULL && next != NULL) {
    assert(prev != next);
  }
  assert(cur != prev);
  assert(cur != next);
  if(prev) *thing_next_ptr(rp, prev) = next;
  if(next) *thing_prev_ptr(rp, next) = prev;
  if(cur == rp->first) rp->first = next;
  if(cur == rp->last)  rp->last = prev;
  rp->reap(cur);
  rp->count--;
  if(rp->first == NULL) {
    assert(rp->count == 0);
  }
  assert(rp->count >= 0 && rp->count < 1000);
  DBG("reaped %s %p (waiting to be reaped: %l)", rp->name, cur, rp->count);
}

static void its_reaping_time(nchan_reaper_t *rp, uint8_t force) {
  void                *cur = rp->first, *next;
  int                  max_notready = rp->max_notready, notready = 0; 
  while(cur != NULL && notready <= max_notready) {
    next = thing_next(rp, cur);
    if(force || rp->ready(cur) == NGX_OK) {
      reap_ready_thing(rp, cur, next);
    }
    else if(max_notready > 0){
      DBG("not ready to reap %s %p", rp->name, cur);
      notready++;
    }
    cur = next;
  } 
}

static void its_reaping_rotating_time(nchan_reaper_t *rp, uint8_t force) {
  void                *cur = rp->first, *next, *prev;
  void                *firstmoved = NULL;
  void               **next_ptr, **prev_ptr;
  int                  max_notready = rp->max_notready, notready = 0; 
  while(cur != NULL && cur != firstmoved && notready <= max_notready) {
    next = thing_next(rp, cur);
    if(force || rp->ready(cur) == NGX_OK) {
      reap_ready_thing(rp, cur, next);
    }
    else {
      //move to the end of the list
      //TODO: bulk move
      if(max_notready > 0) notready++;
        
      prev = thing_prev(rp, cur);
      if(prev) *thing_next_ptr(rp, prev) = next;
      if(next) *thing_prev_ptr(rp, next) = prev;
      if(!firstmoved) firstmoved = cur;
      
      next_ptr = thing_next_ptr(rp, cur);
      prev_ptr = thing_prev_ptr(rp, cur);
      
      if(rp->last) {
        *thing_next_ptr(rp, rp->last) = cur;
      }
      
      *prev_ptr = rp->last;
      *next_ptr = NULL;
      rp->last = cur;
      
      if(rp->first == NULL) {
        rp->first = cur;
      }
      
    }
    cur = next;
  } 
}

static void reaper_timer_handler(ngx_event_t *ev) {
  nchan_reaper_t      *rp = ev->data;
  if(rp->rotate) {
    its_reaping_rotating_time(rp, 0);
  }
  else {
    its_reaping_time(rp, 0);
  }
  reaper_reset_timer(rp);
}