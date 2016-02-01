#include <nchan_module.h>
#include <uthash.h>
#define THING_HASH_FIND(tc, id_str, p)    HASH_FIND( hh, tc->things, (id_str)->data, (id_str)->len, p)
#define THING_HASH_ADD(tc, thing)      HASH_ADD_KEYPTR( hh, tc->things, (thing->id).data, (thing->id).len, thing)
#define THING_HASH_DEL(tc, thing)      HASH_DEL(tc->things, thing)

#undef uthash_malloc
#undef uthash_free
#define uthash_malloc(sz) ngx_alloc(sz, ngx_cycle->log)
#define uthash_free(ptr,sz) ngx_free(ptr)

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "THINGCACHE: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "THINGCACHE: " fmt, ##args)

typedef struct {
  ngx_str_t          id;
  void              *data;
  UT_hash_handle     hh;
} thing_t;

typedef struct {
  void *               (*create)(ngx_str_t *id);
  ngx_int_t            (*destroy)(ngx_str_t *id, void *);
  char                  *name;
  ngx_uint_t             ttl;
  thing_t               *things;
  nchan_llist_timed_t   *thing_head;
  nchan_llist_timed_t   *thing_tail;
  ngx_event_t            gc_timer;
} nchan_thing_cache_t;


static void enqueue_llist_thing(nchan_thing_cache_t *tc, nchan_llist_timed_t *cur) {
  if(tc->thing_head == NULL) {
    cur->prev = NULL;
    tc->thing_head = cur;
  }
  if(tc->thing_tail) {
    tc->thing_tail->next = cur;
  }
  cur->prev = tc->thing_tail;
  cur->next = NULL;
  cur->time = ngx_time() + tc->ttl;
  tc->thing_tail = cur;
  
  //start the clock
  if(! tc->gc_timer.timer_set) {
    ngx_add_timer(&tc->gc_timer, tc->ttl * 1000);
  }
}

static void nchan_thingcache_gc_timer_handler(ngx_event_t *ev) {
  nchan_llist_timed_t    *cur, *next;
  nchan_thing_cache_t    *tc = (nchan_thing_cache_t *)ev->data;
  time_t                 time = ngx_time();
  cur = tc->thing_head;
  DBG("timer for %s %p", tc->name, tc);
  while(cur != NULL) {
    next = cur->next;
    if(cur->time <= time) {
      if(cur->prev) {
        cur->prev->next = next;
      }
      if(next) {
        next->prev = cur->prev;
      }
      if(!tc->destroy(&((thing_t *)cur->data)->id, ((thing_t *)cur->data)->data)) {
        //time extended!
        enqueue_llist_thing(tc, cur);
      }
      else {
        if(tc->thing_head == cur) {
          tc->thing_head = cur->next;
        }
        if(tc->thing_tail) {
          tc->thing_tail = cur->prev;
        }
        THING_HASH_DEL(tc, (thing_t *)cur->data);
        ngx_free(cur);
      }
    }
    else {
      break;
    }
    cur = next;
  }
  
  if(tc->thing_head != NULL) {
    ngx_add_timer(&tc->gc_timer, tc->ttl * 1000);
  }
}

void *nchan_thingcache_init(char *name, void *(*create)(ngx_str_t *), ngx_int_t(*destroy)(ngx_str_t *, void *), ngx_uint_t ttl) {
  nchan_thing_cache_t *tc;
  if((tc = ngx_alloc(sizeof(*tc), ngx_cycle->log)) == NULL) {
    return NULL;
  }
  DBG("init %s %p", name, tc);
  
  tc->name = name;
  tc->create = create;
  tc->destroy = destroy;
  tc->ttl = ttl;
  tc->thing_head = NULL;
  tc->thing_tail = NULL;
  tc->things = NULL;
  ngx_memzero(&tc->gc_timer, sizeof(tc->gc_timer));
  tc->gc_timer.cancelable = 1;
  tc->gc_timer.handler = nchan_thingcache_gc_timer_handler;
  tc->gc_timer.data = tc;
  tc->gc_timer.log=ngx_cycle->log;
  return (void *)tc;
}

ngx_int_t nchan_thingcache_shutdown(void *tcv) {
  nchan_thing_cache_t   *tc = (nchan_thing_cache_t *)tcv;
  nchan_llist_timed_t   *cur = tc->thing_head;
  nchan_llist_timed_t   *next; 
  DBG("shutdown %s %p", tc->name, tc);
  
  while(cur != NULL) {
    next = cur->next;
    tc->destroy(&((thing_t *)cur->data)->id, ((thing_t *)cur->data)->data);
    THING_HASH_DEL(tc, (thing_t *)cur->data);
    ngx_free(cur);
    cur = next;
  }
  
  if(tc->gc_timer.timer_set) {
    ngx_del_timer(&tc->gc_timer);
  }
  
  ngx_free(tc);
  return NGX_OK;
}

typedef struct {
  nchan_llist_timed_t  ll;
  thing_t              t;
} whole_thing_t;


void *nchan_thingcache_find(void *tcv, ngx_str_t *id) {
  nchan_thing_cache_t   *tc = (nchan_thing_cache_t *)tcv;
  thing_t               *thing;
  
  THING_HASH_FIND(tc, id, thing);
  return thing ? thing->data : NULL;
}

void *nchan_thingcache_get(void *tcv, ngx_str_t *id) {
  nchan_thing_cache_t   *tc = (nchan_thing_cache_t *)tcv;
  nchan_llist_timed_t   *cur;
  thing_t               *thing;
  
  THING_HASH_FIND(tc, id, thing);
  if(thing == NULL) {
    whole_thing_t   *whole;
    
    DBG("not found in %s %p", tc->name, tc);
    
    if((whole = ngx_alloc(sizeof(*whole) + id->len, ngx_cycle->log)) == NULL) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan thingcache %p: can't allocate memory for thing with id %V", tc, id);
      return NULL;
    }
    cur = &whole->ll;
    thing = &whole->t;
    cur->data = (void *) thing;
    thing->id.len = id->len;
    thing->id.data = (u_char *)(&whole[1]);
    ngx_memcpy(thing->id.data, id->data, id->len);
    
    thing->data = tc->create(id);
    
    enqueue_llist_thing(tc, cur);
    THING_HASH_ADD(tc, thing);
  }
  else {
    DBG("thing %p found in %s %p", thing->data, tc->name, tc);
  }
  
  return thing->data;
}