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
  ngx_str_t            id;
  nchan_llist_timed_t  ll;
  UT_hash_handle       hh;
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

/*
static ngx_int_t walk_tc(nchan_thing_cache_t *tc) {
  nchan_llist_timed_t    *cur;
  int                     head_to_tail = 0, tail_to_head = 0;
  for(cur = tc->thing_head; cur != NULL; cur = cur->next) {
    head_to_tail++;
  }
  for(cur = tc->thing_tail; cur != NULL; cur = cur->prev) {
    tail_to_head++;
  }
  
  assert(head_to_tail == tail_to_head);
  return tail_to_head;
}

static void verify_tc(nchan_thing_cache_t *tc) {
  thing_t                *thing, *ttmp;
  int                    hash = 0;
  HASH_ITER(hh, (tc->things), thing, ttmp) {  
    hash++;
  }
  assert(walk_tc(tc) == hash);
}
*/

static void enqueue_llist_thing(nchan_thing_cache_t *tc, nchan_llist_timed_t *cur) {
  //verify_tc(tc);
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
  //walk_tc(tc);
}

static void nchan_thingcache_gc_timer_handler(ngx_event_t *ev) {
  nchan_llist_timed_t    *cur, *next;
  nchan_thing_cache_t    *tc = (nchan_thing_cache_t *)ev->data;
  time_t                  time = ngx_time();
  thing_t                *thing;
  cur = tc->thing_head;
  while(cur != NULL) {
    //verify_tc(tc);
    next = cur->next;
    if(cur->time <= time) {
      if(cur->prev) {
        cur->prev->next = next;
      }
      if(next) {
        next->prev = cur->prev;
      }
      thing = container_of(cur, thing_t, ll);
      if(!tc->destroy(&thing->id, thing->ll.data)) {
        //time extended!
        enqueue_llist_thing(tc, cur);
      }
      else {
        if(tc->thing_head == cur) {
          tc->thing_head = cur->next;
        }
        if(tc->thing_tail == cur) {
          tc->thing_tail = cur->prev;
        }
        THING_HASH_DEL(tc, thing);
        //verify_tc(tc);
        ngx_free(thing);
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
  //verify_tc(tc);
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
  nchan_init_timer(&tc->gc_timer, nchan_thingcache_gc_timer_handler, tc);
  return (void *)tc;
}

ngx_int_t nchan_thingcache_shutdown(void *tcv) {
  nchan_thing_cache_t   *tc = (nchan_thing_cache_t *)tcv;
  nchan_llist_timed_t   *cur = tc->thing_head;
  thing_t               *thing;
  nchan_llist_timed_t   *next; 
  DBG("shutdown %s %p", tc->name, tc);
  
  while(cur != NULL) {
    thing = container_of(cur, thing_t, ll);
    next = cur->next;
    tc->destroy(&thing->id, cur->data);
    THING_HASH_DEL(tc, thing);
    ngx_free(thing);
    cur = next;
  }
  
  if(tc->gc_timer.timer_set) {
    ngx_del_timer(&tc->gc_timer);
  }
  
  ngx_free(tc);
  return NGX_OK;
}

void *nchan_thingcache_find(void *tcv, ngx_str_t *id) {
  nchan_thing_cache_t   *tc = (nchan_thing_cache_t *)tcv;
  thing_t               *thing;
  
  //verify_tc(tc);
  THING_HASH_FIND(tc, id, thing);
  return thing ? thing->ll.data : NULL;
}

void *nchan_thingcache_get(void *tcv, ngx_str_t *id) {
  nchan_thing_cache_t   *tc = (nchan_thing_cache_t *)tcv;
  nchan_llist_timed_t   *cur;
  thing_t               *thing;
  
  //verify_tc(tc);
  
  THING_HASH_FIND(tc, id, thing);
  if(thing == NULL) {
    DBG("not found in %s %p", tc->name, tc);
    
    if((thing = ngx_alloc(sizeof(*thing) + id->len, ngx_cycle->log)) == NULL) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan thingcache %p: can't allocate memory for thing with id %V", tc, id);
      return NULL;
    }
    cur = &thing->ll;
    cur->data = thing;
    thing->id.len = id->len;
    thing->id.data = (u_char *)(&thing[1]);
    ngx_memcpy(thing->id.data, id->data, id->len);
    
    thing->ll.data = tc->create(id);
    
    enqueue_llist_thing(tc, cur);
    THING_HASH_ADD(tc, thing);
  }
  
  //verify_tc(tc);
  
  return thing->ll.data;
}
