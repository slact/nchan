#include <ngx_http_push_module.h>

#include <assert.h>
#include "store.h"
#include <store/ngx_rwlock.h>
#include "rbtree_util.h"
#include "shmem.h"
#include "ipc.h"
#include <pthread.h>

typedef struct nhpm_channel_head_s nhpm_channel_head_t;
typedef struct nhpm_channel_head_cleanup_s nhpm_channel_head_cleanup_t;
typedef struct nhpm_subscriber_cleanup_s nhpm_subscriber_cleanup_t;
typedef struct nhpm_subscriber_s nhpm_subscriber_t;
typedef struct nhpm_message_s nhpm_message_t;

struct nhpm_subscriber_cleanup_s {
  nhpm_channel_head_cleanup_t  *shared;
  nhpm_subscriber_t            *sub;
}; //nhpm_subscriber_cleanup_t

struct nhpm_subscriber_s {
  ngx_uint_t                  id;
  subscriber_t                *subscriber;
  ngx_event_t                 ev;
  ngx_pool_t                 *pool;
  struct nhpm_subscriber_s   *prev;
  struct nhpm_subscriber_s   *next;
  ngx_http_cleanup_t         *r_cln;
  nhpm_subscriber_cleanup_t   clndata;
};

typedef enum {INACTIVE, NOTREADY, READY} chanhead_pubsub_status_t;

struct nhpm_message_s {
  ngx_http_push_msg_t       msg;
  nhpm_message_t           *prev;
  nhpm_message_t           *next;
}; //nhpm_message_t

typedef struct {
  nhpm_channel_head_cleanup_t *shared_cleanup;
  nhpm_subscriber_t           *sub;
  ngx_atomic_t                 sub_count;
  ngx_atomic_uint_t            pid;
  ngx_pool_t                  *pool;
} nhpm_worker_chanhead_data_t;

struct nhpm_channel_head_s {
  pthread_rwlock_t               rwl;
  ngx_str_t                      id; //channel id
  ngx_http_push_channel_t        channel;
  ngx_atomic_t                   generation; //subscriber pool generation.
  chanhead_pubsub_status_t       status;
  ngx_atomic_t                   sub_count;
  ngx_uint_t                     min_messages;
  ngx_uint_t                     max_messages;
  nhpm_message_t                *msg_first;
  nhpm_message_t                *msg_last;
  nhpm_worker_chanhead_data_t   *worker;
  nhpm_llist_timed_t             cleanlink;
  UT_hash_handle                 hh;
};

struct nhpm_channel_head_cleanup_s {
  nhpm_channel_head_t        *head;
  ngx_str_t                   id; //channel id
  ngx_uint_t                  sub_count;
  ngx_pool_t                 *pool;
};

static ngx_int_t max_workers = 0;


static ipc_t *ipc;

#define CHANNEL_HASH_FIND(id_buf, p)    HASH_FIND( hh, shdata->subhash, (id_buf)->data, (id_buf)->len, p)
#define CHANNEL_HASH_ADD(chanhead)      HASH_ADD_KEYPTR( hh, shdata->subhash, (chanhead->id).data, (chanhead->id).len, chanhead)
#define CHANNEL_HASH_DEL(chanhead)      HASH_DEL( shdata->subhash, chanhead)

#undef uthash_malloc
#undef uthash_free
#define uthash_malloc(sz) shm_alloc(shm, sz, "uthash")
#define uthash_free(ptr,sz) shm_free(shm, ptr)

#define STR(buf) (buf)->data, (buf)->len
#define BUF(buf) (buf)->pos, ((buf)->last - (buf)->pos)

#define NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_POOL_SIZE (5 * 1024)
#define NGX_HTTP_PUSH_DEFAULT_CHANHEAD_CLEANUP_INTERVAL 1000
#define NGX_HTTP_PUSH_CHANHEAD_EXPIRE_SEC 1

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, __VA_ARGS__)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)

#define PUBLISH_STATUS 1
#define PUBLISH_MESSAGE 2

#define DEBUG_RWLOCKS 0

static void rwl_init(pthread_rwlock_t *lock, const char *dbg) {
  #if (DEBUG_RWLOCKS == 1)
  ERR("rwl %p INIT      (%s)", lock, dbg);
  #endif
  pthread_rwlock_init(lock, NULL);
}
static void rwl_rdlock(pthread_rwlock_t *lock, const char *dbg) {
  #if (DEBUG_RWLOCKS == 1)
  ERR("rwl %p READLOCK  (%s)", lock, dbg);
  #endif
  pthread_rwlock_rdlock(lock);
}
static void rwl_wrlock(pthread_rwlock_t *lock, const char *dbg) {
  #if (DEBUG_RWLOCKS == 1)
  ERR("rwl %p WRITELOCK (%s)", lock, dbg);
  #endif
  pthread_rwlock_wrlock(lock);
}
static void rwl_unlock(pthread_rwlock_t *lock, const char *dbg) {
  #if (DEBUG_RWLOCKS == 1)
  ERR("rwl %p UNLOCK    (%s)", lock, dbg);
  #endif
  pthread_rwlock_unlock(lock);
}
static ngx_int_t chanhead_gc_add(nhpm_channel_head_t *head);
static ngx_int_t chanhead_gc_withdraw(nhpm_channel_head_t *chanhead);

static ngx_int_t chanhead_messages_gc(nhpm_channel_head_t *ch);

static void ngx_http_push_store_chanhead_gc_timer_handler(ngx_event_t *);
static ngx_int_t ngx_http_push_store_publish_locally(nhpm_channel_head_t *, ngx_http_push_msg_t *, ngx_int_t, const ngx_str_t *);

typedef struct {
  nhpm_channel_head_t *subhash;
  pthread_rwlock_t     hash_lock;

  
  pthread_rwlock_t     gc_lock;
  ngx_shmtx_t          gc_mutex;
  ngx_shmtx_sh_t       gc_mutex_lock;
  ngx_event_t         *chanhead_gc_timer;
  nhpm_llist_timed_t  *chanhead_gc_head;
  nhpm_llist_timed_t  *chanhead_gc_tail;
} shm_data_t;

shmem_t *shm = NULL;
shm_data_t *shdata = NULL;

static ngx_int_t initialize_shm(ngx_shm_zone_t *zone, void *data) {
  shm_data_t     *d;

  if(data) { //zone being passed after restart
    zone->data = data;
    return NGX_OK;
  }

  if((d = shm_alloc(shm, sizeof(shm_data_t), "root shared data")) == NULL) {
    return NGX_ERROR;
  }
  zone->data = d;
  shdata = d;
  
  shm_init(shm);
  
  d->subhash = NULL;
  d->chanhead_gc_timer = NULL;
  d->chanhead_gc_head = NULL;
  d->chanhead_gc_tail = NULL;
  
  //initialize locks
  rwl_init(&d->hash_lock, "hash");
  rwl_init(&d->gc_lock, "gc");
  
  ngx_shmtx_create(&d->gc_mutex, &d->gc_mutex_lock, (u_char *)"garbage collector running lock");
  
  return NGX_OK;
}

static ngx_int_t ngx_http_push_store_init_worker(ngx_cycle_t *cycle) {
  ngx_core_conf_t    *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  max_workers = ccf->worker_processes; //quite important for other logic
  ngx_event_t  *t;
  rwl_wrlock(&shdata->gc_lock, "gc");
  if(shdata->chanhead_gc_timer == NULL) {
    if((t = shm_alloc(shm, sizeof(*t), "chanhead cleanup timer")) == NULL) {
      return NGX_ERROR;
    }
    shdata->chanhead_gc_timer = t;
    t->handler=&ngx_http_push_store_chanhead_gc_timer_handler;
    t->log=ngx_cycle->log;
  }
  rwl_unlock(&shdata->gc_lock, "gc");

  ipc_start(ipc, cycle);
  
  DBG("init memstore worker pid:%i slot:%i max workers:%i", ngx_pid, ngx_process_slot, max_workers);
  return NGX_OK;
}


static nhpm_worker_chanhead_data_t *chanhead_worker_data(nhpm_channel_head_t *head) {
  nhpm_worker_chanhead_data_t *data;
  rwl_rdlock(&head->rwl, "get worker data");
  data = &head->worker[ngx_process_slot];
  rwl_unlock(&head->rwl, "get worker data");
  return data;
}

static ngx_int_t nhpm_subscriber_register(nhpm_channel_head_t *chanhead, nhpm_subscriber_t *sub) {
  nhpm_worker_chanhead_data_t *data = chanhead_worker_data(chanhead);
  
  data->sub_count++; //atomic
  chanhead->sub_count++; //atomic
  chanhead->channel.subscribers++; //also atomic
  data->sub = sub;
  
  return NGX_OK;
}

static ngx_int_t nhpm_subscriber_unregister(nhpm_channel_head_t *chanhead, nhpm_subscriber_t *sub) {
  nhpm_worker_chanhead_data_t *data = chanhead_worker_data(chanhead);
  chanhead->sub_count--; //atomic
  chanhead->channel.subscribers--; //also atomic
  data->sub_count--; //also atomic
  if(data->sub == sub) { //head subscriber
    data->sub = NULL;
  }
  return NGX_OK;
}

static ngx_int_t ensure_chanhead_is_ready(nhpm_channel_head_t *head) {
  nhpm_channel_head_cleanup_t   *hcln;
  chanhead_pubsub_status_t       status;
  nhpm_worker_chanhead_data_t   *data;
  
  if(head == NULL) {
    return NGX_OK;
  }
  rwl_rdlock(&head->rwl, "chanhead is ready?");
  status = head->status;
  data = &head->worker[ngx_process_slot];
  rwl_unlock(&head->rwl, "chanhead is ready?");
  
  //do we have everything ready for this worker?
  if(data->pid == 0) {
    data->pid = ngx_pid;
  }
  else if (data->pid != ngx_pid) {
    ERR("Got some chanhead data that used to belong to process %i. taking over.");
    data->pid = ngx_pid;
  }
  
  if(data->pool == NULL) {
    if((data->pool = ngx_create_pool(NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_POOL_SIZE, ngx_cycle->log))==NULL) {
      ERR("can't allocate memory for channel subscriber pool");
    }
  }
  if(data->shared_cleanup == NULL) {
    if((hcln=ngx_pcalloc(data->pool, sizeof(*hcln)))==NULL) {
      ERR("can't allocate memory for channel head cleanup");
    }
    data->shared_cleanup = hcln;
  }
  
  if(status == INACTIVE) {
    rwl_wrlock(&head->rwl, "set chanhead stuff");
    if (head->status == INACTIVE) { //recycled chanhead
      chanhead_gc_withdraw(head);
      head->status = READY;
    }
    rwl_unlock(&head->rwl, "set chanhead stuff");
  }
  return NGX_OK;
}


static nhpm_channel_head_t * chanhead_memstore_find(ngx_str_t *channel_id) {
  nhpm_channel_head_t     *head;
  rwl_rdlock(&shdata->hash_lock, "chanhead find");
  CHANNEL_HASH_FIND(channel_id, head);
  rwl_unlock(&shdata->hash_lock, "chanhead find");
  return head;
}

static nhpm_channel_head_t *chanhead_memstore_create(ngx_str_t *channel_id) {
  nhpm_channel_head_t         *head;
  nhpm_worker_chanhead_data_t *data;
  ngx_int_t                    num_processes = max_workers;
  head=shm_alloc(shm, sizeof(*head) + sizeof(*data)*num_processes +  sizeof(u_char)*(channel_id->len), "chanhead");
  if(head == NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "can't allocate memory for (new) channel subscriber head");
    return NULL;
  }
  
  //no lock needed, no one else knows about this chanhead yet.
  head->worker = (nhpm_worker_chanhead_data_t *)&head[1];
  head->id.len = channel_id->len;
  head->id.data = (u_char *)&head->worker[num_processes];
  ngx_memcpy(head->id.data, channel_id->data, channel_id->len);
  head->sub_count=0;
  head->status = NOTREADY;
  head->msg_last = NULL;
  head->msg_first = NULL;
  ngx_memzero(head->worker, sizeof(*data)*num_processes);
  //set channel
  ngx_memcpy(&head->channel.id, &head->id, sizeof(ngx_str_t));
  head->channel.message_queue=NULL;
  head->channel.messages = 0;
  head->channel.subscribers = 0;
  head->channel.last_seen = ngx_time();
  head->min_messages = 0;
  head->max_messages = (ngx_int_t) -1;
  rwl_init(&head->rwl, "channel");
  
  rwl_wrlock(&shdata->hash_lock, "hash add");
  CHANNEL_HASH_ADD(head);
  rwl_unlock(&shdata->hash_lock, "hash add");
  
  return head;
}

static nhpm_channel_head_t * ngx_http_push_store_find_chanhead(ngx_str_t *channel_id) {
  nhpm_channel_head_t     *head;
  if((head = chanhead_memstore_find(channel_id)) != NULL) {
    ensure_chanhead_is_ready(head);
  }
  return head;
}

static nhpm_channel_head_t * ngx_http_push_store_get_chanhead(ngx_str_t *channel_id) {
  nhpm_channel_head_t          *head;
  head = chanhead_memstore_find(channel_id);
  if(head==NULL) {
    head = chanhead_memstore_create(channel_id);
  }
  ensure_chanhead_is_ready(head);
  return head;
}

static ngx_int_t nhpm_subscriber_remove(nhpm_channel_head_t *head, nhpm_subscriber_t *sub) {
  nhpm_worker_chanhead_data_t   *data;
  //remove subscriber from list
  if(sub->prev != NULL) {
    sub->prev->next=sub->next;
  }
  if(sub->next != NULL) {
    sub->next->prev=sub->prev;
  }
  
  sub->next = sub->prev = NULL;
  
  if(sub->ev.timer_set) {
    ngx_del_timer(&sub->ev);
  }
  if(head != NULL) {
    data = chanhead_worker_data(head);
    if(data->sub == sub) {
      data->sub = NULL;
    }
  }
  return NGX_OK;
}

static void subscriber_publishing_cleanup_callback(nhpm_subscriber_cleanup_t *cln) {
  nhpm_subscriber_t            *sub = cln->sub;
  nhpm_channel_head_cleanup_t  *shared = cln->shared;
  ngx_int_t                     i_am_the_last;
  
  i_am_the_last = sub->prev==NULL && sub->next==NULL;
  
  nhpm_subscriber_remove(shared->head, sub);
  
  if(i_am_the_last) {
    //release pool
    DBG("I am the last subscriber.");
    assert(shared->sub_count != 0);
    ngx_destroy_pool(shared->pool);
  }
}

static ngx_int_t chanhead_gc_add(nhpm_channel_head_t *head) {
  nhpm_llist_timed_t         *chanhead_cleanlink;
  chanhead_pubsub_status_t    status;
  unsigned                    gc_timer_set;
  
  rwl_rdlock(&head->rwl, "chanhead gc_add");
  DBG("gc_add chanhead %p (%V)", head, &head->id);
  status = head->status;
  chanhead_cleanlink = &head->cleanlink;
  rwl_unlock(&head->rwl, "chanhead gc_add");
  if(status != INACTIVE) {
    chanhead_cleanlink->data=(void *)head;
    chanhead_cleanlink->time=ngx_time();
    chanhead_cleanlink->prev=shdata->chanhead_gc_tail;
    if(shdata->chanhead_gc_tail != NULL) {
      shdata->chanhead_gc_tail->next=chanhead_cleanlink;
    }
    chanhead_cleanlink->next=NULL;
    shdata->chanhead_gc_tail=chanhead_cleanlink;
    if(shdata->chanhead_gc_head==NULL) {
      shdata->chanhead_gc_head = chanhead_cleanlink;
    }
    rwl_wrlock(&head->rwl, "gc_add set head status");
    if(head->status != INACTIVE) {
      head->status = INACTIVE;
    }
    else {
      ERR("someone beat me to it");
    }
    rwl_unlock(&head->rwl, "gc_add set head status");
  }
  else {
    ERR("gc_add chanhead %V: already added", &head->id);
  }
  

  //initialize gc timer
  rwl_rdlock(&shdata->gc_lock, "check gc timer");
  gc_timer_set = shdata->chanhead_gc_timer->timer_set;
  rwl_unlock(&shdata->gc_lock, "check gc timer");
  if(! gc_timer_set) {
    rwl_wrlock(&shdata->gc_lock, "start gc timer");
    if(! shdata->chanhead_gc_timer->timer_set) {
      shdata->chanhead_gc_timer->data=shdata->chanhead_gc_head; //don't really care whre this points, so long as it's not null (for some debugging)
      ngx_add_timer(shdata->chanhead_gc_timer, NGX_HTTP_PUSH_DEFAULT_CHANHEAD_CLEANUP_INTERVAL);
    }
    else {
      ERR("someone else did it");
    }
    rwl_unlock(&shdata->gc_lock, "start gc timer");
  }

  return NGX_OK;
}

static ngx_int_t chanhead_gc_withdraw(nhpm_channel_head_t *chanhead) {
  //remove from gc list if we're there
  nhpm_llist_timed_t    *cl;
  nhpm_llist_timed_t   *gc_head, *gc_tail;
  DBG("gc_withdraw chanhead %V", &chanhead->id);
  
  if(chanhead->status == INACTIVE) {
    cl=&chanhead->cleanlink;
    if(cl->prev!=NULL)
      cl->prev->next=cl->next;
    if(cl->next!=NULL)
      cl->next->prev=cl->prev;
    
    rwl_rdlock(&shdata->gc_lock, "gc withdraw check");
    gc_head = shdata->chanhead_gc_head;
    gc_tail = shdata->chanhead_gc_tail;
    rwl_unlock(&shdata->gc_lock, "gc withdraw check");
    if(gc_head == cl || gc_tail==cl) {
      rwl_wrlock(&shdata->gc_lock, "gc withdraw set");
      if(shdata->chanhead_gc_head==cl) {
        shdata->chanhead_gc_head=cl->next;
      }
      else {
        ERR("Someone beat us to setting chanhead_gc_head");
      }
      if(shdata->chanhead_gc_tail==cl) {
        shdata->chanhead_gc_tail=cl->prev;
      }
      else {
        ERR("Someone beat us to setting chanhead_gc_tail");
      }
      rwl_unlock(&shdata->gc_lock, "gc withdraw set");
    }
    cl->prev = cl->next = NULL;
  }
  else {
    DBG("gc_withdraw chanhead %p (%V), but already inactive", chanhead, &chanhead->id);
  }
  
  return NGX_OK;
}


static ngx_str_t *msg_to_str(ngx_http_push_msg_t *msg) {
  static ngx_str_t str;
  ngx_buf_t *buf = msg->buf;
  if(ngx_buf_in_memory(buf)) {
    str.data = buf->start;
    str.len = buf->end - buf->start;
  }
  else {
    str.data= (u_char *)"{not in memory}";
    str.len =  15;
  }
  return &str;
}

static ngx_str_t *chanhead_msg_to_str(nhpm_message_t *msg) {
  static ngx_str_t str;
  if (msg == NULL) {
    str.data=(u_char *)"{NULL}";
    str.len = 6;
    return &str;
  }
  else {
    return msg_to_str(&msg->msg);
  }
}

static void ipc_alert_handler(ngx_uint_t code, void *data[]) {
  nhpm_channel_head_t *head = NULL;
  ngx_http_push_msg_t *msg = NULL;
  ngx_int_t            status_code = NGX_ERROR;
  const ngx_str_t     *status_line = NULL;
  
  switch(code) {
    case PUBLISH_MESSAGE:
      head = (nhpm_channel_head_t *)data[0];
      msg = (ngx_http_push_msg_t *)data[1];
      break;
    case PUBLISH_STATUS:
      head = (nhpm_channel_head_t *)data[0];
      status_code = (ngx_int_t )data[1];
      status_line = (ngx_str_t *)data[2];
      break;
  }
  ngx_http_push_store_publish_locally(head, msg, status_code, status_line);
}

static ngx_int_t ngx_http_push_store_publish_locally(nhpm_channel_head_t *head, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line);

static ngx_int_t ngx_http_push_store_publish_generic(nhpm_channel_head_t *head, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line){
  ngx_uint_t   alert_code;
  void        *alert_data[3];
  ngx_int_t    i;
  if(msg == NULL) {
    alert_code = PUBLISH_STATUS;
    alert_data[0]=(void *)head;
    alert_data[1]=(void *)status_code;
    alert_data[2]=(void *)status_line;
  }
  else {
    alert_code = PUBLISH_MESSAGE;
    alert_data[0]=(void *)head;
    alert_data[1]=(void *)msg;
    alert_data[2]=NULL;
  }
  
  nhpm_worker_chanhead_data_t *data;
  rwl_rdlock(&head->rwl, "publish to other workers");
  for(i=0; i < max_workers; i++) {
    data = &head->worker[i];
    if (ngx_process_slot == i) {
      if(ngx_pid != data->pid) {
        ERR("found chanhead worker data with matching slot (%i) but differen pids (%i %i)", i, ngx_pid, data->pid);
      }
    }
    else {
      if(data->pid != 0 && data->sub_count > 0) {
        ipc_alert(ipc, data->pid, i, alert_code, alert_data);
      }
      else {
        DBG("No subscribers to channel %p at slot %i", head, i);
      }
    }
  }
  rwl_unlock(&head->rwl, "publish to other workers");
  return ngx_http_push_store_publish_locally(head, msg, status_code, status_line);
}

//only publish to this worker's subscribers
static ngx_int_t ngx_http_push_store_publish_locally(nhpm_channel_head_t *head, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line){
  nhpm_subscriber_t          *sub, *next;
  nhpm_channel_head_cleanup_t *hcln;
  nhpm_worker_chanhead_data_t *data;
  if(head==NULL) {
    return NGX_HTTP_PUSH_MESSAGE_QUEUED;
  }
  
  data = chanhead_worker_data(head);
  if (data->sub_count == 0) { //SUPPOSED TO BE ATOMIC.
    return NGX_HTTP_PUSH_MESSAGE_QUEUED;
  }
  
  //set some things the cleanup callback will need
  hcln = data->shared_cleanup;
  data->shared_cleanup = NULL;
  hcln->sub_count=data->sub_count;
  hcln->head=NULL;
  
  //IS THIS SAFE? i think so...
  hcln->id.len = head->id.len;
  hcln->id.data = head->id.data;
  
  //ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "hcln->id.len == %i, head cleanup: %p", hcln->id.len, hcln);
  hcln->pool=data->pool;
  data->pool=NULL; //pool will be destroyed on cleanup
  
  //DBG("chanhead_gc_add from publish_raw adding %p %V", head, &head->id);
  chanhead_gc_add(head);
  
  head->channel.subscribers -= data->sub_count; //atomic
  head->sub_count -= data->sub_count; //also atomic
  data->sub_count = 0;
  sub = data->sub;
  data->sub=NULL;
  
  for( ; sub!=NULL; sub=next) {
    subscriber_t      *rsub = sub->subscriber;

    if(sub->ev.timer_set) { //remove timeout timer right away
      ngx_del_timer(&sub->ev);
    }
    sub->r_cln->handler = (ngx_http_cleanup_pt) subscriber_publishing_cleanup_callback;
    
    next = sub->next; //becase the cleanup callback will dequeue this subscriber
    
    if(sub->clndata.shared != hcln) {
      ERR("wrong shared cleanup for subscriber %p: should be %p, is %p", sub, hcln, sub->clndata.shared);
    }

    if(msg!=NULL) {
      rsub->respond_message(rsub, msg);
    }
    else {
      rsub->respond_status(rsub, status_code, status_line);
    }
  }

  head->generation++; //should be atomic

  return NGX_HTTP_PUSH_MESSAGE_RECEIVED;
}

static ngx_int_t chanhead_messages_delete(nhpm_channel_head_t *ch);

static void handle_chanhead_gc_queue(ngx_int_t force_delete) {
  nhpm_llist_timed_t          *orig_head, *cur, *next;
  nhpm_channel_head_t         *ch = NULL;
  nhpm_message_t              *firstmsg;
  nhpm_worker_chanhead_data_t *workers;
  nhpm_worker_chanhead_data_t *data;
  ngx_int_t                    sub_count;
  ngx_int_t                    i;
  DBG("handling chanhead GC queue");
  
  if(force_delete) {
    DBG("Forced delete");
    ngx_shmtx_lock(&shdata->gc_mutex);
  }
  else {
    if(!ngx_shmtx_trylock(&shdata->gc_mutex)) {
      ERR("garbage collector already running.");
      return;
    }
  }
  
  rwl_rdlock(&shdata->gc_lock, "chanhhead gc queue");
  cur=shdata->chanhead_gc_head;
  orig_head = cur;
  rwl_unlock(&shdata->gc_lock, "chanhhead gc queue");
  
  
  for( ; cur != NULL; cur=next) {
    next=cur->next;
    if(force_delete || ngx_time() - cur->time > NGX_HTTP_PUSH_CHANHEAD_EXPIRE_SEC) {
      ch = (nhpm_channel_head_t *)cur->data;
      rwl_rdlock(&ch->rwl, "chanhead properties read firstsub");
      sub_count = ch->sub_count;
      rwl_unlock(&ch->rwl, "chanhead properties read firstsub");
      if (sub_count > 0 ) { //there are subscribers
        ERR("chanhead %p (%V) is still in use by %i subscribers.", ch, &ch->id, sub_count);
        break;
      }
      if (!force_delete) {
        chanhead_messages_gc(ch);
      }
      else {
        chanhead_messages_delete(ch);
      }
      rwl_rdlock(&ch->rwl, "chanhead properties read firstmsg");
      firstmsg = ch->msg_first;
      workers = ch->worker;
      rwl_unlock(&ch->rwl, "chanhead properties read firstmsg");
      if(firstmsg != NULL) {
        ERR("chanhead %p (%V) is still storing %i messages.", ch, &ch->id, ch->channel.messages);
        break;
      }
      //unsubscribe now
      DBG("chanhead %p (%V) is empty and expired. delete.", ch, &ch->id);
      //do we need a read lock here? I don't think so...
      for(i=0; i < max_workers; i++) {
        data = &workers[i];
        if(i == ngx_process_slot) {
          //that's me!
          assert(data->sub_count == 0);
          if(data->pool != NULL) {
            ngx_destroy_pool(data->pool);
          }
        }
        else {
          ERR("someone else's crap");
        }
      }
      rwl_wrlock(&shdata->hash_lock, "hash del");
      CHANNEL_HASH_DEL(ch);
      rwl_unlock(&shdata->hash_lock, "hash del");
      pthread_rwlock_destroy(&ch->rwl);
      shm_free(shm, ch);
    }
    else {
      if(force_delete) {
        ERR("failed to force-delete %p", cur);
      }
      else {
        DBG("nothing to erase right now");
        break; //dijkstra probably hates this
      }
    }
  }
  
  rwl_wrlock(&shdata->gc_lock, "chanhhead gc queue update");
  if(orig_head == shdata->chanhead_gc_head) {
    shdata->chanhead_gc_head=cur;
    if (cur==NULL) { //we went all the way to the end
      shdata->chanhead_gc_tail=NULL;
    }
    else {
      cur->prev=NULL;
    }
  }
  else {
    ERR("someone beat us to it?....");
  }
  DBG("done with handle chanhead gc. gc head:%p", shdata->chanhead_gc_head);
  rwl_unlock(&shdata->gc_lock, "chanhhead gc queue  update");
  ngx_shmtx_unlock(&shdata->gc_mutex);
}

static void ngx_http_push_store_chanhead_gc_timer_handler(ngx_event_t *ev) {
  nhpm_llist_timed_t  *head;
  handle_chanhead_gc_queue(0);
  rwl_rdlock(&shdata->gc_lock, "gc timer check");
  head = shdata->chanhead_gc_head;
  rwl_unlock(&shdata->gc_lock, "gc timer check");
  if (!(ngx_quit || ngx_terminate || ngx_exiting || head == NULL)) {
    DBG("re-adding chanhead gc event timer");
    ngx_add_timer(ev, NGX_HTTP_PUSH_DEFAULT_CHANHEAD_CLEANUP_INTERVAL);
  }
  else if(head == NULL) {
    DBG("chanhead gc queue looks empty, stop gc_queue handler");
  }
}


static ngx_int_t chanhead_delete_message(nhpm_channel_head_t *ch, nhpm_message_t *msg);


static ngx_int_t ngx_http_push_store_delete_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  nhpm_channel_head_t      *ch;
  nhpm_message_t           *msg = NULL, *nextmsg;
  if((ch = ngx_http_push_store_find_chanhead(channel_id))) {
    ngx_http_push_store_publish_generic(ch, NULL, NGX_HTTP_GONE, &NGX_HTTP_PUSH_HTTP_STATUS_410);
    //TODO: publish to other workers
    rwl_rdlock(&ch->rwl, "delete channel");
    callback(NGX_OK, &ch->channel, privdata);
    msg = ch->msg_first;
    rwl_unlock(&ch->rwl, "delete channel");
    //delete all messages
    while(msg != NULL) {
      nextmsg = msg->next;
      chanhead_delete_message(ch, msg);
      msg = nextmsg;
    }
    chanhead_gc_add(ch);
  }
  else{
    callback(NGX_OK, NULL, privdata);
  }
  return NGX_OK;
}

static ngx_int_t ngx_http_push_store_find_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  nhpm_channel_head_t      *ch;
  if((ch = ngx_http_push_store_find_chanhead(channel_id))) {
    rwl_rdlock(&ch->rwl, "find channel callback");
    callback(NGX_OK, &ch->channel, privdata);
    rwl_unlock(&ch->rwl, "find channel callback");
  }
  else{
    callback(NGX_OK, NULL, privdata);
  }
  return NGX_OK;
}

static ngx_int_t ngx_http_push_store_async_get_message(ngx_str_t *channel_id, ngx_http_push_msg_id_t *msg_id, callback_pt callback, void *privdata) {
  
  if(callback==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "no callback given for async get_message. someone's using the API wrong!");
    return NGX_ERROR;
  }
  
  // TODO: do this?
  
  return NGX_OK; //async only now!
}

//initialization
static ngx_int_t ngx_http_push_store_init_module(ngx_cycle_t *cycle) {
  ngx_core_conf_t                *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  ngx_int_t                       max_worker_processes = ccf->worker_processes;
  
  DBG("memstore init_module pid %p", ngx_pid);

  //initialize our little IPC
  ipc = ipc_create(cycle);
  ipc_open(ipc,cycle, max_worker_processes);
  ipc_set_handler(ipc, ipc_alert_handler);

  return NGX_OK;
}

static ngx_int_t ngx_http_push_store_init_postconfig(ngx_conf_t *cf) {
  ngx_http_push_main_conf_t *conf = ngx_http_conf_get_module_main_conf(cf, ngx_http_push_module);
  ngx_str_t                  name = ngx_string("memstore");
  if(conf->shm_size==NGX_CONF_UNSET_SIZE) {
    conf->shm_size=NGX_HTTP_PUSH_DEFAULT_SHM_SIZE;
  }
  shm = shm_create(&name, cf, conf->shm_size, initialize_shm, &ngx_http_push_module);
  return NGX_OK;
}

static void ngx_http_push_store_create_main_conf(ngx_conf_t *cf, ngx_http_push_main_conf_t *mcf) {
  mcf->shm_size=NGX_CONF_UNSET_SIZE;
}

static void ngx_http_push_store_exit_worker(ngx_cycle_t *cycle) {
  DBG("exit worker %i", ngx_pid);
  nhpm_channel_head_t         *head, *cur, *tmp;
  nhpm_subscriber_t           *sub;
  subscriber_t                *rsub;
  nhpm_worker_chanhead_data_t *data;
  ngx_int_t                    i;
  
    
  rwl_rdlock(&shdata->hash_lock, "exit worker");
  head = shdata->subhash;
  rwl_unlock(&shdata->hash_lock, "exit worker");
  HASH_ITER(hh, head, cur, tmp) {
    //any subscribers?
    
    for(i = 0; i < max_workers; i++) {
      data = &cur->worker[i];
      if (i == ngx_process_slot) {
        //my own subs
        sub = data->sub;
        while (sub != NULL) {
          rsub = sub->subscriber;
          rsub->dequeue_after_response = 1;
          rsub->respond_status(rsub, NGX_HTTP_CLOSE, NULL);
          sub = sub->next;
        }
      }
      else {
        ERR("I don't know how to do this yet");
        assert(0);
      }
    }
    chanhead_gc_add(cur);
  }

  handle_chanhead_gc_queue(1);
  
  rwl_wrlock(&shdata->gc_lock, "clear gc timer");
  if(shdata->chanhead_gc_timer->timer_set) {
    ngx_del_timer(shdata->chanhead_gc_timer);
  }
  rwl_unlock(&shdata->gc_lock, "clear gc timer");
  
  ipc_close(ipc, cycle);
  ipc_destroy(ipc, cycle); //only for this worker...
  shm_destroy(shm); //just for this worker...
}

static void ngx_http_push_store_exit_master(ngx_cycle_t *cycle) {
  DBG("memstore exit master from pid %i", ngx_pid);
  
  ipc_close(ipc, cycle);
  ipc_destroy(ipc, cycle);
  
  shm_destroy(shm);
}

static void subscriber_cleanup_callback(nhpm_subscriber_cleanup_t *cln) {
  
  nhpm_subscriber_t           *sub = cln->sub;
  nhpm_channel_head_cleanup_t *shared = cln->shared;
  nhpm_channel_head_t         *head = shared->head;
  nhpm_worker_chanhead_data_t *data = chanhead_worker_data(head);
  ngx_int_t                    subs;
  
  //DBG("subscriber_cleanup_callback for %p on %V", sub, &head->id);
  
  ngx_int_t done;
  done = sub->prev==NULL && sub->next==NULL;
  
  nhpm_subscriber_unregister(shared->head, sub);
  nhpm_subscriber_remove(shared->head, sub);
  sub->subscriber->dequeue(sub->subscriber);
  
  if(done) {
    //add chanhead to gc list
    data->sub=NULL;
    rwl_rdlock(&head->rwl, "cleanup callback get sub count");
    subs = head->sub_count;
    rwl_unlock(&head->rwl, "cleanup callback get sub count");
    if(subs == 0) {
      chanhead_gc_add(head);
    }
    else {
      ERR("subscriber count is nonzero during subscriber cleanup callback");
    }
  }
}

static ngx_int_t ngx_http_push_store_set_subscriber_cleanup_callback(nhpm_channel_head_t *head, nhpm_subscriber_t *sub, ngx_http_cleanup_pt *cleanup_callback) {
  nhpm_channel_head_cleanup_t *headcln;
  nhpm_worker_chanhead_data_t *data;
  rwl_rdlock(&head->rwl, "read for set subscriber cleanup");
  data = &head->worker[ngx_process_slot];
  headcln = data->shared_cleanup;
  headcln->id.len = head->id.len;
  headcln->id.data = head->id.data;
  rwl_unlock(&head->rwl, "read for set subscriber cleanup");
  //ngx_http_push_loc_conf_t  *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  
  headcln = data->shared_cleanup;
  headcln->head = head;
  
  headcln->pool = data->pool;
  headcln->sub_count = 0;
  
  if(sub->r_cln == NULL) {
    if((sub->r_cln = sub->subscriber->add_next_response_cleanup(sub->subscriber, 0)) == NULL) {
      ERR("unable to add subscriber request cleanup and callback");
      return NGX_ERROR;
    }
  }
  
  sub->r_cln->data = &sub->clndata;
  sub->r_cln->handler = (ngx_http_cleanup_pt) cleanup_callback;
  
  sub->clndata.sub = sub;
  sub->clndata.shared = headcln;
  
  return NGX_OK; 
}

static void subscriber_no_cleanup_callback(nhpm_subscriber_cleanup_t *cln) {
  
}

static void nhpm_subscriber_timeout(ngx_event_t *ev) {
  nhpm_subscriber_cleanup_t *cln = ev->data;
  nhpm_subscriber_t         *sub = cln->sub;
  subscriber_t              *rsub = sub->subscriber;
  sub->r_cln->handler = (ngx_http_cleanup_pt )subscriber_no_cleanup_callback;
  rsub->dequeue_after_response = 1;
  nhpm_subscriber_unregister(cln->shared->head, sub);
  nhpm_subscriber_remove(cln->shared->head, sub);
  rsub->respond_status(rsub, NGX_HTTP_NOT_MODIFIED, NULL);
}

static ngx_int_t nhpm_subscriber_create(nhpm_channel_head_t *chanhead, subscriber_t *sub) {
  //this is the new shit
  ngx_http_push_loc_conf_t    *cf = ngx_http_get_module_loc_conf(sub->request, ngx_http_push_module);
  nhpm_subscriber_t           *nextsub;
  nhpm_worker_chanhead_data_t *data = chanhead_worker_data(chanhead);
  
  if((nextsub=ngx_pcalloc(data->pool, sizeof(*nextsub)))==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "can't allocate memory for (new) subscriber in channel sub pool");
    return NGX_ERROR;
  }

  //let's be explicit about this
  nextsub->prev=NULL;
  nextsub->next=NULL;
  nextsub->id = 0;

  nextsub->subscriber = sub;
  nextsub->pool= sub->request->pool;
  
  if(data->sub != NULL) {
    data->sub->prev = nextsub;
    nextsub->next = data->sub;
  }
  
  else {
    //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "first subscriber for %V (%p): %p", &chanhead->id, chanhead, nextsub);
  }
  nhpm_subscriber_register(chanhead, nextsub);
  
  sub->enqueue(sub, cf->subscriber_timeout);
  
  //add teardown callbacks and cleaning data
  if(ngx_http_push_store_set_subscriber_cleanup_callback(chanhead, nextsub, (ngx_http_cleanup_pt *)subscriber_cleanup_callback) != NGX_OK) {
    ngx_pfree(data->pool, nextsub);
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "can't allocate memory for (new) subscriber cleanup in channel pool");
    return NGX_ERROR;
  }
  
  if(cf->subscriber_timeout > 0) {
    //add timeout timer
    //nextsub->ev should be zeroed;
    nextsub->ev.handler = nhpm_subscriber_timeout;
    nextsub->ev.data = &nextsub->clndata;
    nextsub->ev.log = ngx_cycle->log;
    ngx_add_timer(&nextsub->ev, cf->subscriber_timeout * 1000);
  }
  return NGX_OK;
}

static ngx_int_t chanhead_withdraw_message(nhpm_channel_head_t *ch, nhpm_message_t *msg) {
  //DBG("withdraw message %i:%i from ch %p %V", msg->msg.message_time, msg->msg.message_tag, ch, &ch->id);
  if(msg->msg.refcount > 0) {
    ERR("trying to withdraw (remove) message %p with refcount %i", msg, msg->msg.refcount);
    return NGX_ERROR;
  }
  rwl_wrlock(&ch->rwl, "withdraw message set");
  if(ch->msg_first == msg) {
    //DBG("first message removed");
    ch->msg_first = msg->next;
  }
  if(ch->msg_last == msg) {
    //DBG("last message removed");
    ch->msg_last = msg->prev;
  }
  if(msg->next != NULL) {
    //DBG("set next");
    msg->next->prev = msg->prev;
  }
  if(msg->prev != NULL) {
    //DBG("set prev");
    msg->prev->next = msg->next;
  }
  rwl_unlock(&ch->rwl, "withdraw message set");
  ch->channel.messages --; //supposed to be atomic
  return NGX_OK;
}
static ngx_int_t delete_withdrawn_message( nhpm_message_t *msg ) {
  ngx_buf_t         *buf = msg->msg.buf;
  ngx_file_t        *f = buf->file;
  if(f != NULL) {
    if(f->fd != NGX_INVALID_FILE) {
      DBG("close fd %u ", f->fd);
      ngx_close_file(f->fd);
    }
    else {
      DBG("delete withdrawn fd invalid");
    }
    ngx_delete_file(f->name.data); // assumes string is zero-terminated, which required trickery during allocation
  }
  //DBG("free msg %p", msg);
  shm_free(shm, msg);
  return NGX_OK;
}

static ngx_int_t chanhead_delete_message(nhpm_channel_head_t *ch, nhpm_message_t *msg) {
  if(chanhead_withdraw_message(ch, msg) == NGX_OK) {
    DBG("delete msg %i:%i", msg->msg.message_time, msg->msg.message_tag);
    delete_withdrawn_message(msg);
  } 
  else {
    ERR("failed to withdraw and delete message %i:%i", msg->msg.message_time, msg->msg.message_tag);
  }
  return NGX_OK;
}

static ngx_int_t chanhead_messages_gc_custom(nhpm_channel_head_t *ch, ngx_uint_t min_messages, ngx_uint_t max_messages) {
  nhpm_message_t *cur = ch->msg_first;
  nhpm_message_t *next = NULL;
  time_t          now = ngx_time();
  //DBG("chanhead_gc max %i min %i count %i", max_messages, min_messages, ch->channel.messages);
  
  //is the message queue too big?
  //ch->channel.messages is ATOMIC
  while(cur != NULL && ch->channel.messages > max_messages) {
    next = cur->next;
    if(cur->msg.refcount > 0) {
      ERR("msg %p refcount %i > 0", &cur->msg, cur->msg.refcount);
    }
    else {
      DBG("delete queue-too-big msg %i:%i", cur->msg.message_time, cur->msg.message_tag);
      chanhead_delete_message(ch, cur);
    }
    cur = next;
  }
  
  while(cur != NULL && ch->channel.messages > min_messages && now > cur->msg.expires) {
    next = cur->next;
    if(cur->msg.refcount > 0) {
      ERR("msg %p refcount %i > 0", &cur->msg, cur->msg.refcount);
    }
    if(chanhead_withdraw_message(ch, cur) == NGX_OK) {
       DBG("delete msg %i:%i", cur->msg.message_time, cur->msg.message_tag);
       delete_withdrawn_message(cur);
    }
    else {
      ERR("error deleting chanhead_message");
    }
    cur = next;
  }
  //DBG("Tried deleting %i mesages", count);
  return NGX_OK;
}

static ngx_int_t chanhead_messages_gc(nhpm_channel_head_t *ch) {
  //DBG("messages gc for ch %p %V", ch, &ch->id);
  ngx_uint_t      min_messages, max_messages;
  rwl_rdlock(&ch->rwl, "chanhead message queue size");
  min_messages = ch->min_messages;
  max_messages = ch->max_messages;
  rwl_unlock(&ch->rwl, "chanhead message queue size");
  return chanhead_messages_gc_custom(ch, min_messages, max_messages);
}

static ngx_int_t chanhead_messages_delete(nhpm_channel_head_t *ch) {
  chanhead_messages_gc_custom(ch, 0, 0);
  return NGX_OK;
}

static nhpm_message_t *chanhead_find_next_message(nhpm_channel_head_t *ch, ngx_http_push_msg_id_t *msgid, ngx_int_t *status) {
  DBG("find next message %i:%i", msgid->time, msgid->tag);
  chanhead_messages_gc(ch);
  nhpm_message_t *cur, *first, *last;
  rwl_rdlock(&ch->rwl, "find next message (long)");
  first = ch->msg_first;
  last = ch->msg_last;
  cur = last;
  
  if(cur == NULL) {
    *status = msgid == NULL ? NGX_HTTP_PUSH_MESSAGE_EXPECTED : NGX_HTTP_PUSH_MESSAGE_NOTFOUND;
    rwl_unlock(&ch->rwl, "find next message (long)");
    return NULL;
  }

  if(msgid == NULL || (msgid->time == 0 && msgid->tag == 0)) {
    DBG("found message %i:%i", first->msg.message_time, first->msg.message_tag);
    *status = NGX_HTTP_PUSH_MESSAGE_FOUND;
    rwl_unlock(&ch->rwl, "find next message (long)");
    return first;
  }

  while(cur != NULL) {
    //DBG("cur: %i:%i %V", cur->msg.message_time, cur->msg.message_tag, chanhead_msg_to_str(cur));
    
    if(msgid->time > cur->msg.message_time || (msgid->time == cur->msg.message_time && msgid->tag >= cur->msg.message_tag)){
      if(cur->next != NULL) {
        *status = NGX_HTTP_PUSH_MESSAGE_FOUND;
        DBG("found message %i:%i", cur->next->msg.message_time, cur->next->msg.message_tag);
        rwl_unlock(&ch->rwl, "find next message (long)");
        return cur->next;
      }
      else {
        *status = NGX_HTTP_PUSH_MESSAGE_EXPECTED;
        rwl_unlock(&ch->rwl, "find next message (long)");
        return NULL;
      }
    }
    cur=cur->prev;
  }
  //DBG("looked everywhere, not found");
  *status = NGX_HTTP_PUSH_MESSAGE_NOTFOUND;
  rwl_unlock(&ch->rwl, "find next message (long)");
  return NULL;
}


static ngx_int_t ngx_http_push_store_subscribe(ngx_str_t *channel_id, ngx_http_push_msg_id_t *msg_id, subscriber_t *sub, callback_pt callback, void *privdata) {
  ngx_http_push_loc_conf_t     *cf = ngx_http_get_module_loc_conf(sub->request, ngx_http_push_module);
  nhpm_channel_head_t          *chanhead;
  nhpm_message_t               *chmsg;
  ngx_int_t                     findmsg_status;
  assert(callback != NULL);
  
  DBG("subscribe msgid %i:%i", msg_id->time, msg_id->tag);
  
  if(cf->authorize_channel && (chanhead = ngx_http_push_store_find_chanhead(channel_id)) == NULL) {
      sub->respond_status(sub, NGX_HTTP_FORBIDDEN, NULL);
      callback(NGX_HTTP_NOT_FOUND, NULL, privdata);
      return NGX_OK;
    }
  else {
    chanhead = ngx_http_push_store_get_chanhead(channel_id);
  }
  chmsg = chanhead_find_next_message(chanhead, msg_id, &findmsg_status);
  
  switch(findmsg_status) {
    ngx_int_t                   ret;
    ngx_http_push_msg_t        *msg;
    
    case NGX_HTTP_PUSH_MESSAGE_FOUND: //ok
      msg = &chmsg->msg;
      DBG("subscribe found message %i:%i", msg->message_time, msg->message_tag);
      if(msg == NULL) {
        sub->respond_status(sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL);
        ERR("msg is NULL... why?...");
        return NGX_ERROR;
      }
      switch(cf->subscriber_concurrency) {

        case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_LASTIN:
          //kick everyone elese out, then subscribe
          ngx_http_push_store_publish_raw(chanhead, NULL, NGX_HTTP_CONFLICT, &NGX_HTTP_PUSH_HTTP_STATUS_409);
          //FALL-THROUGH to BROADCAST

        case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_BROADCAST:
            ret = sub->respond_message(sub, msg);
            callback(ret, msg, privdata);
          break;

        case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_FIRSTIN:
          ERR("first-in concurrency setting not supported");
            ret = sub->respond_status(sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL);
            callback(ret, msg, privdata);
          break;

        default:
          ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "unexpected subscriber_concurrency config value");
          ret = sub->respond_status(sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL);
          callback(ret, msg, privdata);
      }
      break;
    case NGX_HTTP_PUSH_MESSAGE_EXPECTED: //not yet available
    case NGX_HTTP_PUSH_MESSAGE_NOTFOUND: //not found
      // ♫ It's gonna be the future soon ♫
      ret = nhpm_subscriber_create(chanhead, sub);
      callback(ret == NGX_OK ? NGX_DONE : NGX_ERROR, NULL, privdata);
      break;
      
    case NGX_HTTP_PUSH_MESSAGE_EXPIRED: //gone
      //subscriber wants an expired message
      //TODO: maybe respond with entity-identifiers for oldest available message?
      sub->respond_status(sub, NGX_HTTP_NO_CONTENT, NULL);
      callback(NGX_HTTP_NO_CONTENT, NULL, privdata);
      break;
    default: //shouldn't be here!
      sub->respond_status(sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL);
      callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, privdata);
  }

  return NGX_OK;
}

//big ol' writelock here
static ngx_int_t chanhead_push_message(nhpm_channel_head_t *ch, nhpm_message_t *msg) {
  msg->next = NULL;
  rwl_wrlock(&ch->rwl, "push message write");
  msg->prev = ch->msg_last;
  if(msg->prev != NULL) {
    msg->prev->next = msg;
  }

  if(ch->msg_first == NULL) {
    ch->msg_first = msg;
  }

  //set time and tag
  if(msg->msg.message_time == 0) {
    msg->msg.message_time = ngx_time();
  }
  if(ch->msg_last && ch->msg_last->msg.message_time == msg->msg.message_time) {
    msg->msg.message_tag = ch->msg_last->msg.message_tag + 1;
  }
  else {
    msg->msg.message_tag = 0;
  }

  ch->channel.messages++;

  ch->msg_last = msg;
  rwl_unlock(&ch->rwl, "push message write");
  
  //DBG("create %i:%i %V", msg->msg.message_time, msg->msg.message_tag, chanhead_msg_to_str(msg));
  chanhead_messages_gc(ch);
  return NGX_OK;
}

typedef struct {
  nhpm_message_t          chmsg;
  ngx_buf_t               buf;
  ngx_file_t              file;
} shmsg_memspace_t;

static nhpm_message_t *create_shared_message(ngx_http_push_msg_t *m) {
  shmsg_memspace_t        *stuff;
  nhpm_message_t          *chmsg;
  ngx_http_push_msg_t     *msg;
  ngx_buf_t               *mbuf = NULL, *buf=NULL;
  mbuf = m->buf;
  
  
  size_t                   buf_body_size = 0, content_type_size = 0, buf_filename_size = 0;
  
  content_type_size += m->content_type.len;
  if(ngx_buf_in_memory_only(mbuf)) {
    buf_body_size = ngx_buf_size(mbuf);
  }
  if(mbuf->in_file && mbuf->file != NULL) {
    buf_filename_size = mbuf->file->name.len;
    if (buf_filename_size > 0) {
      buf_filename_size ++; //for null-termination
    }
  }

  if((stuff = shm_calloc(shm, sizeof(*stuff) + (buf_filename_size + content_type_size + buf_body_size), "message")) == NULL) {
    ERR("can't allocate 'shared' memory for msg for channel id");
    return NULL;
  }
  // shmsg memory chunk: |chmsg|buf|fd|filename|content_type_data|msg_body|

  chmsg = &stuff->chmsg;
  msg = &stuff->chmsg.msg;
  buf = &stuff->buf;
  chmsg->prev=NULL;
  chmsg->next=NULL;

  ngx_memcpy(msg, m, sizeof(*msg));
  ngx_memcpy(buf, mbuf, sizeof(*buf));
  
  msg->buf = buf;

  msg->content_type.data = (u_char *)&stuff[1] + buf_filename_size;

  msg->content_type.len = content_type_size;

  ngx_memcpy(msg->content_type.data, m->content_type.data, content_type_size);

  if(buf_body_size > 0) {
    buf->pos = (u_char *)&stuff[1] + buf_filename_size + content_type_size;
    buf->last = buf->pos + buf_body_size;
    buf->start = buf->pos;
    buf->end = buf->last;
    ngx_memcpy(buf->pos, mbuf->pos, buf_body_size);
  }
  
  if(buf->file!=NULL) {
    buf->file = &stuff->file;
    ngx_memcpy(buf->file, mbuf->file, sizeof(*buf->file));
    
    /*
    buf->file->fd = ngx_open_file(mbuf->file->name.data, NGX_FILE_RDONLY, NGX_FILE_OPEN, 0);
    */
    buf->file->fd =NGX_INVALID_FILE;
    buf->file->log = ngx_cycle->log;

    buf->file->name.data = (u_char *)&stuff[1];

    ngx_memcpy(buf->file->name.data, mbuf->file->name.data, buf_filename_size-1);
    
    //don't mmap it
    /*
    if((buf->start = mmap(NULL, buf->file_last, PROT_READ, MAP_SHARED, buf->file->fd, 0))==NULL){
      ERR("mmap failed");
    }
    buf->last=buf->start + buf->file_last;
    buf->pos=buf->start + buf->file_pos;
    buf->end = buf->start + buf->file_last;
    //buf->file_pos=0;
    //buf->file_last=0;
    
    buf->last=buf->end;
    //buf->in_file=0;
    buf->mmap=1;
    //buf->file=NULL;
    */
  }
  
  return chmsg;
}

static ngx_int_t ngx_http_push_store_publish_message(ngx_str_t *channel_id, ngx_http_push_msg_t *msg, ngx_http_push_loc_conf_t *cf, callback_pt callback, void *privdata) {

  nhpm_channel_head_t     *chead;
  ngx_http_push_channel_t  channel_copy_data;
  ngx_http_push_channel_t *channel_copy = &channel_copy_data;
  nhpm_message_t          *shmsg_link;
  ngx_int_t                sub_count;
  ngx_http_push_msg_t     *publish_msg;
  
  assert(callback != NULL);

  if(msg->message_time==0) {
    msg->message_time = ngx_time();
  }
  msg->expires = ngx_time() + cf->buffer_timeout;

  if((chead = ngx_http_push_store_get_chanhead(channel_id)) == NULL) {
    ERR("can't get chanhead for id %V", channel_id);
    return NGX_ERROR;
  }
  rwl_wrlock(&chead->rwl, "publish");
  chead->channel.expires = ngx_time() + cf->buffer_timeout;
  sub_count = chead->sub_count;
  
  //TODO: address this weirdness
  //chead->min_messages = cf->min_messages;
  chead->min_messages = 0; // for backwards-compatibility, this value is ignored? weird...
  
  chead->max_messages = cf->max_messages;
  
  rwl_unlock(&chead->rwl, "publish");
  
  chanhead_messages_gc(chead);
  if(cf->max_messages == 0) {
    rwl_rdlock(&chead->rwl, "publish channel copy");
    ngx_memcpy(channel_copy, &chead->channel, sizeof(*channel_copy)); //to avoid a read-lock later
    rwl_unlock(&chead->rwl, "publish channel copy");
    publish_msg = msg;
    DBG("publish %i:%i expire %i ", msg->message_time, msg->message_tag, cf->buffer_timeout);
  }
  else {
    if((shmsg_link = create_shared_message(msg)) == NULL) {
      callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, privdata);
      ERR("can't create shared message for channel %V", channel_id);
      return NGX_ERROR;
    }

    if(chanhead_push_message(chead, shmsg_link) != NGX_OK) {
      callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, privdata);
      ERR("can't enqueue shared message for channel %V", channel_id);
      return NGX_ERROR;
    }
    rwl_rdlock(&chead->rwl, "publish channel copy alt");
    ngx_memcpy(channel_copy, &chead->channel, sizeof(*channel_copy));
    rwl_unlock(&chead->rwl, "publish channel copy alt");
    channel_copy->subscribers = sub_count;
    publish_msg = &shmsg_link->msg;
  }
  
  //do the actual publishing
  
  DBG("publish %i:%i expire %i", publish_msg->message_time, publish_msg->message_tag, cf->buffer_timeout);
  if(publish_msg->buf && publish_msg->buf->file) {
    DBG("fd %i", publish_msg->buf->file->fd);
  }
  ;
  callback(ngx_http_push_store_publish_generic(chead, publish_msg, 0, NULL), channel_copy, privdata);
  
  return NGX_OK;
}

ngx_http_push_store_t  ngx_http_push_store_memory = {
    //init
    &ngx_http_push_store_init_module,
    &ngx_http_push_store_init_worker,
    &ngx_http_push_store_init_postconfig,
    &ngx_http_push_store_create_main_conf,
    
    //shutdown
    &ngx_http_push_store_exit_worker,
    &ngx_http_push_store_exit_master,
    
    //async-friendly functions with callbacks
    &ngx_http_push_store_async_get_message, //+callback
    &ngx_http_push_store_subscribe, //+callback
    &ngx_http_push_store_publish_message, //+callback
    
    &ngx_http_push_store_delete_channel, //+callback
    &ngx_http_push_store_find_channel, //+callback
    
    //message stuff
    NULL,
    NULL,
    
};

