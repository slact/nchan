#include <ngx_http_push_module.h>

#include <assert.h>
#include "store.h"
#include <store/ngx_rwlock.h>
#include "rbtree_util.h"
#include "shmem.h"
#include "ipc.h"
#include "ipc-handlers.h"
#include "store-private.h"
#include "../spool.h"

static ngx_int_t max_worker_processes = 0;

typedef struct {
  ngx_event_t             gc_timer;
  nhpm_llist_timed_t     *gc_head;
  nhpm_llist_timed_t     *gc_tail;
  nhpm_channel_head_t    *hash;
#if FAKESHARD
  ngx_int_t               fake_slot;
#endif
} memstore_data_t;

#if FAKESHARD

static memstore_data_t  mdata[MAX_FAKE_WORKERS];
static memstore_data_t fake_default_mdata = {{0}, NULL, NULL, NULL};
memstore_data_t *mpt = &fake_default_mdata;

ngx_int_t memstore_slot() {
  return mpt->fake_slot;
}

#else

static memstore_data_t  mdata = {{0}, NULL, NULL, NULL};
memstore_data_t *mpt = &mdata;

ngx_int_t memstore_slot() {
  return ngx_process_slot;
}

#endif


static shmem_t         *shm = NULL;
static shm_data_t      *shdata = NULL;
static ipc_t           *ipc;

shmem_t *ngx_http_push_memstore_get_shm(void){
  return shm;
}

ipc_t *ngx_http_push_memstore_get_ipc(void){
  return ipc;
}

#define CHANNEL_HASH_FIND(id_buf, p)    HASH_FIND( hh, mpt->hash, (id_buf)->data, (id_buf)->len, p)
#define CHANNEL_HASH_ADD(chanhead)      HASH_ADD_KEYPTR( hh, mpt->hash, (chanhead->id).data, (chanhead->id).len, chanhead)
#define CHANNEL_HASH_DEL(chanhead)      HASH_DEL( mpt->hash, chanhead)

#undef uthash_malloc
#undef uthash_free
#define uthash_malloc(sz) ngx_alloc(sz, ngx_cycle->log)
#define uthash_free(ptr,sz) ngx_free(ptr)

#define STR(buf) (buf)->data, (buf)->len
#define BUF(buf) (buf)->pos, ((buf)->last - (buf)->pos)

#define NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_POOL_SIZE (5 * 1024)
#define NGX_HTTP_PUSH_DEFAULT_CHANHEAD_CLEANUP_INTERVAL 1000
#define NGX_HTTP_PUSH_CHANHEAD_EXPIRE_SEC 1


//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG


#if FAKESHARD

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "MEMSTORE:(fake)%i: " fmt, memstore_slot(), ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "MEMSTORE:(fake)%i: " fmt, memstore_slot(), ##args)

static nhpm_llist_timed_t *fakeprocess_top = NULL;
void memstore_fakeprocess_push(ngx_int_t slot) {
  assert(slot < MAX_FAKE_WORKERS);
  nhpm_llist_timed_t *link = ngx_calloc(sizeof(*fakeprocess_top), ngx_cycle->log);
  link->data = (void *)slot;
  link->time = ngx_time();
  link->next = fakeprocess_top;
  if(fakeprocess_top != NULL) {
    fakeprocess_top->prev = link;
  }
  fakeprocess_top = link;
  //DBG("push fakeprocess %i onto stack", slot);
  mpt = &mdata[slot];
}

void memstore_fakeprocess_pop(void) {
  nhpm_llist_timed_t   *next;
  if(fakeprocess_top == NULL) {
    DBG("can't pop empty fakeprocess stack");
    return;
  }
  else if((next = fakeprocess_top->next) == NULL) {
    DBG("can't pop last item off of fakeprocess stack");
    return;
  }
  //DBG("pop fakeprocess to return to %i", (ngx_int_t)next->data);
  ngx_free(fakeprocess_top);
  next->prev = NULL;
  fakeprocess_top = next;
  mpt = &mdata[(ngx_int_t )fakeprocess_top->data];
}

void memstore_fakeprocess_push_random(void) {
  return memstore_fakeprocess_push(rand() % MAX_FAKE_WORKERS);
}

#else

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "MEMSTORE:%i: " fmt, memstore_slot(), ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "MEMSTORE:%i: " fmt, memstore_slot(), ##args)

#endif


ngx_int_t memstore_channel_owner(ngx_str_t *id) {
  ngx_int_t h = ngx_crc32_short(id->data, id->len);
#if FAKESHARD
  #ifdef ONE_FAKE_CHANNEL_OWNER
  h++; //just to avoid the unused variable warning
  return ONE_FAKE_CHANNEL_OWNER;
  #else
  return h % MAX_FAKE_WORKERS;
  #endif
#else
  return h % max_worker_processes;
#endif
}


static ngx_int_t chanhead_messages_gc(nhpm_channel_head_t *ch);

static void ngx_http_push_store_chanhead_gc_timer_handler(ngx_event_t *);

static ngx_int_t initialize_shm(ngx_shm_zone_t *zone, void *data) {
  shm_data_t     *d;

  if(data) { //zone being passed after restart
    zone->data = data;
    return NGX_OK;
  }
  shm_init(shm);
  
  if((d = shm_alloc(shm, sizeof(shm_data_t), "root shared data")) == NULL) {
    return NGX_ERROR;
  }
  zone->data = d;
  shdata = d;

  return NGX_OK;
}


static ngx_int_t ngx_http_push_store_init_worker(ngx_cycle_t *cycle) {

#if FAKESHARD
  ngx_int_t        i;
  for(i = 0; i < MAX_FAKE_WORKERS; i++) {
  memstore_fakeprocess_push(i);
#endif
  if(mpt->gc_timer.handler == NULL) {
    mpt->gc_timer.handler=&ngx_http_push_store_chanhead_gc_timer_handler;
    mpt->gc_timer.log=ngx_cycle->log;
  }

#if FAKESHARD
  memstore_fakeprocess_pop();
  }
#endif

  ipc_start(ipc, cycle);
  
  DBG("init memstore worker pid:%i slot:%i max workers :%i", ngx_pid, memstore_slot(), max_worker_processes);
  return NGX_OK;
}


ngx_int_t nhpm_memstore_subscriber_register(nhpm_channel_head_t *chanhead, subscriber_t *sub) {
  //nothing....
  //chanhead->shared.sub_count = chanhead->channel.subscribers;
  return NGX_OK;
}

ngx_int_t nhpm_memstore_subscriber_unregister(nhpm_channel_head_t *chanhead, subscriber_t *sub) {
  //don't do anything, really
  //chanhead->shared.sub_count = chanhead->channel.subscribers;
  return NGX_OK;
}


static void spooler_add_handler(channel_spooler_t *spl, subscriber_t *sub, void *privdata) {
  nhpm_channel_head_t   *head = (nhpm_channel_head_t *)privdata;
  head->sub_count++;
  head->channel.subscribers++;
  if(sub->type == INTERNAL) {
    head->internal_sub_count++;
    head->shared->internal_sub_count++;
  }
  else {
    head->shared->sub_count++;
  }
  head->last_subscribed = ngx_time();
  head->shared->last_seen = ngx_time();
  assert(head->sub_count >= head->internal_sub_count);
}

static void spooler_bulk_dequeue_handler(channel_spooler_t *spl, subscriber_type_t type, ngx_int_t count, void *privdata) {
  nhpm_channel_head_t   *head = (nhpm_channel_head_t *)privdata;
  if (type == INTERNAL) {
    //internal subscribers are *special* and don't really count
    head->internal_sub_count -= count;
    head->shared->internal_sub_count -= count;
  }
  else {
    head->shared->sub_count -= count;
  }
  head->sub_count -= count;
  head->channel.subscribers = head->sub_count - head->internal_sub_count;
  assert(head->sub_count >= 0);
  assert(head->internal_sub_count >= 0);
  assert(head->channel.subscribers >= 0);
  assert(head->sub_count >= head->internal_sub_count);
  if(head->sub_count == 0 && head->ipc_sub == NULL) {
    chanhead_gc_add(head, "sub count == 0 after spooler dequeue");
  }
}

static ngx_int_t start_chanhead_spooler(nhpm_channel_head_t *head) {
  start_spooler(&head->spooler);
  head->spooler.set_add_handler(&head->spooler, spooler_add_handler, head);
  head->spooler.set_bulk_dequeue_handler(&head->spooler, spooler_bulk_dequeue_handler, head);
  return NGX_OK;
}

static ngx_int_t stop_chanhead_spooler(nhpm_channel_head_t *head) {
  stop_spooler(&head->spooler);
  return NGX_OK;
}

static ngx_int_t ensure_chanhead_is_ready(nhpm_channel_head_t *head) {
  ngx_int_t                      owner = memstore_channel_owner(&head->id);
  if(head == NULL) {
    return NGX_OK;
  }
  DBG("(ensure_chanhead is_ready) setting chanhead %p, status %i, ipc_sub:%p", head, head->status, head->ipc_sub);
  if(head->status == INACTIVE) {//recycled chanhead
    chanhead_gc_withdraw(head, "readying INACTIVE");
  }
  if(!head->spooler.running) {
    DBG("Spooler for channel %p %V wasn't running. start it.", head, &head->id);
    start_chanhead_spooler(head);
  }
  
  if(owner != memstore_slot()) {
    if(head->ipc_sub == NULL && head->status != WAITING) {
      head->status = WAITING;
      memstore_ipc_send_subscribe(owner, &head->id, head);
    }
    else if(head->ipc_sub != NULL && head->status == WAITING) {
      head->status = READY;
    }
  }
  else {
    head->status = READY;
  }
  return NGX_OK;
}


nhpm_channel_head_t * chanhead_memstore_find(ngx_str_t *channel_id) {
  nhpm_channel_head_t     *head;
  CHANNEL_HASH_FIND(channel_id, head);
  return head;
}


static nhpm_channel_head_t *chanhead_memstore_create(ngx_str_t *channel_id) {
  nhpm_channel_head_t         *head;
  head=ngx_alloc(sizeof(*head) + sizeof(u_char)*(channel_id->len), ngx_cycle->log);
  ngx_int_t                owner = memstore_channel_owner(channel_id);
  if(head == NULL) {
    ERR("can't allocate memory for (new) chanhead");
    return NULL;
  }
  head->slot = memstore_slot();
  head->owner = owner;
  head->shutting_down = 0;

  if(head->slot == owner) {
    if((head->shared = shm_alloc(shm, sizeof(*head->shared), "channel shared data")) == NULL) {
      ERR("can't allocate shared memory for (new) chanhead");
      return NULL;
    }
    head->shared->sub_count = 0;
    head->shared->internal_sub_count = 0;
    head->shared->total_message_count = 0;
    head->shared->stored_message_count = 0;
    head->shared->last_seen = ngx_time();
  }
  else {
    head->shared = NULL;
  }
  
  //no lock needed, no one else knows about this chanhead yet.
  head->id.len = channel_id->len;
  head->id.data = (u_char *)&head[1];
  ngx_memcpy(head->id.data, channel_id->data, channel_id->len);
  head->sub_count=0;
  head->internal_sub_count=0;
  head->status = NOTREADY;
  head->waiting_for_publish_response = NULL;
  head->msg_last = NULL;
  head->msg_first = NULL;
  head->ipc_sub = NULL;
  head->last_subscribed = 0;
  //set channel
  ngx_memcpy(&head->channel.id, &head->id, sizeof(ngx_str_t));
  head->channel.message_queue=NULL;
  head->channel.messages = 0;
  head->channel.subscribers = 0;
  head->channel.last_seen = ngx_time();
  head->min_messages = 0;
  head->max_messages = (ngx_int_t) -1;
  
  head->last_msgid.time=0;
  head->last_msgid.tag=0;
  
  head->spooler.running=0;
  start_chanhead_spooler(head);

  CHANNEL_HASH_ADD(head);
  
  return head;
}

nhpm_channel_head_t * ngx_http_push_memstore_find_chanhead(ngx_str_t *channel_id) {
  nhpm_channel_head_t     *head;
  if((head = chanhead_memstore_find(channel_id)) != NULL) {
    ensure_chanhead_is_ready(head);
  }
  return head;
}

nhpm_channel_head_t * ngx_http_push_memstore_get_chanhead(ngx_str_t *channel_id) {
  nhpm_channel_head_t          *head;
  head = chanhead_memstore_find(channel_id);
  if(head==NULL) {
    head = chanhead_memstore_create(channel_id);
  }
  ensure_chanhead_is_ready(head);
  return head;
}

ngx_int_t chanhead_gc_add(nhpm_channel_head_t *head, const char *reason) {
  nhpm_llist_timed_t         *chanhead_cleanlink;
  
  DBG("Chanhead gc add %p %V: %s", head, &head->id, reason);
  chanhead_cleanlink = &head->cleanlink;
  if(!head->shutting_down) {
    assert(head->ipc_sub == NULL); //we don't accept still-subscribed chanheads
  }
  if(head->slot != head->owner) {
    head->shared = NULL;
  }
  assert(head->slot == memstore_slot());
  if(head->status != INACTIVE) {
    chanhead_cleanlink->data=(void *)head;
    chanhead_cleanlink->time=ngx_time();
    chanhead_cleanlink->prev=mpt->gc_tail;
    if(mpt->gc_tail != NULL) {
      mpt->gc_tail->next=chanhead_cleanlink;
    }
    chanhead_cleanlink->next=NULL;
    mpt->gc_tail=chanhead_cleanlink;
    if(mpt->gc_head==NULL) {
      mpt->gc_head = chanhead_cleanlink;
    }
    head->status = INACTIVE;
  }
  else {
    ERR("gc_add chanhead %V: already added", &head->id);
  }

  //initialize gc timer
  if(! mpt->gc_timer.timer_set) {
    mpt->gc_timer.data=mpt->gc_head; //don't really care whre this points, so long as it's not null (for some debugging)
    ngx_add_timer(&mpt->gc_timer, NGX_HTTP_PUSH_DEFAULT_CHANHEAD_CLEANUP_INTERVAL);
  }

  return NGX_OK;
}

ngx_int_t chanhead_gc_withdraw(nhpm_channel_head_t *chanhead, const char *reason) {
  //remove from gc list if we're there
  nhpm_llist_timed_t    *cl;
  DBG("Chanhead gc withdraw %p %V: %s", chanhead, &chanhead->id, reason);
  
  if(chanhead->status == INACTIVE) {
    cl=&chanhead->cleanlink;
    if(cl->prev!=NULL)
      cl->prev->next=cl->next;
    if(cl->next!=NULL)
      cl->next->prev=cl->prev;

    if(mpt->gc_head==cl) {
      mpt->gc_head=cl->next;
    }
    if(mpt->gc_tail==cl) {
      mpt->gc_tail=cl->prev;
    }
    cl->prev = cl->next = NULL;
  }
  else {
    DBG("gc_withdraw chanhead %p (%V), but already inactive", chanhead, &chanhead->id);
  }
  
  return NGX_OK;
}

/*
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
    return msg_to_str(msg->msg); //WHOA, shared space!
  }
}
*/

ngx_int_t ngx_http_push_memstore_publish_generic(nhpm_channel_head_t *head, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line) {
  ngx_int_t          shared_sub_count = head->shared->sub_count;

  if(head==NULL) {
    return NGX_HTTP_PUSH_MESSAGE_QUEUED;
  }

  if (head->sub_count == 0) {
    return NGX_HTTP_PUSH_MESSAGE_QUEUED;
  }

  if(msg) {
    head->last_msgid.time = msg->message_time;
    head->last_msgid.tag = msg->message_tag;
    head->spooler.respond_message(&head->spooler, msg);
  }
  else {
    head->spooler.respond_status(&head->spooler, status_code, status_line);
  }

  //TODO: be smarter about garbage-collecting chanheads
  if(memstore_channel_owner(&head->id) == memstore_slot()) {
    chanhead_gc_add(head, "add owner chanhead after publish");
  }

  if(head->shared) {
    head->channel.subscribers = head->shared->sub_count;
  }
  
  return (shared_sub_count > 0) ? NGX_HTTP_PUSH_MESSAGE_RECEIVED : NGX_HTTP_PUSH_MESSAGE_QUEUED;
}

static ngx_int_t chanhead_messages_delete(nhpm_channel_head_t *ch);

static void handle_chanhead_gc_queue(ngx_int_t force_delete) {
  nhpm_llist_timed_t          *cur, *next;
  nhpm_channel_head_t         *ch = NULL;
  ngx_int_t                    owner;
  DBG("handling chanhead GC queue");
#if FAKESHARD
  ngx_int_t        i;
  for(i = 0; i < MAX_FAKE_WORKERS; i++) {
  memstore_fakeprocess_push(i);
#endif
  
  
  for(cur=mpt->gc_head ; cur != NULL; cur=next) {
    ch = (nhpm_channel_head_t *)cur->data;
    next=cur->next;
    if(force_delete || ngx_time() - cur->time > NGX_HTTP_PUSH_CHANHEAD_EXPIRE_SEC) {
      if (ch->sub_count > 0 ) { //there are subscribers
        DBG("chanhead %p (%V) is still in use by %i subscribers.", ch, &ch->id, ch->sub_count);
        if(force_delete) {
          ERR("chanhead %p (%V) is still in use by %i subscribers. Delete it anyway.", ch, &ch->id, ch->sub_count);
          //ch->spooler.prepare_to_stop(&ch->spooler);
          ch->spooler.respond_status(&ch->spooler, NGX_HTTP_GONE, &NGX_HTTP_PUSH_HTTP_STATUS_410);
        }
        else {
          DBG("chanhead %p (%V) is still in use by %i subscribers. Abort GC scan.", ch, &ch->id, ch->sub_count);
          //break;
        }
      }
      stop_chanhead_spooler(ch);
      assert(ch->sub_count == 0);
      force_delete ? chanhead_messages_delete(ch) : chanhead_messages_gc(ch);

      if(ch->msg_first != NULL) {
        assert(ch->channel.messages != 0);
        DBG("chanhead %p (%V) is still storing %i messages.", ch, &ch->id, ch->channel.messages);
        break;
      }
      //unsubscribe now
      //do we need a read lock here? I don't think so...
      owner = memstore_channel_owner(&ch->id);
      if(owner == memstore_slot()) {
        shm_free(shm, ch->shared);
      }
      DBG("chanhead %p (%V) is empty and expired. DELETE.", ch, &ch->id);
      CHANNEL_HASH_DEL(ch);
      ngx_free(ch);
    }
    else {
      break; //dijkstra probably hates this
    }
  }
  mpt->gc_head=cur;
  if (cur==NULL) { //we went all the way to the end
    mpt->gc_tail=NULL;
  }
  else {
    cur->prev=NULL;
  }
  
#if FAKESHARD
  memstore_fakeprocess_pop();
  }
  for(i = 0; i < MAX_FAKE_WORKERS; i++) {
    memstore_fakeprocess_push(i);
    memstore_fakeprocess_pop();
  }
#endif
}

static void ngx_http_push_store_chanhead_gc_timer_handler(ngx_event_t *ev) {
  nhpm_llist_timed_t  *head = mpt->gc_head;
  handle_chanhead_gc_queue(0);
  if (!(ngx_quit || ngx_terminate || ngx_exiting || head == NULL)) {
    DBG("re-adding chanhead gc event timer");
    ngx_add_timer(ev, NGX_HTTP_PUSH_DEFAULT_CHANHEAD_CLEANUP_INTERVAL);
  }
  else if(head == NULL) {
    DBG("chanhead gc queue looks empty, stop gc_queue handler");
  }
}

static ngx_int_t empty_callback(){
  return NGX_OK;
}

static ngx_int_t chanhead_delete_message(nhpm_channel_head_t *ch, nhpm_message_t *msg);

static ngx_int_t ngx_http_push_store_delete_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  ngx_int_t                owner = memstore_channel_owner(channel_id);
  if(memstore_slot() != owner) {
    memstore_ipc_send_delete(owner, channel_id, callback, privdata);
  }
  else {
    ngx_http_push_memstore_force_delete_channel(channel_id, callback, privdata);
  }
  return NGX_OK;
}

ngx_int_t ngx_http_push_memstore_force_delete_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  nhpm_channel_head_t      *ch;
  ngx_http_push_channel_t   chaninfo_copy;
  nhpm_message_t           *msg = NULL;
  
  assert(memstore_channel_owner(channel_id) == memstore_slot());
  
  if(callback == NULL) {
    callback = empty_callback;
  }
  if((ch = ngx_http_push_memstore_find_chanhead(channel_id))) {
    chaninfo_copy.messages = ch->shared->stored_message_count;
    chaninfo_copy.subscribers = ch->shared->sub_count;
    chaninfo_copy.last_seen = ch->shared->last_seen;
    
    ngx_http_push_memstore_publish_generic(ch, NULL, NGX_HTTP_GONE, &NGX_HTTP_PUSH_HTTP_STATUS_410);
    callback(NGX_OK, &chaninfo_copy, privdata);
    //delete all messages
    while((msg = ch->msg_first) != NULL) {
      chanhead_delete_message(ch, msg);
    }
    chanhead_gc_add(ch, "forced delete");
  }
  else{
    callback(NGX_OK, NULL, privdata);
  }
  return NGX_OK;
}

static ngx_int_t ngx_http_push_store_find_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  ngx_int_t owner = memstore_channel_owner(channel_id);
  if(memstore_slot() == owner) {
    nhpm_channel_head_t      *ch = ngx_http_push_memstore_find_chanhead(channel_id);
    callback(NGX_OK, ch != NULL ? &ch->channel : NULL , privdata);
  }
  else {
    memstore_ipc_send_get_channel_info(owner, channel_id, callback, privdata);
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

#if FAKESHARD

  max_worker_processes = MAX_FAKE_WORKERS;
  
  ngx_int_t        i;
  memstore_data_t *cur;
  for(i = 0; i < MAX_FAKE_WORKERS; i++) {
    cur = &mdata[i];
    cur->fake_slot = i;
  }
  
  memstore_fakeprocess_push(0);

#else

  ngx_core_conf_t   *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  max_worker_processes = ccf->worker_processes;

#endif
  
  DBG("memstore init_module pid %p", ngx_pid);

  //initialize our little IPC
  ipc = ipc_create(cycle);
  ipc_open(ipc,cycle, max_worker_processes);
  ipc_set_handler(ipc, memstore_ipc_alert_handler);

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
  nhpm_channel_head_t         *cur, *tmp;
  
#if FAKESHARD
  ngx_int_t        i;
  for(i = 0; i < MAX_FAKE_WORKERS; i++) {
  memstore_fakeprocess_push(i);
#endif
  HASH_ITER(hh, mpt->hash, cur, tmp) {
    cur->shutting_down = 1;
    chanhead_gc_add(cur, "exit worker");
  }
  handle_chanhead_gc_queue(1);
  
  if(mpt->gc_timer.timer_set) {
    ngx_del_timer(&mpt->gc_timer);
  }
#if FAKESHARD
  memstore_fakeprocess_pop();
  }
#endif
  
  ipc_close(ipc, cycle);
  ipc_destroy(ipc, cycle); //only for this worker...
  shm_free(shm, shdata);
  shm_destroy(shm); //just for this worker...
#if FAKESHARD
  ngx_free(fakeprocess_top);
#endif
}

static void ngx_http_push_store_exit_master(ngx_cycle_t *cycle) {
  DBG("exit master from pid %i", ngx_pid);
  
  ipc_close(ipc, cycle);
  ipc_destroy(ipc, cycle);
  
  shm_destroy(shm);
}

static ngx_int_t validate_chanhead_messages(nhpm_channel_head_t *ch) {
  ngx_int_t              count = ch->channel.messages;
  ngx_int_t              rev_count = count;
  ngx_int_t              owner = memstore_channel_owner(&ch->id);
  nhpm_message_t        *cur;
  
  if(memstore_slot() == owner) {
    assert(ch->shared->stored_message_count == ch->channel.messages);
  }
  //walk it forwards
  for(cur = ch->msg_first; cur != NULL; cur=cur->next){
    count--;
  }
  for(cur = ch->msg_last; cur != NULL; cur=cur->prev){
    rev_count--;
  }
  
  assert(count == 0);
  assert(rev_count == 0);
  return NGX_OK;
}

static ngx_int_t chanhead_withdraw_message(nhpm_channel_head_t *ch, nhpm_message_t *msg) {
  //DBG("withdraw message %i:%i from ch %p %V", msg->msg->message_time, msg->msg->message_tag, ch, &ch->id);
  validate_chanhead_messages(ch);
  if(msg->msg->refcount > 0) {
    ERR("trying to withdraw (remove) message %p with refcount %i", msg, msg->msg->refcount);
    return NGX_ERROR;
  }
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
  
  ch->channel.messages--; //supposed to be atomic
  ch->shared->stored_message_count--;
  if(ch->channel.messages == 0) {
    assert(ch->msg_first == NULL);
    assert(ch->msg_last == NULL);
  }
  validate_chanhead_messages(ch);
  return NGX_OK;
}

static ngx_int_t delete_withdrawn_message( nhpm_message_t *msg ) {
  ngx_buf_t         *buf = msg->msg->buf;
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
  shm_free(shm, msg->msg);
  ngx_free(msg);
  return NGX_OK;
}

static ngx_int_t chanhead_delete_message(nhpm_channel_head_t *ch, nhpm_message_t *msg) {
  validate_chanhead_messages(ch);
  if(chanhead_withdraw_message(ch, msg) == NGX_OK) {
    DBG("delete msg %i:%i", msg->msg->message_time, msg->msg->message_tag);
    delete_withdrawn_message(msg);
  }
  else {
    ERR("failed to withdraw and delete message %i:%i", msg->msg->message_time, msg->msg->message_tag);
  }
  validate_chanhead_messages(ch);
  return NGX_OK;
}

static ngx_int_t chanhead_messages_gc_custom(nhpm_channel_head_t *ch, ngx_uint_t min_messages, ngx_uint_t max_messages) {
  validate_chanhead_messages(ch);
  nhpm_message_t *cur = ch->msg_first;
  nhpm_message_t *next = NULL;
  time_t          now = ngx_time();
  //DBG("chanhead_gc max %i min %i count %i", max_messages, min_messages, ch->channel.messages);
  
  //is the message queue too big?
  while(cur != NULL && ch->channel.messages > max_messages) {
    next = cur->next;
    if(cur->msg->refcount > 0) {
      ERR("msg %p refcount %i > 0", &cur->msg, cur->msg->refcount);
    }
    else {
      DBG("delete queue-too-big msg %i:%i", cur->msg->message_time, cur->msg->message_tag);
      chanhead_delete_message(ch, cur);
    }
    cur = next;
  }
  
  while(cur != NULL && ch->channel.messages > min_messages && now > cur->msg->expires) {
    next = cur->next;
    if(cur->msg->refcount > 0) {
      ERR("msg %p refcount %i > 0", &cur->msg, cur->msg->refcount);
    }
    else {
      chanhead_delete_message(ch, cur);
    }
    cur = next;
  }
  //DBG("Tried deleting %i mesages", count);
  validate_chanhead_messages(ch);
  return NGX_OK;
}

static ngx_int_t chanhead_messages_gc(nhpm_channel_head_t *ch) {
  //DBG("messages gc for ch %p %V", ch, &ch->id);
  return chanhead_messages_gc_custom(ch, ch->min_messages, ch->max_messages);
}

static ngx_int_t chanhead_messages_delete(nhpm_channel_head_t *ch) {
  chanhead_messages_gc_custom(ch, 0, 0);
  return NGX_OK;
}

nhpm_message_t *chanhead_find_next_message(nhpm_channel_head_t *ch, ngx_http_push_msg_id_t *msgid, ngx_int_t *status) {
  DBG("find next message %i:%i", msgid->time, msgid->tag);
  chanhead_messages_gc(ch);
  nhpm_message_t *cur, *first;
  first = ch->msg_first;
  cur = ch->msg_last;
  
  if(cur == NULL) {
    *status = msgid == NULL ? NGX_HTTP_PUSH_MESSAGE_EXPECTED : NGX_HTTP_PUSH_MESSAGE_NOTFOUND;
    return NULL;
  }

  if(msgid == NULL || (msgid->time == 0 && msgid->tag == 0)) {
    DBG("found message %i:%i", first->msg->message_time, first->msg->message_tag);
    *status = NGX_HTTP_PUSH_MESSAGE_FOUND;
    return first;
  }

  while(cur != NULL) {
    //DBG("cur: %i:%i %V", cur->msg->message_time, cur->msg->message_tag, chanhead_msg_to_str(cur));
    
    if(msgid->time > cur->msg->message_time || (msgid->time == cur->msg->message_time && msgid->tag >= cur->msg->message_tag)){
      if(cur->next != NULL) {
        *status = NGX_HTTP_PUSH_MESSAGE_FOUND;
        DBG("found message %i:%i", cur->next->msg->message_time, cur->next->msg->message_tag);
        return cur->next;
      }
      else {
        *status = NGX_HTTP_PUSH_MESSAGE_EXPECTED;
        return NULL;
      }
    }
    cur=cur->prev;
  }
  //DBG("looked everywhere, not found");
  *status = NGX_HTTP_PUSH_MESSAGE_NOTFOUND;
  return NULL;
}

typedef struct {
  subscriber_t             *sub;
  ngx_int_t                 channel_owner;
  nhpm_channel_head_t      *chanhead;
  ngx_str_t                *channel_id;
  ngx_http_push_msg_id_t   *msg_id;
  callback_pt               cb;
  void                     *cb_privdata;
  unsigned                  already_enqueued:1;
  unsigned                  allocd:1;
} subscribe_data_t;

#define SUB_CHANNEL_UNAUTHORIZED 0
#define SUB_CHANNEL_AUTHORIZED 1
#define SUB_CHANNEL_NOTSURE 2

static ngx_int_t ngx_http_push_store_subscribe_continued(ngx_int_t channel_status, void* _, subscribe_data_t *d);
static ngx_int_t ngx_http_push_store_subscribe_sub_reserved_check(ngx_int_t channel_status, void* _, subscribe_data_t *d);

static ngx_int_t ngx_http_push_store_subscribe(ngx_str_t *channel_id, ngx_http_push_msg_id_t *msg_id, subscriber_t *sub, callback_pt callback, void *privdata) {
  ngx_int_t                    owner = memstore_channel_owner(channel_id);
  subscribe_data_t             data;
  subscribe_data_t            *d;
  assert(callback != NULL);
  if(memstore_slot() != owner) {
    d = ngx_alloc(sizeof(*d), ngx_cycle->log);
    d->allocd = 1;
    d->already_enqueued = 1;
  }
  else {
    d = &data;
    d->allocd = 0;
    d->already_enqueued = 0;
  }
  d->msg_id = msg_id;
  d->channel_owner = owner;
  d->channel_id = channel_id;
  d->cb = callback;
  d->cb_privdata = privdata;
  d->sub = sub;
  
  DBG("subscribe msgid %i:%i", msg_id->time, msg_id->tag);
  
  if(sub->cf->authorize_channel) {
    if(memstore_slot() != owner) {
      sub->reserve(sub);
      memstore_ipc_send_does_channel_exist(owner, channel_id, (callback_pt )ngx_http_push_store_subscribe_sub_reserved_check, d);
    }
    else {
      ngx_http_push_store_subscribe_continued(SUB_CHANNEL_NOTSURE, NULL, d);
    }
  }
  else {
    ngx_http_push_store_subscribe_continued(SUB_CHANNEL_AUTHORIZED, NULL, d);
  }
  
  return NGX_OK;
}

static ngx_int_t ngx_http_push_store_subscribe_sub_reserved_check(ngx_int_t channel_status, void* _, subscribe_data_t *d) {
  if(d->sub->release(d->sub) == NGX_OK) {
    return ngx_http_push_store_subscribe_continued(channel_status, _, d);
  }
  else {//don't go any further, the sub has been deleted
    return NGX_OK;
  }
}

static ngx_int_t ngx_http_push_store_subscribe_continued(ngx_int_t channel_status, void* _, subscribe_data_t *d) {
  nhpm_channel_head_t       *chanhead;
  nhpm_message_t            *chmsg;
    ngx_int_t                findmsg_status;
  switch(channel_status) {
    case SUB_CHANNEL_AUTHORIZED:
      chanhead = ngx_http_push_memstore_get_chanhead(d->channel_id);
      break;
    case SUB_CHANNEL_UNAUTHORIZED:
      chanhead = NULL;
      break;
    case SUB_CHANNEL_NOTSURE:
      chanhead = ngx_http_push_memstore_find_chanhead(d->channel_id);
      break;
  }
  
  if (chanhead == NULL) {
    d->sub->respond_status(d->sub, NGX_HTTP_FORBIDDEN, NULL);
    d->cb(NGX_HTTP_NOT_FOUND, NULL, d->cb_privdata);
    return NGX_OK;
  }
  
  d->chanhead = chanhead;
  
  if(memstore_slot() != d->channel_owner) {
    //check if we need to ask for a message
    d->sub->enqueue(d->sub);
    //TODO: use knowledge about the last message id to determine if the owner needs to be queried
    /*if(msg_id->time != 0 && msg_id->time == chanhead->last_msgid.time && msg_id->tag == chanhead->last_msgid.tag) {
      //we're here for the latest message, no need to check.
      return ngx_http_push_memstore_handle_get_message_reply(NULL, NGX_HTTP_PUSH_MESSAGE_EXPECTED, d);
    }
    else {
      
    }*/
    memstore_ipc_send_get_message(d->channel_owner, d->channel_id, d->msg_id, d);
    return NGX_OK;
  }
  else {
    chmsg = chanhead_find_next_message(chanhead, d->msg_id, &findmsg_status);
    return ngx_http_push_memstore_handle_get_message_reply(chmsg == NULL ? NULL : chmsg->msg, findmsg_status, d);
  }
}

ngx_int_t ngx_http_push_memstore_handle_get_message_reply(ngx_http_push_msg_t *msg, ngx_int_t findmsg_status, void *data) {
  subscribe_data_t           *d = (subscribe_data_t *)data;
  ngx_int_t                   ret;
  subscriber_t               *sub = d->sub;
  nhpm_channel_head_t        *chanhead = d->chanhead;
  callback_pt                 callback = d->cb;
  void                       *privdata = d->cb_privdata;
  
  switch(findmsg_status) {
    
    case NGX_HTTP_PUSH_MESSAGE_FOUND: //ok
      assert(msg != NULL);
      DBG("subscribe found message %i:%i", msg->message_time, msg->message_tag);
      switch(sub->cf->subscriber_concurrency) {

        case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_LASTIN:
          //kick everyone elese out, then subscribe
          ngx_http_push_memstore_publish_generic(chanhead, NULL, NGX_HTTP_CONFLICT, &NGX_HTTP_PUSH_HTTP_STATUS_409);
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

    case NGX_HTTP_PUSH_MESSAGE_NOTFOUND: //not found
      if(sub->cf->authorize_channel) {
        sub->respond_status(sub, NGX_HTTP_FORBIDDEN, NULL);
        callback(NGX_HTTP_NOT_FOUND, NULL, privdata);
        break;
      }
      //fall-through
    case NGX_HTTP_PUSH_MESSAGE_EXPECTED: //not yet available
      // ♫ It's gonna be the future soon ♫
      if(!d->already_enqueued) {
        ERR("memstore: Sub %p should already have been enqueued. ...", sub);
        sub->enqueue(sub);
      }
      ret = chanhead->spooler.add(&chanhead->spooler, sub);
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
  
  if(d->allocd) {
    ngx_free(d);
  }
  return NGX_OK;
}

static ngx_int_t chanhead_push_message(nhpm_channel_head_t *ch, nhpm_message_t *msg) {
  msg->next = NULL;
  msg->prev = ch->msg_last;
  if(msg->prev != NULL) {
    msg->prev->next = msg;
  }

  //set time and tag
  if(msg->msg->message_time == 0) {
    msg->msg->message_time = ngx_time();
  }
  if(ch->msg_last && ch->msg_last->msg->message_time == msg->msg->message_time) {
    msg->msg->message_tag = ch->msg_last->msg->message_tag + 1;
  }
  else {
    msg->msg->message_tag = 0;
  }

  if(ch->msg_first == NULL) {
    ch->msg_first = msg;
  }
  ch->channel.messages++;
  ch->shared->stored_message_count++;
  ch->shared->total_message_count++;

  ch->msg_last = msg;
  
  //DBG("create %i:%i %V", msg->msg->message_time, msg->msg->message_tag, chanhead_msg_to_str(msg));
  chanhead_messages_gc(ch);
  return ch->msg_last == msg ? NGX_OK : NGX_ERROR;
}

typedef struct {
  ngx_http_push_msg_t     msg;
  ngx_buf_t               buf;
  ngx_file_t              file;
} shmsg_memspace_t;

static ngx_http_push_msg_t *create_shm_msg(ngx_http_push_msg_t *m) {
  shmsg_memspace_t        *stuff;
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

  msg = &stuff->msg;
  buf = &stuff->buf;

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
  
  return msg;
}




static nhpm_message_t *create_shared_message(ngx_http_push_msg_t *m, ngx_int_t msg_in_shm) {
  nhpm_message_t          *chmsg;
  ngx_http_push_msg_t     *msg;
  
  if(msg_in_shm) {
    msg = m;
  }
  else {
    if((msg=create_shm_msg(m)) == NULL ) {
      return NULL;
    }
  }
  if((chmsg = ngx_alloc(sizeof(*chmsg), ngx_cycle->log)) != NULL) {
    chmsg->prev = NULL;
    chmsg->next = NULL;
    chmsg->msg  = msg;
  }
  return chmsg;
}

static ngx_int_t ngx_http_push_store_publish_message(ngx_str_t *channel_id, ngx_http_push_msg_t *msg, ngx_http_push_loc_conf_t *cf, callback_pt callback, void *privdata) {
  return ngx_http_push_store_publish_message_generic(channel_id, msg, 0, cf->buffer_timeout, cf->max_messages, cf->min_messages, callback, privdata);
}
  
ngx_int_t ngx_http_push_store_publish_message_generic(ngx_str_t *channel_id, ngx_http_push_msg_t *msg, ngx_int_t msg_in_shm, ngx_int_t msg_timeout, ngx_int_t max_msgs,  ngx_int_t min_msgs, callback_pt callback, void *privdata) {
  nhpm_channel_head_t     *chead;
  ngx_http_push_channel_t  channel_copy_data;
  ngx_http_push_channel_t *channel_copy = &channel_copy_data;
  nhpm_message_t          *shmsg_link;
  ngx_int_t                sub_count;
  ngx_http_push_msg_t     *publish_msg;
  ngx_int_t                owner = memstore_channel_owner(channel_id);
  ngx_int_t                rc;
  if(callback == NULL) {
    callback = empty_callback;
  }

  //this coould be dangerous!!
  if(msg->message_time==0) {
    msg->message_time = ngx_time();
  }
  msg->expires = ngx_time() + msg_timeout;
  
  if((chead = ngx_http_push_memstore_get_chanhead(channel_id)) == NULL) {
    ERR("can't get chanhead for id %V", channel_id);
    callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, privdata);
    return NGX_ERROR;
  }
  
  if(memstore_slot() != owner) {
    publish_msg = create_shm_msg(msg);
    memstore_ipc_send_publish_message(owner, channel_id, publish_msg, msg_timeout, max_msgs, min_msgs, callback, privdata);
    return NGX_OK;
  }
  
  chead->channel.expires = ngx_time() + msg_timeout;
  sub_count = chead->shared->sub_count;
  
  //TODO: address this weirdness
  //chead->min_messages = cf->min_messages;
  chead->min_messages = 0; // for backwards-compatibility, this value is ignored? weird...
  
  chead->max_messages = max_msgs;
  
  chanhead_messages_gc(chead);
  if(max_msgs == 0) {
    channel_copy=&chead->channel;
    publish_msg = msg;
    DBG("publish %i:%i expire %i ", msg->message_time, msg->message_tag, msg_timeout);
  }
  else {
    if((shmsg_link = create_shared_message(msg, msg_in_shm)) == NULL) {
      callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, privdata);
      ERR("can't create shared message for channel %V", channel_id);
      return NGX_ERROR;
    }

    if(chanhead_push_message(chead, shmsg_link) != NGX_OK) {
      callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, privdata);
      ERR("can't enqueue shared message for channel %V", channel_id);
      return NGX_ERROR;
    }
    ngx_memcpy(channel_copy, &chead->channel, sizeof(*channel_copy));
    channel_copy->subscribers = sub_count;
    assert(shmsg_link != NULL);
    assert(chead->msg_last == shmsg_link);
    publish_msg = shmsg_link->msg;
  }
  
  //do the actual publishing
  
  DBG("publish %i:%i expire %i", publish_msg->message_time, publish_msg->message_tag, msg_timeout);
  if(publish_msg->buf && publish_msg->buf->file) {
    DBG("fd %i", publish_msg->buf->file->fd);
  }
  rc = ngx_http_push_memstore_publish_generic(chead, publish_msg, 0, NULL);
  callback(rc, channel_copy, privdata);

  return rc;
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

