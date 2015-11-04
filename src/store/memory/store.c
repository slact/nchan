#include <nchan_module.h>

#include <assert.h>
#include "store.h"
#include "shmem.h"
#include "ipc.h"
#include "ipc-handlers.h"
#include "store-private.h"
#include "../spool.h"

static ngx_int_t max_worker_processes = 0;

typedef struct {
  ngx_event_t                     gc_timer;
  nchan_llist_timed_t            *gc_head;
  nchan_llist_timed_t            *gc_tail;
  nchan_store_channel_head_t      unbuffered_dummy_chanhead;
  store_channel_head_shm_t        dummy_shared_chaninfo;
  nchan_store_channel_head_t     *hash;
#if FAKESHARD
  ngx_int_t                       fake_slot;
#endif
} memstore_data_t;


static void init_mpt(memstore_data_t *m) {
  ngx_memzero(&m->gc_timer, sizeof(m->gc_timer));
  m->gc_head = NULL;
  m->gc_tail = NULL;
  ngx_memzero(&m->unbuffered_dummy_chanhead, sizeof(nchan_store_channel_head_t));
  m->unbuffered_dummy_chanhead.id.data= (u_char *)"unbuffered fake";
  m->unbuffered_dummy_chanhead.id.len=15;
  m->unbuffered_dummy_chanhead.owner = memstore_slot();
  m->unbuffered_dummy_chanhead.slot = memstore_slot();
  m->unbuffered_dummy_chanhead.status = READY;
  m->unbuffered_dummy_chanhead.min_messages = 0;
  m->unbuffered_dummy_chanhead.max_messages = (ngx_uint_t )-1;
  m->unbuffered_dummy_chanhead.shared = &m->dummy_shared_chaninfo;
}

#if FAKESHARD

static memstore_data_t  mdata[MAX_FAKE_WORKERS];
static memstore_data_t fake_default_mdata;
memstore_data_t *mpt = &fake_default_mdata;

ngx_int_t memstore_slot() {
  return mpt->fake_slot;
}

#else

static memstore_data_t  mdata;
memstore_data_t *mpt = &mdata;


ngx_int_t memstore_slot() {
  return ngx_process_slot;
}

#endif


static shmem_t         *shm = NULL;
static shm_data_t      *shdata = NULL;
static ipc_t           *ipc;

shmem_t *nchan_memstore_get_shm(void){
  return shm;
}

ipc_t *nchan_memstore_get_ipc(void){
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

#define NCHAN_DEFAULT_SUBSCRIBER_POOL_SIZE (5 * 1024)
#define NCHAN_DEFAULT_CHANHEAD_CLEANUP_INTERVAL 1000
#define NCHAN_CHANHEAD_EXPIRE_SEC 1
#define NCHAN_NOBUFFER_MSG_EXPIRE_SEC 10



//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG


#if FAKESHARD

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "MEMSTORE:(fake)%02i: " fmt, memstore_slot(), ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "MEMSTORE:(fake)%02i: " fmt, memstore_slot(), ##args)

static nchan_llist_timed_t *fakeprocess_top = NULL;
void memstore_fakeprocess_push(ngx_int_t slot) {
  assert(slot < MAX_FAKE_WORKERS);
  nchan_llist_timed_t *link = ngx_calloc(sizeof(*fakeprocess_top), ngx_cycle->log);
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

ngx_int_t memstore_fakeprocess_pop(void) {
  nchan_llist_timed_t   *next;
  if(fakeprocess_top == NULL) {
    DBG("can't pop empty fakeprocess stack");
    return 0;
  }
  next = fakeprocess_top->next;
  ngx_free(fakeprocess_top);
  if(next == NULL) {
    DBG("can't pop last item off of fakeprocess stack");
    return 0;
  }
  //DBG("pop fakeprocess to return to %i", (ngx_int_t)next->data);
  next->prev = NULL;
  fakeprocess_top = next;
  mpt = &mdata[(ngx_int_t )fakeprocess_top->data];
  return 1;
}

void memstore_fakeprocess_push_random(void) {
  return memstore_fakeprocess_push(rand() % MAX_FAKE_WORKERS);
}

#else

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "MEMSTORE:%02i: " fmt, memstore_slot(), ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "MEMSTORE:%02i: " fmt, memstore_slot(), ##args)

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


static ngx_int_t chanhead_messages_gc(nchan_store_channel_head_t *ch);

static void nchan_store_chanhead_gc_timer_handler(ngx_event_t *);

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


static ngx_int_t nchan_store_init_worker(ngx_cycle_t *cycle) {

#if FAKESHARD
  ngx_int_t        i;
  for(i = 0; i < MAX_FAKE_WORKERS; i++) {
  memstore_fakeprocess_push(i);
#endif
  
  
  init_mpt(mpt);
  
  if(mpt->gc_timer.handler == NULL) {
    mpt->gc_timer.handler=&nchan_store_chanhead_gc_timer_handler;
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


static void spooler_add_handler(channel_spooler_t *spl, subscriber_t *sub, void *privdata) {
  nchan_store_channel_head_t   *head = (nchan_store_channel_head_t *)privdata;
  head->sub_count++;
  head->channel.subscribers++;
  if(sub->type == INTERNAL) {
    head->internal_sub_count++;
    if(head->status == READY) {
      assert(head->shared != NULL);
      ngx_atomic_fetch_add(&head->shared->internal_sub_count, 1);
    }
  }
  else {
    if(head->status == READY) {
      assert(head->shared != NULL);
      ngx_atomic_fetch_add(&head->shared->sub_count, 1);
    }
  }
  head->last_subscribed = ngx_time();
  if(head->status == READY) {
    head->shared->last_seen = ngx_time();
  }
  assert(head->sub_count >= head->internal_sub_count);
}

static void spooler_bulk_dequeue_handler(channel_spooler_t *spl, subscriber_type_t type, ngx_int_t count, void *privdata) {
  nchan_store_channel_head_t   *head = (nchan_store_channel_head_t *)privdata;
  if (type == INTERNAL) {
    //internal subscribers are *special* and don't really count
    head->internal_sub_count -= count;
    head->shared->internal_sub_count -= count;
  }
  else if(head->shared){
    head->shared->sub_count -= count;
  }
  else if(head->shared == NULL) {
    assert(head->shutting_down == 1);
  }
  head->sub_count -= count;
  head->channel.subscribers = head->sub_count - head->internal_sub_count;
  assert(head->sub_count >= 0);
  assert(head->internal_sub_count >= 0);
  assert(head->channel.subscribers >= 0);
  assert(head->sub_count >= head->internal_sub_count);
  if(head->sub_count == 0 && head->foreign_owner_ipc_sub == NULL) {
    chanhead_gc_add(head, "sub count == 0 after spooler dequeue");
  }
}

static ngx_int_t start_chanhead_spooler(nchan_store_channel_head_t *head) {
  start_spooler(&head->spooler, &head->id, &head->status, &nchan_store_memory);
  head->spooler.fn->set_add_handler(&head->spooler, spooler_add_handler, head);
  head->spooler.fn->set_bulk_dequeue_handler(&head->spooler, spooler_bulk_dequeue_handler, head);
  return NGX_OK;
}

static ngx_int_t ensure_chanhead_is_ready(nchan_store_channel_head_t *head) {
  ngx_int_t                      owner = memstore_channel_owner(&head->id);
  if(head == NULL) {
    return NGX_OK;
  }
  DBG("ensure chanhead ready: chanhead %p, status %i, foreign_ipc_sub:%p", head, head->status, head->foreign_owner_ipc_sub);
  if(head->status == INACTIVE) {//recycled chanhead
    chanhead_gc_withdraw(head, "readying INACTIVE");
  }
  if(!head->spooler.running) {
    DBG("ensure chanhead ready: Spooler for channel %p %V wasn't running. start it.", head, &head->id);
    start_chanhead_spooler(head);
  }
  
  if(owner != memstore_slot()) {
    if(head->foreign_owner_ipc_sub == NULL && head->status != WAITING) {
      DBG("ensure chanhead ready: request for %V from %i to %i", &head->id, memstore_slot(), owner);
      head->status = WAITING;
      memstore_ipc_send_subscribe(owner, &head->id, head);
    }
    else if(head->foreign_owner_ipc_sub != NULL && head->status == WAITING) {
      DBG("ensure chanhead ready: subscribe request for %V from %i to %i", &head->id, memstore_slot(), owner);
      head->status = READY;
      head->spooler.fn->handle_channel_status_change(&head->spooler);
    }
  }
  else {
    head->status = READY;
    head->spooler.fn->handle_channel_status_change(&head->spooler);
  }
  return NGX_OK;
}


nchan_store_channel_head_t * chanhead_memstore_find(ngx_str_t *channel_id) {
  nchan_store_channel_head_t     *head;
  assert(channel_id->data != NULL);
  CHANNEL_HASH_FIND(channel_id, head);
  return head;
}


static nchan_store_channel_head_t *chanhead_memstore_create(ngx_str_t *channel_id) {
  nchan_store_channel_head_t         *head;
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
  head->foreign_owner_ipc_sub = NULL;
  head->last_subscribed = 0;
  //set channel
  ngx_memcpy(&head->channel.id, &head->id, sizeof(ngx_str_t));
  head->channel.message_queue=NULL;
  head->channel.messages = 0;
  head->channel.subscribers = 0;
  head->channel.last_seen = ngx_time();
  head->min_messages = 0;
  head->max_messages = (ngx_int_t) -1;
  
  head->latest_msgid.time = 0;
  head->latest_msgid.tag = 0;
  head->oldest_msgid.time = 0;
  head->oldest_msgid.tag = 0;
  
  head->spooler.running=0;
  start_chanhead_spooler(head);

  CHANNEL_HASH_ADD(head);
  
  return head;
}

nchan_store_channel_head_t * nchan_memstore_find_chanhead(ngx_str_t *channel_id) {
  nchan_store_channel_head_t     *head = NULL;
  if((head = chanhead_memstore_find(channel_id)) != NULL) {
    ensure_chanhead_is_ready(head);
  }
  return head;
}

nchan_store_channel_head_t * nchan_memstore_get_chanhead(ngx_str_t *channel_id) {
  nchan_store_channel_head_t          *head;
  head = chanhead_memstore_find(channel_id);
  if(head==NULL) {
    head = chanhead_memstore_create(channel_id);
  }
  ensure_chanhead_is_ready(head);
  return head;
}

ngx_int_t chanhead_gc_add(nchan_store_channel_head_t *head, const char *reason) {
  nchan_llist_timed_t         *chanhead_cleanlink;
  ngx_int_t                   slot = memstore_slot();
  DBG("Chanhead gc add %p %V: %s", head, &head->id, reason);
  chanhead_cleanlink = &head->cleanlink;
  if(!head->shutting_down) {
    assert(head->foreign_owner_ipc_sub == NULL); //we don't accept still-subscribed chanheads
  }
  if(head->slot != head->owner) {
    head->shared = NULL;
  }
  assert(head->slot == slot);
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
    DBG("gc_add chanhead %V: already added", &head->id);
  }

  //initialize gc timer
  if(! mpt->gc_timer.timer_set) {
    mpt->gc_timer.data=mpt->gc_head; //don't really care whre this points, so long as it's not null (for some debugging)
    ngx_add_timer(&mpt->gc_timer, NCHAN_DEFAULT_CHANHEAD_CLEANUP_INTERVAL);
  }

  return NGX_OK;
}

ngx_int_t chanhead_gc_withdraw(nchan_store_channel_head_t *chanhead, const char *reason) {
  //remove from gc list if we're there
  nchan_llist_timed_t    *cl;
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
static ngx_str_t *msg_to_str(nchan_msg_t *msg) {
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

static ngx_str_t *chanhead_msg_to_str(store_message_t *msg) {
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

ngx_int_t nchan_memstore_publish_generic(nchan_store_channel_head_t *head, nchan_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line) {
  
  ngx_int_t          shared_sub_count = 0;
  
  if(head->shared) {
    assert(head->status == READY);
    shared_sub_count = head->shared->sub_count;
  }

  if(head==NULL) {
    if(msg) {
      ERR("tried publishing %i:%i with a NULL chanhead", msg->id.time, msg->id.tag);
    }
    else {
      ERR("tried publishing status %i msg with a NULL chanhead", status_code);
    }
    return NCHAN_MESSAGE_QUEUED;
  }

  if(msg) {
    DBG("tried publishing %i:%i to chanhead %p (subs: %i)", msg->id.time, msg->id.tag, head, head->sub_count);
    head->spooler.fn->respond_message(&head->spooler, msg);
  }
  else {
    DBG("tried publishing status %i to chanhead %p (subs: %i)", status_code, head, head->sub_count);
    head->spooler.fn->respond_status(&head->spooler, status_code, status_line);
  }
    
  //TODO: be smarter about garbage-collecting chanheads
  if(memstore_channel_owner(&head->id) == memstore_slot()) {
    //the owner is responsible for the chanhead and its interprocess siblings
    //when removed, said siblings will be notified via IPC
    chanhead_gc_add(head, "add owner chanhead after publish");
  }
  
  if(head->shared) {
    head->channel.subscribers = head->shared->sub_count;
  }
  
  return (shared_sub_count > 0) ? NCHAN_MESSAGE_RECEIVED : NCHAN_MESSAGE_QUEUED;
}

static ngx_int_t chanhead_messages_delete(nchan_store_channel_head_t *ch);

static void handle_chanhead_gc_queue(ngx_int_t force_delete) {
  nchan_llist_timed_t          *cur, *next;
  nchan_store_channel_head_t   *ch = NULL;
  ngx_int_t                     owner;
  DBG("handling chanhead GC queue");
#if FAKESHARD
  ngx_int_t        i;
  for(i = 0; i < MAX_FAKE_WORKERS; i++) {
  memstore_fakeprocess_push(i);
#endif
  
  
  for(cur=mpt->gc_head ; cur != NULL; cur=next) {
    ch = (nchan_store_channel_head_t *)cur->data;
    next=cur->next;
    if(force_delete || ngx_time() - cur->time > NCHAN_CHANHEAD_EXPIRE_SEC) {
      
      
      if (ch->sub_count > 0) { //there are subscribers
        if(force_delete) {
          DBG("chanhead %p (%V) is still in use by %i subscribers. Try to delete it anyway.", ch, &ch->id, ch->sub_count);
          //ch->spooler.prepare_to_stop(&ch->spooler);
          ch->spooler.fn->respond_status(&ch->spooler, NGX_HTTP_GONE, &NCHAN_HTTP_STATUS_410);
          assert(ch->sub_count == 0);
        }
      }
      
      force_delete ? chanhead_messages_delete(ch) : chanhead_messages_gc(ch);
      
      if(ch->msg_first != NULL) {
        assert(ch->channel.messages != 0);
        DBG("chanhead %p (%V) is still storing %i messages.", ch, &ch->id, ch->channel.messages);
        break;
      }
      
      if (ch->sub_count == 0 && ch->channel.messages == 0) {
        //end this crazy channel
        assert(ch->msg_first == NULL);
        
        stop_spooler(&ch->spooler);
        owner = memstore_channel_owner(&ch->id);
        if(owner == memstore_slot()) {
          shm_free(shm, ch->shared);
        }
        DBG("chanhead %p (%V) is empty and expired. DELETE.", ch, &ch->id);
        CHANNEL_HASH_DEL(ch);
        ngx_free(ch);
      }
      else if(force_delete) {
        //counldn't force-delete channel, even though we tried
        assert(0);
      }
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

static ngx_int_t handle_unbuffered_messages_gc(ngx_int_t force_delete);

static void nchan_store_chanhead_gc_timer_handler(ngx_event_t *ev) {
  nchan_llist_timed_t  *head = mpt->gc_head;
  handle_chanhead_gc_queue(0);
  handle_unbuffered_messages_gc(0);
  if (!(ngx_quit || ngx_terminate || ngx_exiting || head == NULL || mpt->unbuffered_dummy_chanhead.msg_first == NULL)) {
    DBG("re-adding chanhead gc event timer");
    ngx_add_timer(ev, NCHAN_DEFAULT_CHANHEAD_CLEANUP_INTERVAL);
  }
  else if(head == NULL) {
    DBG("chanhead gc queue looks empty, stop gc_queue handler");
  }
}

static ngx_int_t empty_callback(){
  return NGX_OK;
}

static ngx_int_t chanhead_delete_message(nchan_store_channel_head_t *ch, store_message_t *msg);

static ngx_int_t nchan_store_delete_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  ngx_int_t                owner = memstore_channel_owner(channel_id);
  if(memstore_slot() != owner) {
    memstore_ipc_send_delete(owner, channel_id, callback, privdata);
  }
  else {
    nchan_memstore_force_delete_channel(channel_id, callback, privdata);
  }
  return NGX_OK;
}

ngx_int_t nchan_memstore_force_delete_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  nchan_store_channel_head_t    *ch;
  nchan_channel_t                chaninfo_copy;
  store_message_t               *msg = NULL;
  
  assert(memstore_channel_owner(channel_id) == memstore_slot());
  
  if(callback == NULL) {
    callback = empty_callback;
  }
  if((ch = nchan_memstore_find_chanhead(channel_id))) {
    chaninfo_copy.messages = ch->shared->stored_message_count;
    chaninfo_copy.subscribers = ch->shared->sub_count;
    chaninfo_copy.last_seen = ch->shared->last_seen;
    
    nchan_memstore_publish_generic(ch, NULL, NGX_HTTP_GONE, &NCHAN_HTTP_STATUS_410);
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

static ngx_int_t nchan_store_find_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  ngx_int_t                    owner = memstore_channel_owner(channel_id);
  nchan_store_channel_head_t  *ch;
  
  if(memstore_slot() == owner) {
    ch = nchan_memstore_find_chanhead(channel_id);
    callback(NGX_OK, ch != NULL ? &ch->channel : NULL , privdata);
  }
  else {
    memstore_ipc_send_get_channel_info(owner, channel_id, callback, privdata);
  }
  return NGX_OK;
}

//initialization
static ngx_int_t nchan_store_init_module(ngx_cycle_t *cycle) {

#if FAKESHARD

  max_worker_processes = MAX_FAKE_WORKERS;
  
  ngx_int_t          i;
  memstore_data_t   *cur;
  for(i = 0; i < MAX_FAKE_WORKERS; i++) {
    cur = &mdata[i];
    cur->fake_slot = i;
  }
  
  memstore_fakeprocess_push(0);

#else

  ngx_core_conf_t    *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  max_worker_processes = ccf->worker_processes;

#endif
  
  DBG("memstore init_module pid %p", ngx_pid);

  //initialize our little IPC
  ipc = ipc_create(cycle);
  ipc_open(ipc,cycle, max_worker_processes);
  ipc_set_handler(ipc, memstore_ipc_alert_handler);

  return NGX_OK;
}

static ngx_int_t nchan_store_init_postconfig(ngx_conf_t *cf) {
  nchan_main_conf_t     *conf = ngx_http_conf_get_module_main_conf(cf, nchan_module);
  ngx_str_t              name = ngx_string("memstore");
  if(conf->shm_size==NGX_CONF_UNSET_SIZE) {
    conf->shm_size=NCHAN_DEFAULT_SHM_SIZE;
  }
  shm = shm_create(&name, cf, conf->shm_size, initialize_shm, &nchan_module);
  return NGX_OK;
}

static void nchan_store_create_main_conf(ngx_conf_t *cf, nchan_main_conf_t *mcf) {
  mcf->shm_size=NGX_CONF_UNSET_SIZE;
}

static void nchan_store_exit_worker(ngx_cycle_t *cycle) {
  nchan_store_channel_head_t         *cur, *tmp;
  DBG("exit worker %i", ngx_pid);
  
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
  handle_unbuffered_messages_gc(1);
  
  if(mpt->gc_timer.timer_set) {
    ngx_del_timer(&mpt->gc_timer);
  }
#if FAKESHARD
  memstore_fakeprocess_pop();
  }
#endif
  
  ipc_close(ipc, cycle);
  ipc_destroy(ipc, cycle); //only for this worker...
  
  shm_destroy(shm); //just for this worker...
#if FAKESHARD
  while(memstore_fakeprocess_pop()) {  };
#endif
}

static void nchan_store_exit_master(ngx_cycle_t *cycle) {
  DBG("exit master from pid %i", ngx_pid);
  
  ipc_close(ipc, cycle);
  ipc_destroy(ipc, cycle);
#if FAKESHARD
  while(memstore_fakeprocess_pop()) {  };
 #endif
  shm_free(shm, shdata);
  shm_destroy(shm);
}

static ngx_int_t validate_chanhead_messages(nchan_store_channel_head_t *ch) {
  /*
  ngx_int_t              count = ch->channel.messages;
  ngx_int_t              rev_count = count;
  ngx_int_t              owner = memstore_channel_owner(&ch->id);
  store_message_t        *cur;
  
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
  */
  return NGX_OK;
}

static ngx_int_t chanhead_withdraw_message(nchan_store_channel_head_t *ch, store_message_t *msg) {
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
  
  ch->channel.messages--;
  
  ngx_atomic_fetch_add(&ch->shared->stored_message_count, -1);
  
  if(ch->channel.messages == 0) {
    assert(ch->msg_first == NULL);
    assert(ch->msg_last == NULL);
  }
  validate_chanhead_messages(ch);
  return NGX_OK;
}

static ngx_int_t delete_withdrawn_message( store_message_t *msg ) {
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
#if NCHAN_MSG_LEAK_DEBUG  
  msg_debug_remove(msg->msg);
#endif
  ngx_memset(msg->msg, 0xFA, sizeof(*msg->msg)); //debug stuff
  shm_free(shm, msg->msg);
  
  ngx_memset(msg, 0xBC, sizeof(*msg)); //debug stuff
  ngx_free(msg);
  return NGX_OK;
}

static ngx_int_t chanhead_delete_message(nchan_store_channel_head_t *ch, store_message_t *msg) {
  validate_chanhead_messages(ch);
  if(chanhead_withdraw_message(ch, msg) == NGX_OK) {
    DBG("delete msg %i:%i", msg->msg->id.time, msg->msg->id.tag);
    delete_withdrawn_message(msg);
  }
  else {
    ERR("failed to withdraw and delete message %i:%i", msg->msg->id.time, msg->msg->id.tag);
  }
  validate_chanhead_messages(ch);
  return NGX_OK;
}

static ngx_int_t chanhead_messages_gc_custom(nchan_store_channel_head_t *ch, ngx_uint_t min_messages, ngx_uint_t max_messages) {
  validate_chanhead_messages(ch);
  store_message_t   *cur = ch->msg_first;
  store_message_t   *next = NULL;
  time_t             now = ngx_time();
  ngx_int_t          started_count, tried_count, deleted_count;
  DBG("chanhead_gc max %i min %i count %i", max_messages, min_messages, ch->channel.messages);
  
  started_count = ch->channel.messages;
  tried_count = 0;
  deleted_count = 0;
  
  //is the message queue too big?
  while(cur != NULL && ch->channel.messages > max_messages) {
    tried_count++;
    next = cur->next;
    if(cur->msg->refcount > 0) {
      DBG("msg %p refcount %i > 0", &cur->msg, cur->msg->refcount); //not a big deal
    }
    else {
      DBG("delete queue-too-big msg %i:%i", cur->msg->id.time, cur->msg->id.tag);
      chanhead_delete_message(ch, cur);
      deleted_count++;
    }
    cur = next;
  }
  
  while(cur != NULL && ch->channel.messages > min_messages && now > cur->msg->expires) {
    tried_count++;
    next = cur->next;
    if(cur->msg->refcount > 0) {
      DBG("msg %p refcount %i > 0", &cur->msg, cur->msg->refcount);
    }
    else {
      DBG("delete msg %p");
      chanhead_delete_message(ch, cur);
      deleted_count++;
    }
    cur = next;
  }
  DBG("message GC results: started with %i, walked %i, deleted %i msgs", started_count, tried_count, deleted_count);
  validate_chanhead_messages(ch);
  return NGX_OK;
}

static ngx_int_t chanhead_messages_gc(nchan_store_channel_head_t *ch) {
  //DBG("messages gc for ch %p %V", ch, &ch->id);
  return chanhead_messages_gc_custom(ch, ch->min_messages, ch->max_messages);
}

static ngx_int_t chanhead_messages_delete(nchan_store_channel_head_t *ch) {
  chanhead_messages_gc_custom(ch, 0, 0);
  return NGX_OK;
}

static ngx_int_t handle_unbuffered_messages_gc(ngx_int_t force_delete) {
  nchan_store_channel_head_t         *ch = &mpt->unbuffered_dummy_chanhead;
  DBG("handling unbuffered messages GC queue");
  
  #if FAKESHARD
  ngx_int_t        i;
  for(i = 0; i < MAX_FAKE_WORKERS; i++) {
  memstore_fakeprocess_push(i);
  #endif
  if(!force_delete) {
    chanhead_messages_gc(ch);
  }
  else {
    chanhead_messages_delete(ch);
  }
  
  #if FAKESHARD
  memstore_fakeprocess_pop();
  }
  #endif
  return NGX_OK;
}

store_message_t *chanhead_find_next_message(nchan_store_channel_head_t *ch, nchan_msg_id_t *msgid, nchan_msg_status_t *status) {
  store_message_t      *cur, *first;
  DBG("find next message %i:%i", msgid->time, msgid->tag);
  if(ch == NULL) {
    *status = MSG_NOTFOUND;
    return NULL;
  }
  chanhead_messages_gc(ch);
  
  first = ch->msg_first;
  cur = ch->msg_last;
  
  if(cur == NULL) {
    if(msgid->time == 0 && msgid->tag == 0) {
      *status = MSG_EXPECTED;
    }
    else {
      *status = MSG_NOTFOUND;
    }
    return NULL;
  }

  if(msgid == NULL || (msgid->time == 0 && msgid->tag == 0)) {
    DBG("found message %i:%i", first->msg->id.time, first->msg->id.tag);
    *status = MSG_FOUND;
    return first;
  }

  while(cur != NULL) {
    //DBG("cur: %i:%i %V", cur->msg->id.time, cur->msg->id.tag, chanhead_msg_to_str(cur));
    
    if(msgid->time > cur->msg->id.time || (msgid->time == cur->msg->id.time && msgid->tag >= cur->msg->id.tag)){
      if(cur->next != NULL) {
        *status = MSG_FOUND;
        DBG("found message %i:%i", cur->next->msg->id.time, cur->next->msg->id.tag);
        return cur->next;
      }
      else {
        *status = MSG_EXPECTED;
        return NULL;
      }
    }
    cur=cur->prev;
  }
  //DBG("looked everywhere, not found");
  *status = MSG_NOTFOUND;
  return NULL;
}

typedef struct {
  subscriber_t                *sub;
  ngx_int_t                    channel_owner;
  nchan_store_channel_head_t  *chanhead;
  ngx_str_t                   *channel_id;
  nchan_msg_id_t               msg_id;
  callback_pt                  cb;
  void                        *cb_privdata;
  unsigned                     reserved:1;
  unsigned                     subbed:1;
  unsigned                     allocd:1;
} subscribe_data_t;

static subscribe_data_t        static_subscribe_data;

static subscribe_data_t *subscribe_data_alloc(ngx_int_t owner) {
  subscribe_data_t            *d;
  if(memstore_slot() != owner) {
    d = ngx_alloc(sizeof(*d), ngx_cycle->log);
    d->allocd = 1;
  }
  else {
    d = &static_subscribe_data;
    d->allocd = 0;
  }
  return d;
}

static void subscribe_data_free(subscribe_data_t *d) {
  if(d->allocd) {
    ngx_free(d);
  }
}

#define SUB_CHANNEL_UNAUTHORIZED 0
#define SUB_CHANNEL_AUTHORIZED 1
#define SUB_CHANNEL_NOTSURE 2

static ngx_int_t nchan_store_subscribe_sub_reserved_check(ngx_int_t channel_status, void* _, subscribe_data_t *d);
static ngx_int_t nchan_store_subscribe_continued(ngx_int_t channel_status, void* _, subscribe_data_t *d);

static ngx_int_t nchan_store_subscribe(ngx_str_t *channel_id, nchan_msg_id_t *msg_id, subscriber_t *sub, callback_pt callback, void *privdata) {
  ngx_int_t                    owner = memstore_channel_owner(channel_id);
  subscribe_data_t            *d = subscribe_data_alloc(owner);
  
  assert(d != NULL);
  assert(callback != NULL);
  
  d->msg_id = *msg_id;
  
  d->channel_owner = owner;
  d->channel_id = channel_id;
  d->cb = callback;
  d->cb_privdata = privdata;
  d->sub = sub;
  d->subbed = 0;
  d->reserved = 0;
  
  DBG("subscribe msgid %i:%i", msg_id->time, msg_id->tag);
  
  if(sub->cf->authorize_channel) {
    sub->fn->reserve(sub);
    d->reserved = 1;
    if(memstore_slot() != owner) {
      memstore_ipc_send_does_channel_exist(owner, channel_id, (callback_pt )nchan_store_subscribe_sub_reserved_check, d);
    }
    else {
      nchan_store_subscribe_continued(SUB_CHANNEL_NOTSURE, NULL, d);
    }
  }
  else {
    nchan_store_subscribe_continued(SUB_CHANNEL_AUTHORIZED, NULL, d);
  }
  
  return NGX_OK;
}

static ngx_int_t nchan_store_subscribe_sub_reserved_check(ngx_int_t channel_status, void* _, subscribe_data_t *d) {
  if(d->sub->fn->release(d->sub, 0) == NGX_OK) {
    d->reserved = 0;
    return nchan_store_subscribe_continued(channel_status, _, d);
  }
  else {//don't go any further, the sub has been deleted
    assert(d->sub->reserved == 0);
    subscribe_data_free(d);
    return NGX_OK;
  }
}

static ngx_int_t nchan_store_subscribe_continued(ngx_int_t channel_status, void* _, subscribe_data_t *d) {
  nchan_store_channel_head_t  *chanhead = NULL;
  //store_message_t             *chmsg;
  //nchan_msg_status_t           findmsg_status;
  switch(channel_status) {
    case SUB_CHANNEL_AUTHORIZED:
      chanhead = nchan_memstore_get_chanhead(d->channel_id);
      break;
    case SUB_CHANNEL_UNAUTHORIZED:
      chanhead = NULL;
      break;
    case SUB_CHANNEL_NOTSURE:
      chanhead = nchan_memstore_find_chanhead(d->channel_id);
      break;
  }
  
  if (chanhead == NULL) {
    d->sub->fn->respond_status(d->sub, NGX_HTTP_FORBIDDEN, NULL);
    
    if(d->reserved) {
      d->sub->fn->release(d->sub, 0);
      d->reserved = 0;
    }
    
    //sub should be destroyed by now.
    
    d->sub = NULL; //debug
    d->cb(NGX_HTTP_NOT_FOUND, NULL, d->cb_privdata);
    subscribe_data_free(d);
    return NGX_OK;
  }
  
  d->chanhead = chanhead;

  if(d->reserved) {
    d->sub->fn->release(d->sub, 1);
    d->reserved = 0;
  }
  
  chanhead->spooler.fn->add(&chanhead->spooler, d->sub);

  subscribe_data_free(d);

  return NGX_OK;
}

static ngx_int_t nchan_store_async_get_message(ngx_str_t *channel_id, nchan_msg_id_t *msg_id, callback_pt callback, void *privdata) {
  store_message_t             *chmsg;
  ngx_int_t                    owner = memstore_channel_owner(channel_id);
  subscribe_data_t            *d = subscribe_data_alloc(owner);
  nchan_msg_status_t           findmsg_status;
  
  if(callback==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "no callback given for async get_message. someone's using the API wrong!");
    return NGX_ERROR;
  }
  
  d->channel_owner = owner;
  d->channel_id = channel_id;
  d->cb = callback;
  d->cb_privdata = privdata;
  d->sub = NULL;
  ngx_memcpy(&d->msg_id, msg_id, sizeof(*msg_id));
  d->chanhead = nchan_memstore_find_chanhead(d->channel_id);
  
  if(memstore_slot() != owner) {
    //check if we need to ask for a message
    memstore_ipc_send_get_message(d->channel_owner, d->channel_id, &d->msg_id, d);
  }
  else {
    chmsg = chanhead_find_next_message(d->chanhead, &d->msg_id, &findmsg_status);
    return nchan_memstore_handle_get_message_reply(chmsg == NULL ? NULL : chmsg->msg, findmsg_status, d);
  }
  
  return NGX_OK; //async only now!
}

ngx_int_t nchan_memstore_handle_get_message_reply(nchan_msg_t *msg, nchan_msg_status_t findmsg_status, void *data) {
  subscribe_data_t           *d = (subscribe_data_t *)data;
  //nchan_store_channel_head_t *chanhead = d->chanhead;
  
  d->cb(findmsg_status, msg, d->cb_privdata);
  
  subscribe_data_free(d);
  return NGX_OK;
}

static ngx_int_t chanhead_push_message(nchan_store_channel_head_t *ch, store_message_t *msg) {
  msg->next = NULL;
  msg->prev = ch->msg_last;
  if(msg->prev != NULL) {
    msg->prev->next = msg;
    msg->msg->prev_id.time = msg->prev->msg->id.time;
    msg->msg->prev_id.tag = msg->prev->msg->id.tag;
  }
  else {
    msg->msg->prev_id.time = 0;
    msg->msg->prev_id.tag = 0;
  }
  
  //set time and tag
  if(msg->msg->id.time == 0) {
    msg->msg->id.time = ngx_time();
  }
  if(ch->msg_last && ch->msg_last->msg->id.time == msg->msg->id.time) {
    msg->msg->id.tag = ch->msg_last->msg->id.tag + 1;
  }
  else {
    msg->msg->id.tag = 0;
  }

  if(ch->msg_first == NULL) {
    ch->msg_first = msg;
  }
  ch->channel.messages++;
  ngx_atomic_fetch_add(&ch->shared->stored_message_count, 1);
  ngx_atomic_fetch_add(&ch->shared->total_message_count, 1);

  ch->msg_last = msg;
  
  //DBG("create %i:%i %V", msg->msg->message_time, msg->msg->message_tag, chanhead_msg_to_str(msg));
  chanhead_messages_gc(ch);
  assert(ch->msg_last == msg); //why does this happen?
  return ch->msg_last == msg ? NGX_OK : NGX_ERROR;
}

typedef struct {
  nchan_msg_t             msg;
  ngx_buf_t               buf;
  ngx_file_t              file;
} shmsg_memspace_t;

static nchan_msg_t *create_shm_msg(nchan_msg_t *m) {
  shmsg_memspace_t        *stuff;
  nchan_msg_t             *msg;
  ngx_buf_t               *mbuf = NULL, *buf=NULL;
  mbuf = m->buf;
  
  size_t                  total_sz, buf_body_size = 0, content_type_size = 0, buf_filename_size = 0;
  
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
  
  total_sz = sizeof(*stuff) + (buf_filename_size + content_type_size + buf_body_size);
#if NCHAN_MSG_LEAK_DEBUG
  size_t    debug_sz = m->lbl.len;
  total_sz += debug_sz;
#endif
  
  if((stuff = shm_calloc(shm, total_sz, "message")) == NULL) {
    ERR("can't allocate 'shared' memory for msg for channel id");
    return NULL;
  }
  
  msg = &stuff->msg;
  buf = &stuff->buf;
  
  ngx_memcpy(msg, m, sizeof(*msg));
  ngx_memcpy(buf, mbuf, sizeof(*buf));
  
  msg->buf = buf;
  
  msg->content_type.data = (u_char *)&stuff[1] + buf_filename_size;
  
  msg->content_type.len = content_type_size;
  if(content_type_size > 0) {
    ngx_memcpy(msg->content_type.data, m->content_type.data, content_type_size);
  }
  else {
    msg->content_type.data = NULL; //mostly for debugging, this isn't really necessary for correct operation.
  }
  
  if(buf_body_size > 0) {
    buf->pos = (u_char *)&stuff[1] + buf_filename_size + content_type_size;
    buf->last = buf->pos + buf_body_size;
    buf->start = buf->pos;
    buf->end = buf->last;
    ngx_memcpy(buf->pos, mbuf->pos, buf_body_size);
  }
  
  if(mbuf->file!=NULL) {
    buf->file = &stuff->file;
    ngx_memcpy(buf->file, mbuf->file, sizeof(*buf->file));
    
    buf->file->fd =NGX_INVALID_FILE;
    buf->file->log = ngx_cycle->log;
    
    buf->file->name.data = (u_char *)&stuff[1];
    
    ngx_memcpy(buf->file->name.data, mbuf->file->name.data, buf_filename_size-1);
  }
  msg->shared=1;
  
#if NCHAN_MSG_LEAK_DEBUG  
  msg->rsv = NULL;
  msg->lbl.len = m->lbl.len;
  msg->lbl.data = (u_char *)stuff + (total_sz - debug_sz);
  ngx_memcpy(msg->lbl.data, m->lbl.data, msg->lbl.len);
  
  msg_debug_add(msg);
#endif
  
  return msg;
}

void msg_reserve(nchan_msg_t *msg, char *lbl) {
  ngx_atomic_fetch_add(&msg->refcount, 1);
#if NCHAN_MSG_LEAK_DEBUG  
  msg_rsv_dbg_t     *rsv;
  rsv=shm_alloc(shm, sizeof(*rsv) + ngx_strlen(lbl), "msgdebug");
  rsv->lbl = (char *)(&rsv[1]);
  ngx_memcpy(rsv->lbl, lbl, ngx_strlen(lbl));
  if(msg->rsv == NULL) {
    msg->rsv = rsv;
    rsv->prev = NULL;
    rsv->next = NULL;
  }
  else {
    msg->rsv->prev = rsv;
    rsv->next = msg->rsv;
    rsv->prev = NULL;
    msg->rsv = rsv;
  }
#endif
}
void msg_release(nchan_msg_t *msg, char *lbl) {
  ngx_atomic_fetch_add(&msg->refcount, -1);
  assert(msg->refcount >= 0);
#if NCHAN_MSG_LEAK_DEBUG
  msg_rsv_dbg_t     *cur, *prev, *next;
  size_t             sz = ngx_strlen(lbl);
  for(cur = msg->rsv; cur != NULL; cur = cur->next) {
    if(ngx_memcmp(lbl, cur->lbl, sz) == 0) {
      prev = cur->prev;
      next = cur->next;
      if(prev) {
        prev->next = next;
      }
      if(next) {
        next->prev = prev;
      }
      if(cur == msg->rsv) {
        msg->rsv = next;
      }
      shm_free(shm, cur);
      break;
    }
  }
#endif
}

static store_message_t *create_shared_message(nchan_msg_t *m, ngx_int_t msg_already_in_shm) {
  store_message_t          *chmsg;
  nchan_msg_t              *msg;
  
  if(msg_already_in_shm) {
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

static ngx_int_t nchan_store_publish_message(ngx_str_t *channel_id, nchan_msg_t *msg, nchan_loc_conf_t *cf, callback_pt callback, void *privdata) {
  return nchan_store_publish_message_generic(channel_id, msg, 0, cf->buffer_timeout, cf->max_messages, cf->min_messages, callback, privdata);
}
  
ngx_int_t nchan_store_publish_message_generic(ngx_str_t *channel_id, nchan_msg_t *msg, ngx_int_t msg_in_shm, ngx_int_t msg_timeout, ngx_int_t max_msgs,  ngx_int_t min_msgs, callback_pt callback, void *privdata) {
  nchan_store_channel_head_t  *chead;
  nchan_channel_t              channel_copy_data;
  nchan_channel_t             *channel_copy = &channel_copy_data;
  store_message_t             *shmsg_link;
  ngx_int_t                    sub_count;
  nchan_msg_t                 *publish_msg;
  ngx_int_t                    owner = memstore_channel_owner(channel_id);
  ngx_int_t                    rc;
  if(callback == NULL) {
    callback = empty_callback;
  }

  //this coould be dangerous!!
  if(msg->id.time==0) {
    msg->id.time = ngx_time();
  }
  msg->expires = ngx_time() + msg_timeout;
  
  if((chead = nchan_memstore_get_chanhead(channel_id)) == NULL) {
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
    ///no buffer
    channel_copy=&chead->channel;
    
    if((shmsg_link = create_shared_message(msg, msg_in_shm)) == NULL) {
      callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, privdata);
      ERR("can't create unbuffered message for channel %V", channel_id);
      return NGX_ERROR;
    }
    publish_msg= shmsg_link->msg;
    publish_msg->expires = ngx_time() + NCHAN_NOBUFFER_MSG_EXPIRE_SEC;
    
    if(chanhead_push_message(&mpt->unbuffered_dummy_chanhead, shmsg_link) != NGX_OK) {
      callback(NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, privdata);
      ERR("can't enqueue unbuffered message for channel %V", channel_id);
      return NGX_ERROR;
    }
    
    publish_msg->prev_id = chead->latest_msgid;
    
    DBG("publish unbuffer msg %i:%i expire %i ", publish_msg->id.time, publish_msg->id.tag, msg_timeout);
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
  
  chead->latest_msgid = publish_msg->id;
  
  //do the actual publishing
  
  DBG("publish %i:%i expire %i", publish_msg->id.time, publish_msg->id.tag, msg_timeout);
  if(publish_msg->buf && publish_msg->buf->file) {
    DBG("fd %i", publish_msg->buf->file->fd);
  }
  rc = nchan_memstore_publish_generic(chead, publish_msg, 0, NULL);
  callback(rc, channel_copy, privdata);

  return rc;
}

nchan_store_t  nchan_store_memory = {
    //init
    &nchan_store_init_module,
    &nchan_store_init_worker,
    &nchan_store_init_postconfig,
    &nchan_store_create_main_conf,
    
    //shutdown
    &nchan_store_exit_worker,
    &nchan_store_exit_master,
    
    //async-friendly functions with callbacks
    &nchan_store_async_get_message, //+callback
    &nchan_store_subscribe, //+callback
    &nchan_store_publish_message, //+callback
    
    &nchan_store_delete_channel, //+callback
    &nchan_store_find_channel, //+callback
    
    //message stuff
    NULL,
    NULL,
    
};

