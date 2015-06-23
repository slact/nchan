#include <ngx_http_push_module.h>

#include <assert.h>
#include "store.h"
#include <store/ngx_rwlock.h>
#include "rbtree_util.h"

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

struct nhpm_channel_head_s {
  ngx_str_t                    id; //channel id
  ngx_http_push_channel_t      channel;
  ngx_pool_t                  *pool;
  ngx_uint_t                   generation; //subscriber pool generation.
  nhpm_subscriber_t           *sub;
  chanhead_pubsub_status_t     status;
  ngx_uint_t                   sub_count;
  ngx_uint_t                   min_messages;
  ngx_uint_t                   max_messages;
  nhpm_message_t              *msg_first;
  nhpm_message_t              *msg_last;
  nhpm_channel_head_cleanup_t *shared_cleanup;
  nhpm_llist_timed_t           cleanlink;
  UT_hash_handle               hh;
};

struct nhpm_channel_head_cleanup_s {
  nhpm_channel_head_t        *head;
  ngx_str_t                   id; //channel id
  ngx_uint_t                  sub_count;
  ngx_pool_t                 *pool;
};


#define CHANNEL_HASH_FIND(id_buf, p)    HASH_FIND( hh, subhash, (id_buf)->data, (id_buf)->len, p)
#define CHANNEL_HASH_ADD(chanhead)      HASH_ADD_KEYPTR( hh, subhash, (chanhead->id).data, (chanhead->id).len, chanhead)
#define CHANNEL_HASH_DEL(chanhead)      HASH_DEL( subhash, chanhead)

#undef uthash_malloc
#undef uthash_free
#define uthash_malloc(sz) ngx_alloc(sz, ngx_cycle->log)
#define uthash_free(ptr,sz) ngx_free(ptr)

#define STR(buf) (buf)->data, (buf)->len
#define BUF(buf) (buf)->pos, ((buf)->last - (buf)->pos)

#define NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_POOL_SIZE (5 * 1024)
#define NGX_HTTP_PUSH_DEFAULT_CHANHEAD_CLEANUP_INTERVAL 1000
#define NGX_HTTP_PUSH_CHANHEAD_EXPIRE_SEC 1

//#define DEBUG_SHM_ALLOC 1
//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, __VA_ARGS__)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)

static nhpm_channel_head_t *subhash = NULL;

//garbage collection for channel heads
static ngx_event_t         chanhead_cleanup_timer = {0};
static nhpm_llist_timed_t *chanhead_cleanup_head = NULL;
static nhpm_llist_timed_t *chanhead_cleanup_tail = NULL;

static ngx_int_t chanhead_gc_add(nhpm_channel_head_t *head);
static ngx_int_t chanhead_gc_withdraw(nhpm_channel_head_t *chanhead);

static ngx_int_t chanhead_messages_gc(nhpm_channel_head_t *ch);

static void ngx_http_push_store_chanhead_cleanup_timer_handler(ngx_event_t *);
static ngx_int_t ngx_http_push_store_publish_raw(nhpm_channel_head_t *, ngx_http_push_msg_t *, ngx_int_t, const ngx_str_t *);

static ngx_int_t ngx_http_push_store_init_worker(ngx_cycle_t *cycle) {
  chanhead_cleanup_timer.data=NULL;
  chanhead_cleanup_timer.handler=&ngx_http_push_store_chanhead_cleanup_timer_handler;
  chanhead_cleanup_timer.log=ngx_cycle->log;
  
  return NGX_OK;
}

void *shalloc(size_t size) {
  return ngx_alloc(size, ngx_cycle->log);
}
void *shcalloc(size_t size) {
  return ngx_calloc(size, ngx_cycle->log);
}
void shfree(void *pt) {
  ngx_free(pt);
}

static ngx_int_t nhpm_subscriber_register(nhpm_channel_head_t *chanhead, nhpm_subscriber_t *sub) {
  return NGX_OK;
}

static ngx_int_t nhpm_subscriber_unregister(ngx_str_t *channel_id, nhpm_subscriber_t *sub) {
  return NGX_OK;
}

static ngx_int_t ensure_chanhead_is_ready(nhpm_channel_head_t *head) {
  nhpm_channel_head_cleanup_t *hcln;
  if(head->pool==NULL) {
    if((head->pool=ngx_create_pool(NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_POOL_SIZE, ngx_cycle->log))==NULL) {
      ERR("can't allocate memory for channel subscriber pool");
    }
  }
  if(head->shared_cleanup == NULL) {
    if((hcln=ngx_pcalloc(head->pool, sizeof(*hcln)))==NULL) {
      ERR("can't allocate memory for channel head cleanup");
    }
    head->shared_cleanup = hcln;
  }
  
  if (head->status == INACTIVE) { //recycled chanhead
    chanhead_gc_withdraw(head);
    head->status = READY;
  }
  return NGX_OK;
}

static nhpm_channel_head_t * ngx_http_push_store_find_chanhead(ngx_str_t *channel_id) {
  nhpm_channel_head_t     *head;
  CHANNEL_HASH_FIND(channel_id, head);
  if(head) {
    ensure_chanhead_is_ready(head);
  }
  return head;
}

static nhpm_channel_head_t * ngx_http_push_store_get_chanhead(ngx_str_t *channel_id) {
  nhpm_channel_head_t          *head;
  CHANNEL_HASH_FIND(channel_id, head);
  if(head==NULL) {
    head=(nhpm_channel_head_t *)ngx_calloc(sizeof(*head) + sizeof(u_char)*(channel_id->len), ngx_cycle->log);
    if(head==NULL) {
      ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "can't allocate memory for (new) channel subscriber head");
      return NULL;
    }
    head->id.len = channel_id->len;
    head->id.data = (u_char *)&head[1];
    ngx_memcpy(head->id.data, channel_id->data, channel_id->len);
    head->sub_count=0;
    head->status = NOTREADY;

    //set channel
    ngx_memcpy(&head->channel.id, &head->id, sizeof(ngx_str_t));
    head->channel.message_queue=NULL;
    head->channel.messages = 0;
    head->channel.subscribers = 0;
    head->channel.last_seen = ngx_time();
    head->min_messages = 0;
    head->max_messages = (ngx_int_t) -1;
    // head->channel.expires = ???

    //TODO: SUBSCRIBE for change notification maybe?
    CHANNEL_HASH_ADD(head);
  }
  ensure_chanhead_is_ready(head);

  return head;
}

static ngx_int_t nhpm_subscriber_remove(nhpm_subscriber_t *sub) {
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
  
  return NGX_OK;
}

static void subscriber_publishing_cleanup_callback(nhpm_subscriber_cleanup_t *cln) {
  nhpm_subscriber_t            *sub = cln->sub;
  nhpm_channel_head_cleanup_t  *shared = cln->shared;
  ngx_int_t                     i_am_the_last;
  
  i_am_the_last = sub->prev==NULL && sub->next==NULL;
  
  nhpm_subscriber_unregister(&shared->id, sub);
  nhpm_subscriber_remove(sub);
  
  if(i_am_the_last) {
    //release pool
    DBG("I am the last subscriber.");
    assert(shared->sub_count != 0);
    ngx_destroy_pool(shared->pool);
  }
}

static ngx_int_t chanhead_gc_add(nhpm_channel_head_t *head) {
  nhpm_llist_timed_t         *chanhead_cleanlink;
  
  if(head->status != INACTIVE) {
    chanhead_cleanlink = &head->cleanlink;
    
    chanhead_cleanlink->data=(void *)head;
    chanhead_cleanlink->time=ngx_time();
    chanhead_cleanlink->prev=chanhead_cleanup_tail;
    if(chanhead_cleanup_tail != NULL) {
      chanhead_cleanup_tail->next=chanhead_cleanlink;
    }
    chanhead_cleanlink->next=NULL;
    chanhead_cleanup_tail=chanhead_cleanlink;
    if(chanhead_cleanup_head==NULL) {
      chanhead_cleanup_head = chanhead_cleanlink;
    }
    
    head->status = INACTIVE;
    
    //DBG("gc_add chanhead %V", &head->id);
  }
  else {
    ERR("gc_add chanhead %V: already added", &head->id);
  }

  //initialize cleanup timer
  if(!chanhead_cleanup_timer.timer_set) {
    ngx_add_timer(&chanhead_cleanup_timer, NGX_HTTP_PUSH_DEFAULT_CHANHEAD_CLEANUP_INTERVAL);
  }
  return NGX_OK;
}

static ngx_int_t chanhead_gc_withdraw(nhpm_channel_head_t *chanhead) {
  //remove from cleanup list if we're there
  nhpm_llist_timed_t    *cl;
  //DBG("gc_withdraw chanhead %V", &chanhead->id);
  if(chanhead->status == INACTIVE) {
    cl=&chanhead->cleanlink;
    if(cl->prev!=NULL)
      cl->prev->next=cl->next;
    if(cl->next!=NULL)
      cl->next->prev=cl->prev;
    if(chanhead_cleanup_head==cl)
      chanhead_cleanup_head=cl->next;
    if(chanhead_cleanup_tail==cl)
      chanhead_cleanup_tail=cl->prev;

    cl->prev = cl->next = NULL;
  }
  else {
    //DBG("gc_withdraw chanhead %p (%V), but already inactive", chanhead, &chanhead->id);
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

static ngx_int_t ngx_http_push_store_publish_raw(nhpm_channel_head_t *head, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line){
  nhpm_subscriber_t          *sub, *next;
  nhpm_channel_head_cleanup_t *hcln;
  
  if(head==NULL) {
    return NGX_HTTP_PUSH_MESSAGE_QUEUED;
  }
  
  sub = head->sub;
  if(sub==NULL) { //no subscribers
    return NGX_HTTP_PUSH_MESSAGE_QUEUED;
  }
  
  //set some things the cleanup callback will need
  hcln = head->shared_cleanup;
  head->shared_cleanup = NULL;
  hcln->sub_count=head->sub_count;
  hcln->head=NULL;
  hcln->id.len = head->id.len;
  hcln->id.data = head->id.data;
  //ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "hcln->id.len == %i, head cleanup: %p", hcln->id.len, hcln);
  hcln->pool=head->pool;
  
  //DBG("chanhead_gc_add from publish_raw adding %p %V", head, &head->id);
  chanhead_gc_add(head);
  
  head->sub_count = 0;
  head->channel.subscribers = 0;
  
  head->pool=NULL; //pool will be destroyed on cleanup
  
  head->sub=NULL;
  
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
    if(rsub->dequeue_after_response) {
      rsub->dequeue(rsub);
    }
  }

  head->generation++;

  return NGX_HTTP_PUSH_MESSAGE_RECEIVED;
}

static void handle_chanhead_gc_queue(ngx_int_t force_delete) {
  nhpm_llist_timed_t    *cur, *next;
  nhpm_channel_head_t   *ch = NULL;
  
  DBG("handle_chanhead_gc_queue");
  
  for(cur=chanhead_cleanup_head; cur != NULL; cur=next) {
    next=cur->next;
    if(force_delete || ngx_time() - cur->time > NGX_HTTP_PUSH_CHANHEAD_EXPIRE_SEC) {
      ch = (nhpm_channel_head_t *)cur->data;
      if (ch->sub==NULL) { //still no subscribers here
        chanhead_messages_gc(ch);
        if(ch->msg_first == NULL) {
          //unsubscribe now
          //DBG("UNSUBSCRIBING from channel:pubsub:%V", &ch->id);
          ERR("chanhead %p (%V) is empty and expired. delete.", ch, &ch->id);
          if(ch->pool != NULL) {
            ERR("destroy pool %p.", ch->pool);
            ngx_destroy_pool(ch->pool);
          }
          else {
            ERR("ch->pool == NULL");
          }
          CHANNEL_HASH_DEL(ch);
          ngx_free(ch);
        }
        else {
          ERR("chanhead %p (%V) is still storing %i messages.", ch, &ch->id, ch->channel.messages);
          break;
        }
      }
      else {
        ERR("chanhead %p (%V) is still in use.", ch, &ch->id);
        break;
      }
    }
    else {
      break;
    }
  }
  chanhead_cleanup_head=cur;
  if (cur==NULL) { //we went all the way to the end
    chanhead_cleanup_tail=NULL;
  }
  else {
    cur->prev=NULL;
  }
}

static void ngx_http_push_store_chanhead_cleanup_timer_handler(ngx_event_t *ev) {
  handle_chanhead_gc_queue(0);
  if (!(ngx_quit || ngx_terminate || ngx_exiting || chanhead_cleanup_head==NULL)) {
    ngx_add_timer(ev, NGX_HTTP_PUSH_DEFAULT_CHANHEAD_CLEANUP_INTERVAL);
  }
  else if(chanhead_cleanup_head==NULL) {
    DBG("chanhead gc queue looks empty, stop gc_queue handler");
  }
}


static ngx_int_t chanhead_delete_message(nhpm_channel_head_t *ch, nhpm_message_t *msg);


static ngx_int_t ngx_http_push_store_delete_channel(ngx_str_t *channel_id, callback_pt callback, void *privdata) {
  nhpm_channel_head_t      *ch;
  nhpm_message_t           *msg = NULL;
  if((ch = ngx_http_push_store_find_chanhead(channel_id))) {
    ngx_http_push_store_publish_raw(ch, NULL, NGX_HTTP_GONE, &NGX_HTTP_PUSH_HTTP_STATUS_410);
    //TODO: publish to other workers
    callback(NGX_OK, &ch->channel, privdata);
    //delete all messages
    for(msg = ch->msg_first; msg != NULL; msg = ch->msg_first) {
      chanhead_delete_message(ch, msg);
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
    callback(NGX_OK, &ch->channel, privdata);
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
  ngx_http_push_worker_processes = ccf->worker_processes;
  //initialize our little IPC
  return NGX_OK;
}

static ngx_int_t ngx_http_push_store_init_postconfig(ngx_conf_t *cf) {
  //nothing to do but be OK.
  return NGX_OK;
}

static void ngx_http_push_store_create_main_conf(ngx_conf_t *cf, ngx_http_push_main_conf_t *mcf) {
  mcf->shm_size=NGX_CONF_UNSET_SIZE;
}

static void ngx_http_push_store_exit_worker(ngx_cycle_t *cycle) {
  DBG("exit worker %i", ngx_pid);
  nhpm_channel_head_t   *cur, *tmp;
  nhpm_subscriber_t     *sub;
  subscriber_t          *rsub;
    
  HASH_ITER(hh, subhash, cur, tmp) {
    //any subscribers?
    sub = cur->sub;
    while (sub != NULL) {
      rsub = sub->subscriber;
      rsub->dequeue_after_response = 1;
      rsub->respond_status(rsub, NGX_HTTP_CLOSE, NULL);
      sub = sub->next;
    }
    chanhead_gc_add(cur);
  }

  handle_chanhead_gc_queue(1);
  
  if(chanhead_cleanup_timer.timer_set) {
    ngx_del_timer(&chanhead_cleanup_timer);
  }
}

static void ngx_http_push_store_exit_master(ngx_cycle_t *cycle) {
  //destroy channel tree in shared memory
  //ngx_http_push_walk_rbtree(ngx_http_push_movezig_channel_locked, ngx_http_push_shm_zone);
  //deinitialize IPC
  
}

static void subscriber_cleanup_callback(nhpm_subscriber_cleanup_t *cln) {
  
  nhpm_subscriber_t           *sub = cln->sub;
  nhpm_channel_head_cleanup_t *shared = cln->shared;
  nhpm_channel_head_t         *head = shared->head;
  
  //DBG("subscriber_cleanup_callback for %p on %V", sub, &head->id);
  
  ngx_int_t done;
  done = sub->prev==NULL && sub->next==NULL;
  
  nhpm_subscriber_unregister(&shared->id, sub);
  nhpm_subscriber_remove(sub);

  head->sub_count--;
  head->channel.subscribers--;
  
  if(done) {
    //add chanhead to gc list
    head->sub=NULL;
    chanhead_gc_add(head);
  }
}

static ngx_int_t ngx_http_push_store_set_subscriber_cleanup_callback(nhpm_channel_head_t *head, nhpm_subscriber_t *sub, ngx_http_cleanup_pt *cleanup_callback) {
  nhpm_channel_head_cleanup_t *headcln;
  //ngx_http_push_loc_conf_t  *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  headcln = head->shared_cleanup;
  headcln->head = head;
  
  headcln->id.len = head->id.len;
  headcln->id.data = head->id.data;
  
  headcln->pool = head->pool;
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

static void nhpm_subscriber_timeout(ngx_event_t *ev) {
  nhpm_subscriber_cleanup_t *cln = ev->data;
  nhpm_subscriber_t         *sub = cln->sub;
  subscriber_t              *rsub = sub->subscriber;

  rsub->dequeue_after_response = 1;
  rsub->respond_status(rsub, NGX_HTTP_NOT_MODIFIED, NULL);
  //TODO: nhpm_subscriber_destroy
}

static ngx_int_t nhpm_subscriber_create(nhpm_channel_head_t *chanhead, subscriber_t *sub) {
  //this is the new shit
  ngx_http_push_loc_conf_t  *cf = ngx_http_get_module_loc_conf(sub->request, ngx_http_push_module);
  nhpm_subscriber_t         *nextsub;

  if((nextsub=ngx_pcalloc(chanhead->pool, sizeof(*nextsub)))==NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "can't allocate memory for (new) subscriber in channel sub pool");
    return NGX_ERROR;
  }

  //let's be explicit about this
  nextsub->prev=NULL;
  nextsub->next=NULL;
  nextsub->id = 0;

  nextsub->subscriber = sub;
  nextsub->pool= sub->request->pool;
  if(chanhead->sub != NULL) {
    chanhead->sub->prev = nextsub;
    nextsub->next = chanhead->sub;
  }
  else {
    //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "first subscriber for %V (%p): %p", &chanhead->id, chanhead, nextsub);
  }
  chanhead->sub = nextsub;

  chanhead->sub_count++;
  chanhead->channel.subscribers++;

  sub->enqueue(sub, cf->subscriber_timeout);
  
  //add teardown callbacks and cleaning data
  if(ngx_http_push_store_set_subscriber_cleanup_callback(chanhead, nextsub, (ngx_http_cleanup_pt *)subscriber_cleanup_callback) != NGX_OK) {
    ngx_pfree(chanhead->pool, nextsub);
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
  ch->channel.messages --;
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
  shfree(msg);
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
static ngx_int_t chanhead_messages_gc(nhpm_channel_head_t *ch) {
  //DBG("messages gc for ch %p %V", ch, &ch->id);
  ngx_uint_t      min_messages = ch->min_messages;
  ngx_uint_t      max_messages = ch->max_messages;
  nhpm_message_t *cur = ch->msg_first;
  nhpm_message_t *next = NULL;
  time_t          now = ngx_time();
  //DBG("chanhead_gc max %i min %i count %i", max_messages, min_messages, ch->channel.messages);
  
  //is the message queue too big?
  while(cur != NULL && ch->channel.messages > max_messages) {
    if(cur->msg.refcount > 0) {
      ERR("msg %p refcount %i > 0", &cur->msg, cur->msg.refcount);
    }
    else {
      DBG("delete queue-too-big msg %i:%i", cur->msg.message_time, cur->msg.message_tag);
      chanhead_delete_message(ch, cur);
      cur = ch->msg_first;
    }
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

static nhpm_message_t *chanhead_find_next_message(nhpm_channel_head_t *ch, ngx_http_push_msg_id_t *msgid, ngx_int_t *status) {
  DBG("find next message %i:%i", msgid->time, msgid->tag);
  chanhead_messages_gc(ch);
  nhpm_message_t *cur = ch->msg_last;
  
  if(cur == NULL) {
    *status = msgid == NULL ? NGX_HTTP_PUSH_MESSAGE_EXPECTED : NGX_HTTP_PUSH_MESSAGE_NOTFOUND;
    return NULL;
  }

  if(msgid == NULL || (msgid->time == 0 && msgid->tag == 0)) {
    DBG("found message %i:%i", ch->msg_first->msg.message_time, ch->msg_first->msg.message_tag);
    *status = NGX_HTTP_PUSH_MESSAGE_FOUND;
    return ch->msg_first;
  }

  while(cur != NULL) {
    //DBG("cur: %i:%i %V", cur->msg.message_time, cur->msg.message_tag, chanhead_msg_to_str(cur));
    
    if(msgid->time > cur->msg.message_time || (msgid->time == cur->msg.message_time && msgid->tag >= cur->msg.message_tag)){
      if(cur->next != NULL) {
        *status = NGX_HTTP_PUSH_MESSAGE_FOUND;
        DBG("found message %i:%i", ch->msg_first->msg.message_time, ch->msg_first->msg.message_tag);
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


static ngx_int_t ngx_http_push_store_subscribe(ngx_str_t *channel_id, ngx_http_push_msg_id_t *msg_id, subscriber_t *sub, callback_pt callback, void *privdata) {
  ngx_int_t                     create_channel_ttl;
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
  
  create_channel_ttl = cf->authorize_channel==1 ? 0 : cf->channel_timeout;
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

static ngx_int_t chanhead_push_message(nhpm_channel_head_t *ch, nhpm_message_t *msg) {
  msg->next = NULL;
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

  if((stuff = shcalloc(sizeof(*stuff) + (buf_filename_size + content_type_size + buf_body_size))) == NULL) {
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

  ngx_buf_t               *buf;
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

  buf = msg->buf;

  if((chead = ngx_http_push_store_get_chanhead(channel_id)) == NULL) {
    ERR("can't get chanhead for id %V", channel_id);
    return NGX_ERROR;
  }

  chead->channel.expires = ngx_time() + cf->buffer_timeout;
  sub_count = chead->sub_count;
  
  //TODO: address this weirdness
  //chead->min_messages = cf->min_messages;
  chead->min_messages = 0; // for backwards-compatibility, this value is ignored? weird...
  
  chead->max_messages = cf->max_messages;
  //TODO: channel timeout stuff
  
  chanhead_messages_gc(chead);
  
  if(cf->max_messages == 0) {
    channel_copy = &chead->channel;
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
    
    ngx_memcpy(channel_copy, &chead->channel, sizeof(*channel_copy));
    channel_copy->subscribers = sub_count;
    publish_msg = &shmsg_link->msg;
  }
  
  //do the actual publishing
  
  DBG("publish %i:%i expire %i", publish_msg->message_time, publish_msg->message_tag, cf->buffer_timeout);
  if(publish_msg->buf && publish_msg->buf->file) {
    DBG("fd %i", publish_msg->buf->file->fd);
  }
  ngx_http_push_store_publish_raw(chead, publish_msg, 0, NULL);
  callback(sub_count > 0 ? NGX_HTTP_PUSH_MESSAGE_RECEIVED : NGX_HTTP_PUSH_MESSAGE_QUEUED, channel_copy, privdata);
  
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

