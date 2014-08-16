#include <ngx_http_push_module.h>

#include "store.h"
#include <store/rbtree_util.h>
#include <store/ngx_rwlock.h>
#include <store/ngx_http_push_module_ipc.h>

#define NGX_HTTP_PUSH_BROADCAST_CHECK(val, fail, r, errormessage)             \
    if (val == fail) {                                                        \
        ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, errormessage);    \
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);         \
        return NULL;                                                          \
  }

#define NGX_HTTP_PUSH_BROADCAST_CHECK_LOCKED(val, fail, r, errormessage, shpool) \
    if (val == fail) {                                                        \
        ngx_shmtx_unlock(&(shpool)->mutex);                                   \
        ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, errormessage);    \
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);         \
        return NULL;                                                          \
    }

#define NGX_HTTP_BUF_ALLOC_SIZE(buf)                                          \
    (sizeof(*buf) +                                                           \
   (((buf)->temporary || (buf)->memory) ? ngx_buf_size(buf) : 0) +          \
   (((buf)->file!=NULL) ? (sizeof(*(buf)->file) + (buf)->file->name.len + 1) : 0))

#define ENQUEUED_DBG "msg %p enqueued.  ref:%i, p:%p n:%p"
#define CREATED_DBG  "msg %p created    ref:%i, p:%p n:%p"
#define FREED_DBG    "msg %p freed.     ref:%i, p:%p n:%p"
#define RESERVED_DBG "msg %p reserved.  ref:%i, p:%p n:%p"
#define RELEASED_DBG "msg %p released.  ref:%i, p:%p n:%p"

//#define DEBUG_SHM_ALLOC 1

static ngx_http_push_channel_queue_t channel_gc_sentinel;
static ngx_slab_pool_t    *ngx_http_push_shpool = NULL;
static ngx_shm_zone_t     *ngx_http_push_shm_zone = NULL;

static ngx_int_t ngx_http_push_store_send_worker_message(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *subscriber_sentinel, ngx_pid_t pid, ngx_int_t worker_slot, ngx_http_push_msg_t *msg, ngx_int_t status_code);

static ngx_int_t ngx_http_push_channel_collector(ngx_http_push_channel_t * channel) {
  if((ngx_http_push_clean_channel_locked(channel))!=NULL) { //we're up for deletion
    ngx_http_push_channel_queue_t *trashy;
    if((trashy = ngx_alloc(sizeof(*trashy), ngx_cycle->log))!=NULL) {
      //yeah, i'm allocating memory during garbage collection. sue me.
      trashy->channel=channel;
      ngx_queue_insert_tail(&channel_gc_sentinel.queue, &trashy->queue);
      return NGX_OK;
    }
    return NGX_ERROR;
  }
  return NGX_OK;
}

static void ngx_http_push_store_lock_shmem(void){
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
}
static void ngx_http_push_store_unlock_shmem(void){
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
}

//garbage-collecting slab allocator
static void * ngx_http_push_slab_alloc_locked(size_t size, char *label) {
  void  *p;
  if((p = ngx_slab_alloc_locked(ngx_http_push_shpool, size))==NULL) {
    ngx_http_push_channel_queue_t *ccur, *cnext;
    ngx_uint_t                  collected = 0;
    //failed. emergency garbage sweep, then.
    
    //collect channels
    ngx_queue_init(&channel_gc_sentinel.queue);
    ngx_http_push_walk_rbtree(ngx_http_push_channel_collector, ngx_http_push_shm_zone);
    for(ccur=(ngx_http_push_channel_queue_t *)ngx_queue_next(&channel_gc_sentinel.queue); ccur != &channel_gc_sentinel; ccur=cnext) {
      cnext = (ngx_http_push_channel_queue_t *)ngx_queue_next(&ccur->queue);
      ngx_http_push_delete_channel_locked(ccur->channel, ngx_http_push_shm_zone);
      ngx_free(ccur);
      collected++;
    }
    
    //todo: collect worker messages maybe
    
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "push module: out of shared memory. emergency garbage collection deleted %ui unused channels.", collected);
    
    p = ngx_slab_alloc_locked(ngx_http_push_shpool, size);
  }
#if (DEBUG_SHM_ALLOC == 1)
  if (p != NULL) {
    if(label==NULL)
      label="none";
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "shpool alloc addr %p size %ui label %s", p, size, label);
  }
#endif
  return p;
}

static void * ngx_http_push_slab_alloc(size_t size, char *label) {
  void * p;
  ngx_http_push_store_lock_shmem();
  p= ngx_http_push_slab_alloc_locked(size, label);
  ngx_http_push_store_unlock_shmem();
  return p;
}

static void ngx_http_push_slab_free_locked(void *ptr) {
  ngx_slab_free_locked(ngx_http_push_shpool, ptr);
  #if (DEBUG_SHM_ALLOC == 1)
  ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "shpool free addr %p", ptr);
  #endif
}
/*
static void ngx_http_push_slab_free(void *ptr) {
  ngx_http_push_store_lock_shmem();
  ngx_http_push_slab_free_locked(ptr);
  ngx_http_push_store_unlock_shmem();
}*/

//shpool is assumed to be locked.
static ngx_http_push_msg_t *ngx_http_push_get_latest_message_locked(ngx_http_push_channel_t * channel) {
  ngx_queue_t                    *sentinel = &channel->message_queue->queue; 
  if(ngx_queue_empty(sentinel)) {
    return NULL;
  }
  ngx_queue_t                    *qmsg = ngx_queue_last(sentinel);
  return ngx_queue_data(qmsg, ngx_http_push_msg_t, queue);
}

//shpool must be locked. No memory is freed. O(1)
static ngx_http_push_msg_t *ngx_http_push_get_oldest_message_locked(ngx_http_push_channel_t * channel) {
  ngx_queue_t                    *sentinel = &channel->message_queue->queue; 
  if(ngx_queue_empty(sentinel)) {
    return NULL;
  }
  ngx_queue_t                    *qmsg = ngx_queue_head(sentinel);
  return ngx_queue_data(qmsg, ngx_http_push_msg_t, queue);
}

static void ngx_http_push_store_reserve_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
  if(msg == NULL) {
    return;
  }
  msg->refcount++;
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, RESERVED_DBG, msg, msg->refcount, msg->queue.prev, msg->queue.next);
  //we need a refcount because channel messages MAY be dequed before they are used up. It thus falls on the IPC stuff to free it.
}

static void ngx_http_push_store_reserve_message_num_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t reservations) {
  if(msg == NULL) {
    return;
  }
  msg->refcount+=reservations;
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, RESERVED_DBG, msg, msg->refcount, msg->queue.prev, msg->queue.next);
  //we need a refcount because channel messages MAY be dequed before they are used up. It thus falls on the IPC stuff to free it.
}

static void ngx_http_push_store_reserve_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  ngx_http_push_store_reserve_message_locked(channel, msg);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  //we need a refcount because channel messages MAY be dequed before they are used up. It thus falls on the IPC stuff to free it.
}



//free memory for a message. 
static ngx_inline void ngx_http_push_free_message_locked(ngx_http_push_msg_t *msg, ngx_slab_pool_t *shpool) {
  if(msg->buf->file!=NULL) {
    // i'd like to release the shpool lock here while i do stuff to this file, but that 
    // might unlock during channel rbtree traversal, which is Bad News.
    if(msg->buf->file->fd!=NGX_INVALID_FILE) {
      ngx_close_file(msg->buf->file->fd);
    }
    ngx_delete_file(msg->buf->file->name.data); //should I care about deletion errors? doubt it.
  }
  ngx_http_push_slab_free_locked(msg->buf); //separate block, remember?
  ngx_http_push_slab_free_locked(msg);
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, FREED_DBG, msg, msg->refcount, msg->queue.prev, msg->queue.next);
  if(msg->refcount < 0) { //something worth exploring went wrong
    raise(SIGSEGV);
  }
  ((ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data)->messages--;
}

// remove a message from queue and free all associated memory. assumes shpool is already locked.
static ngx_int_t ngx_http_push_delete_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t force) {
  if (msg==NULL) {
    return NGX_OK;
  }
  if(channel!=NULL) {
    ngx_queue_remove(&msg->queue);
    channel->messages--;
  }
  if(msg->refcount<=0 || force) {
    //nobody needs this message, or we were forced at integer-point to delete
    ngx_http_push_free_message_locked(msg, ngx_http_push_shpool);
  }
  return NGX_OK;
}

static void ngx_http_push_store_release_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
  if(msg == NULL) {
    return;
  }
  msg->refcount--;
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, RELEASED_DBG, msg, msg->refcount, msg->queue.prev, msg->queue.next);
  if(msg->queue.next==NULL && msg->refcount<=0) { 
    //message had been dequeued and nobody needs it anymore
    ngx_http_push_free_message_locked(msg, ngx_http_push_shpool);
  }
  if(channel != NULL && channel->messages > msg->delete_oldest_received_min_messages && ngx_http_push_get_oldest_message_locked(channel) == msg) {
    ngx_http_push_delete_message_locked(channel, msg, 0);
  }
}

static void ngx_http_push_store_release_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  ngx_http_push_store_release_message_locked(channel, msg);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
}

static ngx_int_t ngx_http_push_delete_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t force) {
  ngx_int_t ret;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  ret = ngx_http_push_delete_message_locked(channel, msg, force);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return ret;
}


/** find message with entity tags matching those of the request r.
  * @param r subscriber request
  */
static ngx_http_push_msg_t * ngx_http_push_find_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_id_t *msgid, ngx_int_t *status) {
  //TODO: consider using an RBTree for message storage.
  ngx_queue_t                    *sentinel = &channel->message_queue->queue;
  ngx_queue_t                    *cur = ngx_queue_head(sentinel);
  ngx_http_push_msg_t            *msg;
  
  time_t                          time = msgid->time;
  ngx_int_t                       tag = msgid->tag;
  
  //channel's message buffer empty?
  if(channel->messages==0) {
    *status=NGX_HTTP_PUSH_MESSAGE_EXPECTED; //wait.
    return NULL;
  }
  
  // do we want a future message?
  msg = ngx_queue_data(sentinel->prev, ngx_http_push_msg_t, queue); 
  if(time <= msg->message_time) { //that's an empty check (Sentinel's values are zero)
    if(time == msg->message_time) {
      if(tag >= msg->message_tag) {
        *status=NGX_HTTP_PUSH_MESSAGE_EXPECTED;
        return NULL;
      }
    }
  }
  else {
    *status=NGX_HTTP_PUSH_MESSAGE_EXPECTED;
    return NULL;
  }
  
  while(cur!=sentinel) {
    msg = ngx_queue_data(cur, ngx_http_push_msg_t, queue);
    if (time < msg->message_time) {
      *status = NGX_HTTP_PUSH_MESSAGE_FOUND;
      return msg;
    }
    else if(time == msg->message_time) {
      while (tag >= msg->message_tag  && time == msg->message_time && ngx_queue_next(cur)!=sentinel) {
        cur=ngx_queue_next(cur);
        msg = ngx_queue_data(cur, ngx_http_push_msg_t, queue);
      }
      if(time == msg->message_time && tag < msg->message_tag) {
        *status = NGX_HTTP_PUSH_MESSAGE_FOUND;
        return msg;
      }
      continue;
    }
    cur=ngx_queue_next(cur);
  }
  *status = NGX_HTTP_PUSH_MESSAGE_EXPIRED; //message too old and was not found.
  return NULL;
}

static ngx_http_push_channel_t * ngx_http_push_store_find_channel(ngx_str_t *id, time_t channel_timeout, ngx_int_t (*callback)(ngx_http_push_channel_t *channel)) {
  //get the channel and check channel authorization while we're at it.
  ngx_http_push_channel_t        *channel;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  channel = ngx_http_push_find_channel(id, channel_timeout, ngx_http_push_shm_zone);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  if(callback!=NULL) {
    callback(channel);
  }
  return channel;
}

//temporary cheat
static ngx_int_t ngx_http_push_store_publish_raw(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line) {
  
  //subscribers are queued up in a local pool. Queue heads, however, are located
  //in shared memory, identified by pid.
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  ngx_http_push_pid_queue_t     *sentinel = channel->workers_with_subscribers;
  ngx_http_push_subscriber_t    *subscriber_sentinels[NGX_MAX_PROCESSES];
  ngx_http_push_pid_queue_t     *pid_queues[NGX_MAX_PROCESSES];
  ngx_int_t                      sub_sentinel_count=0;
  
  ngx_http_push_pid_queue_t     *cur;
  ngx_int_t                      i, received;
  received = channel->subscribers > 0 ? NGX_HTTP_PUSH_MESSAGE_RECEIVED : NGX_HTTP_PUSH_MESSAGE_QUEUED;
  
  //we need to reserve the message for all the workers in advance
  for(cur=(ngx_http_push_pid_queue_t *)ngx_queue_next(&sentinel->queue); cur != sentinel; cur=(ngx_http_push_pid_queue_t *)ngx_queue_next(&cur->queue)) {
    if(cur->subscriber_sentinel != NULL) {
      pid_queues[sub_sentinel_count] = cur;
      subscriber_sentinels[sub_sentinel_count] = cur->subscriber_sentinel;
      /*
       *     each time all of a worker's subscribers are removed, so is the sentinel. 
       *     this is done to make garbage collection easier. Assuming we want to avoid
       *     placing the sentinel in shared memory (for now -- it's a little tricky
       *     to debug), the owner of the worker pool must be the one to free said sentinel.
       *     But channels may be deleted by different worker processes, and it seems unwieldy
       *     (for now) to do IPC just to delete one stinkin' sentinel. Hence a new sentinel
       *     is used every time the subscriber queue is emptied.
       */
      cur->subscriber_sentinel = NULL; //think about it it terms of garbage collection. it'll make sense. sort of.
      sub_sentinel_count++;
    }
  }
  if(sub_sentinel_count > 0) {
    ngx_http_push_store_reserve_message_num_locked(channel, msg, sub_sentinel_count);
  }
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  
  ngx_http_push_subscriber_t *subscriber_sentinel=NULL;
  for(i=0; i < sub_sentinel_count; i++) {
    ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
    subscriber_sentinel = subscriber_sentinels[i];
    pid_t           worker_pid  = pid_queues[i]->pid;
    ngx_int_t       worker_slot = pid_queues[i]->slot;
    ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
    //if(msg != NULL)
    //  ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "publish msg %p (ref: %i) for worker %i (slot %i)", msg, msg->refcount, worker_pid, worker_slot);
    
    
    if(subscriber_sentinel != NULL) {
      if(worker_pid == ngx_pid) {
        //my subscribers
        ngx_http_push_respond_to_subscribers(channel, subscriber_sentinel, msg, status_code, status_line);
      }
      else {
        //some other worker's subscribers
        //interprocess communication breakdown
        if(ngx_http_push_store_send_worker_message(channel, subscriber_sentinel, worker_pid, worker_slot, msg, status_code) != NGX_ERROR) {
          ngx_http_push_alert_worker(worker_pid, worker_slot);
        }
        else {
          ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: error communicating with some other worker process");
        }
      }
    } else {
      //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "subscriber sentinel is NULL");
    }
    
    
  }
  return received;
}

static ngx_int_t ngx_http_push_store_delete_channel(ngx_str_t *channel_id) {
  ngx_http_push_channel_t        *channel;
  ngx_http_push_msg_t            *msg, *sentinel;
  ngx_http_push_store_lock_shmem();
  channel = ngx_http_push_find_channel(channel_id, NGX_HTTP_PUSH_DEFAULT_CHANNEL_TIMEOUT, ngx_http_push_shm_zone);
  if (channel == NULL) {
    ngx_http_push_store_unlock_shmem();
    return NGX_OK;
  }
  sentinel = channel->message_queue; 
  msg = sentinel;
        
  while((msg=(ngx_http_push_msg_t *)ngx_queue_next(&msg->queue))!=sentinel) {
    //force-delete all the messages
    ngx_http_push_delete_message_locked(NULL, msg, 1);
  }
  channel->messages=0;
  
  //410 gone
  ngx_http_push_store_unlock_shmem();
  
  ngx_http_push_store_publish_raw(channel, NULL, NGX_HTTP_GONE, &NGX_HTTP_PUSH_HTTP_STATUS_410);
  
  ngx_http_push_store_lock_shmem();
  ngx_http_push_delete_channel_locked(channel, ngx_http_push_shm_zone);
  ngx_http_push_store_unlock_shmem();
  return NGX_OK;
}

static ngx_http_push_channel_t * ngx_http_push_store_get_channel(ngx_str_t *id, time_t channel_timeout, ngx_int_t (*callback)(ngx_http_push_channel_t *channel)) {
  //get the channel and check channel authorization while we're at it.
  ngx_http_push_channel_t        *channel;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  channel = ngx_http_push_get_channel(id, channel_timeout, ngx_http_push_shm_zone);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  if(channel==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate memory for new channel");
  }
  if(callback!=NULL) {
    callback(channel);
  }
  return channel;
}

static ngx_http_push_msg_t * ngx_http_push_store_get_channel_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_id_t *msgid, ngx_int_t *msg_search_outcome, ngx_http_push_loc_conf_t *cf) {
  ngx_http_push_msg_t *msg;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  msg = ngx_http_push_find_message_locked(channel, msgid, msg_search_outcome);
  if(*msg_search_outcome == NGX_HTTP_PUSH_MESSAGE_FOUND) {
    ngx_http_push_store_reserve_message_locked(channel, msg);
  }
  channel->last_seen = ngx_time();
  channel->expires = ngx_time() + cf->channel_timeout;
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return msg;
}

static ngx_int_t default_get_message_callback(ngx_http_push_msg_t *msg, ngx_int_t msg_search_outcome, ngx_http_request_t *r) {
  return NGX_OK;
}

static ngx_http_push_msg_t * ngx_http_push_store_get_message(ngx_str_t *channel_id, ngx_http_push_msg_id_t *msg_id, ngx_int_t *msg_search_outcome, ngx_http_request_t *r, ngx_int_t (*callback)(ngx_http_push_msg_t *msg, ngx_int_t msg_search_outcome, ngx_http_request_t *r)) {
  ngx_http_push_channel_t            *channel;
  ngx_http_push_msg_t                *msg;
  if(callback==NULL) {
    callback=&default_get_message_callback;
  }
  ngx_http_push_store_lock_shmem();
  channel = ngx_http_push_get_channel(channel_id, NGX_HTTP_PUSH_DEFAULT_CHANNEL_TIMEOUT, ngx_http_push_shm_zone);
  ngx_http_push_store_unlock_shmem();
  if (channel == NULL) {
    return NULL;
  }
  msg = ngx_http_push_store_get_channel_message(channel, msg_id, msg_search_outcome, ngx_http_get_module_loc_conf(r, ngx_http_push_module));
  callback(msg, *msg_search_outcome, r);
  return msg;
}

// shared memory zone initializer
static ngx_int_t  ngx_http_push_init_shm_zone(ngx_shm_zone_t * shm_zone, void *data) {
  if(data) { /* zone already initialized */
    shm_zone->data = data;
    return NGX_OK;
  }

  ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;
  ngx_rbtree_node_t              *sentinel;
  ngx_http_push_shm_data_t       *d;
  
  ngx_http_push_shpool = shpool; //we'll be using this a bit.
  #if (DEBUG_SHM_ALLOC == 1)
  ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "ngx_http_push_shpool start %p size %i", shpool->start, (u_char *)shpool->end - (u_char *)shpool->start);
  #endif
  
  if ((d = (ngx_http_push_shm_data_t *)ngx_http_push_slab_alloc(sizeof(*d), "shm data")) == NULL) { //shm_data
    return NGX_ERROR;
  }
  d->channels=0;
  d->messages=0;
  shm_zone->data = d;
  d->ipc=NULL;
  //initialize rbtree
  if ((sentinel = ngx_http_push_slab_alloc(sizeof(*sentinel), "channel rbtree sentinel"))==NULL) {
    return NGX_ERROR;
  }
  ngx_rbtree_init(&d->tree, sentinel, ngx_http_push_rbtree_insert);
  return NGX_OK;
}

//shared memory
static ngx_str_t  ngx_push_shm_name = ngx_string("push_module"); //shared memory segment name
static ngx_int_t  ngx_http_push_set_up_shm(ngx_conf_t *cf, size_t shm_size) {
  ngx_http_push_shm_zone = ngx_shared_memory_add(cf, &ngx_push_shm_name, shm_size, &ngx_http_push_module);
  if (ngx_http_push_shm_zone == NULL) {
    return NGX_ERROR;
  }
  ngx_http_push_shm_zone->init = ngx_http_push_init_shm_zone;
  ngx_http_push_shm_zone->data = (void *) 1; 
  return NGX_OK;
}

//initialization
static ngx_int_t ngx_http_push_store_init_module(ngx_cycle_t *cycle) {
  ngx_core_conf_t                *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  ngx_http_push_worker_processes = ccf->worker_processes;
  //initialize our little IPC
  return ngx_http_push_init_ipc(cycle, ngx_http_push_worker_processes);
}

//will be called once per worker
static ngx_int_t ngx_http_push_store_init_ipc_shm(ngx_int_t workers) {
  ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
  ngx_http_push_shm_data_t       *d = (ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data;
  ngx_http_push_worker_msg_sentinel_t     *worker_messages=NULL;
  ngx_shmtx_lock(&shpool->mutex);
  if(d->ipc==NULL) {
    //ipc uninitialized. get it done!
    if((worker_messages = ngx_http_push_slab_alloc_locked(sizeof(*worker_messages)*NGX_MAX_PROCESSES, "IPC worker message sentinel array"))==NULL) {
      ngx_shmtx_unlock(&shpool->mutex);
      return NGX_ERROR;
    }
    d->ipc=worker_messages;
  }
  else {
    worker_messages=d->ipc;
  }
  
  ngx_queue_init(&worker_messages[ngx_process_slot].queue);
  ngx_rwlock_init(&worker_messages[ngx_process_slot].lock);
  
  ngx_shmtx_unlock(&shpool->mutex);
  return NGX_OK;
}

static ngx_int_t ngx_http_push_store_init_worker(ngx_cycle_t *cycle) {
  ngx_core_conf_t                *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  if(ngx_http_push_store_init_ipc_shm(ccf->worker_processes) == NGX_OK) {
    return ngx_http_push_ipc_init_worker(cycle);
  }
  else {
    return NGX_ERROR;
  }
}

static ngx_int_t ngx_http_push_store_init_postconfig(ngx_conf_t *cf) {
  ngx_http_push_main_conf_t *conf = ngx_http_conf_get_module_main_conf(cf, ngx_http_push_module);

  //initialize shared memory
  size_t                       shm_size;
  if(conf->shm_size==NGX_CONF_UNSET_SIZE) {
    conf->shm_size=NGX_HTTP_PUSH_DEFAULT_SHM_SIZE;
  }
  shm_size = ngx_align(conf->shm_size, ngx_pagesize);
  if (shm_size < 8 * ngx_pagesize) {
        ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "The push_max_reserved_memory value must be at least %udKiB", (8 * ngx_pagesize) >> 10);
        shm_size = 8 * ngx_pagesize;
    }
  if(ngx_http_push_shm_zone && ngx_http_push_shm_zone->shm.size != shm_size) {
    ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "Cannot change memory area size without restart, ignoring change");
  }
  ngx_conf_log_error(NGX_LOG_INFO, cf, 0, "Using %udKiB of shared memory for push module", shm_size >> 10);
  
  return ngx_http_push_set_up_shm(cf, shm_size);
}

static void ngx_http_push_store_create_main_conf(ngx_conf_t *cf, ngx_http_push_main_conf_t *mcf) {
  mcf->shm_size=NGX_CONF_UNSET_SIZE;
}

//great justice appears to be at hand
static ngx_int_t ngx_http_push_movezig_channel_locked(ngx_http_push_channel_t * channel) {
  ngx_queue_t                 *sentinel = &channel->message_queue->queue;
  ngx_http_push_msg_t         *msg=NULL;
  while(!ngx_queue_empty(sentinel)) {
    msg = ngx_queue_data(ngx_queue_head(sentinel), ngx_http_push_msg_t, queue);
    ngx_http_push_delete_message_locked(channel, msg, 1);
  }
  return NGX_OK;
}
static ngx_int_t ngx_http_push_store_channel_subscribers(ngx_http_push_channel_t * channel) {
  ngx_int_t subs;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  subs = channel->subscribers;
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return subs;
}

static ngx_int_t ngx_http_push_store_channel_worker_subscribers(ngx_http_push_subscriber_t * worker_sentinel) {
  ngx_http_push_subscriber_t *cur;
  ngx_int_t count=0;
  cur=(ngx_http_push_subscriber_t *)ngx_queue_head(&worker_sentinel->queue);
  while(cur!=worker_sentinel) {
    count++;
    cur=(ngx_http_push_subscriber_t *)ngx_queue_next(&cur->queue);
  }
  return count;
}

static ngx_http_push_subscriber_t *ngx_http_push_store_channel_next_subscriber(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *sentinel, ngx_http_push_subscriber_t *cur, int release_previous) {
  ngx_http_push_subscriber_t *next;
  if(cur==NULL) {
    next=(ngx_http_push_subscriber_t *)ngx_queue_head(&sentinel->queue);
  }
  else{
    next=(ngx_http_push_subscriber_t *)ngx_queue_next(&cur->queue);
    if(release_previous==1 && cur!=sentinel) {
      //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "freeing subscriber cursor at %p.", cur);
      ngx_pfree(ngx_http_push_pool, cur);
    }
  }
  return next!=sentinel ? next : NULL;
}

static ngx_int_t ngx_http_push_store_channel_release_subscriber_sentinel(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *sentinel) {
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "freeing subscriber sentinel at %p.", sentinel);
  ngx_pfree(ngx_http_push_pool, sentinel);
  return NGX_OK;
}

static void ngx_http_push_store_exit_worker(ngx_cycle_t *cycle) {
  ngx_http_push_ipc_exit_worker(cycle);
}

static void ngx_http_push_store_exit_master(ngx_cycle_t *cycle) {
  //destroy channel tree in shared memory
  ngx_http_push_walk_rbtree(ngx_http_push_movezig_channel_locked, ngx_http_push_shm_zone);
  //deinitialize IPC
  ngx_http_push_shutdown_ipc(cycle);
}

static ngx_http_push_subscriber_t * ngx_http_push_store_subscribe_raw(ngx_http_push_channel_t *channel, ngx_http_request_t *r) {
  ngx_http_push_pid_queue_t  *sentinel, *cur, *found;
  ngx_http_push_subscriber_t *subscriber;
  ngx_http_push_subscriber_t *subscriber_sentinel;
  //ngx_http_push_loc_conf_t   *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  
  //subscribers are queued up in a local pool. Queue sentinels are separate and also local, but not in the pool.
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  sentinel = channel->workers_with_subscribers;
  cur = (ngx_http_push_pid_queue_t *)ngx_queue_head(&sentinel->queue);
  found = NULL;
  
  while(cur!=sentinel) {
    if(cur->pid==ngx_pid) {
      found = cur;
      break;
    }
    cur = (ngx_http_push_pid_queue_t *)ngx_queue_next(&cur->queue);
  }
  if(found == NULL) { //found nothing
    if((found=ngx_http_push_slab_alloc_locked(sizeof(*found), "worker subscriber sentinel"))==NULL) {
      ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate worker subscriber queue marker in shared memory");
      return NULL;
    }
    //initialize
    ngx_queue_insert_tail(&sentinel->queue, &found->queue);
    found->pid=ngx_pid;
    found->slot=ngx_process_slot;
    found->subscriber_sentinel=NULL;
  }
  if((subscriber = ngx_palloc(ngx_http_push_pool, sizeof(*subscriber)))==NULL) { //unable to allocate request queue element
    ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate subscriber worker's memory pool");
    return NULL;
  }
  channel->subscribers++; // do this only when we know everything went okay.
  
  //figure out the subscriber sentinel
  subscriber_sentinel = ((ngx_http_push_pid_queue_t *)found)->subscriber_sentinel;
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "reserve subscriber sentinel at %p", subscriber_sentinel);
  
  if(subscriber_sentinel==NULL) {
    //it's perfectly normal for the sentinel to be NULL.
    if((subscriber_sentinel=ngx_palloc(ngx_http_push_pool, sizeof(*subscriber_sentinel)))==NULL) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate channel subscriber sentinel");
      return NULL;
    }
    ngx_queue_init(&subscriber_sentinel->queue);
    ((ngx_http_push_pid_queue_t *)found)->subscriber_sentinel=subscriber_sentinel;
  }
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "add to subscriber sentinel at %p", subscriber_sentinel);
  ngx_queue_insert_tail(&subscriber_sentinel->queue, &subscriber->queue);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  
  subscriber->request = r;
  return subscriber;
}

static ngx_int_t ngx_http_push_handle_subscriber_concurrency(ngx_http_push_channel_t *channel, ngx_http_request_t *r, ngx_http_push_loc_conf_t *cf) {
  ngx_int_t                      max_subscribers = cf->max_channel_subscribers;
  ngx_int_t                      current_subscribers = ngx_http_push_store->channel_subscribers(channel) ;
  
  
  if(current_subscribers==0) { 
    //empty channels are always okay.
    return NGX_OK;
  }  
  
  if(max_subscribers!=0 && current_subscribers >= max_subscribers) {
    //max_channel_subscribers setting
    ngx_http_push_respond_status_only(r, NGX_HTTP_FORBIDDEN, NULL);
    return NGX_DECLINED;
  }
  
  //nonzero number of subscribers present
  switch(cf->subscriber_concurrency) {
    case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_BROADCAST:
      return NGX_OK;
      
    case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_LASTIN:
      //send "everyone" a 409 Conflict response.
      //in most reasonable cases, there'll be at most one subscriber on the
      //channel. However, since settings are bound to locations and not
      //specific channels, this assumption need not hold. Hence this broadcast.
      ngx_http_push_store_publish_raw(channel, NULL, NGX_HTTP_NOT_FOUND, &NGX_HTTP_PUSH_HTTP_STATUS_409);
      
      return NGX_OK;
      
    case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_FIRSTIN:
      ngx_http_push_respond_status_only(r, NGX_HTTP_NOT_FOUND, &NGX_HTTP_PUSH_HTTP_STATUS_409);
      return NGX_DECLINED;
      
    default:
      return NGX_ERROR;
  }
}

static ngx_int_t default_subscribe_callback(ngx_int_t status, ngx_http_request_t *r) {
  return status;
}

static ngx_int_t ngx_http_push_store_subscribe(ngx_str_t *channel_id, ngx_http_push_msg_id_t *msg_id, ngx_http_request_t *r, ngx_int_t (*callback)(ngx_int_t status, ngx_http_request_t *r)) {
  ngx_http_push_channel_t        *channel;
  ngx_http_push_msg_t            *msg;
  ngx_int_t                       msg_search_outcome;
  ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);

  
  if(callback == NULL) {
    callback=&default_subscribe_callback;
  }
  
  if (cf->authorize_channel==1) {
    channel = ngx_http_push_store_find_channel(channel_id, cf->channel_timeout, NULL);
  }else{
    channel = ngx_http_push_store_get_channel(channel_id, cf->channel_timeout, NULL);
  }
  if (channel==NULL) {
    //unable to allocate channel OR channel not found
    if(cf->authorize_channel) {
      return callback(NGX_HTTP_FORBIDDEN, r);
    }
    else {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate shared memory for channel");
      return callback(NGX_HTTP_INTERNAL_SERVER_ERROR, r);
    }
  }
  
  switch(ngx_http_push_handle_subscriber_concurrency(channel, r, cf)) {
    case NGX_DECLINED: //this request was declined for some reason.
      //status codes and whatnot should have already been written. just get out of here quickly.
      return callback(NGX_OK, r);
    case NGX_ERROR:
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: error handling subscriber concurrency setting");
      return callback(NGX_ERROR, r);
  }
  
  
  msg = ngx_http_push_store->get_channel_message(channel, msg_id, &msg_search_outcome, cf);
  
  if (cf->ignore_queue_on_no_cache && !ngx_http_push_allow_caching(r)) {
    msg_search_outcome = NGX_HTTP_PUSH_MESSAGE_EXPECTED; 
    msg = NULL;
  }
  
  switch(msg_search_outcome) {
    //for message-found:
    ngx_str_t                  *etag;
    ngx_str_t                  *content_type;
    ngx_chain_t                *chain;
    time_t                      last_modified;
    ngx_http_push_subscriber_t *subscriber;
    
    case NGX_HTTP_PUSH_MESSAGE_EXPECTED:
      // ♫ It's gonna be the future soon ♫
      if ((subscriber = ngx_http_push_store_subscribe_raw(channel, r))==NULL) {
        return callback(NGX_HTTP_INTERNAL_SERVER_ERROR, r);
      }
      if(ngx_push_longpoll_subscriber_enqueue(channel, subscriber, cf->subscriber_timeout) == NGX_OK) {
        return callback(NGX_DONE, r);
      }
      else {
        return callback(NGX_ERROR, r);
      }
    
    case NGX_HTTP_PUSH_MESSAGE_EXPIRED:
      //subscriber wants an expired message
      //TODO: maybe respond with entity-identifiers for oldest available message?
      return callback(NGX_HTTP_NO_CONTENT, r);
      
    case NGX_HTTP_PUSH_MESSAGE_FOUND:
      ngx_http_push_alloc_for_subscriber_response(r->pool, 0, msg, &chain, &content_type, &etag, &last_modified);
      ngx_int_t ret=ngx_http_push_prepare_response_to_subscriber_request(r, chain, content_type, etag, last_modified);
      ngx_http_push_store->release_message(channel, msg);
      return callback(ret, r);
      
    default: //we shouldn't be here.
      return callback(NGX_HTTP_INTERNAL_SERVER_ERROR, r);
  }
}

static ngx_str_t * ngx_http_push_store_etag_from_message(ngx_http_push_msg_t *msg, ngx_pool_t *pool){
  ngx_str_t *etag;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  if(pool!=NULL && (etag = ngx_palloc(pool, sizeof(*etag) + NGX_INT_T_LEN))==NULL) {
    return NULL;
  }
  else if(pool==NULL && (etag = ngx_alloc(sizeof(*etag) + NGX_INT_T_LEN, ngx_cycle->log))==NULL) {
    return NULL;
  }
  etag->data = (u_char *)(etag+1);
  etag->len = ngx_sprintf(etag->data,"%ui", msg->message_tag)- etag->data;
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return etag;
}

static ngx_str_t * ngx_http_push_store_content_type_from_message(ngx_http_push_msg_t *msg, ngx_pool_t *pool){
  ngx_str_t *content_type;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  if(pool != NULL && (content_type = ngx_palloc(pool, sizeof(*content_type) + msg->content_type.len))==NULL) {
    return NULL;
  }
  else if(pool == NULL && (content_type = ngx_alloc(sizeof(*content_type) + msg->content_type.len, ngx_cycle->log))==NULL) {
    return NULL;
  }
  content_type->data = (u_char *)(content_type+1);
  content_type->len = msg->content_type.len;
  ngx_memcpy(content_type->data, msg->content_type.data, content_type->len);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return content_type;
}

// this function adapted from push stream module. thanks Wandenberg Peixoto <wandenberg@gmail.com> and Rogério Carvalho Schneider <stockrt@gmail.com>
static ngx_buf_t * ngx_http_push_request_body_to_single_buffer(ngx_http_request_t *r) {
  ngx_buf_t *buf = NULL;
  ngx_chain_t *chain;
  ssize_t n;
  off_t len;

  chain = r->request_body->bufs;
  if (chain->next == NULL) {
    return chain->buf;
  }
  if (chain->buf->in_file) {
    if (ngx_buf_in_memory(chain->buf)) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: can't handle a buffer in a temp file and in memory ");
    }
    if (chain->next != NULL) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: error reading request body with multiple ");
    }
    return chain->buf;
  }
  buf = ngx_create_temp_buf(r->pool, r->headers_in.content_length_n + 1);
  if (buf != NULL) {
    ngx_memset(buf->start, '\0', r->headers_in.content_length_n + 1);
    while ((chain != NULL) && (chain->buf != NULL)) {
      len = ngx_buf_size(chain->buf);
      // if buffer is equal to content length all the content is in this buffer
      if (len >= r->headers_in.content_length_n) {
        buf->start = buf->pos;
        buf->last = buf->pos;
        len = r->headers_in.content_length_n;
      }
      if (chain->buf->in_file) {
        n = ngx_read_file(chain->buf->file, buf->start, len, 0);
        if (n == NGX_FILE_ERROR) {
          ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: cannot read file with request body");
          return NULL;
        }
        buf->last = buf->last + len;
        ngx_delete_file(chain->buf->file->name.data);
        chain->buf->file->fd = NGX_INVALID_FILE;
      } else {
        buf->last = ngx_copy(buf->start, chain->buf->pos, len);
      }

      chain = chain->next;
      buf->start = buf->last;
    }
  }
  return buf;
}


static ngx_http_push_msg_t * ngx_http_push_store_create_message(ngx_http_push_channel_t *channel, ngx_http_request_t *r) {
  ngx_buf_t                      *buf = NULL, *buf_copy;
  size_t                          content_type_len;
  ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  ngx_http_push_msg_t            *msg, *previous_msg;
  
  //first off, we'll want to extract the body buffer
  
  //note: this works mostly because of r->request_body_in_single_buf = 1; 
  //which, i suppose, makes this module a little slower than it could be.
  //this block is a little hacky. might be a thorn for forward-compatibility.
  if(r->headers_in.content_length_n == -1 || r->headers_in.content_length_n == 0) {
    buf = ngx_create_temp_buf(r->pool, 0);
    //this buffer will get copied to shared memory in a few lines, 
    //so it does't matter what pool we make it in.
  }
  else if(r->request_body->bufs!=NULL) {
    buf = ngx_http_push_request_body_to_single_buffer(r);
  }
  else {
    ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, "push module: unexpected publisher message request body buffer location. please report this to the push module developers.");
    return NULL;
  }
  
  NGX_HTTP_PUSH_BROADCAST_CHECK(buf, NULL, r, "push module: can't find or allocate publisher request body buffer");
      
  content_type_len = (r->headers_in.content_type!=NULL ? r->headers_in.content_type->value.len : 0);
  
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  
  //create a buffer copy in shared mem
  msg = ngx_http_push_slab_alloc_locked(sizeof(*msg) + content_type_len, "message + content_type");
  NGX_HTTP_PUSH_BROADCAST_CHECK_LOCKED(msg, NULL, r, "push module: unable to allocate message in shared memory", ngx_http_push_shpool);
  previous_msg=ngx_http_push_get_latest_message_locked(channel); //need this for entity-tags generation
  
  buf_copy = ngx_http_push_slab_alloc_locked(NGX_HTTP_BUF_ALLOC_SIZE(buf), "message buffer copy");
  NGX_HTTP_PUSH_BROADCAST_CHECK_LOCKED(buf_copy, NULL, r, "push module: unable to allocate buffer in shared memory", ngx_http_push_shpool) //magic nullcheck
  ngx_http_push_copy_preallocated_buffer(buf, buf_copy);
  
  msg->buf=buf_copy;
  
  //Stamp the new message with entity tags
  msg->message_time=ngx_time(); //ESSENTIAL TODO: make sure this ends up producing GMT time
  msg->message_tag=(previous_msg!=NULL && msg->message_time == previous_msg->message_time) ? (previous_msg->message_tag + 1) : 0;    
  
  //store the content-type
  if(content_type_len>0) {
    msg->content_type.len=r->headers_in.content_type->value.len;
    msg->content_type.data=(u_char *) (msg+1); //we had reserved a contiguous chunk, myes?
    ngx_memcpy(msg->content_type.data, r->headers_in.content_type->value.data, msg->content_type.len);
  }
  else {
    msg->content_type.len=0;
    msg->content_type.data=NULL;
  }
  
  //queue stuff ought to be NULL
  msg->queue.prev=NULL;
  msg->queue.next=NULL;
  
  msg->refcount=0;
  
  //set message expiration time
  time_t                  message_timeout = cf->buffer_timeout;
  msg->expires = (message_timeout==0 ? 0 : (ngx_time() + message_timeout));
  
  msg->delete_oldest_received_min_messages = cf->delete_oldest_received_message ? (ngx_uint_t) cf->min_messages : NGX_MAX_UINT32_VALUE;
  //NGX_MAX_UINT32_VALUE to disable, otherwise = min_message_buffer_size of the publisher location from whence the message came
  
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, CREATED_DBG, msg, msg->refcount, msg->queue.prev, msg->queue.next);
  return msg;
}

static ngx_int_t ngx_http_push_store_enqueue_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_http_push_loc_conf_t *cf) {
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  ngx_queue_insert_tail(&channel->message_queue->queue, &msg->queue);
  channel->messages++;
  
  //now see if the queue is too big
  if(channel->messages > (ngx_uint_t) cf->max_messages) {
    //exceeeds max queue size. don't force it, someone might still be using this message.
    ngx_http_push_delete_message_locked(channel, ngx_http_push_get_oldest_message_locked(channel), 0);
  }
  if(channel->messages > (ngx_uint_t) cf->min_messages) {
    //exceeeds min queue size. maybe delete the oldest message
    //no, don't do anything for now. This feature is badly implemented and I think I'll deprecate it.
  }

  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, ENQUEUED_DBG, msg, msg->refcount, msg->queue.prev, msg->queue.next);
  return NGX_OK;
}

static ngx_int_t default_publish_callback(ngx_int_t status, ngx_http_push_channel_t *ch, ngx_http_request_t *r) {
  return status;
}

static ngx_int_t ngx_http_push_store_publish_message(ngx_str_t *channel_id, ngx_http_request_t *r, ngx_int_t (*callback)(ngx_int_t status, ngx_http_push_channel_t *ch, ngx_http_request_t *r)) {
  ngx_http_push_channel_t        *channel;
  ngx_http_push_msg_t            *msg;
  ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  ngx_int_t                       result=0;
  if(callback==NULL) {
    callback=&default_publish_callback;
  }
  if((channel=ngx_http_push_store_get_channel(channel_id, cf->channel_timeout, NULL))==NULL) { //always returns a channel, unless no memory left
    return callback(NGX_ERROR, NULL, r);
    //ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
  }
  
  if((msg = ngx_http_push_store_create_message(channel, r))==NULL) {
    return callback(NGX_ERROR, channel, r);
    //ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
  }
  
  if(cf->max_messages > 0) { //channel buffers exist
    ngx_http_push_store_enqueue_message(channel, msg, cf);
  }
  else if(cf->max_messages == 0) {
    ngx_http_push_store_reserve_message(NULL, msg);
  }
  result= ngx_http_push_store_publish_raw(channel, msg, 0, NULL);
  return callback(result, channel, r);
}

static ngx_int_t ngx_http_push_store_send_worker_message(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *subscriber_sentinel, ngx_pid_t pid, ngx_int_t worker_slot, ngx_http_push_msg_t *msg, ngx_int_t status_code) {
  ngx_http_push_worker_msg_sentinel_t   *worker_messages = ((ngx_http_push_shm_data_t *)ngx_http_push_shm_zone->data)->ipc;
  ngx_http_push_worker_msg_sentinel_t   *sentinel = &worker_messages[worker_slot];
  ngx_http_push_worker_msg_t            *newmessage;
  if((newmessage=ngx_http_push_slab_alloc(sizeof(*newmessage), "IPC worker message"))==NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate worker message");
    return NGX_ERROR;
  }
  newmessage->msg = msg;
  newmessage->status_code = status_code;
  newmessage->pid = pid;
  newmessage->subscriber_sentinel = subscriber_sentinel;
  newmessage->channel = channel;
  
  ngx_http_push_store_lock_shmem();
  ngx_queue_insert_tail(&sentinel->queue, &newmessage->queue);
  ngx_http_push_store_unlock_shmem();
  return NGX_OK;
  
}

static void ngx_http_push_store_receive_worker_message(void) {
  ngx_http_push_worker_msg_t     *prev_worker_msg, *worker_msg;
  ngx_http_push_worker_msg_sentinel_t    *sentinel;
  const ngx_str_t                *status_line = NULL;
  ngx_http_push_channel_t        *channel;
  ngx_http_push_subscriber_t     *subscriber_sentinel;
  ngx_int_t                       worker_msg_pid;
  
  ngx_int_t                       status_code;
  ngx_http_push_msg_t            *msg;
  
  sentinel = &(((ngx_http_push_shm_data_t *)ngx_http_push_shm_zone->data)->ipc)[ngx_process_slot];
  
  ngx_http_push_store_lock_shmem();
  worker_msg = (ngx_http_push_worker_msg_t *)ngx_queue_next(&sentinel->queue);
  ngx_http_push_store_unlock_shmem();
  while((void *)worker_msg != (void *)sentinel) {
    
    ngx_http_push_store_lock_shmem();
    worker_msg_pid = worker_msg->pid;
    ngx_http_push_store_unlock_shmem();
    
    if(worker_msg_pid == ngx_pid) {
      //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "process_worker_message processing proper worker_msg ");
      //everything is okay.
      
      ngx_http_push_store_lock_shmem();
      status_code = worker_msg->status_code;
      msg = worker_msg->msg;
      channel = worker_msg->channel;
      subscriber_sentinel = worker_msg->subscriber_sentinel;
      ngx_http_push_store_unlock_shmem();
      
      if(msg==NULL) {
        //just a status line, is all    
        //status code only.
        switch(status_code) {
          case NGX_HTTP_CONFLICT:
            status_line=&NGX_HTTP_PUSH_HTTP_STATUS_409;
            break;
            
          case NGX_HTTP_GONE:
            status_line=&NGX_HTTP_PUSH_HTTP_STATUS_410;
            break;
            
          case 0:
            ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: worker message contains neither a channel message nor a status code");
            //let's let the subscribers know that something went wrong and they might've missed a message
            status_code = NGX_HTTP_INTERNAL_SERVER_ERROR; 
            //intentional fall-through
          default:
            status_line=NULL;
        }
      }
      
      ngx_http_push_respond_to_subscribers(channel, subscriber_sentinel, msg, status_code, status_line);
    }
    else {
      //that's quite bad you see. a previous worker died with an undelivered message.
      //but all its subscribers' connections presumably got canned, too. so it's not so bad after all.
      //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "process_worker_message processing INVALID worker_msg ");
      
      ngx_http_push_store_lock_shmem();
      
      ngx_http_push_pid_queue_t     *channel_worker_sentinel = worker_msg->channel->workers_with_subscribers;
      
      ngx_http_push_pid_queue_t     *channel_worker_cur = channel_worker_sentinel;
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: worker %i intercepted a message intended for another worker process (%i) that probably died", ngx_pid, worker_msg->pid);
      
      //delete that invalid sucker.
      while((channel_worker_cur=(ngx_http_push_pid_queue_t *)ngx_queue_next(&channel_worker_cur->queue))!=channel_worker_sentinel) {
        if(channel_worker_cur->pid == worker_msg->pid) {
          ngx_queue_remove(&channel_worker_cur->queue);
          ngx_http_push_slab_free_locked(channel_worker_cur);
          break;
        }
      }
      
      ngx_http_push_store_unlock_shmem();
      
    }
    //It may be worth it to memzero worker_msg for debugging purposes.
    prev_worker_msg = worker_msg;
    
    ngx_http_push_store_lock_shmem();
    worker_msg = (ngx_http_push_worker_msg_t *)ngx_queue_next(&worker_msg->queue);
    ngx_http_push_slab_free_locked(prev_worker_msg);
    ngx_http_push_store_unlock_shmem();

  }
  ngx_http_push_store_lock_shmem();
  ngx_queue_init(&sentinel->queue); //reset the worker message sentinel
  ngx_http_push_store_unlock_shmem();
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "process_worker_message finished");
  return;
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
    &ngx_http_push_store_get_message, //+callback
    &ngx_http_push_store_subscribe, //+callback
    &ngx_http_push_store_publish_message, //+callback
    
    //channel stuff,
    &ngx_http_push_store_get_channel, //creates channel if not found, +callback
    &ngx_http_push_store_find_channel, //returns channel or NULL if not found, +callback
    &ngx_http_push_store_delete_channel,
    
    &ngx_http_push_store_get_channel_message,
    &ngx_http_push_store_reserve_message,
    &ngx_http_push_store_release_message,
    
    //channel properties
    &ngx_http_push_store_channel_subscribers,
    &ngx_http_push_store_channel_worker_subscribers,
    &ngx_http_push_store_channel_next_subscriber,
    &ngx_http_push_store_channel_release_subscriber_sentinel,

    //legacy shared-memory store helpers
    &ngx_http_push_store_lock_shmem,
    &ngx_http_push_store_unlock_shmem,
    &ngx_http_push_slab_alloc_locked,
    &ngx_http_push_slab_free_locked,
    
    //message stuff
    &ngx_http_push_store_create_message,
    &ngx_http_push_delete_message,
    &ngx_http_push_delete_message_locked,
    &ngx_http_push_store_enqueue_message,
    &ngx_http_push_store_etag_from_message,
    &ngx_http_push_store_content_type_from_message,
    
    //interprocess communication
    &ngx_http_push_store_send_worker_message,
    &ngx_http_push_store_receive_worker_message
    

};
