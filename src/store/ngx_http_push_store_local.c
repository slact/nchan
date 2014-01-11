//fuck it, copypaste
#define NGX_HTTP_PUSH_MAKE_ETAG(message_tag, etag, alloc_func, pool)                 \
    etag = alloc_func(pool, sizeof(*etag) + NGX_INT_T_LEN);                          \
    if(etag!=NULL) {                                                                 \
        etag->data = (u_char *)(etag+1);                                             \
        etag->len = ngx_sprintf(etag->data,"%ui", message_tag)- etag->data;          \
    }

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


ngx_http_push_channel_queue_t channel_gc_sentinel;
ngx_slab_pool_t    *ngx_http_push_shpool = NULL;
ngx_shm_zone_t     *ngx_http_push_shm_zone = NULL;

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

//garbage-collecting slab allocator
void * ngx_http_push_slab_alloc_locked(size_t size) {
  void  *p;
  if((p = ngx_slab_alloc_locked(ngx_http_push_shpool, size))==NULL) {
    ngx_http_push_channel_queue_t *ccur, *cnext;
    ngx_uint_t                  collected = 0;
    //failed. emergency garbage sweep, then.
    
    //collect channels
    ngx_queue_init(&channel_gc_sentinel.queue);
    ngx_http_push_walk_rbtree(ngx_http_push_channel_collector);
    for(ccur=(ngx_http_push_channel_queue_t *)ngx_queue_next(&channel_gc_sentinel.queue); ccur != &channel_gc_sentinel; ccur=cnext) {
      cnext = (ngx_http_push_channel_queue_t *)ngx_queue_next(&ccur->queue);
      ngx_http_push_delete_channel_locked(ccur->channel);
      ngx_free(ccur);
      collected++;
    }
    
    //todo: collect worker messages maybe
    
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "push module: out of shared memory. emergency garbage collection deleted %ui unused channels.", collected);
    
    return ngx_slab_alloc_locked(ngx_http_push_shpool, size);
  }
  return p;
}

static void ngx_http_push_store_lock_shmem(void){
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
}
static void ngx_http_push_store_unlock_shmem(void){
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
}
void * ngx_http_push_slab_alloc(size_t size) {
  void  *p;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  p = ngx_http_push_slab_alloc_locked(size);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return p;
}


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
  msg->refcount++;
  //we need a refcount because channel messages MAY be dequed before they are used up. It thus falls on the IPC stuff to free it.
}

static void ngx_http_push_store_reserve_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  ngx_http_push_store_reserve_message_locked(channel, msg);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  //we need a refcount because channel messages MAY be dequed before they are used up. It thus falls on the IPC stuff to free it.
}

static void ngx_http_push_store_release_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
  msg->refcount--;
  if(msg->queue.next==NULL && msg->refcount<=0) { 
    //message had been dequeued and nobody needs it anymore
    ngx_http_push_free_message_locked(msg, ngx_http_push_shpool);
  }
  if(channel->messages > msg->delete_oldest_received_min_messages && ngx_http_push_get_oldest_message_locked(channel) == msg) {
    ngx_http_push_delete_message_locked(channel, msg, ngx_http_push_shpool);
  }
}

static void ngx_http_push_store_release_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  ngx_http_push_store_release_message_locked(channel, msg);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
}

// remove a message from queue and free all associated memory. assumes shpool is already locked.
static ngx_inline void ngx_http_push_general_delete_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t force, ngx_slab_pool_t *shpool) {
  if (msg==NULL) { 
    return; 
  }
  if(channel!=NULL) {
    ngx_queue_remove(&msg->queue);
    channel->messages--;
  }
  if(msg->refcount<=0 || force) {
    //nobody needs this message, or we were forced at integer-point to delete
    ngx_http_push_free_message_locked(msg, shpool);
  }
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
  ngx_slab_free_locked(shpool, msg->buf); //separate block, remember?
  ngx_slab_free_locked(shpool, msg);
}

/** find message with entity tags matching those of the request r.
  * @param r subscriber request
  */
static ngx_http_push_msg_t * ngx_http_push_find_message_locked(ngx_http_push_channel_t *channel, ngx_http_request_t *r, ngx_int_t *status) {
  //TODO: consider using an RBTree for message storage.
  ngx_queue_t                    *sentinel = &channel->message_queue->queue;
  ngx_queue_t                    *cur = ngx_queue_head(sentinel);
  ngx_http_push_msg_t            *msg;
  ngx_int_t                       tag = -1;
  time_t                          time = (r->headers_in.if_modified_since == NULL) ? 0 : ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
  
  //channel's message buffer empty?
  if(channel->messages==0) {
    *status=NGX_HTTP_PUSH_MESSAGE_EXPECTED; //wait.
    return NULL;
  }
  
  // do we want a future message?
  msg = ngx_queue_data(sentinel->prev, ngx_http_push_msg_t, queue); 
  if(time <= msg->message_time) { //that's an empty check (Sentinel's values are zero)
    if(time == msg->message_time) {
      if(tag<0) { tag = ngx_http_push_subscriber_get_etag_int(r); }
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
      if(tag<0) { tag = ngx_http_push_subscriber_get_etag_int(r); }
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

static ngx_http_push_channel_t * ngx_http_push_store_find_channel(ngx_str_t *id, time_t channel_timeout, ngx_log_t *log) {
  //get the channel and check channel authorization while we're at it.
  ngx_http_push_channel_t        *channel;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  channel = ngx_http_push_find_channel(id, channel_timeout, log);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return channel;
}

//temporary cheat
static ngx_int_t ngx_http_push_store_publish(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line, ngx_log_t *log) {
 //subscribers are queued up in a local pool. Queue heads, however, are located
  //in shared memory, identified by pid.
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  ngx_http_push_pid_queue_t     *sentinel = &channel->workers_with_subscribers;
  ngx_http_push_pid_queue_t     *cur = sentinel;
  ngx_int_t                      received;
  received = channel->subscribers > 0 ? NGX_HTTP_PUSH_MESSAGE_RECEIVED : NGX_HTTP_PUSH_MESSAGE_QUEUED;

  if(msg!=NULL && received==NGX_HTTP_PUSH_MESSAGE_RECEIVED) {
    //just for now
    ngx_http_push_store_reserve_message_locked(channel, msg);
  }
  
  while((cur=(ngx_http_push_pid_queue_t *)ngx_queue_next(&cur->queue))!=sentinel) {
    pid_t           worker_pid  = cur->pid;
    ngx_int_t       worker_slot = cur->slot;
    ngx_http_push_subscriber_t *subscriber_sentinel= cur->subscriber_sentinel;
    /*
    each time all of a worker's subscribers are removed, so is the sentinel. 
    this is done to make garbage collection easier. Assuming we want to avoid
    placing the sentinel in shared memory (for now -- it's a little tricky
    to debug), the owner of the worker pool must be the one to free said sentinel.
    But channels may be deleted by different worker processes, and it seems unwieldy
    (for now) to do IPC just to delete one stinkin' sentinel. Hence a new sentinel
    is used every time the subscriber queue is emptied.
    */
    cur->subscriber_sentinel = NULL; //think about it it terms of garbage collection. it'll make sense. sort of.
    ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
    if(subscriber_sentinel != NULL) {
      if(worker_pid == ngx_pid) {
        //my subscribers
        ngx_http_push_respond_to_subscribers(channel, subscriber_sentinel, msg, status_code, status_line);
      }
      else {
        //some other worker's subscribers
        //interprocess communication breakdown
        if(ngx_http_push_send_worker_message(channel, subscriber_sentinel, worker_pid, worker_slot, msg, status_code, log) != NGX_ERROR) {
          ngx_http_push_alert_worker(worker_pid, worker_slot, log);
        }
        else {
          ngx_log_error(NGX_LOG_ERR, log, 0, "push module: error communicating with some other worker process");
        }
        
      }
    }
    ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  }
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return received;
}


static ngx_int_t ngx_http_push_store_delete_channel(ngx_http_push_channel_t *channel, ngx_http_request_t *r) {
  ngx_http_push_msg_t            *msg, *sentinel;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  sentinel = channel->message_queue; 
  msg = sentinel;
        
  while((msg=(ngx_http_push_msg_t *)ngx_queue_next(&msg->queue))!=sentinel) {
    //force-delete all the messages
    ngx_http_push_force_delete_message_locked(NULL, msg, ngx_http_push_shpool);
  }
  channel->messages=0;
  
  //410 gone
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  
  ngx_http_push_store_publish(channel, NULL, NGX_HTTP_GONE, &NGX_HTTP_PUSH_HTTP_STATUS_410, r->connection->log);
  
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  ngx_http_push_delete_channel_locked(channel);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return NGX_OK;
}
static ngx_http_push_channel_t * ngx_http_push_store_get_channel(ngx_str_t *id, time_t channel_timeout, ngx_log_t *log) {
  //get the channel and check channel authorization while we're at it.
  ngx_http_push_channel_t        *channel;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  channel = ngx_http_push_get_channel(id, channel_timeout, log);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return channel;
}

static ngx_http_push_msg_t * ngx_http_push_store_get_message(ngx_http_push_channel_t *channel, ngx_http_request_t *r, ngx_int_t                       *msg_search_outcome, ngx_http_push_loc_conf_t *cf, ngx_log_t *log) {
  ngx_http_push_msg_t *msg;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  msg = ngx_http_push_find_message_locked(channel, r, msg_search_outcome);
  channel->last_seen = ngx_time();
  channel->expires = ngx_time() + cf->channel_timeout;
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
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
  
  if ((d = (ngx_http_push_shm_data_t *)ngx_slab_alloc(shpool, sizeof(*d))) == NULL) { //shm_data plus an array.
    return NGX_ERROR;
  } 
  shm_zone->data = d;
  d->ipc=NULL;
  //initialize rbtree
  if ((sentinel = ngx_slab_alloc(shpool, sizeof(*sentinel)))==NULL) {
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

static ngx_int_t ngx_http_push_store_init_worker(ngx_cycle_t *cycle) {
  ngx_core_conf_t                *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  return ngx_http_push_init_ipc_shm(ccf->worker_processes);
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
    ngx_http_push_force_delete_message_locked(channel, msg, ngx_http_push_shpool);
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

static void ngx_http_push_store_exit_worker(ngx_cycle_t *cycle) {
  ngx_http_push_ipc_exit_worker(cycle);
}

static void ngx_http_push_store_exit_master(ngx_cycle_t *cycle) {
  //destroy channel tree in shared memory
  ngx_http_push_walk_rbtree(ngx_http_push_movezig_channel_locked);
}

static ngx_http_push_subscriber_t * ngx_http_push_store_subscribe(ngx_http_push_channel_t *channel, ngx_http_request_t *r) {
  ngx_http_push_pid_queue_t  *sentinel, *cur, *found;
  ngx_http_push_subscriber_t *subscriber;
  ngx_http_push_subscriber_t *subscriber_sentinel;
  //ngx_http_push_loc_conf_t   *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  
  //subscribers are queued up in a local pool. Queue sentinels are separate and also local, but not in the pool.
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  sentinel = &channel->workers_with_subscribers;
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
    if((found=ngx_http_push_slab_alloc_locked(sizeof(*found)))==NULL) {
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

static ngx_str_t * ngx_http_push_store_etag_from_message(ngx_http_push_msg_t *msg){
  ngx_str_t *etag;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  NGX_HTTP_PUSH_MAKE_ETAG(msg->message_tag, etag, ngx_palloc, ngx_http_push_pool);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return etag;
}

static ngx_str_t * ngx_http_push_store_content_type_from_message(ngx_http_push_msg_t *msg){
  ngx_str_t *etag;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  NGX_HTTP_PUSH_MAKE_ETAG(msg->message_tag, etag, ngx_palloc, ngx_http_push_pool);
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return etag;
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
  msg = ngx_http_push_slab_alloc_locked(sizeof(*msg) + content_type_len);
  NGX_HTTP_PUSH_BROADCAST_CHECK_LOCKED(msg, NULL, r, "push module: unable to allocate message in shared memory", ngx_http_push_shpool);
  previous_msg=ngx_http_push_get_latest_message_locked(channel); //need this for entity-tags generation
  
  buf_copy = ngx_http_push_slab_alloc_locked(NGX_HTTP_BUF_ALLOC_SIZE(buf));
  NGX_HTTP_PUSH_BROADCAST_CHECK_LOCKED(buf_copy, NULL, r, "push module: unable to allocate buffer in shared memory", ngx_http_push_shpool) //magic nullcheck
  ngx_http_push_copy_preallocated_buffer(buf, buf_copy);
  
  msg->buf=buf_copy;
  
  if(cf->store_messages) {
    ngx_queue_insert_tail(&channel->message_queue->queue, &msg->queue);
    channel->messages++;
  }
  
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
  
  //set message expiration time
  time_t                  message_timeout = cf->buffer_timeout;
  msg->expires = (message_timeout==0 ? 0 : (ngx_time() + message_timeout));
  
  msg->delete_oldest_received_min_messages = cf->delete_oldest_received_message ? (ngx_uint_t) cf->min_messages : NGX_MAX_UINT32_VALUE;
  //NGX_MAX_UINT32_VALUE to disable, otherwise = min_message_buffer_size of the publisher location from whence the message came
  
  //now see if the queue is too big
  if(channel->messages > (ngx_uint_t) cf->max_messages) {
    //exceeeds max queue size. force-delete oldest message
    ngx_http_push_force_delete_message_locked(channel, ngx_http_push_get_oldest_message_locked(channel), ngx_http_push_shpool);
  }
  if(channel->messages > (ngx_uint_t) cf->min_messages) {
    //exceeeds min queue size. maybe delete the oldest message
    ngx_http_push_msg_t    *oldest_msg = ngx_http_push_get_oldest_message_locked(channel);
    NGX_HTTP_PUSH_BROADCAST_CHECK_LOCKED(oldest_msg, NULL, r, "push module: oldest message not found", ngx_http_push_shpool);
  }
  
  ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
  return msg;
}

ngx_http_push_store_t  ngx_http_push_store_local = {
    //init
    &ngx_http_push_store_init_module,
    &ngx_http_push_store_init_worker,
    &ngx_http_push_store_init_postconfig,
    &ngx_http_push_store_create_main_conf,
    
    //shutdown
    &ngx_http_push_store_exit_worker,
    &ngx_http_push_store_exit_master,
  
    //channel stuff
    &ngx_http_push_store_get_channel, //creates channel if not found
    &ngx_http_push_store_find_channel, //returns channel or NULL if not found
    &ngx_http_push_store_delete_channel,
    &ngx_http_push_store_get_message,
    &ngx_http_push_store_reserve_message,
    &ngx_http_push_store_release_message,
    
    //pub/sub
    &ngx_http_push_store_publish,
    &ngx_http_push_store_subscribe,
    
    //channel properties
    &ngx_http_push_store_channel_subscribers,
    &ngx_http_push_store_channel_worker_subscribers,
    
    &ngx_http_push_store_lock_shmem, //legacy shared-memory store helpers
    &ngx_http_push_store_unlock_shmem, //legacy shared-memory store helpers
    
    //message stuff
    &ngx_http_push_store_create_message,
    &ngx_http_push_store_etag_from_message,
    &ngx_http_push_store_content_type_from_message
};
