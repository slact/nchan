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

static void ngx_http_push_reserve_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
  msg->refcount++;
  //we need a refcount because channel messages MAY be dequed before they are used up. It thus falls on the IPC stuff to free it.
}

static void ngx_http_push_release_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
  msg->refcount--;
  if(msg->queue.next==NULL && msg->refcount<=0) { 
    //message had been dequeued and nobody needs it anymore
    ngx_http_push_free_message_locked(msg, ngx_http_push_shpool);
  }
  if(channel->messages > msg->delete_oldest_received_min_messages && ngx_http_push_get_oldest_message_locked(channel) == msg) {
    ngx_http_push_delete_message_locked(channel, msg, ngx_http_push_shpool);
  }
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
    ngx_shmtx_unlock(&shpool->mutex);
    if(msg->buf->file->fd!=NGX_INVALID_FILE) {
      ngx_close_file(msg->buf->file->fd);
    }
    ngx_delete_file(msg->buf->file->name.data); //should I care about deletion errors? doubt it.
    ngx_shmtx_lock(&shpool->mutex);
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


static ngx_http_push_channel_t * ngx_http_push_store_get_channel(ngx_str_t *id, ngx_http_push_loc_conf_t *cf, ngx_log_t *log) {
  //get the channel and check channel authorization while we're at it.
  ngx_http_push_channel_t        *channel;
  ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
  if (cf->authorize_channel==1) {
    channel = ngx_http_push_find_channel(id, log);
  }else{
    channel = ngx_http_push_get_channel(id, log, cf->channel_timeout);
  }
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

static void ngx_http_push_store_exit_worker(ngx_cycle_t *cycle) {
  ngx_http_push_ipc_exit_worker(cycle);
}

static void ngx_http_push_store_exit_master(ngx_cycle_t *cycle) {
  //destroy channel tree in shared memory
  ngx_http_push_walk_rbtree(ngx_http_push_movezig_channel_locked);
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
  
    &ngx_http_push_store_get_channel,
    &ngx_http_push_store_get_message,
};
