//worker processes of the world, unite.

#define NGX_CMD_HTTP_PUSH_CHECK_MESSAGES 8 //some number. (looks hacky)

static void ngx_http_push_channel_handler(ngx_event_t *ev) {
    //mostly copied from ngx_channel_handler (os/unix/ngx_process_cycle.c)
    ngx_int_t          n;
    ngx_channel_t      ch;
    ngx_connection_t  *c;
    if (ev->timedout) {
        ev->timedout = 0;
        return;
    }
    c = ev->data;
    for ( ;; ) {
        n = ngx_read_channel(c->fd, &ch, (size_t) sizeof(ch), ev->log);
        if (n == NGX_ERROR) {
            if (ngx_event_flags & NGX_USE_EPOLL_EVENT) {
                ngx_del_conn(c, 0);
            }
            ngx_close_connection(c);
            return;
        }
        if ((ngx_event_flags & NGX_USE_EVENTPORT_EVENT) && (ngx_add_event(ev, NGX_READ_EVENT, 0) == NGX_ERROR)) {
            return;
        }
        if (n == NGX_AGAIN) { 
			return; 
		}
   
        //the custom command now.
        if (ch.command == NGX_CMD_HTTP_PUSH_CHECK_MESSAGES) {
            //take a look at the message queue for this worker process in shared memory.
			ngx_http_push_process_listener_message_queue();
        }
    }
}

static ngx_int_t ngx_http_push_signal_worker(ngx_int_t worker_slot, ngx_log_t *log) {
	ngx_channel_t                  ch;
	ch.command = NGX_CMD_HTTP_PUSH_CHECK_MESSAGES;
	ch.fd = -1;
	return ngx_write_channel(ngx_processes[worker_slot].channel[0 /*or 1?*/], &ch, (size_t) sizeof(ch), log);
}

static void ngx_http_push_process_listener_message_queue() {
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
	ngx_queue_t                    *sentinel, *cur;
	ngx_http_push_worker_msg_t     *worker_msg;
	ngx_shmtx_lock(&shpool->mutex);
	sentinel = (ngx_queue_t *) (((ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data)->worker_message_queue + ngx_process_slot);
	cur = sentinel->next;
	while(cur!=sentinel) {
		//RAM is not a series of tubes. well, actually, it kind of is... much more so than a dump truck, anyway.
		worker_msg = (ngx_http_push_worker_msg_t *) cur;
		cur=cur->next;
		if(!worker_msg->headers_only) {
			ngx_shmtx_unlock(&shpool->mutex);
			ngx_http_push_respond_to_listener_request(worker_msg->request, worker_msg->msg, shpool);
			ngx_shmtx_lock(&shpool->mutex);
			if(worker_msg->msg->queue.next==NULL && (--worker_msg->msg->refcount)==0) { 
				//message was dequeued, and nobody needs it anymore
				ngx_http_push_free_message_locked(worker_msg->msg, shpool);
			}
		}
		else {
			ngx_http_push_reply_status_only(worker_msg->request, worker_msg->status_code, worker_msg->status_line);
		}
		//free stuff.
		//TODO: don't free anything. instead, set 'dirty' bit to reuse memory. periodically clean the queue with a timer.
		ngx_slab_free_locked(shpool, worker_msg);
	}
	ngx_shmtx_unlock(&shpool->mutex);
}

static ngx_int_t ngx_http_push_queue_worker_message(ngx_int_t worker_slot, ngx_http_request_t *r, ngx_http_push_msg_t *msg, ngx_int_t status_code, ngx_str_t *status_line) {
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
	ngx_queue_t                    *sentinel;
	ngx_shmtx_lock(&shpool->mutex);
	sentinel = (ngx_queue_t *) (((ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data)->worker_message_queue + worker_slot);
	ngx_http_push_worker_msg_t     *worker_msg = ngx_slab_alloc_locked(shpool, sizeof(*worker_msg));
	if(worker_msg==NULL) {
		ngx_shmtx_unlock(&shpool->mutex);
		return NGX_ERROR;
	}
	worker_msg->request=r;
	worker_msg->msg=msg;
	worker_msg->status_code=status_code;
	worker_msg->status_line=status_line;
	
	msg->refcount++;
	
	ngx_queue_insert_tail(sentinel, (&worker_msg->queue));
	ngx_shmtx_unlock(&shpool->mutex);
	return NGX_OK;
}