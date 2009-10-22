//worker processes of the world, unite.
typedef struct {
	ngx_queue_t                     queue;
	ngx_http_push_msg_t            *msg;
	ngx_http_request_t             *request;
	ngx_int_t                       headers_only:1;
	ngx_int_t                       status_code;
	ngx_str_t                      *status_line;
	ngx_pid_t                       pid; 
} ngx_http_push_worker_msg_t;

static void ngx_http_push_channel_handler(ngx_event_t *ev);

#define NGX_CMD_HTTP_PUSH_CHECK_MESSAGES 49

ngx_socket_t *ngx_http_push_socketpairs;

static ngx_int_t ngx_http_push_init_ipc(ngx_cycle_t *cycle, ngx_int_t workers) {
	int                             s, on = 1;
	if((ngx_http_push_socketpairs = (ngx_socket_t *) ngx_calloc(sizeof(ngx_socket_t[2])*workers, cycle->log))==NULL) {
		return NGX_ERROR;
	}
	for(s=0; s < workers; s++) {
		//copypasta from os/unix/ngx_process.c (ngx_spawn_process)
		ngx_socket_t                socks[2];
		if (socketpair(AF_UNIX, SOCK_STREAM, 0, socks) == -1) {
			ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, "socketpair() failed on socketpair while initializing push module");
			return NGX_ERROR;
		}
		if (ngx_nonblocking(socks[0]) == -1) {
			ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, ngx_nonblocking_n " failed on socketpair while initializing push module");
			ngx_close_channel(socks, cycle->log);
			return NGX_ERROR;
		}
		if (ngx_nonblocking(socks[1]) == -1) {
			ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,	ngx_nonblocking_n " failed on socketpair while initializing push module");
			ngx_close_channel(socks, cycle->log);
			return NGX_ERROR;
		}
		if (ioctl(socks[0], FIOASYNC, &on) == -1) {
			ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, "ioctl(FIOASYNC) failed on socketpair while initializing push module");
			ngx_close_channel(socks, cycle->log);
			return NGX_ERROR;
		}

		if (fcntl(socks[0], F_SETOWN, ngx_pid) == -1) {
			ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, "fcntl(F_SETOWN) failed on socketpair while initializing push module");
			ngx_close_channel(socks, cycle->log);
			return NGX_ERROR;
		}
		if (fcntl(socks[0], F_SETFD, FD_CLOEXEC) == -1) {
			ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno,	"fcntl(FD_CLOEXEC) failed on socketpair while initializing push module");
			ngx_close_channel(socks, cycle->log);
			return NGX_ERROR;
		}

		if (fcntl(socks[1], F_SETFD, FD_CLOEXEC) == -1) {
			ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, "fcntl(FD_CLOEXEC) failed while initializing push module");
			ngx_close_channel(socks, cycle->log);
			return NGX_ERROR;
		}
		ngx_http_push_socketpairs[2*s]=socks[0];
		(ngx_http_push_socketpairs[2*s+1])=socks[1];
	}
	return NGX_OK;
}

//will be called many times
static ngx_int_t	ngx_http_push_init_ipc_shm(ngx_int_t workers) {
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
	ngx_http_push_shm_data_t       *d = (ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data;
	ngx_queue_t                    *worker_message_queue;
	ngx_int_t                       i;
	ngx_shmtx_lock(&shpool->mutex);
	if(d->ipc!=NULL) {
		//already initialized...
		ngx_shmtx_unlock(&shpool->mutex);
		return NGX_OK;
	}
	//initialize worker message queues
	if((worker_message_queue = ngx_slab_alloc_locked(shpool, sizeof(ngx_queue_t)*workers))==NULL) {
		ngx_shmtx_unlock(&shpool->mutex);
		return NGX_ERROR;
	}
	d->ipc=worker_message_queue;
	for (i=0; i<workers; i++) {
		ngx_queue_init((&worker_message_queue[i]));
	}
	ngx_shmtx_unlock(&shpool->mutex);
	return NGX_OK;
}

static ngx_int_t ngx_http_push_register_worker_message_handler(ngx_cycle_t *cycle) {
	if (ngx_add_channel_event(cycle, ngx_http_push_socketpairs[2*ngx_process_slot+1], NGX_READ_EVENT, ngx_http_push_channel_handler) == NGX_ERROR) {
		ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, "failed to register channel handler while initializing push module worker");
		return NGX_ERROR;
	}
	return NGX_OK;
}

static void ngx_http_push_channel_handler(ngx_event_t *ev) {
	//copypasta from os/unix/ngx_process_cycle.c (ngx_channel_handler)
	ngx_int_t          n;
	ngx_channel_t      ch;
	ngx_connection_t  *c;
	if (ev->timedout) {
		ev->timedout = 0;
		return;
	}
	c = ev->data;
	
	while(1) {
		n = ngx_read_channel(c->fd, &ch, sizeof(ch), ev->log);
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
		//ngx_log_debug1(NGX_LOG_DEBUG_CORE, ev->log, 0, "push module: channel command: %d", ch.command);

		if (ch.command==NGX_CMD_HTTP_PUSH_CHECK_MESSAGES) {
			ngx_http_push_process_worker_message_queue();
		}
	}
}

static ngx_int_t ngx_http_push_alert_worker(ngx_pid_t pid, ngx_int_t slot, ngx_log_t *log) {
	//seems ch doesn't need to have fd set. odd, but roll with it. pid and process slot also unnecessary.
	static ngx_channel_t            ch = {NGX_CMD_HTTP_PUSH_CHECK_MESSAGES, 0, 0, -1};
	return ngx_write_channel(ngx_http_push_socketpairs[2*slot], &ch, sizeof(ngx_channel_t), log);
}

static void ngx_http_push_process_worker_message_queue() {
	ngx_queue_t                    *sentinel, *cur;
	ngx_http_push_worker_msg_t     *worker_msg;
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *)ngx_http_push_shm_zone->shm.addr;
	 ngx_http_push_shm_data_t      *d = (ngx_http_push_shm_data_t *)ngx_http_push_shm_zone->data;
	ngx_shmtx_lock(&shpool->mutex);
	ngx_queue_t                    *worker_message_queue = (ngx_queue_t *)d->ipc; //why did i make ipc void* again?
	
	sentinel = &worker_message_queue[ngx_process_slot];
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
		else if(worker_msg->pid==ngx_pid) { 
			// make sure the message is intended for this worker. This check is
			// necessary since a worker may be terminated and a new one may use
			// its ngx_process_slot. i think.
			ngx_shmtx_unlock(&shpool->mutex);
			ngx_http_push_reply_status_only(worker_msg->request, worker_msg->status_code, worker_msg->status_line);
			ngx_shmtx_lock(&shpool->mutex);
		}
		//free stuff.
		//TODO: don't free anything. instead, set 'dirty' bit to reuse memory. periodically clean the queue with a timer.
		ngx_queue_remove((&worker_msg->queue));
		ngx_slab_free_locked(shpool, worker_msg); 
	}
	ngx_shmtx_unlock(&shpool->mutex);
}

static ngx_int_t ngx_http_push_queue_worker_message(ngx_pid_t pid, ngx_int_t worker_slot, ngx_http_request_t *r, ngx_http_push_msg_t *msg, ngx_int_t status_code, ngx_str_t *status_line) {
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
	ngx_queue_t                    *sentinel;
	ngx_shmtx_lock(&shpool->mutex);
	sentinel = &((ngx_queue_t *)((ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data)->ipc)[worker_slot];

	ngx_http_push_worker_msg_t     *worker_msg = ngx_slab_alloc_locked(shpool, sizeof(*worker_msg));
	if(worker_msg==NULL) {
		ngx_shmtx_unlock(&shpool->mutex);
		return NGX_ERROR;
	}
	worker_msg->request=r;
	worker_msg->msg=msg;
	worker_msg->status_code=status_code;
	worker_msg->status_line=status_line;
	worker_msg->pid = pid;
	
	//we need a refcount because channel messages MAY be dequed before they are used up. It thus falls on the IPC stuff to free it.
	msg->refcount++; 
	
	ngx_queue_insert_tail(sentinel, (&worker_msg->queue));
	ngx_shmtx_unlock(&shpool->mutex);
	return NGX_OK;
}