//worker processes of the world, unite.

static void ngx_http_push_channel_handler(ngx_event_t *ev);
static ngx_inline void ngx_http_push_process_worker_message(void);
static void ngx_http_push_ipc_exit_worker(ngx_cycle_t *cycle);

#define NGX_CMD_HTTP_PUSH_CHECK_MESSAGES 49

ngx_socket_t                       ngx_http_push_socketpairs[NGX_MAX_PROCESSES][2];

static ngx_int_t ngx_http_push_init_ipc(ngx_cycle_t *cycle, ngx_int_t workers) {
	int                             i, s = 0, on = 1;
	ngx_int_t                       last_expected_process = ngx_last_process;
	
	/* here's the deal: we have no control over fork()ing, nginx's internal 
	  socketpairs are unusable for our purposes (as of nginx 0.8 -- check the 
	  code to see why), and the module initialization callbacks occur before
	  any workers are spawned. Rather than futzing around with existing 
	  socketpairs, we populate our own socketpairs array. 
	  Trouble is, ngx_spawn_process() creates them one-by-one, and we need to 
	  do it all at once. So we must guess all the workers' ngx_process_slots in 
	  advance. Meaning the spawning logic must be copied to the T.
	*/
	
	for(i=0; i < workers; i++) {
	
		while (s < last_expected_process && ngx_processes[s].pid != -1) {
			//find empty existing slot
			s++;
		}
			
		//copypasta from os/unix/ngx_process.c (ngx_spawn_process)
		ngx_socket_t               *socks = ngx_http_push_socketpairs[s];
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
		
		s++; //NEXT!!
	}
	return NGX_OK;
}

static void ngx_http_push_ipc_exit_worker(ngx_cycle_t *cycle) {
	ngx_close_channel((ngx_socket_t *) ngx_http_push_socketpairs[ngx_process_slot], cycle->log);
}
 
//will be called many times
static ngx_int_t	ngx_http_push_init_ipc_shm(ngx_int_t workers) {
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
	ngx_http_push_shm_data_t       *d = (ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data;
	ngx_http_push_worker_msg_t     *worker_messages;
	int                             i;
	ngx_shmtx_lock(&shpool->mutex);
	if(d->ipc!=NULL) {
		//already initialized...
		ngx_shmtx_unlock(&shpool->mutex);
		return NGX_OK;
	}
	//initialize worker message queues
	if((worker_messages = ngx_slab_alloc_locked(shpool, sizeof(*worker_messages)*workers))==NULL) {
		ngx_shmtx_unlock(&shpool->mutex);
		return NGX_ERROR;
	}
	for(i=0; i<workers; i++) {
		ngx_queue_init(&worker_messages[i].queue);
	}
	d->ipc=worker_messages;
	ngx_shmtx_unlock(&shpool->mutex);
	return NGX_OK;
}

static ngx_int_t ngx_http_push_register_worker_message_handler(ngx_cycle_t *cycle) {
	if (ngx_add_channel_event(cycle, ngx_http_push_socketpairs[ngx_process_slot][1], NGX_READ_EVENT, ngx_http_push_channel_handler) == NGX_ERROR) {
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
			ngx_http_push_process_worker_message();
		}
	}
}

static ngx_int_t ngx_http_push_alert_worker(ngx_pid_t pid, ngx_int_t slot, ngx_log_t *log) {
	//seems ch doesn't need to have fd set. odd, but roll with it. pid and process slot also unnecessary.
	static ngx_channel_t            ch = {NGX_CMD_HTTP_PUSH_CHECK_MESSAGES, 0, 0, -1};
	return ngx_write_channel(ngx_http_push_socketpairs[slot][0], &ch, sizeof(ngx_channel_t), log);
}

static ngx_inline void ngx_http_push_process_worker_message(void) {
	ngx_http_push_worker_msg_t     *prev_worker_msg, *worker_msg, *sentinel;
	const ngx_str_t                *status_line = NULL;
	ngx_http_push_channel_t        *channel;
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *)ngx_http_push_shm_zone->shm.addr;
	ngx_http_push_subscriber_t     *subscriber_sentinel;
	
	ngx_shmtx_lock(&shpool->mutex);
	
	ngx_http_push_worker_msg_t     *worker_messages = ((ngx_http_push_shm_data_t *)ngx_http_push_shm_zone->data)->ipc;
	ngx_int_t                       status_code;
	ngx_http_push_msg_t            *msg;
	
	sentinel = &worker_messages[ngx_process_slot];
	worker_msg = (ngx_http_push_worker_msg_t *)ngx_queue_next(&sentinel->queue);
	while(worker_msg != sentinel) {
		if(worker_msg->pid==ngx_pid) {
			//everything is okay.
			status_code = worker_msg->status_code;
			msg = worker_msg->msg;
			channel = worker_msg->channel;
			subscriber_sentinel = worker_msg->subscriber_sentinel;
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
			ngx_shmtx_unlock(&shpool->mutex);
			ngx_http_push_respond_to_subscribers(channel, subscriber_sentinel, msg, status_code, status_line);
			ngx_shmtx_lock(&shpool->mutex);
		}
		else {
			//that's quite bad you see. a previous worker died with an undelivered message.
			//but all its subscribers' connections presumably got canned, too. so it's not so bad after all.
			
			ngx_http_push_pid_queue_t     *channel_worker_sentinel = &worker_msg->channel->workers_with_subscribers;
			ngx_http_push_pid_queue_t     *channel_worker_cur = channel_worker_sentinel;
			ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: worker %i intercepted a message intended for another worker process (%i) that probably died", ngx_pid, worker_msg->pid);
			
			//delete that invalid sucker.
			while((channel_worker_cur=(ngx_http_push_pid_queue_t *)ngx_queue_next(&channel_worker_cur->queue))!=channel_worker_sentinel) {
				if(channel_worker_cur->pid == worker_msg->pid) {
					ngx_queue_remove(&channel_worker_cur->queue);
					ngx_slab_free_locked(shpool, channel_worker_cur);
					break;
				}
			}
		}
		//It may be worth it to memzero worker_msg for debugging purposes.
		prev_worker_msg = worker_msg;
		worker_msg = (ngx_http_push_worker_msg_t *)ngx_queue_next(&worker_msg->queue);
		ngx_slab_free_locked(shpool, prev_worker_msg);
	}
	ngx_queue_init(&sentinel->queue); //reset the worker message sentinel
	ngx_shmtx_unlock(&shpool->mutex);
	return;
}

static ngx_int_t ngx_http_push_send_worker_message(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *subscriber_sentinel, ngx_pid_t pid, ngx_int_t worker_slot, ngx_http_push_msg_t *msg, ngx_int_t status_code, ngx_log_t *log) {
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *)ngx_http_push_shm_zone->shm.addr;
	ngx_http_push_worker_msg_t     *worker_messages = ((ngx_http_push_shm_data_t *)ngx_http_push_shm_zone->data)->ipc;
	ngx_http_push_worker_msg_t     *thisworker_messages = worker_messages + worker_slot;
	ngx_http_push_worker_msg_t     *newmessage;
	ngx_shmtx_lock(&shpool->mutex);
	if((newmessage=ngx_slab_alloc_locked(shpool, sizeof(*newmessage)))==NULL) {
		ngx_shmtx_unlock(&shpool->mutex);
		ngx_log_error(NGX_LOG_ERR, log, 0, "push module: unable to allocate worker message");
		return NGX_ERROR;
	}
	ngx_queue_insert_tail(&thisworker_messages->queue, &newmessage->queue);
	newmessage->msg = msg;
	newmessage->status_code = status_code;
	newmessage->pid = pid;
	newmessage->subscriber_sentinel = subscriber_sentinel;
	newmessage->channel = channel;
	ngx_shmtx_unlock(&shpool->mutex);
	return NGX_OK;
}
