//worker processes of the world, unite.

#define NGX_CMD_HTTP_PUSH_CHECK_MESSAGES 87 //some unlikely number. (looks hacky)

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
        }
    }
}

static ngx_int_t ngx_http_push_signal_worker(ngx_int_t worker_slot, ngx_log_t *log) {
	ngx_channel_t                  ch;
	ch.command = NGX_CMD_HTTP_PUSH_CHECK_MESSAGES;
	ch.fd = -1;
	return ngx_write_channel(ngx_processes[worker_slot].channel[0 /*or 1?*/], &ch, (size_t) sizeof(ch), log);
}