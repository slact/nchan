//worker processes of the world, unite.
#include <ngx_http_push_module.h>
#include <ngx_channel.h>
#include <store/ngx_http_push_module_ipc.h>

static void ngx_http_push_channel_handler(ngx_event_t *ev);
#define NGX_CMD_HTTP_PUSH_CHECK_MESSAGES 49

static ngx_socket_t                       ngx_http_push_socketpairs[NGX_MAX_PROCESSES][2];
ngx_int_t ngx_http_push_init_ipc(ngx_cycle_t *cycle, ngx_int_t workers) {
//initialize socketpairs for workers in advance.
    static int invalid_sockets_initialized = 0;
    int                             i, s = 0, on = 1;
    ngx_int_t                       last_expected_process = ngx_last_process;
	
    if(!invalid_sockets_initialized) {
      for(i=0; i< NGX_MAX_PROCESSES; i++) {
        ngx_http_push_socketpairs[i][0]=NGX_INVALID_FILE;
        ngx_http_push_socketpairs[i][1]=NGX_INVALID_FILE;
      }
      invalid_sockets_initialized=1;
    }
    
    /* here's the deal: we have no control over fork()ing, nginx's internal 
     * socketpairs are unusable for our purposes (as of nginx 0.8 -- check the 
     * code to see why), and the module initialization callbacks occur before
     * any workers are spawned. Rather than futzing around with existing 
     * socketpairs, we populate our own socketpairs array. 
     * Trouble is, ngx_spawn_process() creates them one-by-one, and we need to 
     * do it all at once. So we must guess all the workers' ngx_process_slots in 
     * advance. Meaning the spawning logic must be copied to the T.
     * ... with some allowances for already-opened sockets...
     */
	for(i=0; i < workers; i++) {
		//copypasta from os/unix/ngx_process.c (ngx_spawn_process)
        while (s < last_expected_process && ngx_processes[s].pid != -1) {
          //find empty existing slot
          s++;
        }
        ngx_socket_t               *socks = ngx_http_push_socketpairs[s];
        if(socks[0] == NGX_INVALID_FILE || socks[1] == NGX_INVALID_FILE) {
          //copypasta from os/unix/ngx_process.c (ngx_spawn_process)
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
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, ngx_nonblocking_n " failed on socketpair while initializing push module");
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
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, "fcntl(FD_CLOEXEC) failed on socketpair while initializing push module");
            ngx_close_channel(socks, cycle->log);
            return NGX_ERROR;
          }
          
          if (fcntl(socks[1], F_SETFD, FD_CLOEXEC) == -1) {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, "fcntl(FD_CLOEXEC) failed while initializing push module");
            ngx_close_channel(socks, cycle->log);
            return NGX_ERROR;
          }
        }
        s++; //NEXT!!
	}
	return NGX_OK;
}

ngx_int_t ngx_http_push_shutdown_ipc(ngx_cycle_t *cycle) {
  int i;
  for (i=0; i<NGX_MAX_PROCESSES; i++) {
    if(ngx_http_push_socketpairs[i][0] != NGX_INVALID_FILE || ngx_http_push_socketpairs[i][1] != NGX_INVALID_FILE) {
      ngx_close_channel(ngx_http_push_socketpairs[i], cycle->log);
    }
  }
  return NGX_OK;
}

void ngx_http_push_ipc_exit_worker(ngx_cycle_t *cycle) {
  ngx_http_push_shutdown_ipc(cycle);
}
 
ngx_int_t ngx_http_push_ipc_init_worker(ngx_cycle_t *cycle) {
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
			ngx_http_push_store->receive_worker_message();
		}
	}
}

ngx_int_t ngx_http_push_alert_worker(ngx_pid_t pid, ngx_int_t slot) {
	//seems ch doesn't need to have fd set. odd, but roll with it. pid and process slot also unnecessary.
	static ngx_channel_t            ch = {NGX_CMD_HTTP_PUSH_CHECK_MESSAGES, 0, 0, -1};
	return ngx_write_channel(ngx_http_push_socketpairs[slot][0], &ch, sizeof(ngx_channel_t), ngx_cycle->log);
}

