//worker processes of the world, unite.
#include <ngx_http_push_module.h>
#include <ngx_channel.h>
#include "ipc.h"
#include <assert.h>
#include "shmem.h"
#include "store-private.h"


#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "FP:%i ipc: " fmt, mpt->fake_slot, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "FP:%i ipc: " fmt, mpt->fake_slot, ##args)

static void ipc_channel_handler(ngx_event_t *ev);

ipc_t *ipc_create(ngx_cycle_t *cycle) {
  ipc_t *ipc;
  ipc = ngx_calloc(sizeof(*ipc), cycle->log);
  if(ipc == NULL) {
    return NULL;
  }
  DBG("created IPC %p", ipc);
  return ipc;
}

ngx_int_t ipc_destroy(ipc_t *ipc, ngx_cycle_t *cycle) {
  DBG("destroying IPC %p", ipc);
  ngx_free(ipc);
  return NGX_OK;
}

ngx_int_t ipc_set_handler(ipc_t *ipc, void (*alert_handler)(ngx_int_t, ngx_uint_t, void *)) {
  ipc->handler=alert_handler;
  return NGX_OK;
}

ngx_int_t ipc_open(ipc_t *ipc, ngx_cycle_t *cycle, ngx_int_t workers) {
//initialize socketpairs for workers in advance.
  static int invalid_sockets_initialized = 0;
  int                             i, s = 0, on = 1;
  ngx_int_t                       last_expected_process = ngx_last_process;
  
  if(!invalid_sockets_initialized) {
    for(i=0; i< NGX_MAX_PROCESSES; i++) {
      ipc->socketpairs[i][0]=NGX_INVALID_FILE;
      ipc->socketpairs[i][1]=NGX_INVALID_FILE;
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
    ngx_socket_t               *socks = ipc->socketpairs[s];
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

ngx_int_t ipc_close(ipc_t *ipc, ngx_cycle_t *cycle) {
  int i;
  for (i=0; i<NGX_MAX_PROCESSES; i++) {
    if(ipc->socketpairs[i][0] != NGX_INVALID_FILE || ipc->socketpairs[i][1] != NGX_INVALID_FILE) {
      ngx_close_channel(ipc->socketpairs[i], cycle->log);
    }
  }
  return NGX_OK;
}

ngx_int_t ipc_start(ipc_t *ipc, ngx_cycle_t *cycle) {
  if (ngx_add_channel_event(cycle, ipc->socketpairs[ngx_process_slot][1], NGX_READ_EVENT, ipc_channel_handler) == NGX_ERROR) {
    ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, "failed to register channel handler while initializing push module worker");
    return NGX_ERROR;
  }
  return NGX_OK;
}

typedef struct {
  ipc_t          *ipc;
  ngx_int_t       src_slot;
  ngx_int_t       dst_slot;
  ngx_uint_t      code;
  void           *data[IPC_DATA_SIZE];
} ipc_alert_t;

ngx_int_t ipc_read_channel(ngx_socket_t s, ipc_alert_t *alert, ngx_log_t *log) {
  DBG("IPC read channel");
  ssize_t             n;
  ngx_err_t           err;
  struct iovec        iov[1];
  struct msghdr       msg;
 
#if (NGX_HAVE_MSGHDR_MSG_CONTROL)
  union {
    struct cmsghdr  cm;
    char            space[CMSG_SPACE(sizeof(int))];
  } cmsg;
#else
  int                 fd;
#endif
 
  iov[0].iov_base = (char *) alert;
  iov[0].iov_len = sizeof(*alert);
 
  msg.msg_name = NULL;
  msg.msg_namelen = 0;
  msg.msg_iov = iov;
  msg.msg_iovlen = 1;
 
#if (NGX_HAVE_MSGHDR_MSG_CONTROL)
  msg.msg_control = (caddr_t) &cmsg;
  msg.msg_controllen = sizeof(cmsg);
#else
  msg.msg_accrights = (caddr_t) &fd;
  msg.msg_accrightslen = sizeof(int);
#endif
 
  n = recvmsg(s, &msg, 0);
 
  if (n == -1) {
    err = ngx_errno;
    if (err == NGX_EAGAIN) {
      return NGX_AGAIN;
    }
 
    ngx_log_error(NGX_LOG_ALERT, log, err, "recvmsg() failed");
    return NGX_ERROR;
  }
 
  if (n == 0) {
    ngx_log_debug0(NGX_LOG_DEBUG_CORE, log, 0, "recvmsg() returned zero");
    return NGX_ERROR;
  }
 
  if ((size_t) n < sizeof(*alert)) {
  ngx_log_error(NGX_LOG_ALERT, log, 0, "recvmsg() returned not enough data: %z", n);
    return NGX_ERROR;
  }
 
  return n;
 }

static void ipc_channel_handler(ngx_event_t *ev) {
  DBG("IPC channel handler");
  //copypasta from os/unix/ngx_process_cycle.c (ngx_channel_handler)
  ngx_int_t          n;
  ipc_alert_t        alert = {0};
  ngx_connection_t  *c;
  if (ev->timedout) {
    ev->timedout = 0;
    return;
  }
  c = ev->data;
  
  while(1) {
    n = ipc_read_channel(c->fd, &alert, ev->log);
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

    if(ngx_process_slot != alert.dst_slot) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "process %i got alert intented for pid %i. don';t care, doing it anyway.", ngx_process_slot, alert.dst_slot);
      alert.ipc->handler(alert.src_slot, alert.code, alert.data);
    }
    else {
      alert.ipc->handler(alert.src_slot, alert.code, alert.data);
    }
  }
} 

ngx_int_t ipc_alert(ipc_t *ipc, ngx_int_t slot, ngx_uint_t code, void *data, size_t data_size) {
  DBG("IPC send alert code %i to slot %i", code, slot);
  //ripped from ngx_send_channel
  
  ipc_alert_t         alert = {0};
  
  alert.ipc = ipc;
  alert.src_slot = memstore_slot();
  alert.dst_slot = slot;
  alert.code = code;
  ngx_memcpy(alert.data, data, data_size);
  
  //switch to destination
  memstore_fakeprocess_push(alert.dst_slot);
  alert.ipc->handler(alert.src_slot, alert.code, alert.data);
  memstore_fakeprocess_pop();
  //switch back  
  
  return NGX_OK;
}

