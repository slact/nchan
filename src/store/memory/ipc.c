//worker processes of the world, unite.
#include <nchan_module.h>
#include <ngx_channel.h>
#include <assert.h>
#include "ipc.h"
#include "groups.h"

#include "store-private.h"

#define DEBUG_LEVEL NGX_LOG_DEBUG
//#define DEBUG_LEVEL NGX_LOG_WARN

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "IPC:" fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "IPC:" fmt, ##args)

//#define DEBUG_DELAY_IPC_RECEIVE_ALERT_MSEC 100

static ngx_event_t  receive_alert_delay_log_timer;
static ngx_event_t  send_alert_delay_log_timer;
static void receive_alert_delay_log_timer_handler(ngx_event_t *ev);
static void send_alert_delay_log_timer_handler(ngx_event_t *ev);

static void ipc_read_handler(ngx_event_t *ev);

ngx_int_t ipc_init(ipc_t *ipc) {
  int                             i = 0;
  ipc_process_t                  *proc;
  
  nchan_init_timer(&receive_alert_delay_log_timer, receive_alert_delay_log_timer_handler, NULL);
  nchan_init_timer(&send_alert_delay_log_timer, send_alert_delay_log_timer_handler, NULL);
  
  for(i=0; i< NGX_MAX_PROCESSES; i++) {
    proc = &ipc->process[i];
    proc->ipc = ipc;
    proc->pipe[0]=NGX_INVALID_FILE;
    proc->pipe[1]=NGX_INVALID_FILE;
    proc->c=NULL;
    proc->active = 0;
    ngx_memzero(proc->wbuf.alerts, sizeof(proc->wbuf.alerts));
    proc->wbuf.first = 0;
    proc->wbuf.n = 0;
    proc->wbuf.overflow_first = NULL;
    proc->wbuf.overflow_last = NULL;
    proc->wbuf.overflow_n = 0;
    ipc->worker_slots[i]=NGX_ERROR;
  }
  ipc->workers = NGX_ERROR;
  return NGX_OK;
}

ngx_int_t ipc_set_handler(ipc_t *ipc, void (*alert_handler)(ngx_int_t, ngx_uint_t, void *)) {
  ipc->handler=alert_handler;
  return NGX_OK;
}

static void ipc_try_close_fd(ngx_socket_t *fd) {
  if(*fd != NGX_INVALID_FILE) {
    ngx_close_socket(*fd);
    *fd=NGX_INVALID_FILE;
  }
}

ngx_int_t ipc_open(ipc_t *ipc, ngx_cycle_t *cycle, ngx_int_t workers, void (*slot_callback)(int slot, int worker)) {
//initialize pipes for workers in advance.
  int                             i, j, s = 0;
  ngx_int_t                       last_expected_process = ngx_last_process;
  ipc_process_t                  *proc;
  ngx_socket_t                   *socks;
  
  /* here's the deal: we have no control over fork()ing, nginx's internal 
    * socketpairs are unusable for our purposes (as of nginx 0.8 -- check the 
    * code to see why), and the module initialization callbacks occur before
    * any workers are spawned. Rather than futzing around with existing 
    * socketpairs, we make our own pipes array. 
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
    
    if(slot_callback) {
      slot_callback(s, i);
    }
    
    ipc->worker_slots[i] = s;
    
    proc = &ipc->process[s];

    socks = proc->pipe;
    
    if(proc->active) {
      // reinitialize already active pipes. This is done to prevent IPC alerts
      // from a previous restart that were never read from being received by
      // a newly restarted worker
      ipc_try_close_fd(&socks[0]);
      ipc_try_close_fd(&socks[1]);
      proc->active = 0;
    }
    
    assert(socks[0] == NGX_INVALID_FILE && socks[1] == NGX_INVALID_FILE);
    
    //make-a-pipe
    if (pipe(socks) == -1) {
      ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, "pipe() failed while initializing nchan IPC");
      return NGX_ERROR;
    }
    //make both ends nonblocking
    for(j=0; j <= 1; j++) {
      if (ngx_nonblocking(socks[j]) == -1) {
	ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, ngx_nonblocking_n " failed on pipe socket %i while initializing nchan", j);
	ipc_try_close_fd(&socks[0]);
	ipc_try_close_fd(&socks[1]);
	return NGX_ERROR;
      }
    }
    //It's ALIIIIIVE! ... erm.. active...
    proc->active = 1;
    
    s++; //NEXT!!
  }
  ipc->workers = workers;
  
  //ERR("ipc_alert_t size %i bytes", sizeof(ipc_alert_t));
  
  return NGX_OK;
}



ngx_int_t ipc_close(ipc_t *ipc, ngx_cycle_t *cycle) {
  int i;
  ipc_process_t            *proc;
  ipc_writebuf_overflow_t  *of, *of_next;
  
  DBG("start closing");
  
  for (i=0; i<NGX_MAX_PROCESSES; i++) {
    proc = &ipc->process[i];
    if(!proc->active) continue;
    
    if(proc->c) {
      ngx_close_connection(proc->c);
      proc->c = NULL;
    }
    
    for(of = proc->wbuf.overflow_first; of != NULL; of = of_next) {
      of_next = of->next;
      ngx_free(of);
    }
    
    ipc_try_close_fd(&proc->pipe[0]);
    ipc_try_close_fd(&proc->pipe[1]);
    ipc->process[i].active = 0;
  }
  DBG("done closing");
  return NGX_OK;
}

static ngx_uint_t delayed_sent_alerts_count;
static ngx_uint_t delayed_sent_alerts_delay;

static void send_alert_delay_log_timer_handler(ngx_event_t *ev) {
  nchan_log_notice("Sending %ui interprocess alert%s delayed by %ui sec.", delayed_sent_alerts_count, delayed_sent_alerts_count == 1 ? "" : "s", (ngx_uint_t)(delayed_sent_alerts_count > 0 ? delayed_sent_alerts_delay / delayed_sent_alerts_count : 0));
  
  delayed_sent_alerts_count = 0;
  delayed_sent_alerts_delay = 0;
}

static void ipc_record_alert_send_delay(ngx_uint_t delay) {
  delayed_sent_alerts_count ++;
  delayed_sent_alerts_delay += delay;
  nchan_update_stub_status(ipc_total_send_delay, delay);
  if(!send_alert_delay_log_timer.timer_set && !ngx_exiting && !ngx_quit) {
    ngx_add_timer(&send_alert_delay_log_timer, 1000);
  }
}

static ngx_int_t ipc_write_alert_fd(ngx_socket_t fd, ipc_alert_t *alert) {
  int         n;
  ngx_int_t   err;
  
  n = write(fd, alert, sizeof(*alert));
 
  if (n == -1) {
    err = ngx_errno;
    if (err == NGX_EAGAIN) {
      return NGX_AGAIN;
    }
    
    ngx_log_error(NGX_LOG_ALERT, ngx_cycle->log, err, "write() failed");
    assert(0);
    return NGX_ERROR;
  }
  
  if(ngx_time() - alert->time_sent >= 2) {
    ipc_record_alert_send_delay(ngx_time() - alert->time_sent);
  }
  return NGX_OK;
}

static void ipc_write_handler(ngx_event_t *ev) {
  ngx_connection_t        *c = ev->data;
  ngx_socket_t             fd = c->fd;
  
  ipc_process_t           *proc = (ipc_process_t *) c->data;
  ipc_alert_t             *alerts = proc->wbuf.alerts;
  
  int                      n = proc->wbuf.n;
  int                      i, first = proc->wbuf.first, last = first + n;
  uint8_t                  write_aborted = 0;
  
  //DBG("%i alerts to write, with %i in overflow", proc->wbuf.n, proc->wbuf.overflow_n);
  
  if(!memstore_ready()) {
#if nginx_version >= 1008000
    ev->cancelable = 1;
#endif
    ngx_add_timer(ev, 1000);
    return;
  }
#if nginx_version >= 1008000
  else {
    ev->cancelable = 0;
  }
#endif
  
  for(i = first; i < last; i++) {
    //ERR("send alert at %i", i % IPC_WRITEBUF_SIZE );
    if(ipc_write_alert_fd(fd, &alerts[i % IPC_WRITEBUF_SIZE]) != NGX_OK) {
      write_aborted = 1;
      //DBG("write aborted at %i iter. first: %i, n: %i", i - first, first, proc->wbuf.n);
      break;
    }
    /*
    else {
      DBG("wrote alert at %i", i % IPC_WRITEBUF_SIZE);
    }
    */
  }
  
  if(i==last) { //sent all outstanding alerts
    //DBG("finished writing %i alerts.", proc->wbuf.n);
    proc->wbuf.first = 0; // for debugging and stuff
    proc->wbuf.n = 0;
  }
  else {
    proc->wbuf.first = i;
    proc->wbuf.n -= (i - first);
    //DBG("first now at %i, %i alerts remain", i, proc->wbuf.n);
  }
  
  nchan_update_stub_status(ipc_queue_size, proc->wbuf.n - n);
  
  if(proc->wbuf.overflow_n > 0 && i - first > 0) {
    ipc_writebuf_overflow_t  *of;
    first = proc->wbuf.first + proc->wbuf.n;
    last = first + (IPC_WRITEBUF_SIZE - proc->wbuf.n);
    //DBG("try to squeeze in overflow between %i and %i", first, last);
    for(i = first; i < last; i++) {
      of = proc->wbuf.overflow_first;
      ///DBG("looking at overflow %p next %p", of, of->next);
      alerts[i % IPC_WRITEBUF_SIZE] = of->alert;
      proc->wbuf.overflow_n--;
      proc->wbuf.n++;
      assert(proc->wbuf.overflow_n >= 0);
      
      proc->wbuf.overflow_first = of->next;
      
      ngx_free(of);
      
      //DBG("squeezed in overflow at %i, %i overflow remaining", i, proc->wbuf.overflow_n);
      
      if(proc->wbuf.overflow_first == NULL) {
        proc->wbuf.overflow_last = NULL;
        break;
      }
    }
    
    if(!write_aborted) {
      //retry
      //DBG("retry write after squeezing in overflow");
      ipc_write_handler(ev);
      return;
    }
    
  }
  
  if(write_aborted) {
    //DBG("re-add event because the write failed");
    ngx_handle_write_event(c->write, 0);
  }
}

ngx_int_t ipc_register_worker(ipc_t *ipc, ngx_cycle_t *cycle) {
  int                    i;    
  ngx_connection_t      *c;
  ipc_process_t         *proc;
  
  for(i=0; i< NGX_MAX_PROCESSES; i++) {
    
    proc = &ipc->process[i];
    
    if(!proc->active) continue;
    
    assert(proc->pipe[0] != NGX_INVALID_FILE);
    assert(proc->pipe[1] != NGX_INVALID_FILE);
    
    if(i==ngx_process_slot) {
      //set up read connection
      c = ngx_get_connection(proc->pipe[0], cycle->log);
      c->data = ipc;
      
      c->read->handler = ipc_read_handler;
      c->read->log = cycle->log;
      c->write->handler = NULL;
      
      ngx_add_event(c->read, NGX_READ_EVENT, 0);
      proc->c=c;
    }
    else {
      //set up write connection
      c = ngx_get_connection(proc->pipe[1], cycle->log);
      
      c->data = proc;
      
      c->read->handler = NULL;
      c->write->log = cycle->log;
      c->write->handler = ipc_write_handler;
      
      proc->c=c;
    }
  }
  return NGX_OK;
}

static ngx_int_t ipc_read_socket(ngx_socket_t s, ipc_alert_t *alert, ngx_log_t *log) {
  DBG("IPC read channel");
  ssize_t             n;
  ngx_err_t           err;
  //static char         buf[sizeof(ipc_alert_t) * 2];
  //static char        *cur;
  
  n = read(s, alert, sizeof(ipc_alert_t));
 
  if (n == -1) {
    err = ngx_errno;
    if (err == NGX_EAGAIN) {
      return NGX_AGAIN;
    }
    
    ngx_log_error(NGX_LOG_ERR, log, err, "nchan IPC: read() failed");
    return NGX_ERROR;
  }
 
  if (n == 0) {
    ngx_log_debug0(NGX_LOG_ERR, log, 0, "nchan IPC: read() returned zero");
    return NGX_ERROR;
  }
 
  if ((size_t) n < sizeof(*alert)) {
    ngx_log_error(NGX_LOG_ERR, log, 0, "nchan IPC: read() returned not enough data: %z", n);
    return NGX_ERROR;
  }
  
  return n;
}

static ngx_uint_t delayed_received_alerts_count;
static ngx_uint_t delayed_received_alerts_delay;

static void receive_alert_delay_log_timer_handler(ngx_event_t *ev) {
  nchan_log_notice("Received %ui interprocess alert%s delayed by %ui sec.", delayed_received_alerts_count, delayed_received_alerts_count == 1 ? "" : "s", (ngx_uint_t)(delayed_received_alerts_count > 0 ? delayed_received_alerts_delay / delayed_received_alerts_count : 0));
  
  delayed_received_alerts_count = 0;
  delayed_received_alerts_delay = 0;
}

static void ipc_record_alert_receive_delay(ngx_uint_t delay) {
  delayed_received_alerts_count ++;
  delayed_received_alerts_delay += delay;
  nchan_update_stub_status(ipc_total_receive_delay, delay);
  if(!receive_alert_delay_log_timer.timer_set && !ngx_exiting && !ngx_quit) {
    ngx_add_timer(&receive_alert_delay_log_timer, 1000);
  }
}

#if DEBUG_DELAY_IPC_RECEIVE_ALERT_MSEC
typedef struct {
  ngx_event_t   timer;
  ipc_alert_t   alert;
  ipc_t        *ipc;
} delayed_alert_glob_t;
static void fake_ipc_alert_delay_handler(ngx_event_t *ev) {
  delayed_alert_glob_t *glob = (delayed_alert_glob_t *)ev->data;
  
  if(ngx_time() - glob->alert.time_sent >= 2) {
    ipc_record_alert_receive_delay(ngx_time() - glob->alert.time_sent);
  }
  
  glob->ipc->handler(glob->alert.src_slot, glob->alert.code, glob->alert.data);
  ngx_free(glob);
}
#endif

static void ipc_read_handler(ngx_event_t *ev) {
  DBG("IPC channel handler");
  //copypasta from os/unix/ngx_process_cycle.c (ngx_channel_handler)
  ngx_int_t          n;
  ipc_alert_t        alert;
  ngx_connection_t  *c;
  if (ev->timedout) {
    ev->timedout = 0;
    return;
  }
  c = ev->data;
  
  while(1) {
    n = ipc_read_socket(c->fd, &alert, ev->log);
    if (n == NGX_ERROR) {
      ERR("IPC_READ_SOCKET failed: bad connection. This should never have happened, yet here we are...");
      assert(0);
      return;
    }
    if (n == NGX_AGAIN) {
      return;
    }
    //ngx_log_debug1(NGX_LOG_DEBUG_CORE, ev->log, 0, "nchan: channel command: %d", ch.command);
    
    assert(n == sizeof(alert));
    if(alert.worker_generation < memstore_worker_generation) {
      ERR("Got IPC alert for previous generation's worker. discarding.");
    }
    else {
#if DEBUG_DELAY_IPC_RECEIVE_ALERT_MSEC
      delayed_alert_glob_t   *glob = ngx_alloc(sizeof(*glob), ngx_cycle->log);
      if (NULL == glob) {
          ERR("Couldn't allocate memory for alert glob data.");
          return;
      }
      ngx_memzero(&glob->timer, sizeof(glob->timer));
      nchan_init_timer(&glob->timer, fake_ipc_alert_delay_handler, glob);
      
      glob->alert = alert;
      glob->ipc = (ipc_t *)c->data;
      ngx_add_timer(&glob->timer, DEBUG_DELAY_IPC_RECEIVE_ALERT_MSEC);
#else
      if(ngx_time() - alert.time_sent >= 2) {
        ipc_record_alert_receive_delay(ngx_time() - alert.time_sent);
      }
      nchan_update_stub_status(ipc_total_alerts_received, 1);
      ((ipc_t *)c->data)->handler(alert.src_slot, alert.code, alert.data);
#endif
    }
  }
}


ngx_int_t ipc_broadcast_alert(ipc_t *ipc, ngx_uint_t code, void *data, size_t data_size) {
  ngx_int_t   i, slot, rc, ret = NGX_OK;
  ngx_int_t   my_slot = memstore_slot();
  DBG("broadcast alert");
  for(i=0; i < ipc->workers; i++) {
    slot = ipc->worker_slots[i];
    if(my_slot != slot) {
      if((rc = ipc_alert(ipc, slot, code, data, data_size)) != NGX_OK)  {
        ERR("Error sending alert to slot %i", slot);
        ret = NGX_ERROR;
      }
    }
  }
  return ret;
}

ngx_int_t ipc_alert(ipc_t *ipc, ngx_int_t slot, ngx_uint_t code, void *data, size_t data_size) {
  DBG("IPC send alert code %i to slot %i", code, slot);
  
  if(data_size > IPC_DATA_SIZE) {
    ERR("IPC_DATA_SIZE too small. wanted %i, have %i", data_size, IPC_DATA_SIZE);
    assert(0);
  }
  nchan_update_stub_status(ipc_total_alerts_sent, 1);
#if (FAKESHARD)
  
  ipc_alert_t         alert = {0};
  
  alert.src_slot = memstore_slot();
  alert.time_sent = ngx_time();
  alert.worker_generation = memstore_worker_generation;
  alert.code = code;
  ngx_memcpy(alert.data, data, data_size);
  
  //switch to destination
  memstore_fakeprocess_push(slot);
  ipc->handler(alert.src_slot, alert.code, alert.data);
  memstore_fakeprocess_pop();
  //switch back  
  
#else
  
  ipc_process_t      *proc = &ipc->process[slot];
  ipc_writebuf_t     *wb = &proc->wbuf;
  ipc_alert_t        *alert;
  
  assert(proc->active);
  
  nchan_update_stub_status(ipc_queue_size, 1);
  
  if(wb->n < IPC_WRITEBUF_SIZE) {
    alert = &wb->alerts[(wb->first + wb->n++) % IPC_WRITEBUF_SIZE];
  }
  else { //overflow
    ipc_writebuf_overflow_t  *overflow;
    DBG("writebuf overflow, allocating memory");
    if((overflow = ngx_alloc(sizeof(*overflow), ngx_cycle->log)) == NULL) {
      ERR("can't allocate memory for IPC write buffer overflow");
      return NGX_ERROR;
    }
    overflow->next = NULL;
    alert= &overflow->alert;
    
    if(wb->overflow_first == NULL) {
      wb->overflow_first = overflow;
    }
    if(wb->overflow_last) {
      wb->overflow_last->next = overflow;
    }
    wb->overflow_last = overflow;
  
    wb->overflow_n++;
  }
  
  alert->src_slot = ngx_process_slot;
  alert->time_sent = ngx_time();
  alert->code = code;
  alert->worker_generation = memstore_worker_generation;
  ngx_memcpy(&alert->data, data, data_size);
  
  ipc_write_handler(proc->c->write);
  
  //ngx_handle_write_event(ipc->c[slot]->write, 0);
  //ngx_add_event(ipc->c[slot]->write, NGX_WRITE_EVENT, NGX_CLEAR_EVENT);
  
#endif

  return NGX_OK;
}

