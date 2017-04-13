//worker processes of the world, unite.
#include <ngx_http.h>

#include <nginx.h>
#include <ngx_channel.h>
#include <assert.h>
#include <limits.h>
#include "ipc.h"

#include <sys/uio.h>
#include <sys/mman.h>

//#define IPC_DEBUG_ON

#define LOG(ipc, code, lvl, fmt, args...) ngx_log_error(lvl, ngx_cycle->log, code, "IPC %s: " fmt, ((ipc) ? (ipc)->name : ""), ##args)
#ifdef IPC_DEBUG_ON
#define DBG(fmt, args...) LOG((ipc_t *)NULL, 0, NGX_LOG_WARN, fmt, ##args)
#else
#define DBG(fmt, args...)
#endif
#define ERR(ipc, fmt, args...) LOG(ipc, 0, NGX_LOG_ERR, fmt, ##args); \
  ngx_snprintf((ipc)->last_error, IPC_MAX_ERROR_LEN, fmt "%Z", ##args)
#define ERR_CODE(ipc, code, fmt, args...) LOG(ipc, 0, NGX_LOG_ERR, fmt, ##args); \
  ngx_snprintf((ipc)->last_error, IPC_MAX_ERROR_LEN, fmt "%Z", ##args)

#define NGX_MAX_HELPER_PROCESSES 0 // don't extend IPC to helpers. just workers for now.

//shared memory stuff
typedef struct {
  ngx_pid_t      pid;
  ngx_int_t      slot;
  ngx_int_t      process_type;
} process_slot_tracking_t;

typedef struct {
  process_slot_tracking_t  *process_slots;
  ngx_int_t                 process_count;
  ngx_shmtx_sh_t            lock;
  ngx_shmtx_t               mutex;
  
} ipc_shm_data_t;


static void ipc_worker_read_handler(ngx_event_t *ev);

static ngx_int_t ipc_open(ipc_t *ipc, ngx_cycle_t *cycle, ngx_int_t workers, void (*slot_callback)(int slot, int worker));
static ngx_int_t ipc_register_worker(ipc_t *ipc, ngx_cycle_t *cycle);
static void ipc_free_readbuf(ipc_channel_t *chan, ipc_readbuf_t *rbuf);
static ngx_int_t ipc_close_channel(ipc_channel_t *chan);


static ngx_int_t ipc_init_channel(ipc_t *ipc, ipc_channel_t *chan) {
  chan->ipc = ipc;
  chan->pipe[0]=NGX_INVALID_FILE;
  chan->pipe[1]=NGX_INVALID_FILE;
  chan->read_conn = NULL;
  chan->write_conn = NULL;
  chan->active = 0;
  chan->wbuf.head = NULL;
  chan->wbuf.tail = NULL;
  chan->wbuf.n = 0;
  chan->wbuf.last_iovec.n = 0;
  chan->rbuf_head = NULL;
  return NGX_OK;
}

static ipc_t *ipc_create(const char *ipc_name) {
  ipc_t *ipc=malloc(sizeof(*ipc));
  ngx_memzero(ipc, sizeof(*ipc));
  int                             i = 0;
  for(i=0; i< NGX_MAX_PROCESSES; i++) {
    ipc_init_channel(ipc, &ipc->worker_channel[i]);
  }
  
  ipc->shm = NULL;
  ipc->shm_sz = 0;
  
  ipc->name = ipc_name;
  ipc->worker_process_count = NGX_ERROR;
  
  return ipc;
}

ipc_t *ipc_init_module(const char *ipc_name, ngx_cycle_t *cycle) {
  ipc_t                          *ipc = ipc_create(ipc_name);
  ngx_core_conf_t                *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  ngx_int_t                       max_processes = ccf->worker_processes + NGX_MAX_HELPER_PROCESSES; 
  size_t                          process_slots_sz = sizeof(process_slot_tracking_t) * max_processes;
  ipc_shm_data_t                 *shdata;
  
  
  ipc->worker_process_count = ccf->worker_processes;
  
  ipc->shm_sz = sizeof(ipc_shm_data_t) + process_slots_sz;
  ipc->shm = mmap(NULL, ipc->shm_sz, PROT_READ|PROT_WRITE, MAP_ANON|MAP_SHARED, -1, 0);
  shdata = ipc->shm;
  ngx_memzero(shdata, sizeof(*shdata));
  shdata->process_slots = (process_slot_tracking_t *)&shdata[1];
  shdata->process_count = 0;
  
  ngx_shmtx_create(&shdata->mutex, &shdata->lock, (u_char *)ipc_name);
  
  ipc_open(ipc, cycle, max_processes, NULL);
  return ipc;
}

ngx_int_t ipc_init_worker(ipc_t *ipc, ngx_cycle_t *cycle) {
  ipc_shm_data_t                 *shdata = ipc->shm;
  ngx_int_t                       max_processes = ipc->worker_process_count + NGX_MAX_HELPER_PROCESSES;
  int                             i, found = 0;
  process_slot_tracking_t         *procslot;
  
  if (ngx_process != NGX_PROCESS_WORKER && ngx_process != NGX_PROCESS_SINGLE) {
    //not a worker, stop initializing stuff.
    return NGX_OK;
  }
  
  
  ngx_shmtx_lock(&shdata->mutex);

  for(i=0; !found && i<max_processes; i++) {
    procslot = &shdata->process_slots[i];
    if( procslot->pid == 0) {
      // empty procslot
      found = 1;
    }
    else if(procslot->slot == ngx_process_slot) {
      // replacing previously crashed(?) worker
      found = 1;
    }
  }

  if(found) {
    procslot->pid = ngx_pid;
    procslot->slot = ngx_process_slot; 
    procslot->process_type = ngx_process;
    DBG("ADD  process %i slot %i type %i", ngx_pid, ngx_process_slot, ngx_process);
    shdata->process_count++;
  }
  ngx_shmtx_unlock(&shdata->mutex);
  
  if(found) {
    return ipc_register_worker(ipc, cycle);
  }
  else {
    DBG("SKIP process %i slot %i type %i", ngx_pid, ngx_process_slot, ngx_process);
    return NGX_ERROR;
  }
}

ngx_int_t ipc_destroy(ipc_t *ipc) {
  int                  i;
  
  for (i=0; i<NGX_MAX_PROCESSES; i++) {
    ipc_close_channel(&ipc->worker_channel[i]);
    ipc->worker_channel[i].active = 0;
  }
  
  munmap(ipc->shm, ipc->shm_sz);
  free(ipc);
  return NGX_OK;
}

ngx_pid_t ipc_get_pid(ipc_t *ipc, int process_slot) {
  ipc_shm_data_t         *shdata = ipc->shm;
  int                     max_workers = ipc->worker_process_count;
  int                     i;
  process_slot_tracking_t *process_slots = shdata->process_slots;
  
  for(i=0; i<max_workers; i++) {
    if(process_slots[i].slot == process_slot) {
      return process_slots[i].pid;
    }
  }
  return NGX_INVALID_PID;
}
ngx_int_t ipc_get_slot(ipc_t *ipc, ngx_pid_t pid) {
  ipc_shm_data_t         *shdata = ipc->shm;
  int                     max_workers = ipc->worker_process_count;
  int                     i;
  process_slot_tracking_t *process_slots = shdata->process_slots;
  
  for(i=0; i<max_workers; i++) {
    if(process_slots[i].pid == pid) {
      return process_slots[i].slot;
    }
  }
  return NGX_ERROR;
}

ngx_int_t ipc_set_worker_alert_handler(ipc_t *ipc, ipc_alert_handler_pt alert_handler) {
  ipc->worker_alert_handler=alert_handler;
  return NGX_OK;
}

static void ipc_try_close_fd(ngx_socket_t *fd) {
  if(*fd != NGX_INVALID_FILE) {
    ngx_close_socket(*fd);
    *fd=NGX_INVALID_FILE;
  }
}

static ngx_int_t ipc_activate_channel(ipc_t *ipc, ngx_cycle_t *cycle, ipc_channel_t *channel, ipc_socket_type_t socktype) {
  int                             rc = NGX_OK;
  ngx_socket_t                   *socks = channel->pipe;
  if(channel->active) {
    // reinitialize already active pipes. This is done to prevent IPC alerts
    // from a previous restart that were never read from being received by
    // a newly restarted worker
    ipc_try_close_fd(&socks[0]);
    ipc_try_close_fd(&socks[1]);
    channel->active = 0;
  }
  
  assert(socks[0] == NGX_INVALID_FILE && socks[1] == NGX_INVALID_FILE);
  
  channel->socket_type = socktype;
  if(socktype == IPC_PIPE) {
    //make-a-pipe
    rc = pipe(socks);
  }
  else if(socktype == IPC_SOCKETPAIR) {
    rc = socketpair(AF_LOCAL, SOCK_STREAM, 0, socks);
  }
  
  if(rc == -1) {
    ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, "pipe() failed while initializing IPC %s", ipc->name);
    return NGX_ERROR;
  }
  //make both ends nonblocking
  if (ngx_nonblocking(socks[0]) == -1 || ngx_nonblocking(socks[1]) == -1) {
    ngx_log_error(NGX_LOG_ALERT, cycle->log, ngx_errno, ngx_nonblocking_n " failed on pipe socket %i while initializing IPC %s", ipc->name);
    ipc_try_close_fd(&socks[0]);
    ipc_try_close_fd(&socks[1]);
    return NGX_ERROR;
  }
  //It's ALIIIIIVE! ... erm.. active...
  channel->active = 1;
  
  return NGX_OK;
}

static ngx_int_t ipc_open(ipc_t *ipc, ngx_cycle_t *cycle, ngx_int_t workers, void (*slot_callback)(int slot, int worker)) {
//initialize pipes for workers in advance.
  int                             i, s = 0;
  ngx_int_t                       last_expected_process = ngx_last_process;
  ipc_channel_t                  *worker_channel;
  
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
    
    worker_channel = &ipc->worker_channel[s];

    if(ipc_activate_channel(ipc, cycle, worker_channel, IPC_PIPE) != NGX_OK) {
      return NGX_ERROR;
    }
    
    s++; //NEXT!!
  }
  
  return NGX_OK;
}

static ngx_int_t ipc_close_channel(ipc_channel_t *chan) {
  ipc_alert_link_t         *cur, *cur_next;
  ipc_readbuf_t            *rcur;
  
  if(!chan->active) {
    return NGX_OK;
  }
    
  if(chan->read_conn) {
    ngx_close_connection(chan->read_conn);
    chan->read_conn = NULL;
  }
  if(chan->write_conn) {
    ngx_close_connection(chan->write_conn);
    chan->write_conn = NULL;
  }
  
  for(cur = chan->wbuf.head; cur != NULL; cur = cur_next) {
    cur_next = cur->next;
    free(cur);
  }
  
  while((rcur = chan->rbuf_head) != NULL) {
    ipc_free_readbuf(chan, rcur);
  }
  
  ipc_try_close_fd(&chan->pipe[0]);
  ipc_try_close_fd(&chan->pipe[1]);
  
  return NGX_OK;
}

static inline int ipc_iovec_sz(struct iovec *iov, int n) {
  int sz = 0;
  while(n>0) {
    sz += iov[--n].iov_len;
  }
  return sz;
}

static ngx_int_t ipc_write_iovec(ipc_t *ipc, ngx_socket_t fd, ipc_iovec_t *vec) {
  int          n;
  int expected_iovec_sz = ipc_iovec_sz(vec->iov, vec->n);
  int expected_len = sizeof(ipc_packet_header_t) + ((ipc_packet_header_t *)vec->iov[0].iov_base)->pkt_len;
  n = writev(fd, vec->iov, vec->n);
  if (n == -1 && (ngx_errno) == NGX_EAGAIN) {
    //ngx_log_error(NGX_LOG_ALERT, ngx_cycle->log, err, "write() EAGAINED...");
    return NGX_AGAIN;
  }
  else if(n != expected_iovec_sz) {
    ERR(ipc, "writev() failed with n=%i, expected %i", n, expected_iovec_sz);
    return NGX_ERROR;
  }
  else if(n != expected_len) {
    ERR(ipc, "writev() inconsistent, expected %i, got %i", expected_len, n);
    return NGX_ERROR;
  }
  //DBG("wrote %i byte pkt", n);
  return NGX_OK;
}

static ngx_int_t ipc_enqueue_tmp_iovec(ipc_t *ipc, ipc_writebuf_t *wb) {
  size_t             sz=0, len;
  ipc_alert_link_t  *link;
  
  ipc_iovec_t       *vec = &wb->last_iovec;
  u_char            *cur;
  
  int                i;
  
  if(vec->n == 0) {
    return NGX_OK;
  }
  
  for(i=0; i < vec->n; i++) {
    sz += vec->iov[i].iov_len;
  }
  
  link = malloc(sizeof(*link) + sz);
  
  if(!link) {
    ERR(ipc, "out of memory while allocating buffered iovec for writing");
    return NGX_ERROR;
  }
  
  link->iovec.iov[0].iov_len = sz;
  cur = (u_char *)&link[1];
  link->iovec.iov[0].iov_base = cur;
  link->iovec.n = 1;
  link->next = NULL;
  
  for(i=0; i < vec->n; i++) {
    len = vec->iov[i].iov_len;
    if(len > 0) {
      ngx_memcpy(cur, vec->iov[i].iov_base, len);
      cur += len;
    }
  }
  
  if(!wb->head) {
    wb->head = link;
  }
  if(wb->tail) {
    wb->tail->next = link;
  }
  wb->tail = link;
  
  vec->n = 0;
  return NGX_OK;
}

static ngx_int_t ipc_enqueue_write_iovec(ipc_t *ipc, ipc_writebuf_t *wb, ipc_iovec_t *v) {
  ipc_enqueue_tmp_iovec(ipc, wb);
  wb->last_iovec = *v;
  return NGX_OK;
}

static void ipc_write_handler(ngx_event_t *ev) {
  ngx_connection_t        *c = ev->data;
  ngx_socket_t             fd = c->fd;
  
  ipc_channel_t           *chan = c->data;
  ipc_alert_link_t        *cur;
  
  ngx_int_t                rc = NGX_OK;
  
  while((cur = chan->wbuf.head) != NULL) {
    rc = ipc_write_iovec(chan->ipc, fd, &cur->iovec);
    
    if(rc == NGX_OK) {
      chan->wbuf.head = cur->next;
      if(chan->wbuf.tail == cur) {
        chan->wbuf.tail = NULL;
      }
      free(cur);
    }
    else {
      break;
    }
  }
  
  if(rc == NGX_OK && chan->wbuf.last_iovec.n > 0) {
    rc = ipc_write_iovec(chan->ipc, fd, &chan->wbuf.last_iovec);
  }
  
  if(rc == NGX_OK) {
    assert(chan->wbuf.head == NULL);
    assert(chan->wbuf.tail == NULL);
  }
  else {
    //re-add event because the write failed
    if(chan->wbuf.last_iovec.n > 0) {
      ipc_enqueue_tmp_iovec(chan->ipc, &chan->wbuf);
    }
    ngx_handle_write_event(c->write, 0);
  }
  chan->wbuf.last_iovec.n = 0;
}


typedef enum {IPC_CONN_READ, IPC_CONN_WRITE} ipc_conn_type_t;

static ngx_int_t ipc_channel_setup_conn(ipc_channel_t *chan, ngx_cycle_t *cycle, ipc_conn_type_t conn_type, void (*event_handler)(ngx_event_t *), void *data) {
  ngx_connection_t      *c; 
  //set up read connection
  c = ngx_get_connection(chan->pipe[conn_type == IPC_CONN_READ ? 0 : 1], cycle->log);
  c->data = data;
  
  if(conn_type == IPC_CONN_READ) {
    c->read->handler = event_handler;
    c->read->log = cycle->log;
    c->write->handler = NULL;
    ngx_add_event(c->read, NGX_READ_EVENT, 0);
    chan->read_conn=c;
  }
  else if(conn_type == IPC_CONN_WRITE) {
    c->read->handler = NULL;
    c->write->log = cycle->log;
    c->write->handler = ipc_write_handler;
    chan->write_conn=c;
  }
  else {
    return NGX_ERROR;
  }
  return NGX_OK;
}

static ngx_int_t ipc_register_worker(ipc_t *ipc, ngx_cycle_t *cycle) {
  int                    i;    
  ipc_channel_t         *chan;
  
  for(i=0; i< NGX_MAX_PROCESSES; i++) {
    
    chan = &ipc->worker_channel[i];
    
    if(!chan->active) continue;
    
    assert(chan->pipe[0] != NGX_INVALID_FILE);
    assert(chan->pipe[1] != NGX_INVALID_FILE);
    
    if(i==ngx_process_slot) {
      //set up read connection
      ipc_channel_setup_conn(chan, cycle, IPC_CONN_READ, ipc_worker_read_handler, ipc);
    }
    else {
      //set up write connection
      ipc_channel_setup_conn(chan, cycle, IPC_CONN_WRITE, ipc_write_handler, chan);
    }
  }
  
  return NGX_OK;
}

static void ipc_free_readbuf(ipc_channel_t *chan, ipc_readbuf_t *rbuf) {
  if(rbuf->next) {
    rbuf->next->prev = rbuf->prev;
  }
  if(rbuf->prev) {
    rbuf->prev->next = rbuf->next;
  }
  if(chan->rbuf_head == rbuf) {
    chan->rbuf_head = rbuf->next;
  }
  free(rbuf);
}

static ipc_readbuf_t *channel_get_readbuf(ipc_channel_t *chan, ipc_packet_header_t *header, char **err) {
  ipc_readbuf_t  *cur;
  for(cur = chan->rbuf_head; cur!= NULL; cur = cur->next) {
    if(header->src_slot == cur->pkt.header.src_slot) {
      if(header->src_pid != cur->pkt.header.src_pid) {
        ERR(chan->ipc, "got packets from different processes for the same slot: old %i, new %i. Clearing out old packet buffer.", cur->pkt.header.src_pid, header->src_pid);
        ipc_free_readbuf(chan, cur);
        break;
      }
      else if(header->ctrl != '+') {
        ipc_free_readbuf(chan, cur);
        *err = "unexpected packet ctrl (wanted '+')";
        return NULL;
      }
      else if(header->tot_len != cur->pkt.header.tot_len) {
        ipc_free_readbuf(chan, cur);
        *err = "wrong packet length";
        return NULL;
      }
      else {
        return cur;
      }
    }
  }
  
  if(header->ctrl != '>') {
    *err = "unexpected packet ctrl (wanted '>')";
    return NULL;
  }
  
  cur = malloc(sizeof(*cur) + header->tot_len);
  if(!cur) {
    *err = "out of memory";
    return NULL;
  }
  
  cur->pkt.header = *header;
  cur->body_cur = cur->pkt.body;
  cur->prev = NULL;
  cur->next = chan->rbuf_head;
  
  if(chan->rbuf_head) {
    chan->rbuf_head->prev = cur;
    cur->next = chan->rbuf_head;
  }
  chan->rbuf_head = cur;
  
  return cur;
}

static int ipc_clear_socket_readbuf(ngx_socket_t s) {
  char  buf[PIPE_BUF];
  int   total = 0;
  int   n = sizeof(buf);
  
  do {
    total += n;
    n = read(s, buf, sizeof(buf));
  } while(n > 0);
  
  return total;
}


static ngx_int_t ipc_read(ipc_t *ipc, ipc_channel_t *ipc_channel, ipc_alert_handler_pt handler, ngx_log_t *log) {
  ssize_t             n;
  ngx_socket_t        s = ipc_channel->read_conn->fd;
  ngx_str_t           name, data;
  ipc_packet_buf_t    pkt;
  int                 discarded;
  char               *err;
  ipc_readbuf_t      *rbuf;
  
  while(1) {
    n = read(s, &pkt.header, IPC_PKT_HEADER_SIZE);
    
    if (n == -1 && ngx_errno == NGX_EAGAIN) {
      return NGX_AGAIN;
    }
    else if(n == -1) {
      ERR_CODE(ipc, ngx_errno, "read() failed");
      return NGX_ERROR;
    }
    else if(n != IPC_PKT_HEADER_SIZE) {
      discarded = ipc_clear_socket_readbuf(s);
      ERR(ipc, "unexpected non-atomic read of packet header size %i, expected %i bytes. Discarded %i bytes of data.", n, IPC_PKT_HEADER_SIZE, pkt.header.pkt_len, discarded);
      return NGX_AGAIN;
    }
    else if(pkt.header.pkt_len > IPC_PKT_MAX_BODY_SIZE) {
      discarded = ipc_clear_socket_readbuf(s);
      ERR(ipc, "got corrupt packet size %i. Discarded %i bytes of data.", pkt.header.pkt_len, discarded);
      return NGX_AGAIN;
    }
    else if(pkt.header.name_len > pkt.header.tot_len) {
      discarded = ipc_clear_socket_readbuf(s);
      ERR(ipc, "got corrupt packet alert-name size %i. Discarded %i bytes of data.", pkt.header.name_len, discarded);
      return NGX_AGAIN;
    }
    
    switch (pkt.header.ctrl) {
      case '$':
        if(pkt.header.tot_len != pkt.header.pkt_len) {
          discarded = ipc_clear_socket_readbuf(s);
          ERR(ipc, "got inconsistent whole-packet size %i. Discarded %i bytes of data.", pkt.header.pkt_len, discarded);
          return NGX_AGAIN;
        }
        //assert(n == pkt.pkt_len);
        n = read(s, pkt.body, pkt.header.pkt_len);
        if(n != pkt.header.pkt_len) {
          discarded = ipc_clear_socket_readbuf(s);
          ERR(ipc, "unexpected non-atomic read of size %i, expected %i. Discarded %i bytes of data.", n, pkt.header.pkt_len, discarded);
          return NGX_AGAIN;
        }
        name.len = pkt.header.name_len;
        name.data = pkt.body;
        data.len = pkt.header.tot_len - name.len;
        data.data = name.data + name.len;
        //DBG("read %i byte pkt", n + IPC_PKT_HEADER_SIZE);
        handler(pkt.header.src_pid, pkt.header.src_slot, &name, &data);
        break;
        
      case '>':
      case '+':
        if(pkt.header.tot_len <= pkt.header.pkt_len) {
          discarded = ipc_clear_socket_readbuf(s);
          ERR(ipc, "got unexpectedly small part-packet total size %i. Discarded %i bytes of data.", pkt.header.tot_len, discarded);
          return NGX_AGAIN;
        }
        
        rbuf = channel_get_readbuf(ipc_channel, &pkt.header, &err);
        if(!rbuf) {
          n = read(s, pkt.body, pkt.header.pkt_len);
          ERR(ipc, "dropped weird packet: %s", err);
          return NGX_AGAIN;
        }
        
        n = read(s, rbuf->body_cur, pkt.header.pkt_len);
        if(n != pkt.header.pkt_len) {
          discarded = ipc_clear_socket_readbuf(s);
          ERR(ipc, "unexpected non-atomic read of size %i, expected %i. Discarded %i bytes of data.", n, pkt.header.pkt_len, discarded);
          return NGX_AGAIN;
        }
        //DBG("read %i byte pkt", n + IPC_PKT_HEADER_SIZE);
        rbuf->body_cur += n;
        if((size_t )(rbuf->body_cur - rbuf->pkt.body) == rbuf->pkt.header.tot_len) { //alert finished
          name.len = rbuf->pkt.header.name_len;
          name.data = rbuf->pkt.body;
          data.len = rbuf->pkt.header.tot_len - name.len;
          data.data = name.data + name.len;
          handler(pkt.header.src_pid, pkt.header.src_slot, &name, &data);
          ipc_free_readbuf(ipc_channel, rbuf);
        }
        break;
        
      default:
        discarded = ipc_clear_socket_readbuf(s);
        ERR(ipc, "got unexpected packet ctrl code '%c'. Discarded %i bytes of data.", pkt.header.ctrl, discarded);
        return NGX_AGAIN;
    }
  }
  
  return NGX_OK;
}

static void ipc_worker_read_handler(ngx_event_t *ev) {
  ngx_int_t          rc;
  ngx_connection_t  *c;
  ipc_channel_t     *ipc_channel;
  ipc_t             *ipc;
  
  if (ev->timedout) {
    ev->timedout = 0;
    return;
  }
  c = ev->data;
  ipc = c->data;
  ipc_channel = &ipc->worker_channel[ngx_process_slot];
  
  rc = ipc_read(ipc, ipc_channel, ipc->worker_alert_handler, ev->log);
  if (rc == NGX_ERROR) {
    ERR(ipc, "IPC_READ_SOCKET failed: bad connection. This should never have happened, yet here we are...");
    assert(0);
    return;
  }
  else if (rc == NGX_AGAIN) {
    return;
  }
}

static ngx_int_t ipc_alert_channel(ipc_channel_t *chan, ngx_str_t *name, ngx_str_t *data) {
  ipc_packet_header_t          header;
  ipc_writebuf_t              *wb = &chan->wbuf;
  ipc_iovec_t                  vec;
  
  int                          pad;
  
  if(!chan->active) {
    return NGX_ERROR;
  }
  
  
  if(name->len > IPC_ALERT_NAME_MAX_LEN) {
    ERR(chan->ipc, "alert name length cannot exceed %i, was %i", IPC_ALERT_NAME_MAX_LEN, name->len);
    return NGX_ERROR;
  }
  if(data->len > IPC_ALERT_DATA_MAX_LEN) {
    ERR(chan->ipc, "alert data length cannot exceed %i, was %i", IPC_ALERT_DATA_MAX_LEN, data->len);
    return NGX_ERROR;
  }
  
  header.tot_len = data->len + name->len;
  header.name_len = name->len;
  header.src_slot = ngx_process_slot;
  header.src_pid = ngx_pid;
  
  vec.n = 3;
  
  //zero the struct padding
  pad = (u_char *)(&header + 1) - (&header.ctrl + 1);
  if(pad > 0) {
    ngx_memzero(&header.ctrl + 1, pad);
  }
  
  vec.iov[0].iov_base = &header;
  vec.iov[0].iov_len  = IPC_PKT_HEADER_SIZE;
  
  if(header.tot_len <= IPC_PKT_MAX_BODY_SIZE) {
    header.pkt_len = header.tot_len;
    header.ctrl = '$';
    
    vec.iov[1].iov_base = name->data;
    vec.iov[1].iov_len  = name->len;
    
    vec.iov[2].iov_base = data->data;
    vec.iov[2].iov_len  = data->len;
    
    ipc_enqueue_write_iovec(chan->ipc, wb, &vec);
    ipc_write_handler(chan->write_conn->write);
  }
  else {
    size_t    name_left = name->len;
    size_t    data_left = data->len;
    u_char   *name_cur = name->data;
    u_char   *data_cur = data->data;
    size_t    name_len = 0;
    size_t    data_len = 0;
    
    int pktnum;
    
    for(pktnum = 0; name_left + data_left > 0; pktnum++) {
      
      header.ctrl = pktnum == 0 ? '>' : '+';
      
      if(name_left == 0) {
        name_len = 0;
      }
      else {
        name_cur += name_len;
        name_len = name_left > IPC_PKT_MAX_BODY_SIZE ? IPC_PKT_MAX_BODY_SIZE : name_left;
        name_left -= name_len;
      }
      
      data_cur += data_len;
      data_len = data_left > (IPC_PKT_MAX_BODY_SIZE - name_len) ? (IPC_PKT_MAX_BODY_SIZE - name_len) : data_left;
      data_left -= data_len;
      
      header.pkt_len = name_len + data_len;
      
      vec.iov[1].iov_base = name_cur;
      vec.iov[1].iov_len  = name_len;
      vec.iov[2].iov_base = data_cur;
      vec.iov[2].iov_len  = data_len;
      
      ipc_enqueue_write_iovec(chan->ipc, wb, &vec);
      ipc_write_handler(chan->write_conn->write);
    }
    
    //assert(name_left + data_left == 0);
  }

  return NGX_OK;
}

ngx_int_t ipc_alert_slot(ipc_t *ipc, ngx_int_t slot, ngx_str_t *name, ngx_str_t *data) {
  DBG("send alert '%V' to slot %i", name, slot);
  
  ngx_str_t           empty = {0, NULL};
  if(!name) name = &empty;
  if(!data) data = &empty;
  
  if(slot == ngx_process_slot) {
    ipc->worker_alert_handler(ngx_pid, slot, name, data);
    return NGX_OK;
  }
  return ipc_alert_channel(&ipc->worker_channel[slot], name, data);
}


ngx_int_t ipc_alert_pid(ipc_t *ipc, ngx_pid_t worker_pid, ngx_str_t *name, ngx_str_t *data) {
  ngx_int_t slot = ipc_get_slot(ipc, worker_pid);
  if(slot == NGX_ERROR) {
    ngx_snprintf((ipc)->last_error, IPC_MAX_ERROR_LEN, "No worker process with PID %i%Z", worker_pid);
    return NGX_ERROR;
  }
  return ipc_alert_slot(ipc, slot, name, data);
}

ngx_int_t ipc_alert_all_workers(ipc_t *ipc, ngx_str_t *name, ngx_str_t *data) {
  ipc_shm_data_t         *shdata = ipc->shm;
  int                     max_workers = ipc->worker_process_count;
  int                     i;
  int                     rc = NGX_OK, trc;
  process_slot_tracking_t *process_slots = shdata->process_slots;
  
  for(i=0; i<max_workers; i++) {
    trc = ipc_alert_slot(ipc, process_slots[i].slot, name, data);
    if(trc != NGX_OK) rc = trc;
  }
  return rc;
}

ngx_pid_t *ipc_get_worker_pids(ipc_t *ipc, int *pid_count) {
  static ngx_pid_t pid_array[NGX_MAX_PROCESSES + NGX_MAX_HELPER_PROCESSES];
  ipc_shm_data_t         *shdata = ipc->shm;
  int i;
  for(i=0; i<ipc->worker_process_count; i++) {
    pid_array[i] = shdata->process_slots[i].pid;
  }
  if(pid_count) {
    *pid_count = ipc->worker_process_count;
  }
  
  return pid_array;
}

char *ipc_get_last_error(ipc_t *ipc) {
  return (char *)ipc->last_error;
}
