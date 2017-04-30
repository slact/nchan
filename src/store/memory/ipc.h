#ifndef NGX_IPC_H
#define NGX_IPC_H
typedef struct ipc_alert_link_s ipc_alert_link_t;

typedef struct {
  struct iovec      iov[3];
  int               n;
} ipc_iovec_t;

struct ipc_alert_link_s {
  ipc_alert_link_t *next;
  ipc_iovec_t       iovec;
};

typedef struct ipc_writebuf_s ipc_writebuf_t;
struct ipc_writebuf_s {
  ipc_alert_link_t         *head;
  ipc_alert_link_t         *tail;
  ipc_iovec_t               last_iovec;
  uint32_t                  n;
}; //ipc_writebuf_t


typedef struct {
  ngx_pid_t  src_pid;
  uint32_t   tot_len;
  uint16_t   src_slot;
  uint16_t   pkt_len;  //<=4K (or PIPE_BUF)
  uint8_t    name_len;
  u_char     ctrl; // '$': whole, '>': part start, '+': part piece
} ipc_packet_header_t;
#define IPC_ALERT_NAME_MAX_LEN (UINT8_MAX - 1)
#define IPC_ALERT_DATA_MAX_LEN (UINT32_MAX - IPC_ALERT_NAME_MAX_LEN - 1)

#define IPC_PKT_HEADER_SIZE    (sizeof(ipc_packet_header_t))
#define IPC_PKT_MAX_BODY_SIZE  (PIPE_BUF - IPC_PKT_HEADER_SIZE)

typedef struct {
  ipc_packet_header_t  header;
  u_char               body[IPC_PKT_MAX_BODY_SIZE];
} ipc_packet_buf_t;

typedef struct ipc_readbuf_s ipc_readbuf_t;
struct ipc_readbuf_s {
  ipc_readbuf_t      *prev;
  ipc_readbuf_t      *next;
  u_char             *body_cur;
  ipc_packet_buf_t    pkt;
}; //ipc_readbuf_t

typedef struct ipc_s ipc_t;

typedef enum {IPC_NGX_PROCESS_WORKER, IPC_NGX_PROCESS_CACHE_MANAGER, IPC_NGX_PROCESS_CACHE_LOADER, IPC_NGX_PROCESS_UNKNOWN,  IPC_NGX_PROCESS_ANY} ipc_ngx_process_type_t;

typedef enum {IPC_PIPE, IPC_SOCKETPAIR} ipc_socket_type_t;
typedef struct {
  ipc_t                 *ipc; //need this backrerefence for write events
  ipc_socket_type_t      socket_type;
  ngx_socket_t           pipe[2];
  ngx_connection_t      *read_conn;
  ngx_connection_t      *write_conn;
  ipc_writebuf_t         wbuf;
  ipc_readbuf_t         *rbuf_head;
  unsigned               active:1;
} ipc_channel_t;

typedef void (*ipc_alert_handler_pt)(ngx_pid_t alert_sender_pid, ngx_int_t alert_sender_slot, ngx_str_t *alert_name, ngx_str_t *alert_data);


#define IPC_MAX_ERROR_LEN 512
struct ipc_s {
  const char            *name;
  void                  *shm;
  size_t                 shm_sz;
  ipc_channel_t          worker_channel[NGX_MAX_PROCESSES];
  ngx_int_t              worker_process_count;
  ipc_alert_handler_pt   worker_alert_handler;
  u_char                 last_error[IPC_MAX_ERROR_LEN];
}; //ipc_t

//IPC needs to be initialized in two steps init_module (prefork), and init_worker (post-fork)
ipc_t *ipc_init_module(const char *ipc_name, ngx_cycle_t *cycle);
ngx_int_t ipc_init_worker(ipc_t *ipc, ngx_cycle_t *cycle);

ngx_int_t ipc_set_worker_alert_handler(ipc_t *ipc, ipc_alert_handler_pt handler);

ngx_int_t ipc_destroy(ipc_t *ipc); // for exit_worker, exit_master

ngx_pid_t ipc_get_pid(ipc_t *ipc, int process_slot);
ngx_int_t ipc_get_slot(ipc_t *ipc, ngx_pid_t pid);

ngx_int_t ipc_alert_slot(ipc_t *ipc, ngx_int_t slot, ngx_str_t *name, ngx_str_t *data);
ngx_int_t ipc_alert_pid(ipc_t *ipc, ngx_pid_t pid, ngx_str_t *name, ngx_str_t *data);
ngx_pid_t *ipc_get_worker_pids(ipc_t *ipc, int *pid_count); //useful for debugging

ngx_pid_t *ipc_get_process_pids(ipc_t *ipc, int *pid_count, ipc_ngx_process_type_t type);
ngx_int_t ipc_alert_all_processes(ipc_t *ipc, ipc_ngx_process_type_t type, ngx_str_t *name, ngx_str_t *data);

char *ipc_get_last_error(ipc_t *ipc);

ngx_int_t ipc_alert_all_workers(ipc_t *ipc, ngx_str_t *name, ngx_str_t *data); //poor man's broadcast

#endif //NGX_IPC_H
