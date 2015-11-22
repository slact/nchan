#define IPC_DATA_SIZE 10

typedef struct {
  int16_t         src_slot;
  int16_t         dst_slot;
  uint8_t         code;
  void           *data[IPC_DATA_SIZE];
} ipc_alert_t;

typedef struct ipc_writev_data_s ipc_writev_data_t;
struct ipc_writev_data_s {
  ipc_alert_t          alert;
  ipc_writev_data_t   *next;
}; //ipc_writev_data_t

#define IPC_WRITEBUF_SIZE 256

typedef struct ipc_writebuf_s ipc_writebuf_t;
struct ipc_writebuf_s {
  //a ring buffer for writing alerts
  uint16_t          first;
  uint16_t          last;
  uint16_t          n;
  ipc_alert_t       alerts[IPC_WRITEBUF_SIZE];
  ipc_writebuf_t   *next;
}; //ipc_writebuf_t

typedef struct ipc_s ipc_t;

typedef struct {
  ipc_t                 *ipc; //useful for write events
  ngx_socket_t           pipe[2];
  ngx_connection_t      *c;
  ipc_writev_data_t     *wevd_first;
  ipc_writev_data_t     *wevd_last;
  unsigned               active:1;
} ipc_process_t;

struct ipc_s {
  const char            *name;
  
  ipc_process_t         process[NGX_MAX_PROCESSES];
  
  void                  (*handler)(ngx_int_t, ngx_uint_t, void*);
}; //ipc_t

ipc_t    *ipc_create(ngx_cycle_t *cycle);
ngx_int_t ipc_open(ipc_t *ipc, ngx_cycle_t *cycle,ngx_int_t workers);
ngx_int_t ipc_set_handler(ipc_t *ipc, void (*alert_handler)(ngx_int_t, ngx_uint_t , void *data));
ngx_int_t ipc_start(ipc_t *ipc, ngx_cycle_t *cycle);
ngx_int_t ipc_close(ipc_t *ipc, ngx_cycle_t *cycle);
ngx_int_t ipc_destroy(ipc_t *ipc, ngx_cycle_t *cycle);

ngx_int_t ipc_alert(ipc_t *ipc, ngx_int_t slot, ngx_uint_t code,  void *data, size_t data_size);