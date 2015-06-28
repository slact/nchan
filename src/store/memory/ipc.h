#define IPC_DATA_SIZE 3

typedef struct {
  const char      *name;
  ngx_socket_t     socketpairs[NGX_MAX_PROCESSES][2];
  void             (*handler)(ngx_uint_t, void*[]);
} ipc_t;

ipc_t    *ipc_create(ngx_cycle_t *cycle);
ngx_int_t ipc_open(ipc_t *ipc, ngx_cycle_t *cycle,ngx_int_t workers);
ngx_int_t ipc_set_handler(ipc_t *ipc, void (*alert_handler)(ngx_uint_t , void *data[]));
ngx_int_t ipc_start(ipc_t *ipc, ngx_cycle_t *cycle);
ngx_int_t ipc_close(ipc_t *ipc, ngx_cycle_t *cycle);
ngx_int_t ipc_destroy(ipc_t *ipc, ngx_cycle_t *cycle);

ngx_int_t ipc_alert(ipc_t *ipc, ngx_pid_t pid, ngx_int_t slot, ngx_uint_t code,  void *data[]);