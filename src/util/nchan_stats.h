#ifndef NCHAN_STATS_H
#define NCHAN_STATS_H
#include <nchan_module.h>

#define nchan_stats_global_incr(counter_name, count) \
  __nchan_stats_global_incr(offsetof(nchan_stats_global_t, counter_name), count)
  
#define nchan_stats_worker_incr(counter_name, count) \
  __nchan_stats_worker_incr(offsetof(nchan_stats_worker_t, counter_name), count)

void __nchan_stats_global_incr(off_t offset, int count);
void __nchan_stats_worker_incr(off_t offset, int count);
  
typedef struct {
  //These must all be ngx_atomic_uint_t
  ngx_atomic_uint_t       channels;
  ngx_atomic_uint_t       subscribers;
  ngx_atomic_uint_t       messages;
  ngx_atomic_uint_t       redis_pending_commands;
  ngx_atomic_uint_t       redis_connected_servers;
  ngx_atomic_uint_t       redis_unhealthy_upstreams;
  ngx_atomic_uint_t       ipc_queue_size;
} nchan_stats_worker_t;

typedef struct {
  ngx_atomic_uint_t      total_published_messages;
  ngx_atomic_uint_t      total_ipc_alerts_sent;
  ngx_atomic_uint_t      total_ipc_alerts_received;
  ngx_atomic_uint_t      total_ipc_send_delay;
  ngx_atomic_uint_t      total_ipc_receive_delay;
  ngx_atomic_uint_t      total_redis_commands_sent;
} nchan_stats_global_t;

typedef struct {
  nchan_stats_worker_t worker[NGX_MAX_PROCESSES];
  nchan_stats_global_t global;
} nchan_stats_t;

ngx_int_t nchan_stats_init_postconfig(ngx_conf_t *cf, int enable);
ngx_int_t nchan_stats_init_worker(ngx_cycle_t *cycle);
void nchan_stats_exit_worker(ngx_cycle_t *cycle);
void nchan_stats_exit_master(ngx_cycle_t *cycle);

ngx_int_t nchan_stats_get_all(nchan_stats_worker_t *worker, nchan_stats_global_t *global);


#endif //NCHAN_STATS_H

