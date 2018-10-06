#ifndef NCHAN_BENCHMARK_H
#define NCHAN_BENCHMARK_H

#include <nchan_module.h>
#include <util/hdr_histogram.h>

typedef struct {
  struct hdr_histogram* msg_publishing_latency;
  struct hdr_histogram* msg_delivery_latency;
  struct hdr_histogram* subscriber_readiness_latency;
  uint64_t              msg_sent;
  uint64_t              msg_send_confirmed;
  uint64_t              msg_send_failed;
  uint64_t              msg_received;
} nchan_benchmark_data_t;

typedef struct {
  uint64_t              n;
  ngx_atomic_t          msg_count;
  u_char               *msgbuf;
} nchan_benchmark_channel_t;

typedef struct {
  ngx_atomic_t              *subscribers_enqueued;
  ngx_atomic_t              *subscribers_dequeued;
  nchan_benchmark_channel_t *channels;
} nchan_benchmark_shared_t;


#define NCHAN_BENCHMARK_INACTIVE      0
#define NCHAN_BENCHMARK_INITIALIZING  1
#define NCHAN_BENCHMARK_READY         2
#define NCHAN_BENCHMARK_RUNNING       3
#define NCHAN_BENCHMARK_FINISHED      4

typedef struct nchan_benchmark_s nchan_benchmark_t;
struct nchan_benchmark_s {
  subscriber_t       *client;
  nchan_benchmark_conf_t   *config;
  nchan_loc_conf_t   *loc_conf;
  uint32_t            id;
  struct {
    time_t              init;
    time_t              start;
    time_t              end;
  }                   time;
  struct {
    void             *ready;
    void             *running;
    void             *finishing;
    void            **publishers;
  }                   timer;
  u_char             *msgbuf;
  ngx_atomic_int_t   *state;
  struct {
    size_t              n;
    subscriber_t      **array;
  }                   subs;
  unsigned            base_msg_period;
  int                 waiting_for_results;
  nchan_benchmark_shared_t shared;
  nchan_benchmark_data_t data;
  
}; //nchan_benchmark_t

int nchan_benchmark_active(void);
  
ngx_int_t nchan_benchmark_init_module(ngx_cycle_t *cycle);
ngx_int_t nchan_benchmark_init_worker(ngx_cycle_t *cycle);
ngx_int_t nchan_benchmark_exit_master(ngx_cycle_t *cycle);

ngx_int_t nchan_benchmark_initialize(void);
ngx_int_t nchan_benchmark_initialize_from_ipc(ngx_int_t initiating_worker_slot, nchan_loc_conf_t *cf, time_t time_start, uint32_t id, nchan_benchmark_shared_t *shared_data);
ngx_int_t nchan_benchmark_run(void);

ngx_int_t nchan_benchmark_stop(void);
ngx_int_t nchan_benchmark_dequeue_subscribers(void);
ngx_int_t nchan_benchmark_finish_response(void);
ngx_str_t *nchan_hdrhistogram_serialize(const struct hdr_histogram* hdr, ngx_pool_t *pool);
ngx_int_t nchan_benchmark_receive_finished_data(nchan_benchmark_data_t *data);
ngx_int_t nchan_benchmark_finish(void);
ngx_int_t nchan_benchmark_abort(void);
ngx_int_t nchan_benchmark_cleanup(void);

ngx_int_t nchan_benchmark_channel_id(int n, ngx_str_t *chid);
uint64_t nchan_benchmark_message_delivery_msec(nchan_msg_t *msg);
nchan_benchmark_t *nchan_benchmark_get_active(void);


ngx_int_t nchan_benchmark_ws_initialize(ngx_http_request_t *r);
#endif //NCHAN_BENCHMARK_H
