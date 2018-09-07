#ifndef NCHAN_BENCHMARK_H
#define NCHAN_BENCHMARK_H

#include <nchan_module.h>
#include <util/hdr_histogram.h>

typedef struct {
  struct hdr_histogram* msg_latency;
  uint64_t              msg_sent;
  uint64_t              msg_received;
  uint64_t              subscribers_ready;
} nchan_benchmark_data_t;

typedef struct {
  uint64_t              n;
  uint64_t              msg_count;
} nchan_benchmark_channel_t;

typedef struct {
  ngx_atomic_t              *subscribers_enqueued;
  ngx_atomic_t              *subscribers_dequeued;
  nchan_benchmark_channel_t *channels;
} nchan_benchmark_shared_t;

typedef struct nchan_benchmark_s nchan_benchmark_t;
struct nchan_benchmark_s {
  nchan_loc_conf_t   *cf;
  time_t              time_start;
  time_t              time_end;
  nchan_benchmark_shared_t shared_data;
  nchan_benchmark_data_t data;
  
}; //nchan_benchmark_t

ngx_int_t nchan_benchmark_initialize(ngx_http_request_t *r);
ngx_int_t nchan_benchmark_initialize_from_ipc(ngx_int_t initiating_worker_slot, nchan_loc_conf_t *cf, time_t time_start, nchan_benchmark_shared_t *shared_data);
ngx_int_t nchan_benchmark_start(ngx_int_t initiator_slot);
ngx_int_t nchan_benchmark_finish(void);

ngx_int_t nchan_benchmark_channel_id(int n, ngx_str_t *chid);
#endif //NCHAN_BENCHMARK_H
