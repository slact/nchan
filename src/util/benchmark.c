#include "benchmark.h"
#include <subscribers/benchmark.h>
#include <util/shmem.h>
#include <store/memory/store.h>
#include <store/memory/ipc-handlers.h>
#include <assert.h>

nchan_benchmark_t    bench;
ngx_atomic_int_t     bench_active;
ngx_http_request_t  *bench_initiating_request;

ngx_int_t nchan_benchmark_initialize(ngx_http_request_t *r) {
  nchan_loc_conf_t               *cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  int                             i;
  
  if(!ngx_atomic_cmp_set(&bench_active, 0, 1)) {
    //benchmark already running
  }
  
  bench_initiating_request = r;
  
  bench.cf = cf;
  
  bench.time_start = ngx_time();
  bench.time_end = ngx_time() + cf->benchmark.time;
  
  bench.shared_data.subscribers_enqueued = shm_calloc(nchan_store_memory_shmem, sizeof(*(bench.shared_data.subscribers_enqueued)), "hdrhistogram subscribers_enqueued count");
  bench.shared_data.subscribers_dequeued = shm_calloc(nchan_store_memory_shmem, sizeof(*(bench.shared_data.subscribers_dequeued)), "hdrhistogram subscribers_dequeued count");
  bench.shared_data.channels = shm_calloc(nchan_store_memory_shmem, sizeof(nchan_benchmark_channel_t) * cf->benchmark.channels, "benchmark channel states");
  
  assert(bench.data.msg_latency == NULL);
  hdr_init_nchan_shm(1, 60000, 3, &bench.data.msg_latency);
  
  for(i=0; i<cf->benchmark.channels; i++) {
    bench.shared_data.channels[i].n=i;
    bench.shared_data.channels[i].msg_count=0;
  }
  
  //broadcast workload to other workers
  memstore_ipc_broadcast_benchmark_start(&bench);
  
  nchan_benchmark_start(ngx_process_slot);
  
  return NGX_OK;
}

ngx_int_t nchan_benchmark_initialize_from_ipc(ngx_int_t initiating_worker_slot, nchan_loc_conf_t *cf, time_t start, nchan_benchmark_shared_t *shared_data) {
  bench_initiating_request = NULL;
  bench.cf = cf;
  bench.time_start = start;
  bench.shared_data = *shared_data;
  ngx_memzero(&bench.data, sizeof(bench.data));
  hdr_init_nchan_shm(1, 60000, 3, &bench.data.msg_latency);
  
  nchan_benchmark_start(initiating_worker_slot);
  
  return NGX_OK;
}

ngx_int_t nchan_benchmark_start(ngx_int_t initiating_worker_slot) {
  int           c, i;
  subscriber_t *sub;
  ngx_str_t     channel_id;
  ngx_int_t subs_per_channel = bench.cf->benchmark.subscribers_per_channel / nchan_worker_processes;
  if(ngx_process_slot == initiating_worker_slot) {
    subs_per_channel += bench.cf->benchmark.subscribers_per_channel % nchan_worker_processes;
  }
  
  for(c=0; c<bench.cf->benchmark.channels; c++) {
    for(i=0; i<subs_per_channel; i++) {
      nchan_benchmark_channel_id(c, &channel_id);
      sub = benchmark_subscriber_create(&bench);
      if(sub->fn->subscribe(sub, &channel_id) != NGX_OK) {
        return NGX_ERROR;
      }
    }
  }
  
  return NGX_OK;
}

ngx_int_t nchan_benchmark_channel_id(int n, ngx_str_t *chid) {
  static u_char  id[255];
  u_char        *last;
  chid->data = id;
  last = ngx_snprintf(id, 255, "/benchmark.%d.%d", bench.time_start, n);
  chid->len = last - id;
  return NGX_OK;
}

ngx_int_t nchan_benchmark_finish(void) {
  
  //free all the things
  shm_free(nchan_store_memory_shmem, (void *)bench.shared_data.subscribers_enqueued);
  shm_free(nchan_store_memory_shmem, (void *)bench.shared_data.subscribers_dequeued);
  shm_free(nchan_store_memory_shmem, bench.shared_data.channels);
  hdr_close_nchan_shm(bench.data.msg_latency);
  ngx_memzero(&bench, sizeof(bench));
  bench_active = 0;
  bench_initiating_request = NULL;
  
  return NGX_OK;
}
