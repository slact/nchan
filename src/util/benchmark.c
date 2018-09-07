#include "benchmark.h"
#include <subscribers/benchmark.h>

ngx_int_t nchan_benchmark_start(ngx_http_request_t *r) {
  subscriber_t                   *sub;
  nchan_benchmark_t              *bench;
  nchan_loc_conf_t               *cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  int                             i, j, total_subs;
  
  bench = ngx_alloc(sizeof(*bench), ngx_cycle->log);
  bench->time_start = ngx_time();
  bench->time_end = ngx_time() + cf->benchmark.time;
  bench->msg_rate = cf->benchmark.msg_rate;
  bench->msg_padding = cf->benchmark.msg_padding;
  bench->channels = cf->benchmark.channels;
  bench->subs_per_channel = cf->benchmark.subscribers_per_channel;
  
  total_subs = bench->channels * bench->subs_per_channel;
  
  bench->subs = ngx_alloc(sizeof(subscriber_t *) * total_subs, ngx_cycle->log);

  for(i=0; i<bench->channels; i++) {
    for(j=0; j<bench->subs_per_channel; j++) {
      sub = benchmark_subscriber_create(bench, cf);
      bench->subs[i*j] = sub;
    }
  }
  
  return NGX_OK;
}
