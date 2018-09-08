#include "benchmark.h"
#include <subscribers/benchmark.h>
#include <util/shmem.h>
#include <store/memory/store.h>
#include <store/memory/ipc-handlers.h>
#include <assert.h>
#include <sys/time.h> /* for struct timeval */

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "BENCHMARK: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "BENCHMARK: " fmt, ##args)

nchan_benchmark_t    bench;
ngx_atomic_int_t     bench_active;
ngx_http_request_t  *bench_initiating_request;

ngx_int_t nchan_benchmark_initialize(ngx_http_request_t *r) {
  nchan_loc_conf_t               *cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  int                             i;
  
  if(!ngx_atomic_cmp_set(&bench_active, 0, 1)) {
    //benchmark already running
  }
  
  DBG("init benchmark");
  
  bench_initiating_request = r;
  r->count++; //defer response
  
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
  DBG("init benchmark via IPC (time %d src %d)",start,  initiating_worker_slot);
  bench_initiating_request = NULL;
  bench.cf = cf;
  bench.time_start = start;
  bench.shared_data = *shared_data;
  ngx_memzero(&bench.data, sizeof(bench.data));
  hdr_init_nchan_shm(1, 60000, 3, &bench.data.msg_latency);
  
  nchan_benchmark_start(initiating_worker_slot);
  
  return NGX_OK;
}

static ngx_int_t benchmark_publish_callback(ngx_int_t status, void *data, void *pd) {
  switch(status) {
    case NCHAN_MESSAGE_QUEUED:
    case NCHAN_MESSAGE_RECEIVED:
      bench.data.msg_sent++;
      break;
    default:
      bench.data.msg_send_failed++;
  }
  return NGX_OK;
}

static int benchmark_publish_message(void *pd) {
  time_t      time_start = (time_t)(uintptr_t )pd;
  struct      timeval tv;
  uint64_t    now;
  u_char     *msgbuf, *last;
  uint64_t    msgnum;
  size_t      maxlen;
  int         channel_n;
  nchan_msg_t msg;
  ngx_str_t   channel_id;
  
  nchan_benchmark_channel_t *chan;
  if(!bench_active || bench.time_start != time_start) {
    //TODO: publish "FIN"
    return 0; //we're done here
  }
  
  channel_n = rand() / (RAND_MAX / (bench.cf->benchmark.channels + 1) + 1);
  assert(channel_n < bench.cf->benchmark.channels && channel_n >= 0);
  chan = &bench.shared_data.channels[channel_n];
  nchan_benchmark_channel_id(channel_n, &channel_id);
  
  msgnum = ngx_atomic_fetch_add(&chan->msg_count, 1);
  
  maxlen = bench.cf->benchmark.msg_padding + 64;
  msgbuf = ngx_alloc(maxlen, ngx_cycle->log);
  ngx_memset(msgbuf, 'z', maxlen);
  
  ngx_gettimeofday(&tv);
  now = ((tv.tv_sec - bench.time_start) * (uint64_t)1000) + (tv.tv_usec / 1000);
  
  last = ngx_snprintf(msgbuf, 64, "%d %d ", now, msgnum);
  
  ngx_memzero(&msg, sizeof(msg));
  msg.buf.temporary = 1;
  msg.buf.memory = 1;
  msg.buf.last_buf = 1;
  msg.buf.pos = msg.buf.start = msgbuf;
  msg.buf.last = msg.buf.end = &last[bench.cf->benchmark.msg_padding];
  msg.id.time = 0;
  msg.id.tag.fixed[0] = 0;
  msg.id.tagactive = 0;
  msg.id.tagcount = 1;
  msg.storage = NCHAN_MSG_STACK;
  
  msg.content_type = (ngx_str_t *)&NCHAN_CONTENT_TYPE_TEXT_PLAIN;
  
  bench.cf->storage_engine->publish(&channel_id, &msg, bench.cf, (callback_pt )benchmark_publish_callback, chan);
  
  return 1;
}

static int benchmark_check_ready_to_start_publishing(void *pd) {
  uint64_t required_subs = bench.cf->benchmark.subscribers_per_channel * bench.cf->benchmark.channels;
  if(*bench.shared_data.subscribers_enqueued == required_subs) {
    int       i;
    unsigned  msg_period = 250;
    msg_period *= nchan_worker_processes;
    DBG("ready to begin benchmark");
    for(i=0; i < bench.cf->benchmark.channels; i++) {
      nchan_add_interval_timer(benchmark_publish_message, (void *)(uintptr_t )bench.time_start, msg_period);
    }
    return 0;
  }
  else {
    DBG("not ready to benchmark: subs required: %d, ready: %d", required_subs, *bench.shared_data.subscribers_enqueued);
    return 1; //retry again
  }
}
static ngx_int_t benchmark_finish_phase1_callback(void *pd);

ngx_int_t nchan_benchmark_start(ngx_int_t initiating_worker_slot) {
  int           c, i;
  subscriber_t *sub;
  ngx_str_t     channel_id;
  ngx_int_t subs_per_channel = bench.cf->benchmark.subscribers_per_channel / nchan_worker_processes;
  if(ngx_process_slot == initiating_worker_slot) {
    subs_per_channel += bench.cf->benchmark.subscribers_per_channel % nchan_worker_processes;
    nchan_add_oneshot_timer(benchmark_finish_phase1_callback, NULL, bench.cf->benchmark.time * 1000);
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
  
  nchan_add_interval_timer(benchmark_check_ready_to_start_publishing, NULL, 250);
  
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

uint64_t nchan_benchmark_message_delivery_msec(nchan_msg_t *msg) {
  struct timeval tv;
  ngx_gettimeofday(&tv);
  
  uint64_t now = ((tv.tv_sec - bench.time_start) * (uint64_t)1000) + (tv.tv_usec / 1000);
  int then;
  
  if(ngx_buf_in_memory((&msg->buf))) {
    then = atoi((char *)msg->buf.start);
  }
  else {
    //not supported yet
    then = now;
    raise(SIGABRT);
  }
  
  return now - then;
}

static ngx_int_t benchmark_finish_phase1_callback(void *pd) {
  
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
