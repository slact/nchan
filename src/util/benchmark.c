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
ngx_atomic_int_t    *bench_active;
ngx_atomic_int_t     bench_worker_running = 0;
ngx_http_request_t  *bench_initiating_request;
void               **bench_publisher_timers = NULL;
subscriber_t       **bench_subscribers = NULL;
size_t               bench_subscribers_count = 0;

ngx_atomic_int_t    *worker_counter = NULL;
ngx_int_t            bench_worker_number = 0;
void                *readiness_timer = NULL;

unsigned             bench_base_msg_period = 0;

unsigned bench_msg_period_jittered(void) {
  int max_diff = bench_base_msg_period * ((float )bench.cf->benchmark.msg_rate_jitter_percent/100);
  int range = max_diff * 2;
  int jitter = (rand() / (RAND_MAX / range + 1)) - max_diff;
  int jittered_msg_period = bench_base_msg_period + jitter;
  if(jittered_msg_period <= 0) {
    jittered_msg_period = 1;
  }
  //DBG("jittered %d into %d", bench_base_msg_period, jittered_msg_period);
  return (unsigned )jittered_msg_period;
}

int nchan_benchmark_running(void) {
  return bench_active && *bench_active > 0 && bench_worker_running > 0;
}

ngx_int_t nchan_benchmark_init_module(ngx_cycle_t *cycle) {
  bench_active = shm_calloc(nchan_store_memory_shmem, sizeof(ngx_atomic_int_t), "benchmark active");
  worker_counter = shm_calloc(nchan_store_memory_shmem, sizeof(ngx_atomic_int_t), "benchmark worker counter");
  return NGX_OK;
}

ngx_int_t nchan_benchmark_init_worker(ngx_cycle_t *cycle) {
  DBG("init worker");
  bench_worker_number = ngx_atomic_fetch_add(worker_counter, 1);
  return NGX_OK;
}

ngx_int_t nchan_benchmark_exit_master(ngx_cycle_t *cycle) {
  shm_free(nchan_store_memory_shmem, bench_active);
  shm_free(nchan_store_memory_shmem, worker_counter);
  bench_active = NULL;
  worker_counter = NULL;
  return NGX_OK;
}

ngx_int_t nchan_benchmark_initialize(ngx_http_request_t *r) {
  nchan_loc_conf_t               *cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  int                             i;
  
  if(!ngx_atomic_cmp_set(bench_active, 0, 1)) {
    return nchan_respond_cstring(r, NGX_HTTP_OK, &NCHAN_CONTENT_TYPE_TEXT_PLAIN, "another benchmark is in progress", 0);
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
  hdr_init_nchan_shm(1, 10000000, 4, &bench.data.msg_delivery_latency);
  hdr_init_nchan_shm(1, 10000000, 4, &bench.data.msg_publishing_latency);
  hdr_init_nchan_shm(1, 10000000, 4, &bench.data.subscriber_readiness_latency);
  
  
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
  hdr_init_nchan_shm(1, 10000000, 4, &bench.data.msg_delivery_latency);
  hdr_init_nchan_shm(1, 10000000, 4, &bench.data.msg_publishing_latency);
  hdr_init_nchan_shm(1, 10000000, 4, &bench.data.subscriber_readiness_latency);
  
  nchan_benchmark_start(initiating_worker_slot);
  
  return NGX_OK;
}

static ngx_int_t benchmark_publish_callback(ngx_int_t status, void *data, void *pd) {
  struct      timeval tv;
  uint64_t    t1;
  uintptr_t   t0 = (uintptr_t )pd;
  if(nchan_benchmark_running()) {
    ngx_gettimeofday(&tv);
    t1 = (tv.tv_sec - bench.time_start) * (uint64_t)1000000 + tv.tv_usec;
    switch(status) {
      case NCHAN_MESSAGE_QUEUED:
      case NCHAN_MESSAGE_RECEIVED:
        bench.data.msg_sent++;
        break;
      default:
        bench.data.msg_send_failed++;
    }
    hdr_record_value(bench.data.msg_publishing_latency, t1-t0);
  }
  return NGX_OK;
}

static ngx_int_t benchmark_publish_message(void *pd) {
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
  if(*bench_active == 0 || bench.time_start != time_start) {
    DBG("benchmark not running. stop trying to publish");
    return NGX_ABORT; //we're done here
  }
  
  channel_n = rand() / (RAND_MAX / (bench.cf->benchmark.channels) + 1);
  assert(channel_n < bench.cf->benchmark.channels && channel_n >= 0);
  chan = &bench.shared_data.channels[channel_n];
  nchan_benchmark_channel_id(channel_n, &channel_id);
  
  msgnum = ngx_atomic_fetch_add(&chan->msg_count, 1);
  
  maxlen = bench.cf->benchmark.msg_padding + 64;
  msgbuf = ngx_alloc(maxlen, ngx_cycle->log);
  ngx_memset(msgbuf, 'z', maxlen);
  
  ngx_gettimeofday(&tv);
  now = (tv.tv_sec - bench.time_start) * (uint64_t)1000000 + tv.tv_usec;
  
  last = ngx_snprintf(msgbuf, 64, "%D %D ", now, msgnum);
  
  //DBG("publish to channel %V msg #%D (t: %D)", &channel_id, msgnum, now);
  
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
  
  bench.cf->storage_engine->publish(&channel_id, &msg, bench.cf, (callback_pt )benchmark_publish_callback, (void *)(uintptr_t)now);
  
  return bench_msg_period_jittered();
}

static ngx_int_t benchmark_check_ready_to_start_publishing(void *pd) {
  uint64_t required_subs = bench.cf->benchmark.subscribers_per_channel * bench.cf->benchmark.channels;
  if(*bench.shared_data.subscribers_enqueued == required_subs) {
    int       i;
    bench_base_msg_period = 1000.0/((double)bench.cf->benchmark.msgs_per_minute / 60.0);
    bench_base_msg_period *= nchan_worker_processes;
    DBG("ready to begin benchmark, msg period: %d msec", bench_base_msg_period);
    assert(bench_publisher_timers == NULL);
    bench_publisher_timers = ngx_alloc(sizeof(void *) * bench.cf->benchmark.channels, ngx_cycle->log);
    for(i=0; i < bench.cf->benchmark.channels; i++) {
      bench_publisher_timers[i] = nchan_add_interval_timer(benchmark_publish_message, (void *)(uintptr_t )bench.time_start, bench_msg_period_jittered());
    }
    readiness_timer = NULL;
    return NGX_ABORT;
  }
  else {
    DBG("not ready to benchmark: subs required: %d, ready: %d", required_subs, *bench.shared_data.subscribers_enqueued);
    return NGX_AGAIN;
  }
}
static void benchmark_finish_phase1(void *pd);
static void benchmark_finish_phase2(void *pd);

ngx_int_t nchan_benchmark_start(ngx_int_t initiating_worker_slot) {
  int           c, i;
  subscriber_t **sub;
  ngx_str_t     channel_id;
  ngx_int_t divided_subs = bench.cf->benchmark.subscribers_per_channel / nchan_worker_processes;
  ngx_int_t leftover_subs = bench.cf->benchmark.subscribers_per_channel % nchan_worker_processes;
  ngx_int_t subs_per_channel;
  
  if(ngx_process_slot == initiating_worker_slot) {
    nchan_add_oneshot_timer(benchmark_finish_phase1, NULL, bench.cf->benchmark.time * 1000);
  }
  
  
  assert(bench_worker_running == 0);
  bench_worker_running = 1;
  assert(bench_subscribers == NULL);
  assert(bench_subscribers_count == 0);
  for(c=0; c<bench.cf->benchmark.channels; c++) {
    bench_subscribers_count += divided_subs;
    if (c%nchan_worker_processes == bench_worker_number) {
      bench_subscribers_count += leftover_subs;
    }
  }
  DBG("bench_subscribers_count = %d", bench_subscribers_count);
  bench_subscribers = ngx_alloc(sizeof(subscriber_t *) * bench_subscribers_count, ngx_cycle->log);
  sub = &bench_subscribers[0];
  
  for(c=0; c<bench.cf->benchmark.channels; c++) {
    subs_per_channel = divided_subs + (((c % nchan_worker_processes) == bench_worker_number) ? leftover_subs : 0);
    //DBG("worker number %d channel %d subs %d", bench_worker_number, c, subs_per_channel);
    for(i=0; i<subs_per_channel; i++) {
      nchan_benchmark_channel_id(c, &channel_id);
      *sub = benchmark_subscriber_create(&bench);
      if((*sub)->fn->subscribe(*sub, &channel_id) != NGX_OK) {
        return NGX_ERROR;
      }
      sub++;
    }
  }
  
  readiness_timer = nchan_add_interval_timer(benchmark_check_ready_to_start_publishing, NULL, 250);
  
  return NGX_OK;
}

ngx_int_t nchan_benchmark_dequeue_subscribers(void) {
  unsigned i;
  for(i=0; i < bench_subscribers_count; i++) {
    bench_subscribers[i]->fn->dequeue(bench_subscribers[i]);
  }
  ngx_free(bench_subscribers);
  bench_subscribers = NULL;
  bench_subscribers_count = 0;
  return NGX_OK;
}

static void benchmark_finish_phase1(void *pd) {
  bench.time_end = ngx_time();
  memstore_ipc_broadcast_benchmark_stop(&bench);
  nchan_benchmark_stop();
  nchan_add_oneshot_timer(benchmark_finish_phase2, NULL, 4000);
}
static int waiting_for_data = 0;
static void benchmark_finish_phase2(void *pd) {
  nchan_benchmark_dequeue_subscribers();
  waiting_for_data = nchan_worker_processes - 1;
  if(waiting_for_data == 0) {
    nchan_benchmark_finish_response();
    nchan_benchmark_finish();
  }
  else {
    memstore_ipc_broadcast_benchmark_finish(&bench);
  }
}

ngx_int_t nchan_benchmark_receive_finished_data(nchan_benchmark_data_t *data) {
  DBG("received benchmark data");
  assert(waiting_for_data > 0);
  waiting_for_data --;
  bench.data.msg_sent += data->msg_sent;
  bench.data.msg_send_failed += data->msg_send_failed;
  bench.data.msg_received += data->msg_received;
  hdr_add(bench.data.msg_delivery_latency, data->msg_delivery_latency);
  hdr_close_nchan_shm(data->msg_delivery_latency);
  hdr_add(bench.data.msg_publishing_latency, data->msg_publishing_latency);
  hdr_close_nchan_shm(data->msg_publishing_latency);
  hdr_add(bench.data.subscriber_readiness_latency, data->subscriber_readiness_latency);
  hdr_close_nchan_shm(data->subscriber_readiness_latency);
  
  if(waiting_for_data == 0) {
    nchan_benchmark_finish_response();
    nchan_benchmark_finish();
  }
  return NGX_OK;
}

ngx_int_t nchan_benchmark_finish_response(void) {
  u_char                 str[2048];
  ngx_http_request_t    *r = bench_initiating_request;
  const char            *fmt = 
    "benchmark start time: %d\n"
    "run time (sec):       %d\n"
    "channels:             %d\n"
    "subscribers:          %d\n"
    "message length        %d\n"
    "total messages\n"
    "  sent:               %d\n"
    "  send failed:        %d\n"
    "  received:           %d\n"
    "  unreceived:         %d\n"
    "message publishing latency\n"
    "  min:                %.3fms\n"
    "  avg:                %.3fms\n"
    "  99th percentile:    %.3fms\n"
    "  max:                %.3fms\n"
    "  stddev              %.3fms\n"
    "  samples:            %D\n"
    "message delivery latency\n"
    "  min:                %.3fms\n"
    "  avg:                %.3fms\n"
    "  99th percentile:    %.3fms\n"
    "  max:                %.3fms\n"
    "  stddev              %.3fms\n"
    "  samples:            %D\n"
    "subscriber readiness latency\n"
    "  min:                %.3fms\n"
    "  avg:                %.3fms\n"
    "  99th percentile:    %.3fms\n"
    "  max:                %.3fms\n"
    "  stddev              %.3fms\n"
    "  samples:            %D\n"
    "%Z";
    
  ngx_snprintf(str, 2048, fmt, 
    bench.time_start,
    bench.time_end - bench.time_start,
    bench.cf->benchmark.channels,
    *bench.shared_data.subscribers_enqueued,
    bench.cf->benchmark.msg_padding + 5,
    bench.data.msg_sent,
    bench.data.msg_send_failed,
    bench.data.msg_received,
    bench.data.msg_sent * bench.cf->benchmark.subscribers_per_channel - bench.data.msg_received,
    (double )hdr_min(bench.data.msg_publishing_latency)/1000.0,
    (double )hdr_mean(bench.data.msg_publishing_latency)/1000.0,
    (double )hdr_value_at_percentile(bench.data.msg_publishing_latency, 99.0)/1000.0,
    (double )hdr_max(bench.data.msg_publishing_latency)/1000.0,
    (double )hdr_stddev(bench.data.msg_publishing_latency)/1000.0,
    bench.data.msg_publishing_latency->total_count,
    
    (double )hdr_min(bench.data.msg_delivery_latency)/1000.0,
    (double )hdr_mean(bench.data.msg_delivery_latency)/1000.0,
    (double )hdr_value_at_percentile(bench.data.msg_delivery_latency, 99.0)/1000.0,
    (double )hdr_max(bench.data.msg_delivery_latency)/1000.0,
    (double )hdr_stddev(bench.data.msg_delivery_latency)/1000.0,
    bench.data.msg_delivery_latency->total_count,
    
    (double )hdr_min(bench.data.subscriber_readiness_latency)/1000.0,
    (double )hdr_mean(bench.data.subscriber_readiness_latency)/1000.0,
    (double )hdr_value_at_percentile(bench.data.subscriber_readiness_latency, 99.0)/1000.0,
    (double )hdr_max(bench.data.subscriber_readiness_latency)/1000.0,
    (double )hdr_stddev(bench.data.subscriber_readiness_latency)/1000.0,
    bench.data.subscriber_readiness_latency->total_count
  );
  
  return nchan_respond_cstring(r, NGX_HTTP_OK, &NCHAN_CONTENT_TYPE_TEXT_PLAIN,( char *)str, 1);
}

ngx_int_t nchan_benchmark_stop(void) {
  int i;
  bench_worker_running = 0;
  DBG("stop benchmark");
  if(readiness_timer) {
    nchan_abort_interval_timer(readiness_timer);
    readiness_timer = NULL;
  }
  if(bench_publisher_timers) {
    for(i=0; i< bench.cf->benchmark.channels; i++) {
      if(bench_publisher_timers[i]) {
        nchan_abort_interval_timer(bench_publisher_timers[i]);
      }
    }
    ngx_free(bench_publisher_timers);
    bench_publisher_timers = NULL;
  }
  return NGX_OK;
}

ngx_int_t nchan_benchmark_finish(void) {
  //free all the things
  shm_free(nchan_store_memory_shmem, (void *)bench.shared_data.subscribers_enqueued);
  shm_free(nchan_store_memory_shmem, (void *)bench.shared_data.subscribers_dequeued);
  shm_free(nchan_store_memory_shmem, bench.shared_data.channels);
  hdr_close_nchan_shm(bench.data.msg_publishing_latency);
  hdr_close_nchan_shm(bench.data.msg_delivery_latency);
  hdr_close_nchan_shm(bench.data.subscriber_readiness_latency);
  nchan_benchmark_cleanup();
  DBG("benchmark finish..");
  *bench_active = 0;
  DBG("benchmark finished");
  
  return NGX_OK;
}

ngx_int_t nchan_benchmark_cleanup(void) {
  DBG("benchmark cleanup");
  readiness_timer = NULL;
  ngx_memzero(&bench, sizeof(bench));
  bench_initiating_request = NULL;
  assert(bench_publisher_timers == NULL);
  assert(bench_subscribers == NULL);
  assert(bench_subscribers_count == 0);
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
  
  uint64_t now = (tv.tv_sec - bench.time_start) * (uint64_t)1000000 + tv.tv_usec;
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

nchan_benchmark_t *nchan_benchmark_get_active(void) {
  return &bench;
}
