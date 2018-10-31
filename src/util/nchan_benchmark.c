#include "nchan_benchmark.h"
#include <subscribers/benchmark.h>
#include <subscribers/websocket.h>
#include <util/shmem.h>
#include <store/memory/store.h>
#include <store/memory/ipc-handlers.h>
#include <assert.h>
#include <sys/time.h> /* for struct timeval */
#include <inttypes.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "BENCHMARK: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "BENCHMARK: " fmt, ##args)

nchan_benchmark_t    bench;

ngx_atomic_int_t    *worker_counter = NULL;
ngx_int_t            bench_worker_number = 0;

int nchan_benchmark_active(void) {
  return bench.state && *bench.state > NCHAN_BENCHMARK_INACTIVE;
}

static ngx_int_t benchmark_client_respond(char *cstr) {
  if(!bench.client) {
    return NGX_ERROR;
  }
  nchan_msg_t    msg;
  nchan_msg_id_t msgid = NCHAN_NEWEST_MSGID;
  ngx_memzero(&msg, sizeof(msg));
  msg.storage = NCHAN_MSG_STACK;
  msg.id = msgid;
  ngx_init_set_membuf(&msg.buf, (u_char *)cstr, (u_char *)&cstr[strlen(cstr)]);
  msg.buf.last_buf = 1;
  msg.buf.last_in_chain = 1;
  bench.client->fn->respond_message(bench.client, &msg);
  return NGX_OK;
}

ngx_int_t nchan_benchmark_init_module(ngx_cycle_t *cycle) {
  bench.state = shm_calloc(nchan_store_memory_shmem, sizeof(ngx_atomic_int_t), "benchmark state");
  worker_counter = shm_calloc(nchan_store_memory_shmem, sizeof(ngx_atomic_int_t), "benchmark worker counter");
  bench.config = shm_calloc(nchan_store_memory_shmem, sizeof(*bench.config), "benchmark config (shared)");
  
  return NGX_OK;
}

ngx_int_t nchan_benchmark_init_worker(ngx_cycle_t *cycle) {
  DBG("init worker");
  bench_worker_number = ngx_atomic_fetch_add((ngx_atomic_uint_t *)worker_counter, 1);
  return NGX_OK;
}

ngx_int_t nchan_benchmark_exit_master(ngx_cycle_t *cycle) {
  shm_free(nchan_store_memory_shmem, bench.state);
  shm_free(nchan_store_memory_shmem, worker_counter);
  shm_free(nchan_store_memory_shmem, bench.config);
  bench.state = NULL;
  bench.config = NULL;
  worker_counter = NULL;
  return NGX_OK;
}

static ngx_int_t benchmark_publish_callback(ngx_int_t status, void *data, void *pd) {
  struct      timeval tv;
  uint64_t    t1;
  uintptr_t   t0 = (uintptr_t )pd;
  if(nchan_benchmark_active()) {
    ngx_gettimeofday(&tv);
    t1 = (tv.tv_sec - bench.time.init) * (uint64_t)1000000 + tv.tv_usec;
    switch(status) {
      case NCHAN_MESSAGE_QUEUED:
      case NCHAN_MESSAGE_RECEIVED:
        bench.data.msg_send_confirmed++;
        break;
      default:
        bench.data.msg_send_failed++;
    }
    hdr_record_value(bench.data.msg_publishing_latency, t1-t0);
  }
  return NGX_OK;
}

static void benchmark_publish_message(nchan_benchmark_channel_t *chan) {
  struct      timeval tv;
  uint64_t    now;
  u_char     *last;
  uint64_t    msgnum;
  nchan_msg_t msg;
  ngx_str_t   channel_id;
  
  nchan_benchmark_channel_id(chan->n, &channel_id);
  
  msgnum = ngx_atomic_fetch_add(&chan->msg_count, 1);
  
  ngx_gettimeofday(&tv);
  now = (tv.tv_sec - bench.time.init) * (uint64_t)1000000 + tv.tv_usec;
  
  last = ngx_snprintf(bench.msgbuf, 64, "%D %D zzzzzzzz", now, msgnum);
  
  //DBG("publish to channel %V msg #%D (t: %D)", &channel_id, msgnum, now);
  
  ngx_memzero(&msg, sizeof(msg));
  msg.buf.temporary = 1;
  msg.buf.memory = 1;
  msg.buf.last_buf = 1;
  msg.buf.pos = msg.buf.start = bench.msgbuf;
  msg.buf.last = msg.buf.end = &last[bench.config->msg_padding];
  msg.id.time = 0;
  msg.id.tag.fixed[0] = 0;
  msg.id.tagactive = 0;
  msg.id.tagcount = 1;
  msg.storage = NCHAN_MSG_STACK;
  
  msg.content_type = (ngx_str_t *)&NCHAN_CONTENT_TYPE_TEXT_PLAIN;
  
  bench.loc_conf->storage_engine->publish(&channel_id, &msg, bench.loc_conf, (callback_pt )benchmark_publish_callback, (void *)(uintptr_t)now);
  bench.data.msg_sent++;
}

static ngx_int_t benchmark_publish_message_interval_timer(void *pd) {  
  nchan_benchmark_channel_t *chan = pd;
  if(!nchan_benchmark_active()) {
    DBG("benchmark not running. stop trying to publish");
    bench.timer.publishers[chan->n] = NULL;
    return NGX_ABORT; //we're done here
  }
  
  benchmark_publish_message(chan);
  
  return bench.base_msg_period;
}

static void benchmark_timer_running_stop(void *pd);
static void benchmark_timer_finishing_check(void *pd);

ngx_int_t nchan_benchmark_initialize(void) {
  int           c, i;
  subscriber_t **sub;
  ngx_str_t     channel_id;
  ngx_int_t     subs_per_channel;
      
  assert(bench.subs.array == NULL);
  assert(bench.subs.n == 0);
  
  if(bench.config->subscriber_distribution == NCHAN_BENCHMARK_SUBSCRIBER_DISTRIBUTION_RANDOM) {
    ngx_int_t divided_subs = bench.config->subscribers_per_channel / nchan_worker_processes;
    ngx_int_t leftover_subs = bench.config->subscribers_per_channel % nchan_worker_processes;
    for(c=0; c<bench.config->channels; c++) {
      bench.subs.n += divided_subs;
      if (c%nchan_worker_processes == bench_worker_number) {
        bench.subs.n += leftover_subs;
      }
    }
    DBG("bench.subs.n = %d", bench.subs.n);
    bench.subs.array = ngx_alloc(sizeof(subscriber_t *) * bench.subs.n, ngx_cycle->log);
    sub = &bench.subs.array[0];
    
    for(c=0; c<bench.config->channels; c++) {
      subs_per_channel = divided_subs + (((c % nchan_worker_processes) == bench_worker_number) ? leftover_subs : 0);
      //DBG("worker number %d channel %d subs %d", bench_worker_number, c, subs_per_channel);
      nchan_benchmark_channel_id(c, &channel_id);
      for(i=0; i<subs_per_channel; i++) {
        *sub = benchmark_subscriber_create(&bench);
        if((*sub)->fn->subscribe(*sub, &channel_id) != NGX_OK) {
          return NGX_ERROR;
        }
        sub++;
      }
    }
  }
  else {
    subs_per_channel = bench.config->subscribers_per_channel;
    for(c=0; c<bench.config->channels; c++) {
      nchan_benchmark_channel_id(c, &channel_id);
      if(memstore_channel_owner(&channel_id) == ngx_process_slot) {
        bench.subs.n += subs_per_channel;
      }
    }
    bench.subs.array = ngx_alloc(sizeof(subscriber_t *) * bench.subs.n, ngx_cycle->log);
    sub = &bench.subs.array[0];
    
    for(c=0; c<bench.config->channels; c++) {
      nchan_benchmark_channel_id(c, &channel_id);
      if(memstore_channel_owner(&channel_id) == ngx_process_slot) {
        for(i=0; i<subs_per_channel; i++) {
          *sub = benchmark_subscriber_create(&bench);
          if((*sub)->fn->subscribe(*sub, &channel_id) != NGX_OK) {
            return NGX_ERROR;
          }
          sub++;
        }
      }
    }
  }
  
  return NGX_OK;
}

ngx_int_t nchan_benchmark_run(void) {
  uint64_t required_subs = bench.config->subscribers_per_channel * bench.config->channels;
  assert(*bench.shared.subscribers_enqueued == required_subs);
  int       i;
  size_t msgbuf_maxlen = bench.config->msg_padding + 64;
  unsigned pubstart;
  int64_t total_offset = 0;
  bench.msgbuf = ngx_alloc(msgbuf_maxlen, ngx_cycle->log);
  ngx_memset(bench.msgbuf, 'z', msgbuf_maxlen);
  
  bench.base_msg_period = 1000.0/((double)bench.config->msgs_per_minute / 60.0);
  assert(bench.timer.publishers == NULL);
  bench.timer.publishers = ngx_alloc(sizeof(void *) * bench.config->channels, ngx_cycle->log);
  if(bench.config->publisher_distribution == NCHAN_BENCHMARK_PUBLISHER_DISTRIBUTION_RANDOM) {
    bench.base_msg_period *= nchan_worker_processes;
    for(i=0; i < bench.config->channels; i++) {
      pubstart = (rand() / (RAND_MAX / bench.base_msg_period));
      total_offset += pubstart;
      bench.timer.publishers[i] = nchan_add_interval_timer(benchmark_publish_message_interval_timer, &bench.shared.channels[i], pubstart);
    }
  }
  else if(bench.config->publisher_distribution == NCHAN_BENCHMARK_PUBLISHER_DISTRIBUTION_OPTIMAL) {
    ngx_str_t channel_id;
    for(i=0; i < bench.config->channels; i++) {
      nchan_benchmark_channel_id(i, &channel_id);
      if(memstore_channel_owner(&channel_id) == ngx_process_slot) {
        pubstart = (rand() / (RAND_MAX / bench.base_msg_period));
        total_offset += pubstart;
        bench.timer.publishers[i] = nchan_add_interval_timer(benchmark_publish_message_interval_timer, &bench.shared.channels[i], pubstart);
      }
      else {
        bench.timer.publishers[i] = NULL;
      }
    }
  }
  
  return NGX_OK;
}


ngx_int_t nchan_benchmark_dequeue_subscribers(void) {
  unsigned i;
  for(i=0; i < bench.subs.n; i++) {
    bench.subs.array[i]->fn->dequeue(bench.subs.array[i]);
  }
  ngx_free(bench.subs.array);
  bench.subs.array = NULL;
  bench.subs.n = 0;
  return NGX_OK;
}

static void benchmark_timer_running_stop(void *pd) {
  bench.timer.running = NULL;
  bench.time.end = ngx_time();
  memstore_ipc_broadcast_benchmark_stop();
  nchan_benchmark_stop();
  bench.timer.finishing = nchan_add_oneshot_timer(benchmark_timer_finishing_check, NULL, 3000);
}
static void benchmark_timer_finishing_check(void *pd) {
  bench.timer.finishing = NULL;
  nchan_benchmark_dequeue_subscribers();
  bench.waiting_for_results = nchan_worker_processes - 1;
  if(bench.waiting_for_results == 0) {
    nchan_benchmark_finish_response();
    nchan_benchmark_finish();
  }
  else {
    memstore_ipc_broadcast_benchmark_finish();
  }
}

ngx_int_t nchan_benchmark_receive_finished_data(nchan_benchmark_data_t *data) {
  DBG("received benchmark data");
  assert(bench.waiting_for_results > 0);
  bench.waiting_for_results  --;
  bench.data.msg_sent += data->msg_sent;
  bench.data.msg_send_confirmed += data->msg_send_confirmed;
  bench.data.msg_send_failed += data->msg_send_failed;
  bench.data.msg_received += data->msg_received;
  hdr_add(bench.data.msg_delivery_latency, data->msg_delivery_latency);
  hdr_close_nchan_shm(data->msg_delivery_latency);
  hdr_add(bench.data.msg_publishing_latency, data->msg_publishing_latency);
  hdr_close_nchan_shm(data->msg_publishing_latency);
  hdr_add(bench.data.subscriber_readiness_latency, data->subscriber_readiness_latency);
  hdr_close_nchan_shm(data->subscriber_readiness_latency);
  
  if(bench.waiting_for_results  == 0) {
    nchan_benchmark_finish_response();
    nchan_benchmark_finish();
  }
  return NGX_OK;
}

ngx_int_t nchan_benchmark_finish_response(void) {
  u_char                *str;
  ngx_http_request_t    *r = bench.client->request;
  ngx_str_t             *accept_header = nchan_get_accept_header_value(r);
  const char            *fmt;
  char stats[2048];
  fmt = 
    "  \"start_time\":           %d,\n"
    "  \"run_time_sec\":         %d,\n"
    "  \"channels\":             %d,\n"
    "  \"subscribers\":          %d,\n"
    "  \"message_length\":       %d,\n"
    "  \"messages\": {\n"
    "    \"sent\":               %d,\n"
    "    \"send_confirmed\":     %d,\n"
    "    \"send_unconfirmed\":   %d,\n"
    "    \"send_failed\":        %d,\n"
    "    \"received\":           %d,\n"
    "    \"unreceived\":         %d\n"
    "  },\n"
    "  \"message_publishing_latency\": {\n"
    "    \"min\":               \"%.3fms\",\n"
    "    \"avg\":               \"%.3fms\",\n"
    "    \"99th_percentile\":   \"%.3fms\",\n"
    "    \"max\":               \"%.3fms\",\n"
    "    \"stddev\":            \"%.3fms\",\n"
    "    \"samples\":            %D\n"
    "  },\n"
    "  \"message_delivery_latency\": {\n"
    "    \"min\":               \"%.3fms\",\n"
    "    \"avg\":               \"%.3fms\",\n"
    "    \"99th_percentile\":   \"%.3fms\",\n"
    "    \"max\":               \"%.3fms\",\n"
    "    \"stddev\":            \"%.3fms\",\n"
    "    \"samples\":            %D\n"
    "  }%Z";
    
  ngx_snprintf((u_char *)stats, 2048, fmt, 
    bench.time.start,
    bench.time.end - bench.time.start,
    bench.config->channels,
    *bench.shared.subscribers_enqueued,
    bench.config->msg_padding + 5,
    bench.data.msg_sent,
    bench.data.msg_send_confirmed,
    bench.data.msg_sent - bench.data.msg_send_confirmed,
    bench.data.msg_send_failed,
    bench.data.msg_received,
    bench.data.msg_sent * bench.config->subscribers_per_channel - bench.data.msg_received,
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
    bench.data.msg_delivery_latency->total_count
  );
  if(accept_header && ngx_strnstr(accept_header->data, "text/x-json-hdrhistogram", accept_header->len)) {
    ngx_str_t *serialized_publishing_histogram, *serialized_delivery_histogram;
    size_t sz;
    fmt = 
      "RESULTS\n"
      "{\n"
      "%s,\n"
      "  \"message_publishing_histogram\":\n"
      "    \"%V\",\n"
      "  \"message_delivery_histogram\":\n"
      "    \"%V\"\n"
      "}\n"
      "%Z";
    sz = strlen(stats) + strlen(fmt);
    serialized_publishing_histogram = nchan_hdrhistogram_serialize(bench.data.msg_publishing_latency, r->pool);
    serialized_delivery_histogram = nchan_hdrhistogram_serialize(bench.data.msg_delivery_latency, r->pool);
    
    sz += serialized_publishing_histogram->len;
    sz += serialized_delivery_histogram->len;
    str = ngx_palloc(r->pool, sz);
    if(str == NULL) {
      benchmark_client_respond("ERROR: unable to create results response");
      return NGX_ERROR;
    }
    
    ngx_snprintf(str, sz, fmt, 
      stats,
      serialized_publishing_histogram,
      serialized_delivery_histogram
    );
  }
  else {
        fmt = 
      "RESULTS\n"
      "{\n"
      "%s\n"
      "}\n"
      "%Z";
    str = ngx_palloc(r->pool, strlen(stats) + strlen(fmt));
    ngx_sprintf(str, fmt, stats);
  } 
  
  benchmark_client_respond((char *)str);
  return NGX_OK;
}

ngx_int_t nchan_benchmark_abort(void) {
  int active = nchan_benchmark_active();
  
  nchan_benchmark_dequeue_subscribers();
  nchan_benchmark_stop();
  nchan_benchmark_cleanup();
  
  return active ? NGX_OK : NGX_DECLINED;
}

ngx_int_t nchan_benchmark_stop(void) {
  int i;
  DBG("stop benchmark");
  if(bench.timer.publishers) {
    for(i=0; i< bench.config->channels; i++) {
      if(bench.timer.publishers[i]) {
        nchan_abort_interval_timer(bench.timer.publishers[i]);
      }
    }
    ngx_free(bench.timer.publishers);
    bench.timer.publishers = NULL;
  }
  return NGX_OK;
}

ngx_int_t nchan_benchmark_finish(void) {
  //free all the things
  shm_free(nchan_store_memory_shmem, (void *)bench.shared.subscribers_enqueued);
  shm_free(nchan_store_memory_shmem, (void *)bench.shared.subscribers_dequeued);
  shm_free(nchan_store_memory_shmem, bench.shared.channels);
  hdr_close_nchan_shm(bench.data.msg_publishing_latency);
  hdr_close_nchan_shm(bench.data.msg_delivery_latency);
  hdr_close_nchan_shm(bench.data.subscriber_readiness_latency);
  bench.client->fn->respond_status(bench.client, NGX_HTTP_GONE, NULL, NULL);
  nchan_benchmark_cleanup();
  DBG("benchmark finished");
  return NGX_OK;
}

ngx_int_t nchan_benchmark_cleanup(void) {
  DBG("benchmark cleanup");
  bench.client = NULL;
  assert(bench.timer.publishers == NULL);
  assert(bench.subs.array == NULL);
  assert(bench.subs.n == 0);
  bench.id = 0;
  if(bench.msgbuf) {
    ngx_free(bench.msgbuf);
    bench.msgbuf = NULL;
  }
  
  ngx_memzero(&bench.time, sizeof(bench.time));
  *bench.state = NCHAN_BENCHMARK_INACTIVE;
  bench.waiting_for_results = 0;
  
  if(bench.timer.ready) {
    nchan_abort_interval_timer(bench.timer.ready);
    bench.timer.ready = NULL;
  }
  if(bench.timer.running) {
    nchan_abort_oneshot_timer(bench.timer.running);
    bench.timer.running = NULL;
  }
  if(bench.timer.finishing) {
    nchan_abort_oneshot_timer(bench.timer.finishing);
    bench.timer.finishing = NULL;
  }
  
  return NGX_OK;
}

ngx_int_t nchan_benchmark_channel_id(int n, ngx_str_t *chid) {
  static u_char  id[255];
  u_char        *last;
  chid->data = id;
  last = ngx_snprintf(id, 255, "/benchmark.%T-%D.%D", bench.time.init, bench.id, n);
  chid->len = last - id;
  return NGX_OK;
}

uint64_t nchan_benchmark_message_delivery_msec(nchan_msg_t *msg) {
  struct timeval tv;
  ngx_gettimeofday(&tv);
  
  uint64_t now = (tv.tv_sec - bench.time.init) * (uint64_t)1000000 + tv.tv_usec;
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

char throwaway_buf[128];
static void serialize_int64_t(int write, char **cur, int64_t val) {
  char  *buf;
  buf = write ? *cur : throwaway_buf;
  *cur += sprintf(buf, "%" PRId64 " ", val);
}
static void serialize_int32_t(int write, char **cur, int32_t val) {
  char  *buf;
  buf = write ? *cur : throwaway_buf;
  *cur += sprintf(buf, "%" PRId32 " ", val);
}
static void serialize_double(int write, char **cur, double val) {
  char  *buf;
  buf = write ? *cur : throwaway_buf;
  *cur += sprintf(buf, "%lf ", val);
}
static void serialize_numrun(int write, char **cur, int num, int runcount) {
  char  *numrun="~!@#$%^&*";
  char  *buf;
  assert((size_t)num < strlen(numrun));
  buf = write ? *cur : throwaway_buf;
  if(runcount == 0) {
    *cur += sprintf(buf, "%i ", num);
  }
  else {
    *cur += sprintf(buf, "%c%i ", numrun[num], runcount);
  }
}

size_t hdrhistogram_serialize(int write, char *start, const struct hdr_histogram* hdr) {
  int    i;
  char  *fakestart = NULL;
  char **cur;
  if(start == NULL) {
    start = fakestart;
  }
  cur = &start;
  char *first = start;
  
  serialize_int64_t(write, cur, hdr->lowest_trackable_value);
  serialize_int64_t(write, cur, hdr->highest_trackable_value);
  serialize_int32_t(write, cur, hdr->unit_magnitude);
  serialize_int32_t(write, cur, hdr->significant_figures);
  serialize_int32_t(write, cur, hdr->sub_bucket_half_count_magnitude);
  serialize_int32_t(write, cur, hdr->sub_bucket_half_count);
  serialize_int64_t(write, cur, hdr->sub_bucket_mask);
  serialize_int32_t(write, cur, hdr->sub_bucket_count);
  serialize_int32_t(write, cur, hdr->bucket_count);
  serialize_int64_t(write, cur, hdr->min_value);
  serialize_int64_t(write, cur, hdr->max_value);
  serialize_int32_t(write, cur, hdr->normalizing_index_offset);
  serialize_double (write, cur, hdr->conversion_ratio);
  serialize_int32_t(write, cur, hdr->counts_len);
  serialize_int64_t(write, cur, hdr->total_count);
  
  if(write) {
    **cur='[';
  }
  (*cur)++;
  
  int runcount=0;
  int64_t ncur=0, nprev=0;
  for(i=1, nprev=hdr->counts[0]; i<hdr->counts_len; i++) {
    ncur = hdr->counts[i];
    nprev = hdr->counts[i-1];
    if(ncur <= 8 && ncur == nprev) {
      runcount++;
    }
    else {
      if(runcount > 0) {
        serialize_numrun(write, cur, nprev, runcount+1);
        runcount = 0;
      }
      else {
        serialize_int64_t(write, cur, nprev);
      }
    }
  }
  if(runcount > 0) {
    serialize_numrun(write, cur, ncur, runcount+1);
  }
  else {
    serialize_int64_t(write, cur, ncur);
  }
  
  if(write) {
    **cur=']';
  }
  (*cur)++;
  
  return *cur - first;
}


ngx_str_t *nchan_hdrhistogram_serialize(const struct hdr_histogram* hdr, ngx_pool_t *pool) {
  char *start=NULL;
  ngx_str_t *str = ngx_palloc(pool, sizeof(*str));
  size_t sz = hdrhistogram_serialize(0, NULL, hdr);
  start = ngx_palloc(pool, sz);
  hdrhistogram_serialize(1, start, hdr);
  str->data = (u_char *)start;
  str->len = sz;
  return str;
}

static ngx_int_t benchmark_timer_ready_check(void *pd) {
  uint64_t required_subs = bench.config->subscribers_per_channel * bench.config->channels;
  if(*bench.shared.subscribers_enqueued == required_subs) {
    char     ready_reply[512];
    assert(*bench.state == NCHAN_BENCHMARK_INITIALIZING);
    *bench.state = NCHAN_BENCHMARK_READY;
    ngx_snprintf((u_char *)ready_reply, 512, "READY\n"
      "{\n"
      "  \"init_time\":                        %T,\n"
      "  \"time\":                             %T,\n"
      "  \"messages_per_channel_per_minute\":  %d,\n"
      "  \"message_padding_bytes\":            %d,\n"
      "  \"channels\":                         %d,\n"
      "  \"subscribers_per_channel\":          %d\n"
      "}\n%Z",
      bench.time.init,
      bench.config->time,
      bench.config->msgs_per_minute,
      bench.config->msg_padding,
      bench.config->channels,
      bench.config->subscribers_per_channel);
    
    benchmark_client_respond(ready_reply);
    bench.timer.ready = NULL;
    return NGX_DONE;
  }
  else {
    return NGX_AGAIN;
  }
}

ngx_int_t nchan_benchmark_initialize_from_ipc(ngx_int_t initiating_worker_slot, nchan_loc_conf_t *cf, time_t init_time, uint32_t id, nchan_benchmark_shared_t *shared_data) {
  DBG("init benchmark via IPC (time %d src %d)", init_time, initiating_worker_slot);
  bench.loc_conf = cf;
  bench.time.init = init_time;
  bench.id = id;
  bench.shared = *shared_data;
  ngx_memzero(&bench.data, sizeof(bench.data));
  hdr_init_nchan_shm(1, 10000000, 3, &bench.data.msg_delivery_latency);
  hdr_init_nchan_shm(1, 10000000, 3, &bench.data.msg_publishing_latency);
  hdr_init_nchan_shm(1, 10000000, 3, &bench.data.subscriber_readiness_latency);
  
  nchan_benchmark_initialize();
  
  return NGX_OK;
}

static ngx_int_t init_command_get_config_value(const char *config, ngx_str_t *cmd, ngx_int_t *val) {
  ngx_str_t find;
  u_char   *cur = cmd->data, *end = cmd->data + cmd->len, *vend;
  find.data = (u_char *)config;
  find.len = strlen(config);
  if(nchan_strscanstr(&cur, &find, end)) {
    if((vend = memchr(cur, ' ', end - cur)) == NULL) {
      vend = end;
    }
    if((*val = ngx_atoi(cur, vend - cur)) == NGX_ERROR) {
      return 0;
    }
    else {
      return 1;
    }
  }
  return 0;
}

void benchmark_controller(subscriber_t *sub, nchan_msg_t *msg) {
  ngx_str_t            cmd = {msg->buf.last - msg->buf.pos, msg->buf.pos};
  ngx_http_request_t  *r = sub->request;
  nchan_loc_conf_t   *cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  
  if(nchan_str_startswith(&cmd, "init")) {
    int       i;
    ngx_int_t val;
    
    if(!ngx_atomic_cmp_set((ngx_atomic_uint_t *)bench.state, NCHAN_BENCHMARK_INACTIVE, NCHAN_BENCHMARK_INITIALIZING)) {
      benchmark_client_respond("ERROR: a benchmark is already initialized");
      return;
    }
    
    DBG("init benchmark");
    benchmark_client_respond("INITIALIZING");
    
    bench.loc_conf = cf;
    *bench.config = cf->benchmark;
    
    if(init_command_get_config_value(" time=", &cmd, &val)) {
      bench.config->time = val;
    }
    if(init_command_get_config_value(" messages_per_channel_per_minute=", &cmd, &val)) {
      bench.config->msgs_per_minute = val;
    }
    if(init_command_get_config_value(" message_padding_bytes=", &cmd, &val)) {
      bench.config->msg_padding = val;
    }
    if(init_command_get_config_value(" channels=", &cmd, &val)) {
      bench.config->channels = val;
    }
    if(init_command_get_config_value(" subscribers_per_channel=", &cmd, &val)) {
      bench.config->subscribers_per_channel = val;
    }
    
    bench.time.init = ngx_time();
    bench.id = rand();
    bench.client = sub;
    ngx_memzero(&bench.data, sizeof(bench.data));
    
    bench.shared.subscribers_enqueued = shm_calloc(nchan_store_memory_shmem, sizeof(ngx_atomic_t), "hdrhistogram subscribers_enqueued count");
    bench.shared.subscribers_dequeued = shm_calloc(nchan_store_memory_shmem, sizeof(ngx_atomic_t), "hdrhistogram subscribers_dequeued count");
    bench.shared.channels = shm_calloc(nchan_store_memory_shmem, sizeof(nchan_benchmark_channel_t) * bench.config->channels, "benchmark channel states");
    hdr_init_nchan_shm(1, 10000000, 3, &bench.data.msg_delivery_latency);
    hdr_init_nchan_shm(1, 10000000, 3, &bench.data.msg_publishing_latency);
    hdr_init_nchan_shm(1, 10000000, 3, &bench.data.subscriber_readiness_latency);
    
    for(i=0; i<bench.config->channels; i++) {
      bench.shared.channels[i].n=i;
      bench.shared.channels[i].msg_count=0;
    }
    
    bench.msgbuf = NULL;
    
    //broadcast workload to other workers
    memstore_ipc_broadcast_benchmark_initialize(&bench);
    
    nchan_benchmark_initialize();
    
    bench.timer.ready = nchan_add_interval_timer(benchmark_timer_ready_check, NULL, 250);
  }
  else if(nchan_strmatch(&cmd, 2, "run", "start")) {
    if(!ngx_atomic_cmp_set((ngx_atomic_uint_t *)bench.state, NCHAN_BENCHMARK_READY, NCHAN_BENCHMARK_RUNNING)) {
      benchmark_client_respond(*bench.state < NCHAN_BENCHMARK_READY ? "ERROR: not ready" : "ERROR: already running");
      return;
    }
    
    bench.time.start = ngx_time();
    benchmark_client_respond("RUNNING");
    
    memstore_ipc_broadcast_benchmark_run();
    nchan_benchmark_run();
    
    bench.timer.running = nchan_add_oneshot_timer(benchmark_timer_running_stop, NULL, bench.config->time * 1000);
  }
  else if(nchan_strmatch(&cmd, 2, "finish", "end")) {
    //benchmark_finish();
  }
  else if(nchan_strmatch(&cmd, 1, "abort"))  {
    if(nchan_benchmark_abort() == NGX_OK) {
      memstore_ipc_broadcast_benchmark_abort();
      benchmark_client_respond("ABORTED");
    }
    else {
      benchmark_client_respond("ERROR: no active benchmark to abort");
    }
  }
  else {
    benchmark_client_respond("ERROR: unknown command");
  }
}

void benchmark_request_cleanup_handler(void *pd) {
  if(nchan_benchmark_abort() == NGX_OK) {
    memstore_ipc_broadcast_benchmark_abort();
  }
  bench.client = NULL;
}

ngx_int_t nchan_benchmark_ws_initialize(ngx_http_request_t *r) {
  nchan_msg_id_t          newest_msgid = NCHAN_NEWEST_MSGID;
  ngx_http_cleanup_t     *cln;
  if(!nchan_detect_websocket_request(r)) {
    return NGX_HTTP_BAD_REQUEST;
  }
  
  if(nchan_benchmark_active()) {
    return nchan_respond_cstring(r, NGX_HTTP_CONFLICT, &NCHAN_CONTENT_TYPE_TEXT_PLAIN, "benchmark already running", 0);
  }
  if(bench.client) {
    return nchan_respond_cstring(r, NGX_HTTP_CONFLICT, &NCHAN_CONTENT_TYPE_TEXT_PLAIN, "benchmark client already running", 0);
  }
  if((cln = ngx_http_cleanup_add(r, 0)) == NULL) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }
  cln->data = NULL;
  cln->handler = benchmark_request_cleanup_handler;
  if((bench.client = websocket_subscriber_create(r, &newest_msgid)) == NULL) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }
  websocket_intercept_published_message(bench.client, &benchmark_controller);
  bench.client->fn->enqueue(bench.client);
  
  
  return NGX_DONE;
}
