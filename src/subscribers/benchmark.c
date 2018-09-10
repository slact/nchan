#include <nchan_module.h>
#include <subscribers/common.h>
#include "internal.h"
#include "benchmark.h"
#include <util/benchmark.h>
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:BENCHMARK:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:BENCHMARK:" fmt, ##arg)

typedef struct {
  subscriber_t        *sub;
  nchan_benchmark_t   *bench;
  uint64_t             time_start;
} sub_data_t;

static ngx_int_t sub_enqueue(ngx_int_t status, void *ptr, sub_data_t *d) {
  struct timeval tv;
  uint64_t t1;
  ngx_gettimeofday(&tv);
  if(nchan_benchmark_running()) {
    t1 = (tv.tv_sec - d->bench->time_start) * (uint64_t)1000000 + tv.tv_usec;
    hdr_record_value(d->bench->data.subscriber_readiness_latency, t1 - d->time_start);
    ngx_atomic_fetch_add(d->bench->shared_data.subscribers_enqueued, 1);
  }
  return NGX_OK;
}

static ngx_int_t sub_dequeue(ngx_int_t status, void *ptr, sub_data_t* d) {
  if(nchan_benchmark_running()) {
    ngx_atomic_fetch_add(d->bench->shared_data.subscribers_dequeued, 1);
  }
  return NGX_OK;
}

static ngx_int_t sub_respond_message(ngx_int_t status, void *ptr, sub_data_t* d) {
  nchan_msg_t *msg = ptr;
  uint64_t msec = nchan_benchmark_message_delivery_msec(msg);
  if(nchan_benchmark_running()) {
    hdr_record_value(d->bench->data.msg_delivery_latency, msec);
    d->bench->data.msg_received++;
  }
  return NGX_OK;
}

static ngx_int_t sub_respond_status(ngx_int_t status, void *ptr, sub_data_t *d) {
  return NGX_OK;
}

static ngx_int_t sub_respond_notice(ngx_int_t notice, void *ptr, sub_data_t *d) {
  return NGX_OK;
}

static ngx_str_t  sub_name = ngx_string("benchmark");

subscriber_t *benchmark_subscriber_create(nchan_benchmark_t *bench) {
  static  nchan_msg_id_t      newest_msgid = NCHAN_NEWEST_MSGID;
  sub_data_t                 *d;
  subscriber_t               *sub;
  nchan_loc_conf_t           *cf = bench->cf;
  struct timeval tv;
  
  sub = internal_subscriber_create_init(&sub_name, cf, sizeof(*d), (void **)&d, (callback_pt )sub_enqueue, (callback_pt )sub_dequeue, (callback_pt )sub_respond_message, (callback_pt )sub_respond_status, (callback_pt )sub_respond_notice, NULL);
  
  sub->last_msgid = newest_msgid;
  sub->destroy_after_dequeue = 1;
  d->sub = sub;
  d->bench = bench;
  ngx_gettimeofday(&tv);
  
  d->time_start = (tv.tv_sec - d->bench->time_start) * (uint64_t)1000000 + tv.tv_usec;
  
  DBG("%p benchmark subscriber created with privdata %p", sub, d);
  return sub;
}
