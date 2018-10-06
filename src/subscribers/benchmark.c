#include <nchan_module.h>
#include <subscribers/common.h>
#include "internal.h"
#include "benchmark.h"
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:BENCHMARK:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:BENCHMARK:" fmt, ##arg)

typedef struct {
  subscriber_t        *sub;
  nchan_benchmark_t   *bench;
} sub_data_t;

static ngx_int_t sub_enqueue(ngx_int_t status, void *ptr, sub_data_t *d) {
  if(nchan_benchmark_active()) {
    ngx_atomic_fetch_add(d->bench->shared.subscribers_enqueued, 1);
  }
  nchan_update_stub_status(subscribers, 1); //needs to be done manually for INTERNAL subs
  return NGX_OK;
}

static ngx_int_t sub_dequeue(ngx_int_t status, void *ptr, sub_data_t* d) {
  if(nchan_benchmark_active()) {
    ngx_atomic_fetch_add(d->bench->shared.subscribers_dequeued, 1);
  }
  nchan_update_stub_status(subscribers, -1); //needs to be done manually for INTERNAL subs
  return NGX_OK;
}

static ngx_int_t sub_respond_message(ngx_int_t status, void *ptr, sub_data_t* d) {
  nchan_msg_t *msg = ptr;
  uint64_t msec = nchan_benchmark_message_delivery_msec(msg);
  if(nchan_benchmark_active()) {
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
  nchan_loc_conf_t           *cf = bench->loc_conf;
  struct timeval tv;
  
  sub = internal_subscriber_create_init(&sub_name, cf, sizeof(*d), (void **)&d, (callback_pt )sub_enqueue, (callback_pt )sub_dequeue, (callback_pt )sub_respond_message, (callback_pt )sub_respond_status, (callback_pt )sub_respond_notice, NULL);
  
  sub->last_msgid = newest_msgid;
  sub->destroy_after_dequeue = 1;
  d->sub = sub;
  d->bench = bench;
  ngx_gettimeofday(&tv);
  
  
  DBG("%p benchmark subscriber created with privdata %p", sub, d);
  return sub;
}
