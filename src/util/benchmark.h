#include <nchan_module.h>

typedef struct nchan_benchmark_s nchan_benchmark_t;
struct nchan_benchmark_s {
  ngx_http_request_t *initiating_request;
  nchan_benchmark_t  *parent;
  time_t              time_start;
  time_t              time_end;
  ngx_int_t           msg_rate;
  size_t              msg_padding;
  ngx_int_t           channels;
  ngx_int_t           subs_per_channel;
  subscriber_t      **subs;
}; //nchan_benchmark_t
