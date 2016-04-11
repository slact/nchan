#include <nchan_module.h>
#include <subscribers/common.h>
#include "longpoll.h"
#include "longpoll-private.h"

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:INTERVALPOLL:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:INTERVALPOLL:" fmt, ##arg)
#include <assert.h> 

static       ngx_str_t   sub_name = ngx_string("intervalpoll");

subscriber_t *intervalpoll_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id) {
  subscriber_t         *sub;
  full_subscriber_t    *fsub;
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  sub = longpoll_subscriber_create(r, msg_id);
  
  fsub = (full_subscriber_t *)sub;
  
  fsub->data.act_as_intervalpoll = 1;
  
  sub->name = &sub_name;
  sub->type = INTERVALPOLL;
  
  if(ctx) {
    ctx->subscriber_type = sub->name;
  }
  return sub;
}
