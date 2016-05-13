#include <nchan_module.h>
#include <subscribers/common.h>
#include <util/nchan_bufchainpool.h>
#include "longpoll.h"
#include "longpoll-private.h"

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:RAWSTREAM:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:RAWSTREAM:" fmt, ##arg)
#include <assert.h> 

static nchan_bufchain_pool_t *fsub_bcp(full_subscriber_t *fsub) {
  nchan_request_ctx_t            *ctx = ngx_http_get_module_ctx(fsub->sub.request, ngx_nchan_module);
  return ctx->bcp;
}

static void rawstream_ensure_headers_sent(full_subscriber_t *fsub) {
  ngx_http_request_t             *r = fsub->sub.request;
  ngx_http_core_loc_conf_t       *clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
  //nchan_request_ctx_t            *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  
  if(!fsub->data.shook_hands) {
    clcf->chunked_transfer_encoding = 0;
    nchan_cleverly_output_headers_only_for_later_response(r);
    fsub->data.shook_hands = 1; 
  }
}

static ngx_int_t rawstream_respond_message(subscriber_t *sub,  nchan_msg_t *msg) {
  
  full_subscriber_t      *fsub = (full_subscriber_t  *)sub;
  ngx_buf_t              *buf, *msg_buf = msg->buf;
  ngx_int_t               rc;
  nchan_loc_conf_t       *cf = ngx_http_get_module_loc_conf(fsub->sub.request, ngx_nchan_module);
  nchan_request_ctx_t    *ctx = ngx_http_get_module_ctx(fsub->sub.request, ngx_nchan_module);
  nchan_buf_and_chain_t  *bc;
  ngx_chain_t            *chain;
  ngx_file_t             *file_copy;
  
  if(fsub->data.timeout_ev.timer_set) {
    ngx_del_timer(&fsub->data.timeout_ev);
    ngx_add_timer(&fsub->data.timeout_ev, sub->cf->subscriber_timeout * 1000);
  }
  
  
  if((bc = nchan_bufchain_pool_reserve(ctx->bcp, 2)) == NULL) {
    ERR("cant allocate buf-and-chains for http-raw-stream client output");
    return NGX_ERROR;
  }
  
  chain = &bc->chain;
  
  //message
  buf = chain->buf;
  *buf = *msg_buf;
  if(buf->file) {
    file_copy = nchan_bufchain_pool_reserve_file(ctx->bcp);
    nchan_msg_buf_open_fd_if_needed(buf, file_copy, NULL);
  }
  buf->last_buf = 0;
  buf->last_in_chain = 0;
  buf->flush = 0;
  
  //separator 
  chain = chain->next;
  buf = chain->buf;
  ngx_memzero(buf, sizeof(ngx_buf_t));
  buf->start = cf->subscriber_http_raw_stream_separator.data;
  buf->pos = buf->start;
  buf->end = buf->start + cf->subscriber_http_raw_stream_separator.len;
  buf->last = buf->end;
  buf->memory = 1;
  buf->last_buf = 0;
  buf->last_in_chain = 1;
  buf->flush = 1;
  
  rawstream_ensure_headers_sent(fsub);
  
  DBG("%p output msg to subscriber", sub);
  
  rc = nchan_output_msg_filter(fsub->sub.request, msg, &bc->chain);
  
  return rc;
}

static ngx_int_t rawstream_respond_status(subscriber_t *sub, ngx_int_t status_code, const ngx_str_t *status_line){
  nchan_buf_and_chain_t    *bc;
  static u_char            *end_boundary=(u_char *)"--\r\n";
  full_subscriber_t        *fsub = (full_subscriber_t  *)sub;
  //nchan_request_ctx_t      *ctx = ngx_http_get_module_ctx(fsub->sub.request, ngx_nchan_module);
  
  if(status_code == NGX_HTTP_NO_CONTENT || (status_code == NGX_HTTP_NOT_MODIFIED && !status_line)) {
    //ignore
    return NGX_OK;
  }
  
  if(fsub->data.shook_hands == 0 && status_code >= 400 && status_code <600) {
    nchan_respond_status(sub->request, status_code, status_line, 1);
    return NGX_OK;
  }
  
  rawstream_ensure_headers_sent(fsub);
  
  if((bc = nchan_bufchain_pool_reserve(fsub_bcp(fsub), 1)) == NULL) {
    nchan_respond_status(sub->request, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, 1);
    return NGX_ERROR;
  }
  
  ngx_memzero(&bc->buf, sizeof(ngx_buf_t));
  bc->buf.memory = 1;
  bc->buf.last_buf = 1;
  bc->buf.last_in_chain = 1;
  bc->buf.flush = 1;
  bc->buf.start = end_boundary;
  bc->buf.pos = end_boundary;
  bc->buf.end = end_boundary + 4;
  bc->buf.last = bc->buf.end;
  
  nchan_output_filter(fsub->sub.request, &bc->chain);
  
  subscriber_maybe_dequeue_after_status_response(fsub, status_code);

  return NGX_OK;
}

static ngx_int_t rawstream_enqueue(subscriber_t *sub) {
  ngx_int_t           rc;
  full_subscriber_t  *fsub = (full_subscriber_t *)sub;
  DBG("%p output status to subscriber", sub);
  rc = longpoll_enqueue(sub);
  fsub->data.finalize_request = 0;
  rawstream_ensure_headers_sent(fsub);
  sub->enqueued = 1;
  return rc;
}

static       subscriber_fn_t  rawstream_fn_data;
static       subscriber_fn_t *rawstream_fn = NULL;

static       ngx_str_t   sub_name = ngx_string("http-raw-stream");


subscriber_t *http_raw_stream_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id) {
  subscriber_t         *sub = longpoll_subscriber_create(r, msg_id);
  full_subscriber_t    *fsub = (full_subscriber_t *)sub;
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(fsub->sub.request, ngx_nchan_module);
  
  if(rawstream_fn == NULL) {
    rawstream_fn = &rawstream_fn_data;
    *rawstream_fn = *sub->fn;
    rawstream_fn->enqueue = rawstream_enqueue;
    rawstream_fn->respond_message = rawstream_respond_message;
    rawstream_fn->respond_status = rawstream_respond_status;
  }
  
  fsub->data.shook_hands = 0;
  r->keepalive=0;
  
  ctx->bcp = ngx_palloc(r->pool, sizeof(nchan_bufchain_pool_t));
  nchan_bufchain_pool_init(ctx->bcp, r->pool);
  
  nchan_subscriber_common_setup(sub, HTTP_RAW_STREAM, &sub_name, rawstream_fn, 0);
  return sub;
}

