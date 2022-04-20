#include <nchan_module.h>
#include <subscribers/common.h>
#include <util/nchan_bufchainpool.h>
#include "longpoll.h"
#include "longpoll-private.h"
#include "http-multipart-mixed.h"

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:MULTIPART:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:MULTIPART:" fmt, ##arg)
#include <assert.h> 

typedef struct {
  u_char                 boundary[50];
  u_char                *boundary_end;
} multipart_privdata_t;

typedef struct {
  u_char       charbuf[58 + 10*NCHAN_FIXED_MULTITAG_MAX];
  void        *prev;
  void        *next;
} headerbuf_t;

static nchan_bufchain_pool_t *fsub_bcp(full_subscriber_t *fsub) {
  nchan_request_ctx_t            *ctx = ngx_http_get_module_ctx(fsub->sub.request, ngx_nchan_module);
  return ctx->bcp;
}

static void multipart_ensure_headers_sent(full_subscriber_t *fsub) {
  nchan_buf_and_chain_t          *bc;
  
  ngx_http_request_t             *r = fsub->sub.request;
  ngx_http_core_loc_conf_t       *clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
  nchan_request_ctx_t            *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  multipart_privdata_t           *mpd = (multipart_privdata_t *)fsub->privdata;
  
  if(!fsub->data.shook_hands) {
    clcf->chunked_transfer_encoding = 0;
    nchan_request_set_content_type_multipart_boundary_header(r, ctx);
    
    nchan_cleverly_output_headers_only_for_later_response(r);
    
    //set preamble in the request ctx. it would be nicer to store in in the subscriber data, 
    //but that would mean not reusing longpoll's fsub directly
    
    r->header_only = 0;
    r->chunked = 0;
    
    if((bc = nchan_bufchain_pool_reserve(ctx->bcp, 1)) == NULL) {
      ERR("can't reserve bufchain for multipart headers");
      nchan_respond_status(fsub->sub.request, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, NULL, 1);
      return;
    }
    
    ngx_memzero(&bc->buf, sizeof(ngx_buf_t));
    bc->buf.start = mpd->boundary + 2;
    bc->buf.pos = bc->buf.start;
    bc->buf.end = mpd->boundary_end;
    bc->buf.last = bc->buf.end;
    bc->buf.memory = 1;
    bc->buf.last_buf = 0;
    bc->buf.last_in_chain = 1;
    bc->buf.flush = 1;
    
    nchan_output_filter(r, &bc->chain);
    
    fsub->data.shook_hands = 1; 
  }
}
static void *headerbuf_alloc(void *pd) {
  return ngx_palloc((ngx_pool_t *)pd, sizeof(headerbuf_t));
}

static ngx_int_t multipart_respond_message(subscriber_t *sub,  nchan_msg_t *msg) {
  
  full_subscriber_t      *fsub = (full_subscriber_t  *)sub;
  ngx_buf_t              *buf, *msg_buf = &msg->buf, *msgid_buf;
  ngx_int_t               rc;
  nchan_loc_conf_t       *cf = ngx_http_get_module_loc_conf(fsub->sub.request, ngx_nchan_module);
  nchan_request_ctx_t    *ctx = ngx_http_get_module_ctx(fsub->sub.request, ngx_nchan_module);
  ngx_int_t               n;
  nchan_buf_and_chain_t  *bc;
  ngx_chain_t            *chain;
  ngx_file_t             *file_copy;
  multipart_privdata_t   *mpd = (multipart_privdata_t *)fsub->privdata;
  
  headerbuf_t            *headerbuf = nchan_reuse_queue_push(ctx->output_str_queue);
  u_char                 *cur = headerbuf->charbuf;
  
  if(fsub->data.timeout_ev.timer_set) {
    ngx_del_timer(&fsub->data.timeout_ev);
    ngx_add_timer(&fsub->data.timeout_ev, sub->cf->subscriber_timeout * 1000);
  }
  
  //generate the headers
  if(!cf->msg_in_etag_only) {
    //msgtime
    cur = ngx_cpymem(cur, "\r\nLast-Modified: ", sizeof("\r\nLast-Modified: ") - 1);
    cur = ngx_http_time(cur, msg->id.time);
    *cur++ = CR; *cur++ = LF;
    //msgtag
    cur = ngx_cpymem(cur, "Etag: ", sizeof("Etag: ") - 1);
    cur += msgtag_to_strptr(&msg->id, (char *)cur);
    *cur++ = CR; *cur++ = LF;
  }
  else {
    ngx_str_t   *tmp_etag = msgid_to_str(&msg->id);
    cur = ngx_snprintf(cur, 58 + 10*NCHAN_FIXED_MULTITAG_MAX, "\r\nEtag: %V\r\n", tmp_etag);
  }
  
  n=4;
  if(!msg->content_type) {
    //don't need content_type buf'n'chain
    n--;
  }
  if(ngx_buf_size(msg_buf) == 0) {
    //don't need msgbuf
    n --;
  }
  if((bc = nchan_bufchain_pool_reserve(ctx->bcp, n)) == NULL) {
    ERR("can't allocate buf-and-chains for multipart/mixed client output");
    return NGX_ERROR;
  }
  
  chain = &bc->chain;
  msgid_buf = chain->buf;
  
  //message id
  ngx_memzero(chain->buf, sizeof(ngx_buf_t));
  chain->buf->memory = 1;
  chain->buf->start = headerbuf->charbuf;
  chain->buf->pos = headerbuf->charbuf;
  
  //content_type maybe
  if(msg->content_type) {
    chain = chain->next;
    buf = chain->buf;
    
    msgid_buf->last = cur;
    msgid_buf->end = cur;
    
    ngx_memzero(buf, sizeof(ngx_buf_t));
    buf->memory = 1;
    buf->start = cur;
    buf->pos = cur;
    buf->last = ngx_snprintf(cur, 255, "Content-Type: %V\r\n\r\n", msg->content_type);
    buf->end = buf->last;
  }
  else {
    *cur++ = CR; *cur++ = LF;
    msgid_buf->last = cur;
    msgid_buf->end = cur;
  }
  
  //msgbuf
  if(ngx_buf_size(msg_buf) > 0) {
    chain = chain->next;
    buf = chain->buf;
    ngx_memcpy(buf, msg_buf, sizeof(*msg_buf));
    if(msg_buf->file) {
      file_copy = nchan_bufchain_pool_reserve_file(ctx->bcp);
      nchan_msg_buf_open_fd_if_needed(buf, file_copy, NULL);
    }
    buf->last_buf = 0;
    buf->last_in_chain = 0;
    buf->flush = 0;
  }
  
  chain = chain->next;
  buf = chain->buf;
  ngx_memzero(buf, sizeof(ngx_buf_t));
  buf->start = &mpd->boundary[0];
  buf->pos = buf->start;
  buf->end = mpd->boundary_end;
  buf->last = buf->end;
  buf->memory = 1;
  buf->last_buf = 0;
  buf->last_in_chain = 1;
  buf->flush = 1;
  
  ctx->prev_msg_id = fsub->sub.last_msgid;
  update_subscriber_last_msg_id(sub, msg);
  ctx->msg_id = fsub->sub.last_msgid;
  
  multipart_ensure_headers_sent(fsub);
  
  DBG("%p output msg to subscriber", sub);
  
  rc = nchan_output_msg_filter(fsub->sub.request, msg, &bc->chain);
  
  return rc;
}

static ngx_int_t multipart_respond_status(subscriber_t *sub, ngx_int_t status_code, const ngx_str_t *status_line, ngx_chain_t *status_body){
  nchan_buf_and_chain_t    *bc;
  static u_char            *end_boundary=(u_char *)"--\r\n";
  full_subscriber_t        *fsub = (full_subscriber_t  *)sub;
  //nchan_request_ctx_t      *ctx = ngx_http_get_module_ctx(fsub->sub.request, ngx_nchan_module);
  
  if(status_code == NGX_HTTP_NO_CONTENT || (status_code == NGX_HTTP_NOT_MODIFIED && !status_line)) {
    //ignore
    return NGX_OK;
  }
  
  if(fsub->data.shook_hands == 0 && status_code >= 400 && status_code <600) {
    return subscriber_respond_unqueued_status(fsub, status_code, status_line, status_body);
  }
  
  multipart_ensure_headers_sent(fsub);
  
  if((bc = nchan_bufchain_pool_reserve(fsub_bcp(fsub), 1)) == NULL) {
    nchan_respond_status(sub->request, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, NULL, 1);
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

static ngx_int_t multipart_enqueue(subscriber_t *sub) {
  ngx_int_t           rc;
  full_subscriber_t  *fsub = (full_subscriber_t *)sub;
  DBG("%p output status to subscriber", sub);
  rc = longpoll_enqueue(sub);
  fsub->data.finalize_request = 0;
  multipart_ensure_headers_sent(fsub);
  sub->enqueued = 1;
  return rc;
}

static       subscriber_fn_t  multipart_fn_data;
static       subscriber_fn_t *multipart_fn = NULL;

static       ngx_str_t   sub_name = ngx_string("http-multipart");


subscriber_t *http_multipart_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id) {
  subscriber_t         *sub = longpoll_subscriber_create(r, msg_id);
  full_subscriber_t    *fsub = (full_subscriber_t *)sub;
  multipart_privdata_t *multipart_data;
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(fsub->sub.request, ngx_nchan_module);
  
  if(multipart_fn == NULL) {
    multipart_fn = &multipart_fn_data;
    *multipart_fn = *sub->fn;
    multipart_fn->enqueue = multipart_enqueue;
    multipart_fn->respond_message = multipart_respond_message;
    multipart_fn->respond_status = multipart_respond_status;
  }
  
  fsub->data.shook_hands = 0;
  
  fsub->privdata = ngx_palloc(sub->request->pool, sizeof(multipart_privdata_t));
  multipart_data = (multipart_privdata_t *)fsub->privdata;
  multipart_data->boundary_end = ngx_snprintf(multipart_data->boundary, 50, "\r\n--%V", nchan_request_multipart_boundary(fsub->sub.request, ctx));
  
  //header bufs -- unique per response
  ctx->output_str_queue = ngx_palloc(r->pool, sizeof(*ctx->output_str_queue));
  nchan_reuse_queue_init(ctx->output_str_queue, offsetof(headerbuf_t, prev), offsetof(headerbuf_t, next), headerbuf_alloc, NULL, sub->request->pool);
  
  ctx->bcp = ngx_palloc(r->pool, sizeof(nchan_bufchain_pool_t));
  nchan_bufchain_pool_init(ctx->bcp, r->pool);
  
  nchan_subscriber_common_setup(sub, HTTP_MULTIPART, &sub_name, multipart_fn, 1, 0);
  return sub;
}

ngx_int_t nchan_detect_multipart_subscriber_request(ngx_http_request_t *r) {
  ngx_str_t       *accept_header = nchan_get_accept_header_value(r);

  if(accept_header && ngx_strnstr(accept_header->data, "multipart/mixed", accept_header->len)) {
    return 1;
  }
  
  return 0;
}
