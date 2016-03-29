#include <nchan_module.h>
#include <subscribers/common.h>
#include "longpoll.h"
#include "longpoll-private.h"

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:MULTIPART:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:MULTIPART:" fmt, ##arg)
#include <assert.h> 

typedef struct {
  u_char                boundary[50];
  u_char               *boundary_end;
  nchan_reuse_queue_t   hq;
} multipart_privdata_t;


typedef struct {
  u_char       charbuf[58 + 10*NCHAN_FIXED_MULTITAG_MAX];
  void        *prev;
  void        *next;
} headerbuf_t;

static void multipart_ensure_headers_sent(full_subscriber_t *fsub) {
  nchan_buf_and_chain_t          *bc = nchan_bufchain_pool_reserve(1, NULL);
  
  ngx_http_request_t             *r = fsub->sub.request;
  ngx_http_core_loc_conf_t       *clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
  nchan_request_ctx_t            *ctx = ngx_http_get_module_ctx(r, nchan_module);
  multipart_privdata_t           *multipart_data = (multipart_privdata_t *)fsub->privdata;
  
  if(!fsub->data.shook_hands) {
  
    clcf->chunked_transfer_encoding = 0;
    nchan_request_set_content_type_multipart_boundary_header(r, ctx);
    
    nchan_cleverly_output_headers_only_for_later_response(r);
    
    //set preamble in the request ctx. it would be nicer to store in in the subscriber data, 
    //but that would mean not reusing longpoll's fsub directly
    
    bc->chain.buf = &bc->buf;
    bc->chain.next = NULL;
    
    ngx_memzero(&bc->buf, sizeof(ngx_buf_t));
    bc->buf.start = multipart_data->boundary + 2;
    bc->buf.pos = bc->buf.start;
    bc->buf.end = multipart_data->boundary_end;
    bc->buf.last = bc->buf.end;
    bc->buf.memory = 1;
    bc->buf.last_buf = 0;
    bc->buf.last_in_chain = 1;
    bc->buf.flush = 1;
    
    nchan_output_bufchainpooled_filter(r, &bc->chain, NULL, NULL);
    
    fsub->data.shook_hands = 1; 
  }
}
static void *headerbuf_alloc(void *pd) {
  return ngx_palloc(((ngx_http_request_t *)pd)->pool, sizeof(headerbuf_t));
}

static ngx_int_t headerbuf_pop(ngx_int_t code, void *_, void *pd) {
  full_subscriber_t      *fsub = (full_subscriber_t *)pd;
  multipart_privdata_t   *multipart_data;
  
  multipart_data = (multipart_privdata_t *)fsub->privdata;
  nchan_reuse_queue_pop(&multipart_data->hq);
  
  return NGX_OK;
}

static ngx_int_t multipart_respond_message(subscriber_t *sub,  nchan_msg_t *msg) {
  
  full_subscriber_t      *fsub = (full_subscriber_t  *)sub;
  ngx_buf_t              *msg_buf = msg->buf;
  ngx_int_t               rc;
  nchan_loc_conf_t       *cf = ngx_http_get_module_loc_conf(fsub->sub.request, nchan_module);
  nchan_request_ctx_t    *ctx = ngx_http_get_module_ctx(fsub->sub.request, nchan_module);
  ngx_int_t               i, n;
  nchan_buf_and_chain_t  *bc;
  ngx_file_t             *file_copy;
  multipart_privdata_t   *multipart_data = (multipart_privdata_t *)fsub->privdata;
  
  headerbuf_t            *headerbuf=nchan_reuse_queue_push(&multipart_data->hq);
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
  if(msg->content_type.len == 0) {
    //don't need content_type buf'n'chain
    n--;
  }
  if(ngx_buf_size(msg_buf) == 0) {
    //don't need msgbuf
    n --;
  }
  if((bc = nchan_bufchain_pool_reserve(n, &file_copy)) == NULL) {
    ERR("cant allocate buf-and-chains for multipart/mixed client output");
    return NGX_ERROR;
  }
  i=0;
  
  //message id
  ngx_memzero(&bc[i].buf, sizeof(ngx_buf_t));
  bc[i].buf.memory = 1;
  bc[i].buf.start = headerbuf->charbuf;
  bc[i].buf.pos = headerbuf->charbuf;
  
  
  //content_type maybe
  if(msg->content_type.len > 0) {
    i++;
    bc[i-1].buf.last = cur;
    bc[i-1].buf.end = cur;
    
    ngx_memzero(&bc[i].buf, sizeof(ngx_buf_t));
    bc[i].buf.memory = 1;
    bc[i].buf.start = cur;
    bc[i].buf.pos = cur;
    bc[i].buf.last = ngx_snprintf(cur, 255, "Content-Type: %V\r\n\r\n", &msg->content_type);
    bc[i].buf.end = bc[i].buf.last;
  }
  else {
    *cur++ = CR; *cur++ = LF;
    bc[i].buf.last = cur;
    bc[i].buf.end = cur;
  }
  
  
  //msgbuf
  if(ngx_buf_size(msg_buf) > 0) {
    i++;
    ngx_memcpy(&bc[i].buf, msg_buf, sizeof(*msg_buf));
    nchan_msg_buf_open_fd_if_needed(&bc[i].buf, file_copy, NULL);
    bc[i].buf.last_buf = 0;
    bc[i].buf.last_in_chain = 0;
    bc[i].buf.flush = 0;
  }
  
  
  i++;
  
  ngx_memzero(&bc[i].buf, sizeof(ngx_buf_t));
  bc[i].buf.start = &multipart_data->boundary[0];
  bc[i].buf.pos = bc[i].buf.start;
  bc[i].buf.end = multipart_data->boundary_end;
  bc[i].buf.last = bc[i].buf.end;
  bc[i].buf.memory = 1;
  bc[i].buf.last_buf = 0;
  bc[i].buf.last_in_chain = 1;
  bc[i].buf.flush = 1;
  
  ctx->prev_msg_id = fsub->sub.last_msgid;
  update_subscriber_last_msg_id(sub, msg);
  ctx->msg_id = fsub->sub.last_msgid;
  
  multipart_ensure_headers_sent(fsub);
  
  DBG("%p output msg to subscriber", sub);
  
  rc = nchan_output_bufchainpooled_filter(fsub->sub.request, &bc[0].chain, headerbuf_pop, fsub);
  
  return rc;
}

static ngx_int_t multipart_respond_status(subscriber_t *sub, ngx_int_t status_code, const ngx_str_t *status_line){
  nchan_buf_and_chain_t    *bc = nchan_bufchain_pool_reserve(1, NULL);
  static u_char            *end_boundary=(u_char *)"--\r\n";
  full_subscriber_t        *fsub = (full_subscriber_t  *)sub;
  //nchan_request_ctx_t      *ctx = ngx_http_get_module_ctx(fsub->sub.request, nchan_module);
  
  if(bc == NULL) {
    nchan_respond_status(sub->request, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, 1);
    return NGX_ERROR;
  }
  
  if(status_code == NGX_HTTP_NO_CONTENT || (status_code == NGX_HTTP_NOT_MODIFIED && !status_line)) {
    //ignore
    return NGX_OK;
  }
  
  if(fsub->data.shook_hands == 0 && status_code >= 400 && status_code <600) {
    nchan_respond_status(sub->request, status_code, status_line, 1);
    return NGX_OK;
  }
  
  multipart_ensure_headers_sent(fsub);
  bc->chain.buf = &bc->buf;
  bc->chain.next = NULL;
  
  
  ngx_memzero(&bc->buf, sizeof(ngx_buf_t));
  bc->buf.memory = 1;
  bc->buf.last_buf = 1;
  bc->buf.last_in_chain = 1;
  bc->buf.flush = 1;
  bc->buf.start = end_boundary;
  bc->buf.pos = end_boundary;
  bc->buf.end = end_boundary + 4;
  bc->buf.last = bc->buf.end;
  
  nchan_output_bufchainpooled_filter(fsub->sub.request, &bc->chain, NULL, NULL);
  
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
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(fsub->sub.request, nchan_module);
  
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
  nchan_reuse_queue_init(&multipart_data->hq, offsetof(headerbuf_t, prev), offsetof(headerbuf_t, next), headerbuf_alloc, NULL, sub->request);
  
  nchan_subscriber_common_setup(sub, HTTP_MULTIPART, &sub_name, multipart_fn, 0);
  return sub;
}



ngx_int_t nchan_detect_multipart_subscriber_request(ngx_http_request_t *r) {
  ngx_str_t       *accept_header;
  if(r->headers_in.accept == NULL) {
    return 0;
  }
  accept_header = &r->headers_in.accept->value;

  if(ngx_strnstr(accept_header->data, "multipart/mixed", accept_header->len)) {
    return 1;
  }
  
  return 0;
}
