#include <nchan_module.h>
#include <subscribers/common.h>
#include "longpoll.h"
#include "longpoll-private.h"

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:MULTIPART:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:MULTIPART:" fmt, ##arg)
#include <assert.h> 

static void multipart_ensure_headers_sent(full_subscriber_t *fsub) {
  static const ngx_str_t   everything_ok = ngx_string("200 OK");
  
  u_char                          cbuf[100];
  u_char                         *cur;
  
  nchan_buf_and_chain_t           bc;
  
  ngx_http_request_t             *r = fsub->sub.request;
  ngx_http_core_loc_conf_t       *clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
  nchan_request_ctx_t            *ctx = ngx_http_get_module_ctx(r, nchan_module);
  if(!fsub->data.shook_hands) {
  
    clcf->chunked_transfer_encoding = 0;
    
    cur = ngx_snprintf(cbuf, 100, "multipart/mixed; boundary=%V", ctx->multipart_boundary);
    r->headers_out.content_type.data = cbuf;
    r->headers_out.content_type.len = cur - cbuf;
    
    nchan_cleverly_output_headers_only_for_later_response(r);
    
    //set preamble in the request ctx. it would be nicer to store in in the subscriber data, 
    //but that would mean not reusing longpoll's fsub directly
    
    bc.chain.buf = &bc.buf;
    bc.chain.next = NULL;
    
    ngx_memzero(&bc.buf, sizeof(ngx_buf_t));
    bc.buf.start = cbuf;
    bc.buf.pos = cbuf;
    bc.buf.end = ngx_snprintf(cbuf, 50, ("--%V"), ctx->multipart_boundary);
    bc.buf.last = bc.buf.end;
    bc.buf.memory = 1;
    bc.buf.last_buf = 0;
    bc.buf.last_in_chain = 1;
    bc.buf.flush = 1;
    
    nchan_output_filter(r, &bc.chain);
    
    fsub->data.shook_hands = 1; 
  }
}

static ngx_int_t multipart_respond_message(subscriber_t *sub,  nchan_msg_t *msg) {
  u_char                  headerbuf[58 + 10*NCHAN_MULTITAG_MAX];
  u_char                  boundary[50];
  
  full_subscriber_t      *fsub = (full_subscriber_t  *)sub;
  ngx_buf_t              *msg_buf = msg->buf;
  ngx_int_t               rc;
  nchan_loc_conf_t       *cf = ngx_http_get_module_loc_conf(fsub->sub.request, nchan_module);
  
  nchan_request_ctx_t    *ctx = ngx_http_get_module_ctx(fsub->sub.request, nchan_module);
  
  u_char                 *cur=headerbuf;
  nchan_buf_and_chain_t   bc[4];
  
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
    cur = ngx_snprintf(cur, 58 + 10*NCHAN_MULTITAG_MAX, "\r\nEtag: %V\r\n", tmp_etag);
  }
  
  //content-type maybe
  bc[0].chain.buf = &bc[0].buf;
  
  ngx_memzero(&bc[0].buf, sizeof(ngx_buf_t));
  bc[0].buf.memory = 1;
  bc[0].buf.start = headerbuf;
  bc[0].buf.pos = headerbuf;
  bc[0].buf.last = cur;
  bc[0].buf.end = cur;

  if(msg->content_type.len > 0) {
    bc[0].buf.last = cur;
    bc[0].buf.end = cur;
    bc[0].chain.next = &bc[1].chain;
    
    ngx_memzero(&bc[1].buf, sizeof(ngx_buf_t));
    bc[1].buf.memory = 1;
    bc[1].buf.start = headerbuf;
    bc[1].buf.pos = headerbuf;
    bc[1].buf.last = ngx_snprintf(headerbuf, 255, "Content-Type: %V\r\n\r\n", &msg->content_type);
    bc[1].buf.end = bc[1].buf.last;
    
    bc[1].chain.buf = &bc[1].buf;
    bc[1].chain.next = ngx_buf_size(msg_buf) > 0 ? &bc[2].chain : &bc[3].chain;
  }
  else {
    *cur++ = CR; *cur++ = LF;
    bc[0].buf.last = cur;
    bc[0].buf.end = cur;
    bc[0].chain.next = ngx_buf_size(msg_buf) > 0 ? &bc[2].chain : &bc[3].chain;;
  }
  
  if(ngx_buf_size(msg_buf) > 0) {  
    bc[2].chain.buf = &bc[2].buf;
    bc[2].chain.next = &bc[3].chain;
    
    ngx_memcpy(&bc[2].buf, msg_buf, sizeof(*msg_buf));
    bc[2].buf.last_buf = 0;
    bc[2].buf.last_in_chain = 0;
    bc[2].buf.flush = 0;
  }
  
  bc[3].chain.buf = &bc[3].buf;
  bc[3].chain.next = NULL;
  
  ngx_memzero(&bc[3].buf, sizeof(ngx_buf_t));
  bc[3].buf.start = boundary;
  bc[3].buf.pos = boundary;
  bc[3].buf.end = ngx_snprintf(boundary, 50, "\r\n--%V", ctx->multipart_boundary);
  bc[3].buf.last = bc[3].buf.end;
  bc[3].buf.memory = 1;
  bc[3].buf.last_buf = 0;
  bc[3].buf.last_in_chain = 1;
  bc[3].buf.flush = 1;
  
  ctx->prev_msg_id = fsub->sub.last_msgid;
  update_subscriber_last_msg_id(sub, msg);
  ctx->msg_id = fsub->sub.last_msgid;
  
  multipart_ensure_headers_sent(fsub);
  
  DBG("%p output msg to subscriber", sub);
  
  rc = nchan_output_filter(fsub->sub.request, &bc[0].chain);
  
  return rc;
}

static void empty_handler(void) {}

static ngx_int_t multipart_respond_status(subscriber_t *sub, ngx_int_t status_code, const ngx_str_t *status_line){
  nchan_buf_and_chain_t     bc;
  static u_char            *end_boundary=(u_char *)"--\r\n";
  
  full_subscriber_t        *fsub = (full_subscriber_t  *)sub;
  //nchan_request_ctx_t      *ctx = ngx_http_get_module_ctx(fsub->sub.request, nchan_module);
  
  if(status_code == NGX_HTTP_NO_CONTENT || status_code == NGX_HTTP_NOT_MODIFIED) {
    //ignore
    return NGX_OK;
  }
  
  if(fsub->data.shook_hands == 0 && status_code >= 400 && status_code <600) {
    nchan_respond_status(sub->request, status_code, status_line, 1);
    return NGX_OK;
  }
  
  multipart_ensure_headers_sent(fsub);
  bc.chain.buf = &bc.buf;
  bc.chain.next = NULL;
  
  
  ngx_memzero(&bc.buf, sizeof(ngx_buf_t));
  bc.buf.memory = 1;
  bc.buf.last_buf = 1;
  bc.buf.last_in_chain = 1;
  bc.buf.flush = 1;
  bc.buf.start = end_boundary;
  bc.buf.pos = end_boundary;
  bc.buf.end = end_boundary + 4;
  bc.buf.last = bc.buf.end;
  
  nchan_output_filter(fsub->sub.request, &bc.chain);
  
  if(status_code >=400 && status_code <599) {
    fsub->data.cln->handler = (ngx_http_cleanup_pt )empty_handler;
    fsub->sub.request->keepalive=0;
    fsub->data.finalize_request=1;
    sub->fn->dequeue(sub);
  }

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


static void generate_random_boundary(u_char *buf, int sz) {
  //use the shitty-ass LFSR-based ngx_random. we're not looking for cryptographic randomness, 
  //just something unlikely
  static u_char   itoa64[] ="./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  int             i;
  u_char         *p = buf;
  
  for(i=0; i < sz; i++) {
    *p++ = itoa64[ngx_random() % 64];
  }
}

subscriber_t *http_multipart_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id) {
  subscriber_t         *sub;
  full_subscriber_t    *fsub;
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(r, nchan_module);
  sub = longpoll_subscriber_create(r, msg_id);
  
  if(multipart_fn == NULL) {
    multipart_fn = &multipart_fn_data;
    *multipart_fn = *sub->fn;
    multipart_fn->enqueue = multipart_enqueue;
    multipart_fn->respond_message = multipart_respond_message;
    multipart_fn->respond_status = multipart_respond_status;
  }
  
  fsub = (full_subscriber_t *)sub;
  
  sub->fn = multipart_fn;
  sub->name = &sub_name;
  sub->type = HTTP_MULTIPART;
  
  sub->dequeue_after_response = 0;
  
  fsub->data.shook_hands = 0;
  
  DBG("%p create subscriber", sub);
  
  if(ctx) {
    ctx->subscriber_type = sub->name;
    if((ctx->multipart_boundary = ngx_palloc(r->pool, sizeof(ngx_str_t) + 32)) == NULL) {
      ERR("unable to allocate multipart boundary");
    }
    ctx->multipart_boundary->data=(u_char *)&ctx->multipart_boundary[1];
    ctx->multipart_boundary->len = 32;
    generate_random_boundary(ctx->multipart_boundary->data, 32);
  }
  
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
