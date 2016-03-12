#include <nchan_module.h>
#include <subscribers/common.h>
#include "longpoll.h"
#include "longpoll-private.h"

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:CHUNKED:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:CHUNKED:" fmt, ##arg)
#include <assert.h> 

static void chunked_ensure_headers_sent(full_subscriber_t *fsub) {
  static ngx_str_t         transfer_encoding_header = ngx_string("Transfer-Encoding");
  static ngx_str_t         transfer_encoding = ngx_string("chunked");
  
  ngx_http_request_t             *r = fsub->sub.request;
  ngx_http_core_loc_conf_t       *clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
  if(!fsub->data.shook_hands) {
  
    clcf->chunked_transfer_encoding = 0;
    
    //r->headers_out.content_type.len = content_type.len;
    //r->headers_out.content_type.data = content_type.data;
    
    nchan_add_response_header(r, &transfer_encoding_header, &transfer_encoding);
    
    nchan_cleverly_output_headers_only_for_later_response(r);
    
    fsub->data.shook_hands = 1; 
  }
}

static ngx_int_t chunked_respond_message(subscriber_t *sub,  nchan_msg_t *msg) {
  static u_char           chunk_start[15]; //that's enough
  static u_char          *chunk_end=(u_char *)"\r\n";
  static ngx_file_t       file_copy = {0};
  
  full_subscriber_t      *fsub = (full_subscriber_t  *)sub;
  ngx_buf_t              *msg_buf = msg->buf;
  ngx_int_t               rc;
  nchan_request_ctx_t    *ctx = ngx_http_get_module_ctx(fsub->sub.request, nchan_module);
  
  if (ngx_buf_size(msg_buf) == 0) {
    //empty messages are skipped, because a zero-length chunk finalizes the request
    return NGX_OK;
  }
  
  nchan_buf_and_chain_t   bc[3];
  
  bc[0].chain.buf = &bc[0].buf;
  bc[0].chain.next = &bc[1].chain;
  
  ngx_memzero(&bc[0].buf, sizeof(ngx_buf_t));
  bc[0].buf.memory = 1;
  bc[0].buf.start = chunk_start;
  bc[0].buf.pos = chunk_start;
  bc[0].buf.end = ngx_snprintf(chunk_start, 15, "%xi\r\n", ngx_buf_size(msg_buf));
  bc[0].buf.last = bc[0].buf.end;
  
  bc[1].chain.buf = &bc[1].buf;
  bc[1].chain.next = &bc[2].chain;
  
  ngx_memcpy(&bc[1].buf, msg_buf, sizeof(*msg_buf));
  nchan_msg_buf_open_fd_if_needed(&bc[1].buf, &file_copy, NULL);
  bc[1].buf.last_buf = 0;
  bc[1].buf.last_in_chain = 0;
  bc[1].buf.flush = 0;
  
  bc[2].chain.buf = &bc[2].buf;
  bc[2].chain.next = NULL;
  
  ngx_memzero(&bc[2].buf, sizeof(ngx_buf_t));
  bc[2].buf.start = chunk_end;
  bc[2].buf.pos = chunk_end;
  bc[2].buf.end = chunk_end + 2;
  bc[2].buf.last = bc[2].buf.end;
  bc[2].buf.memory = 1;
  bc[2].buf.last_buf = 0;
  bc[2].buf.last_in_chain = 1;
  bc[2].buf.flush = 1;
  
  ctx->prev_msg_id = fsub->sub.last_msgid;
  update_subscriber_last_msg_id(sub, msg);
  ctx->msg_id = fsub->sub.last_msgid;
  
  chunked_ensure_headers_sent(fsub);
  
  DBG("%p output msg to subscriber", sub);
  
  rc = nchan_output_filter(fsub->sub.request, &bc[0].chain);
  
  return rc;
}

static void empty_handler(void) {}

static ngx_int_t chunked_respond_status(subscriber_t *sub, ngx_int_t status_code, const ngx_str_t *status_line){
  nchan_buf_and_chain_t     bc;
  ngx_chain_t              *chain = NULL;
  
  full_subscriber_t        *fsub = (full_subscriber_t  *)sub;
  
  if(chain == NULL) {
    bc.chain.buf=&bc.buf;
    bc.chain.next=NULL;
    ngx_memzero(&bc.buf, sizeof(ngx_buf_t));
    bc.buf.last_buf = 1;
    bc.buf.last_in_chain = 1;
    bc.buf.flush = 1;
    bc.buf.memory = 1;
    chain = &bc.chain;
  }
  
  bc.buf.start = (u_char *)"0\r\n\r\n";
  bc.buf.end = bc.buf.start + 5;
  bc.buf.pos = bc.buf.start;
  bc.buf.last = bc.buf.end;
  
  if(status_code == NGX_HTTP_NO_CONTENT || status_code == NGX_HTTP_NOT_MODIFIED) {
    //ignore
    return NGX_OK;
  }
  
  if(fsub->data.shook_hands == 0 && status_code >= 400 && status_code <600) {
    nchan_respond_status(sub->request, status_code, status_line, 1);
    return NGX_OK;
  }
  
  chunked_ensure_headers_sent(fsub);
  
  nchan_output_filter(fsub->sub.request, chain);
  
  if(status_code >=400 && status_code <599) {
    fsub->data.cln->handler = (ngx_http_cleanup_pt )empty_handler;
    fsub->sub.request->keepalive=0;
    fsub->data.finalize_request=1;
    sub->fn->dequeue(sub);
  }

  return NGX_OK;
}

static ngx_int_t chunked_enqueue(subscriber_t *sub) {
  ngx_int_t           rc;
  full_subscriber_t  *fsub = (full_subscriber_t *)sub;
  DBG("%p output status to subscriber", sub);
  rc = longpoll_enqueue(sub);
  fsub->data.finalize_request = 0;
  chunked_ensure_headers_sent(fsub);
  sub->enqueued = 1;
  return rc;
}

static       subscriber_fn_t  chunked_fn_data;
static       subscriber_fn_t *chunked_fn = NULL;

static       ngx_str_t   sub_name = ngx_string("http-chunked");

subscriber_t *http_chunked_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id) {
  subscriber_t         *sub;
  full_subscriber_t    *fsub;
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(r, nchan_module);
  sub = longpoll_subscriber_create(r, msg_id);
  
  if(chunked_fn == NULL) {
    chunked_fn = &chunked_fn_data;
    *chunked_fn = *sub->fn;
    chunked_fn->enqueue = chunked_enqueue;
    chunked_fn->respond_message = chunked_respond_message;
    chunked_fn->respond_status = chunked_respond_status;
  }
  
  fsub = (full_subscriber_t *)sub;
  
  sub->fn = chunked_fn;
  sub->name = &sub_name;
  sub->type = HTTP_CHUNKED;
  
  sub->dequeue_after_response = 0;
  
  fsub->data.shook_hands = 0;
  
  DBG("%p create subscriber", sub);
  
  if(ctx) {
    ctx->subscriber_type = sub->name;
  }
  return sub;
}



ngx_int_t nchan_detect_chunked_subscriber_request(ngx_http_request_t *r) {
  static ngx_str_t   TE_HEADER = ngx_string("TE");
  ngx_str_t         *tmp;
  u_char            *cur, *last;
  
  if(r->method != NGX_HTTP_GET) {
    return 0;
  }
  
  if((tmp = nchan_get_header_value(r, TE_HEADER)) != NULL) {
    last = tmp->data + tmp->len;
    cur = ngx_strlcasestrn(tmp->data, last, (u_char *)"chunked", 7 - 1);
    
    if(cur == NULL) {
      return 0;
    }
    
    //see if there's a qvalue
    cur += 7;
    if((cur + 1 <= last) && cur[0]==' ') { 
      //no qvalue. assume non-zero, meaning it's legit
      return 1;
    }
    else if((cur + 4) < last) {
      //maybe there is...
      if(cur[0]==';' && cur[1]=='q' && cur[2]=='=') {
        //parse the freaking qvalue
        cur += 3;
        ngx_int_t qval_fp;
        qval_fp = ngx_atofp(cur, last - cur, 2);
        if(qval_fp == NGX_ERROR) {
          DBG("invalid qval. reject.");
          return 0;
        }
        else if(qval_fp > 0) {
          //got nonzero qval. accept
          return 1;
        }
        else {
          //qval=0. reject
          return 0;
        }
      }
      else {
        //we're looking at  "chunkedsomething", not "chunked;q=<...>". reject.
        return 0;
      }
    }
    else if (cur == last){
      //last thing in the header. "chunked". accept
      return 1;
    }
    else {
      //too small to have a qvalue, not followed by a space. must be "chunkedsomething"
      return 0;
    }
  }
  else return 0;
}
