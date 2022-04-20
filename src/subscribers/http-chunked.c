#include <nchan_module.h>
#include <subscribers/common.h>
#include "longpoll.h"
#include "longpoll-private.h"
#include "http-chunked.h"

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:CHUNKED:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:CHUNKED:" fmt, ##arg)
#include <assert.h> 

#define CHUNKSIZE_BUF_LEN 20

typedef struct chunksizebuf_s chunksizebuf_t;
struct chunksizebuf_s {
  u_char           chr[CHUNKSIZE_BUF_LEN];
  chunksizebuf_t  *prev;
  chunksizebuf_t  *next;
};

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
    r->chunked = 0;
    r->header_only = 0;
  }
}

static ngx_int_t chunked_respond_message(subscriber_t *sub,  nchan_msg_t *msg) {
  full_subscriber_t      *fsub = (full_subscriber_t  *)sub;
  nchan_request_ctx_t    *ctx = ngx_http_get_module_ctx(fsub->sub.request, ngx_nchan_module);
  chunksizebuf_t         *chunksizebuf = nchan_reuse_queue_push(ctx->output_str_queue);
  u_char                 *chunk_start = &chunksizebuf->chr[0];
  static u_char          *chunk_end=(u_char *)"\r\n";
  ngx_file_t             *file_copy;
  nchan_buf_and_chain_t  *bc = nchan_bufchain_pool_reserve(ctx->bcp, 3);
  ngx_chain_t            *chain;
  ngx_buf_t              *buf, *msg_buf = &msg->buf;
  ngx_int_t               rc;
  
  if(fsub->data.timeout_ev.timer_set) {
    ngx_del_timer(&fsub->data.timeout_ev);
    ngx_add_timer(&fsub->data.timeout_ev, sub->cf->subscriber_timeout * 1000);
  }
  
  ctx->prev_msg_id = fsub->sub.last_msgid;
  update_subscriber_last_msg_id(sub, msg);
  ctx->msg_id = fsub->sub.last_msgid;
  
  if (ngx_buf_size(msg_buf) == 0) {
    //empty messages are skipped, because a zero-length chunk finalizes the request
    return NGX_OK;
  }
  
  //chunk size
  chain = &bc->chain;
  buf = chain->buf;
  ngx_memzero(buf, sizeof(*buf));
  buf->memory = 1;
  buf->start = chunk_start;
  buf->pos = chunk_start;
  buf->end = ngx_snprintf(chunk_start, 15, "%xi\r\n", ngx_buf_size(msg_buf));
  buf->last = buf->end;
  
  //message
  chain = chain->next;
  buf = chain->buf;
  *buf = *msg_buf;
  if(buf->file) {
    file_copy = nchan_bufchain_pool_reserve_file(ctx->bcp);
    nchan_msg_buf_open_fd_if_needed(buf, file_copy, NULL);
  }
  buf->last_buf = 0;
  buf->last_in_chain = 0;
  buf->flush = 0;
  
  //trailing newlines
  chain = chain->next;
  buf = chain->buf;
  ngx_memzero(buf, sizeof(*buf));
  buf->start = chunk_end;
  buf->pos = chunk_end;
  buf->end = chunk_end + 2;
  buf->last = buf->end;
  buf->memory = 1;
  buf->last_buf = 0;
  buf->last_in_chain = 1;
  buf->flush = 1;
  
  chunked_ensure_headers_sent(fsub);
  
  DBG("%p output msg to subscriber", sub);
  
  rc = nchan_output_msg_filter(fsub->sub.request, msg, &bc->chain);
  
  return rc;
}

static ngx_int_t chunked_respond_status(subscriber_t *sub, ngx_int_t status_code, const ngx_str_t *status_line,  ngx_chain_t *status_body){
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
  
  if(status_code == NGX_HTTP_NO_CONTENT || (status_code == NGX_HTTP_NOT_MODIFIED && !status_line)) {
    //ignore
    return NGX_OK;
  }
  
  if(fsub->data.shook_hands == 0 && status_code >= 400 && status_code < 600) {
    return subscriber_respond_unqueued_status(fsub, status_code, status_line, status_body);
  }
  
  chunked_ensure_headers_sent(fsub);
  
  nchan_output_filter(fsub->sub.request, chain);
  
  subscriber_maybe_dequeue_after_status_response(fsub, status_code);

  return NGX_OK;
}

static void *chunksizebuf_alloc(void *pd) {
  return ngx_palloc((ngx_pool_t *)pd, sizeof(chunksizebuf_t));
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
  subscriber_t         *sub = longpoll_subscriber_create(r, msg_id);
  full_subscriber_t    *fsub = (full_subscriber_t *)sub;
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(sub->request, ngx_nchan_module);
  if(chunked_fn == NULL) {
    chunked_fn = &chunked_fn_data;
    *chunked_fn = *sub->fn;
    chunked_fn->enqueue = chunked_enqueue;
    chunked_fn->respond_message = chunked_respond_message;
    chunked_fn->respond_status = chunked_respond_status;
  }
  
  fsub->data.shook_hands = 0;
  
  ctx->output_str_queue = ngx_palloc(r->pool, sizeof(*ctx->output_str_queue));
  nchan_reuse_queue_init(ctx->output_str_queue, offsetof(chunksizebuf_t, prev), offsetof(chunksizebuf_t, next), chunksizebuf_alloc, NULL, r->pool);
  
  ctx->bcp = ngx_palloc(r->pool, sizeof(nchan_bufchain_pool_t));
  nchan_bufchain_pool_init(ctx->bcp, r->pool);
  
  nchan_subscriber_common_setup(sub, HTTP_CHUNKED, &sub_name, chunked_fn, 1, 0);
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
