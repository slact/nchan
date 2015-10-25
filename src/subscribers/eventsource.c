#include <nchan_module.h>
#include "longpoll.h"
#include "longpoll-private.h"
//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:EVENTSOURCE:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:EVENTSOURCE:" fmt, ##arg)
#include <assert.h> 

#define NGX_DEFAULT_LINEBREAK_POOL_SIZE 1024

void memstore_fakeprocess_push(ngx_int_t slot);
void memstore_fakeprocess_push_random(void);
void memstore_fakeprocess_pop();
ngx_int_t memstore_slot();

static ngx_inline void ngx_init_set_membuf(ngx_buf_t *buf, u_char *start, u_char *end) {
  ngx_memzero(buf, sizeof(*buf));
  buf->start = start;
  buf->pos = start;
  buf->end = end;
  buf->last = end;
  buf->memory = 1;
}

static void es_ensure_headers_sent(full_subscriber_t *fsub) {
  static const ngx_str_t   content_type = ngx_string("text/event-stream; charset=utf8");
  static const ngx_str_t   everything_ok = ngx_string("200 OK");
  static const ngx_str_t   hello = ngx_string(": hi\n");
  
  ngx_http_request_t             *r = fsub->data.request;
  ngx_http_core_loc_conf_t       *clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
  nchan_buf_and_chain_t           bc;
  if(!fsub->data.shook_hands) {
  
    clcf->chunked_transfer_encoding = 0;
    
    r->headers_out.status=102; //fake it to fool the chunking module (mostly);
    r->headers_out.status_line = everything_ok; //but in reality, we're returning a 200
    
    r->headers_out.content_type.len = content_type.len;
    r->headers_out.content_type.data = content_type.data;
    r->headers_out.content_length_n = -1;
    r->header_only = 1;
    //send headers
    
    ngx_http_send_header(r);
    
    //send a ":hi" comment
    ngx_init_set_membuf(&bc.buf, hello.data, hello.data + hello.len);
    bc.chain.buf = &bc.buf;
    bc.buf.last_buf=1;
    bc.chain.next = NULL;
    nchan_output_filter(fsub->data.request, &bc.chain);
    
    fsub->data.shook_hands = 1; 
  }
}

static ngx_int_t es_respond_message(subscriber_t *sub,  nchan_msg_t *msg) {
  static ngx_str_t        data_prefix=ngx_string("data: ");
  static ngx_buf_t        data_prefix_buf;
  static ngx_str_t        terminal_newlines=ngx_string("\n\n");
  full_subscriber_t      *fsub = (full_subscriber_t  *)sub;
  
  ngx_buf_t              *buf = msg->buf;
  ngx_buf_t              *databuf;
  ngx_pool_t             *pool;
  nchan_buf_and_chain_t  *bc;
  u_char                 *cur, *last;
  size_t                  len;
  ngx_chain_t            *first_link = NULL, *last_link = NULL;
  ngx_int_t               rc;

  es_ensure_headers_sent(fsub);
  
  ngx_init_set_membuf(&data_prefix_buf, data_prefix.data, data_prefix.data + data_prefix.len);
  
  DBG("%p output msg to subscriber", sub);
  
  pool = ngx_create_pool(NGX_DEFAULT_LINEBREAK_POOL_SIZE, ngx_cycle->log);
  assert(pool);
  
  if(!buf->in_file) {
    cur = buf->start;
    last = buf->end;
    do {
      bc = ngx_palloc(pool, sizeof(*bc)*2);
      assert(bc);
      bc[0].chain.next = &bc[1].chain;
      bc[0].chain.buf = &bc[0].buf;
      ngx_memcpy(&bc[0].buf, &data_prefix_buf, sizeof(data_prefix_buf));
      
      databuf = &bc[1].buf;
      bc[1].chain.buf=databuf;
      ngx_memcpy(databuf, last_link ? last_link->buf : buf, sizeof(*databuf));
      databuf->last_buf = 0;
      databuf->start = cur;
      databuf->pos = cur;
      
      if(first_link == NULL) {
        first_link = &bc[0].chain;
      }
      if(last_link != NULL) {
        last_link->next = &bc[0].chain;
      }
      last_link = &bc[1].chain;
      
      cur = ngx_strlchr(cur, last, '\n');
      if(cur == NULL) {
        //sweet, no newlines!
        //let's get out of this hellish loop
        databuf->end = last;
        databuf->last = last;
        cur = last + 1;
      }
      else {
        cur++; //include the newline
        databuf->end = cur;
        databuf->last = cur;
      }
      //no empty buffers please
      if(ngx_buf_size(databuf) == 0) {
        last_link = &bc[0].chain;
      }
      
    } while(cur <= last);
  } 
  else {
    //don't know how to do files yet
    assert(0);
  }
  
  
  
  //now 2 newlines at the end
  bc = ngx_palloc(pool, sizeof(*bc));
  last_link->next=&bc->chain;
  ngx_init_set_membuf(&bc->buf, terminal_newlines.data, terminal_newlines.data + terminal_newlines.len);
  bc->buf.last_buf = 1;
  
  bc->chain.next = NULL;
  bc->chain.buf = &bc->buf;
  
  last_link = &bc->chain;
  
  //okay, this crazy data chain is finished. now how about the mesage tag?
  len = 10 + 2*NGX_INT_T_LEN;
  bc = ngx_palloc(pool, sizeof(*bc) + len);
  ngx_memzero(&bc->buf, sizeof(bc->buf));
  cur = (u_char *)&bc[1];
  ngx_init_set_membuf(&bc->buf, cur, ngx_snprintf(cur, len, "id: %i:%i\n", msg->message_time, msg->message_tag));

  bc->chain.buf = &bc->buf;
  bc->chain.next = first_link;
  first_link=&bc->chain;
  
  rc = nchan_output_filter(fsub->data.request, first_link);
  
  ngx_destroy_pool(pool);
  
  return rc;
}

static void empty_handler(void) {}

static ngx_int_t es_respond_status(subscriber_t *sub, ngx_int_t status_code, const ngx_str_t *status_line){
  full_subscriber_t        *fsub = (full_subscriber_t  *)sub;
  u_char                    resp_buf[256];
  nchan_buf_and_chain_t     bc;
  
  es_ensure_headers_sent(fsub);
  
  DBG("%p output status to subscriber", sub);
  
  bc.chain.buf = &bc.buf;
  bc.chain.next = NULL;
  ngx_init_set_membuf(&bc.buf, resp_buf, ngx_snprintf(resp_buf, 256, ":%i: %V\n", status_code, status_line));
  bc.buf.last_buf = 1;
  
  nchan_output_filter(fsub->data.request, &bc.chain);
  
  if(status_code >=400 && status_code <599) {
    fsub->data.cln->handler = (ngx_http_cleanup_pt )empty_handler;
    fsub->data.request->keepalive=0;
    ngx_http_finalize_request(fsub->data.request, NGX_OK);
    sub->dequeue(sub);
  }

  return NGX_OK;
}


static ngx_int_t es_enqueue(subscriber_t *sub) {
  ngx_int_t           rc;
  full_subscriber_t  *fsub = (full_subscriber_t *)sub;
  DBG("%p output status to subscriber", sub);
  rc = longpoll_enqueue(sub);
  fsub->data.finalize_request = 0;
  es_ensure_headers_sent(fsub);
  return rc;
}

subscriber_t *eventsource_subscriber_create(ngx_http_request_t *r) {
  subscriber_t *sub;
  full_subscriber_t *fsub;
  
  sub = longpoll_subscriber_create(r);
  fsub = (full_subscriber_t *)sub;
  
  
  sub->name = "eventsource";
  sub->type =  EVENTSOURCE;
  
  sub->enqueue = es_enqueue;
  sub->respond_message= es_respond_message;
  sub->respond_status = es_respond_status;
  
  sub->dequeue_after_response = 0;
  
  fsub->data.shook_hands = 0;
  
  DBG("%p create subscriber", sub);
  
  return sub;
}