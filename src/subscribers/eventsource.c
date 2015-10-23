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

static ngx_int_t es_respond_message(subscriber_t *sub,  nchan_msg_t *msg) {
  static ngx_str_t        data_prefix=ngx_string("data: ");
  static ngx_buf_t        data_prefix_buf;
  static ngx_str_t        terminal_newlines=ngx_string("\n\n");
  
  ngx_init_set_membuf(&data_prefix_buf, data_prefix.data, data_prefix.data + data_prefix.len);
  
  full_subscriber_t      *fsub = (full_subscriber_t  *)sub;
  ngx_buf_t              *buf = msg->buf;
  ngx_buf_t              *databuf;
  ngx_pool_t             *pool;
  nchan_buf_and_chain_t  *bc;
  u_char                 *cur;
  size_t                  len;
  ngx_chain_t            *first_link = NULL, *last_link = NULL;
  ngx_int_t               rc;
  
  pool = ngx_create_pool(NGX_DEFAULT_LINEBREAK_POOL_SIZE, ngx_cycle->log);
  assert(pool);
  
  if(!buf->in_file) {
    cur = buf->start;
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
      
      cur = ngx_strlchr(cur, databuf->last, '\n');
      if(cur == NULL) {
        //sweet, no newlines!
        //let's get out of this hellish loop
        break; 
      }
      else {
        cur++; //include the newline
        databuf->end = cur;
        databuf->last = cur;
      }
      
      if(first_link == NULL) {
        first_link = &bc[0].chain;
      }
      last_link = &bc[1].chain;
    } while(cur < buf->last);
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

static ngx_int_t es_respond_status(subscriber_t *sub, ngx_int_t status_code, const ngx_str_t *status_line){
  full_subscriber_t        *fsub = (full_subscriber_t  *)sub;
  u_char                    resp_buf[256];
  nchan_buf_and_chain_t     bc;
  bc.chain.buf = &bc.buf;
  bc.chain.next = NULL;
  ngx_init_set_membuf(&bc.buf, resp_buf, ngx_snprintf(resp_buf, 256, ":%i: %V", status_code, status_line));
  bc.buf.last_buf = 1;
  
  nchan_output_filter(fsub->data.request, &bc.chain);
  
  if(status_code >=400 && status_code <599) {
    ngx_http_finalize_request(fsub->data.request, NGX_OK);
    sub->dequeue(sub);
  }

  return NGX_OK;
}


static ngx_int_t es_enqueue(subscriber_t *sub) {
  static const ngx_str_t   content_type = ngx_string("text/event-stream");
  ngx_int_t                rc = longpoll_enqueue(sub);
  full_subscriber_t       *fsub = (full_subscriber_t *)sub;
  ngx_http_request_t      *r = fsub->data.request;
  if(rc != NGX_OK) {
    return rc;
  }
  
  r->headers_out.status=NGX_HTTP_OK;
  
  r->headers_out.content_type.len = content_type.len;
  r->headers_out.content_type.data = content_type.data;
  //send headers
  
  return ngx_http_send_header(r);
}

subscriber_t *eventsource_subscriber_create(ngx_http_request_t *r) {
  subscriber_t *sub;
  
  sub = longpoll_subscriber_create(r);
  
  sub->name = "eventsource";
  sub->type =  EVENTSOURCE;
  
  sub->enqueue = es_enqueue;
  sub->respond_message= es_respond_message;
  sub->respond_status = es_respond_status;
  
  sub->dequeue_after_response = 0;
  
  return sub;
}