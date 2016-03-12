#include <nchan_module.h>
#include <subscribers/common.h>
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
  static const ngx_str_t   content_type = ngx_string("text/event-stream; charset=utf-8");
  static const ngx_str_t   hello = ngx_string(": hi\n\n");
  ngx_http_request_t             *r = fsub->sub.request;
  ngx_http_core_loc_conf_t       *clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
  nchan_buf_and_chain_t           bc;
  
  if(!fsub->data.shook_hands) {
  
    clcf->chunked_transfer_encoding = 0;
    
    r->headers_out.content_type.len = content_type.len;
    r->headers_out.content_type.data = content_type.data;
    r->headers_out.content_length_n = -1;
    //send headers
    
    nchan_cleverly_output_headers_only_for_later_response(r);
    
    //send a ":hi" comment
    ngx_init_set_membuf(&bc.buf, hello.data, hello.data + hello.len);
    bc.chain.buf = &bc.buf;
    
    bc.buf.last_buf = 0;
    bc.buf.flush = 1;

    bc.chain.next = NULL;
    nchan_output_filter(fsub->sub.request, &bc.chain);
    
    fsub->data.shook_hands = 1; 
  }
}

static ngx_int_t create_dataline_bufchain(ngx_pool_t *pool, ngx_chain_t **first_chain, ngx_chain_t **last_chain, ngx_buf_t *databuf) {
  static ngx_str_t        data_prefix=ngx_string("data: ");
  static ngx_buf_t        data_prefix_real_buf;
  static ngx_buf_t       *data_prefix_buf = NULL;
  nchan_buf_and_chain_t  *bc;

  if(data_prefix_buf == NULL) {
    data_prefix_buf = &data_prefix_real_buf;
    ngx_init_set_membuf(data_prefix_buf, data_prefix.data, data_prefix.data + data_prefix.len);
  }
  
  if((bc = ngx_palloc(pool, sizeof(*bc)*2)) == NULL) {
    return NGX_ERROR;
  }
  if(*last_chain) {
    (*last_chain)->next = &bc[0].chain;
  }
  
  bc[0].chain.next = &bc[1].chain;
  bc[0].chain.buf = &bc[0].buf;
  ngx_memcpy(&bc[0].buf, data_prefix_buf, sizeof(*data_prefix_buf));
  
  bc[1].chain.buf=&bc[1].buf;
  bc[1].chain.next = NULL;
  
  ngx_memcpy(&bc[1].buf, databuf, sizeof(*databuf));
  
  if(*first_chain == NULL) {
    *first_chain = &bc[0].chain;
  }
  
  if(ngx_buf_size(databuf) == 0) {
    //no empty buffers please
    *last_chain = &bc[0].chain;
  }
  else {
    *last_chain = &bc[1].chain;
  }
  
  return NGX_OK;
}

static void prepend_es_response_line(ngx_str_t *fmt, ngx_chain_t **first_chain, ngx_str_t *str, ngx_pool_t *pool) {
  nchan_buf_and_chain_t  *bc;
  size_t                  sz = fmt->len + str->len;
  u_char                 *cur;
  
  if((bc = ngx_palloc(pool, sizeof(*bc) + sz)) == NULL) {
    ERR("unable to allocate buf and chain for EventSource response line %V", fmt);
    return;
  }
  cur = (u_char *)&bc[1];
  ngx_init_set_membuf(&bc->buf, cur, ngx_snprintf(cur, sz, (char *)fmt->data, str));

  bc->chain.buf = &bc->buf;
  bc->chain.next = *first_chain;
  *first_chain=&bc->chain;
}

static ngx_int_t es_respond_message(subscriber_t *sub,  nchan_msg_t *msg) {
  static ngx_str_t        terminal_newlines=ngx_string("\n\n");
  full_subscriber_t      *fsub = (full_subscriber_t  *)sub;
  u_char                 *cur = NULL, *last = NULL;
  ngx_buf_t              *msg_buf = msg->buf;
  ngx_buf_t               databuf;
  ngx_pool_t             *pool;
  nchan_buf_and_chain_t  *bc;
  ngx_chain_t            *first_link = NULL, *last_link = NULL;
  ngx_file_t             *msg_file;
  ngx_int_t               rc;
  
  ngx_str_t               id_line = ngx_string("id: %V\n");
  ngx_str_t               event_line = ngx_string("event: %V\n");
  
  nchan_request_ctx_t    *ctx = ngx_http_get_module_ctx(sub->request, nchan_module);
  
  
  ctx->prev_msg_id = fsub->sub.last_msgid;
  update_subscriber_last_msg_id(sub, msg);
  ctx->msg_id = fsub->sub.last_msgid;
  
  if(fsub->data.timeout_ev.timer_set) {
    ngx_del_timer(&fsub->data.timeout_ev);
    ngx_add_timer(&fsub->data.timeout_ev, sub->cf->subscriber_timeout * 1000);
  }
  
  es_ensure_headers_sent(fsub);
  
  DBG("%p output msg to subscriber", sub);
  
  pool = ngx_create_pool(NGX_DEFAULT_LINEBREAK_POOL_SIZE, ngx_cycle->log);
  assert(pool);
  
  ngx_memcpy(&databuf, msg_buf, sizeof(*msg_buf));
  databuf.last_buf = 0;
  
  if(!databuf.in_file) {
    cur = msg_buf->start;
    last = msg_buf->end;
    do {
      databuf.start = cur;
      databuf.pos = cur;
      databuf.end = last;
      databuf.last = last;
      
      cur = ngx_strlchr(cur, last, '\n');
      if(cur == NULL) {
        //sweet, no newlines!
        //let's get out of this hellish loop
        databuf.end = last;
        databuf.last = last;
        cur = last + 1;
      }
      else {
        cur++; //include the newline
        databuf.end = cur;
        databuf.last = cur;
      }
      
      create_dataline_bufchain(pool, &first_link, &last_link, &databuf);
      
    } while(cur <= last);
  } 
  else {
    //great, we've gotta scan this whole damn file for line breaks.
    //EventStream really isn't designed for large chunks of data
    off_t       fcur, flast;
    ngx_fd_t    fd;
    int         chr_int;
    FILE       *stream;
    
    msg_file = ngx_palloc(pool, sizeof(*msg_file));
    databuf.file = msg_file;
    ngx_memcpy(msg_file, msg_buf->file, sizeof(*msg_file));
    
    if(msg_file->fd == NGX_INVALID_FILE) {
      msg_file->fd = nchan_fdcache_get(&msg_file->name);
    }
    fd = msg_file->fd;
    
    stream = fdopen(dup(fd), "r");
    
    fcur = databuf.file_pos;
    flast = databuf.file_last;
    
    fseek(stream, fcur, SEEK_SET);
    
    do {
      databuf.file_pos = fcur;
      databuf.file_last = flast;
      
      //getc that shit
      for(;;) {
        chr_int = getc(stream);
        if(chr_int == EOF) {
          break;
        }
        else if(chr_int == (int )'\n') {
          fcur++;
          break;
        }
        fcur++;
      }
      
      databuf.file_last = fcur;
      create_dataline_bufchain(pool, &first_link, &last_link, &databuf);
      
    } while(fcur < flast);
    
    fclose(stream);
  }
  
  //now 2 newlines at the end
  if(last_link) {
    bc = ngx_palloc(pool, sizeof(*bc));
    last_link->next=&bc->chain;
    ngx_init_set_membuf(&bc->buf, terminal_newlines.data, terminal_newlines.data + terminal_newlines.len);
    
    bc->buf.flush = 1;
    bc->buf.last_buf = 0;
    
    bc->chain.next = NULL;
    bc->chain.buf = &bc->buf;
    
    last_link = &bc->chain;
  }
  //okay, this crazy data chain is finished. 
  
  
  //now how about the mesage tag?
  prepend_es_response_line(&id_line, &first_link, msgid_to_str(&sub->last_msgid), pool);
  
  //and maybe the event type?
  if(sub->cf->eventsource_event.len > 0) {
    prepend_es_response_line(&event_line, &first_link, &sub->cf->eventsource_event, pool);
  }
  else if(msg->eventsource_event.len > 0) {
    prepend_es_response_line(&event_line, &first_link, &msg->eventsource_event, pool);
  }
  
  rc = nchan_output_filter(fsub->sub.request, first_link);
  
  ngx_destroy_pool(pool);
  
  
  return rc;
}

static void empty_handler(void) {}

static ngx_int_t es_respond_status(subscriber_t *sub, ngx_int_t status_code, const ngx_str_t *status_line){
  
  static ngx_str_t          empty_line = ngx_string("");
  full_subscriber_t        *fsub = (full_subscriber_t  *)sub;
  u_char                    resp_buf[256];
  nchan_buf_and_chain_t     bc;
  
  if(status_code == NGX_HTTP_NO_CONTENT || (status_code == NGX_HTTP_NOT_MODIFIED && !status_line)) {
    //ignore
    return NGX_OK;
  }
  
  if(fsub->data.shook_hands == 0 && status_code >= 400 && status_code <600) {
    nchan_respond_status(sub->request, status_code, status_line, 1);
    return NGX_OK;
  }
  
  es_ensure_headers_sent(fsub);
  
  DBG("%p output status to subscriber", sub);
  
  bc.chain.buf = &bc.buf;
  bc.chain.next = NULL;
  ngx_init_set_membuf(&bc.buf, resp_buf, ngx_snprintf(resp_buf, 256, ":%i: %V\n", status_code, status_line ? status_line : &empty_line));
  bc.buf.flush = 1;
  bc.buf.last_buf = 1;
  
  nchan_output_filter(fsub->sub.request, &bc.chain);
  
  if((status_code >=400 && status_code <599) || status_code == NGX_HTTP_NOT_MODIFIED) {
    fsub->data.cln->handler = (ngx_http_cleanup_pt )empty_handler;
    fsub->sub.request->keepalive=0;
    fsub->data.finalize_request=1;
    sub->fn->dequeue(sub);
  }

  return NGX_OK;
}

static void es_timeout_ev_handler(ngx_event_t *ev) {
  full_subscriber_t *fsub = (full_subscriber_t *)ev->data;
  fsub->sub.fn->respond_status(&fsub->sub, NGX_HTTP_NOT_MODIFIED, &NCHAN_SUBSCRIBER_TIMEOUT);
}

static ngx_int_t es_enqueue(subscriber_t *sub) {
  ngx_int_t           rc;
  full_subscriber_t  *fsub = (full_subscriber_t *)sub;
  DBG("%p output status to subscriber", sub);
  rc = longpoll_enqueue(sub);
  fsub->data.timeout_ev.handler = es_timeout_ev_handler;
  fsub->data.finalize_request = 0;
  es_ensure_headers_sent(fsub);
  sub->enqueued = 1;
  return rc;
}

static       subscriber_fn_t  eventsource_fn_data;
static       subscriber_fn_t *eventsource_fn = NULL;

static       ngx_str_t   sub_name = ngx_string("eventsource");

subscriber_t *eventsource_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id) {
  subscriber_t         *sub;
  full_subscriber_t    *fsub;
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(r, nchan_module);
  sub = longpoll_subscriber_create(r, msg_id);
  
  if(eventsource_fn == NULL) {
    eventsource_fn = &eventsource_fn_data;
    *eventsource_fn = *sub->fn;
    eventsource_fn->enqueue = es_enqueue;
    eventsource_fn->respond_message= es_respond_message;
    eventsource_fn->respond_status = es_respond_status;
  }
  
  fsub = (full_subscriber_t *)sub;
  
  sub->fn = eventsource_fn;
  sub->name = &sub_name;
  sub->type = EVENTSOURCE;
  
  sub->dequeue_after_response = 0;
  
  fsub->data.shook_hands = 0;
  
  DBG("%p create subscriber", sub);
  
  if(ctx) {
    ctx->subscriber_type = sub->name;
  }
  return sub;
}

ngx_int_t nchan_detect_eventsource_request(ngx_http_request_t *r) {
  ngx_str_t       *accept_header;
  if(r->headers_in.accept == NULL) {
    return 0;
  }
  accept_header = &r->headers_in.accept->value;

  if(ngx_strnstr(accept_header->data, "text/event-stream", accept_header->len)) {
    return 1;
  }
  
  return 0;
}
