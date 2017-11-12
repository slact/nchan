#include <nchan_module.h>
#include <subscribers/common.h>
#include <util/nchan_bufchainpool.h>
#include "longpoll.h"
#include "longpoll-private.h"

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:EVENTSOURCE:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:EVENTSOURCE:" fmt, ##arg)
#include <assert.h> 

static nchan_bufchain_pool_t *fsub_bcp(full_subscriber_t *fsub) {
  nchan_request_ctx_t            *ctx = ngx_http_get_module_ctx(fsub->sub.request, ngx_nchan_module);
  return ctx->bcp;
}

/*static ngx_inline void str_to_buf(ngx_buf_t *buf, ngx_str_t *str) {
  buf->start = str->data;
  buf->pos = str->data;
  buf->end = str->data + str->len;
  buf->last = buf->end;
  buf->memory = 1;
}
*/

static void es_ensure_headers_sent(full_subscriber_t *fsub) {
  static const ngx_str_t   content_type = ngx_string("text/event-stream; charset=utf-8");
  static const ngx_str_t   hello = ngx_string(": hi\n\n");
  ngx_http_request_t             *r = fsub->sub.request;
  ngx_http_core_loc_conf_t       *clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
  
  nchan_buf_and_chain_t          *bc;
  
  if(!fsub->data.shook_hands) {
    bc = nchan_bufchain_pool_reserve(fsub_bcp(fsub), 1);
    clcf->chunked_transfer_encoding = 0;
    
    r->headers_out.content_type.len = content_type.len;
    r->headers_out.content_type.data = content_type.data;
    r->headers_out.content_length_n = -1;
    //send headers
    
    nchan_cleverly_output_headers_only_for_later_response(r);
    
    //send a ":hi" comment
    ngx_init_set_membuf(&bc->buf, hello.data, hello.data + hello.len);
    
    bc->buf.last_buf = 0;
    bc->buf.flush = 1;

    nchan_output_filter(fsub->sub.request, &bc->chain);
    
    fsub->data.shook_hands = 1; 
  }
}

static ngx_int_t create_dataline_bufchain(full_subscriber_t *fsub, ngx_chain_t **first_chain, ngx_chain_t **last_chain, ngx_buf_t *databuf) {
  static ngx_str_t        data_prefix=ngx_string("data: ");
  nchan_buf_and_chain_t  *bc = nchan_bufchain_pool_reserve(fsub_bcp(fsub), ngx_buf_size(databuf) == 0 ? 1 : 2);
  ngx_chain_t            *chain = &bc->chain;
  
  if(*last_chain) {
    (*last_chain)->next = chain;
  }
  
  ngx_init_set_membuf(chain->buf, data_prefix.data, data_prefix.data + data_prefix.len);
  
  if(*first_chain == NULL) {
    *first_chain = chain;
  }
  
  if(ngx_buf_size(databuf) > 0) {
    chain = chain->next;
    *chain->buf = *databuf;
    chain->buf->last_buf = 0;
    chain->buf->last_in_chain = 0;
  }
  *last_chain = chain;

  return NGX_OK;
}

static void prepend_es_response_line(full_subscriber_t *fsub, ngx_str_t *lbl, ngx_chain_t **first_chain, ngx_str_t *str) {
  static ngx_str_t        nl = ngx_string("\n");
  nchan_buf_and_chain_t  *bc = nchan_bufchain_pool_reserve(fsub_bcp(fsub), 3);
  ngx_chain_t            *chain;

  chain = &bc->chain;
  ngx_init_set_membuf(chain->buf, lbl->data, lbl->data + lbl->len);
  chain = chain->next;
  ngx_init_set_membuf(chain->buf, str->data, str->data + str->len);
  chain = chain->next;
  ngx_init_set_membuf(chain->buf, nl.data, nl.data + 1);
  
  assert(chain->next == NULL);
  chain->next = *first_chain;
  *first_chain = &bc->chain;
}

static ngx_int_t es_respond_message(subscriber_t *sub,  nchan_msg_t *msg) {
  static ngx_str_t        terminal_newlines=ngx_string("\n\n");
  full_subscriber_t      *fsub = (full_subscriber_t  *)sub;
  u_char                 *cur = NULL, *last = NULL;
  ngx_buf_t              *msg_buf = &msg->buf;
  ngx_buf_t               databuf;
  nchan_buf_and_chain_t  *bc;
  ngx_chain_t            *first_link = NULL, *last_link = NULL;
  ngx_str_t               msgid;
  static ngx_str_t        id_line = ngx_string("id: ");
  static ngx_str_t        event_line = ngx_string("event: ");
  nchan_request_ctx_t    *ctx = ngx_http_get_module_ctx(sub->request, ngx_nchan_module);
  
  
  ctx->prev_msg_id = fsub->sub.last_msgid;
  update_subscriber_last_msg_id(sub, msg);
  ctx->msg_id = fsub->sub.last_msgid;
  
  if(fsub->data.timeout_ev.timer_set) {
    ngx_del_timer(&fsub->data.timeout_ev);
    ngx_add_timer(&fsub->data.timeout_ev, sub->cf->subscriber_timeout * 1000);
  }
  
  es_ensure_headers_sent(fsub);
  
  DBG("%p output msg to subscriber", sub);
  
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
      
      create_dataline_bufchain(fsub, &first_link, &last_link, &databuf);
      
    } while(cur <= last);
  } 
  else {
    //great, we've gotta scan this whole damn file for line breaks.
    //EventStream really isn't designed for large chunks of data
    off_t       fcur, flast;
    //int         chr_int;
    FILE       *stream;
    ngx_file_t *msgfile =  nchan_bufchain_pool_reserve_file(ctx->bcp);
    
    nchan_msg_buf_open_fd_if_needed(&databuf, msgfile, NULL);

    if(msgfile->fd == NGX_INVALID_FILE) {
      msgfile->fd = nchan_fdcache_get(&msgfile->name);
    }
    stream = fdopen(dup(msgfile->fd), "r");
    
    fcur = databuf.file_pos;
    flast = databuf.file_last;
    
    fseek(stream, fcur, SEEK_SET);
    
    do {
      databuf.file_pos = fcur;
      databuf.file_last = flast;
      
      //getc that shit
      /*for(;;) {
        chr_int = getc(stream);
        if(chr_int == EOF) {
          break;
        }
        else if(chr_int == (int )'\n') {
          fcur++;
          break;
        }
        fcur++;
      }*/
      
      if(fscanf(stream,"%*[^\n]\n") != EOF) {
        fcur = ftell(stream);
      }
      else {
        fcur = flast;
      }
      
      databuf.file_last = fcur;
      create_dataline_bufchain(fsub, &first_link, &last_link, &databuf);
      
    } while(fcur < flast);
    
    fclose(stream);
  }
  
  //now 2 newlines at the end
  if(last_link) {
    bc = nchan_bufchain_pool_reserve(ctx->bcp, 1);
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
  
  msgid = nchan_subscriber_set_recyclable_msgid_str(ctx, &sub->last_msgid);
  prepend_es_response_line(fsub, &id_line, &first_link, &msgid);
  
  //and maybe the event type?
  if(sub->cf->eventsource_event.len > 0) {
    prepend_es_response_line(fsub, &event_line, &first_link, &sub->cf->eventsource_event);
  }
  else if(msg->eventsource_event) {
    prepend_es_response_line(fsub, &event_line, &first_link, msg->eventsource_event);
  }
  
  return nchan_output_msg_filter(fsub->sub.request, msg, first_link);
}

static void empty_handler(void) {}

static ngx_int_t es_respond_status(subscriber_t *sub, ngx_int_t status_code, const ngx_str_t *status_line,  ngx_chain_t *status_body){
  
  static ngx_str_t          empty_line = ngx_string("");
  full_subscriber_t        *fsub = (full_subscriber_t  *)sub;
  u_char                    resp_buf[256];
  nchan_buf_and_chain_t     bc;
  
  if(status_code == NGX_HTTP_NO_CONTENT || (status_code == NGX_HTTP_NOT_MODIFIED && !status_line)) {
    //ignore
    return NGX_OK;
  }
  
  if(fsub->data.shook_hands == 0 && status_code >= 400 && status_code <600) {
    return subscriber_respond_unqueued_status(fsub, status_code, status_line, status_body);
  }
  
  es_ensure_headers_sent(fsub);
  
  DBG("%p output status to subscriber", sub);
  
  bc.chain.buf = &bc.buf;
  bc.chain.next = NULL;
  ngx_init_set_membuf(&bc.buf, resp_buf, ngx_snprintf(resp_buf, 256, ":%i: %V\n\n", status_code, status_line ? status_line : &empty_line));
  bc.buf.flush = 1;
  bc.buf.last_buf = 1;
  
  nchan_output_filter(fsub->sub.request, &bc.chain);
  
  if((status_code >=400 && status_code <599) || status_code == NGX_HTTP_NOT_MODIFIED) {
    fsub->data.cln->handler = (ngx_http_cleanup_pt )empty_handler;
    fsub->sub.request->keepalive=0;
    sub->request->headers_out.status = status_code;
    fsub->data.finalize_request=1;
    sub->fn->dequeue(sub);
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
  sub->enqueued = 1;
  return rc;
}

static       subscriber_fn_t  eventsource_fn_data;
static       subscriber_fn_t *eventsource_fn = NULL;

static       ngx_str_t   sub_name = ngx_string("eventsource");


subscriber_t *eventsource_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id) {
  subscriber_t         *sub = longpoll_subscriber_create(r, msg_id);
  full_subscriber_t    *fsub = (full_subscriber_t *)sub;
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  
  if(eventsource_fn == NULL) {
    eventsource_fn = &eventsource_fn_data;
    *eventsource_fn = *sub->fn;
    eventsource_fn->enqueue = es_enqueue;
    eventsource_fn->respond_message= es_respond_message;
    eventsource_fn->respond_status = es_respond_status;
  }
  
  fsub->data.shook_hands = 0;
  
  ctx->bcp = ngx_palloc(r->pool, sizeof(nchan_bufchain_pool_t));
  nchan_bufchain_pool_init(ctx->bcp, r->pool);
  
  //msgid bufs -- unique per response
  nchan_subscriber_init_msgid_reusepool(ctx, r->pool);
  
  nchan_subscriber_common_setup(sub, EVENTSOURCE, &sub_name, eventsource_fn, 1, 0);
  return sub;
}

ngx_int_t nchan_detect_eventsource_request(ngx_http_request_t *r) {
  ngx_str_t       *accept_header = nchan_get_accept_header_value(r);

  if(accept_header && ngx_strnstr(accept_header->data, "text/event-stream", accept_header->len)) {
    return 1;
  }
  
  return 0;
}
