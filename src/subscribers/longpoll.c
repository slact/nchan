#include <nchan_module.h>
#include <subscribers/common.h>
//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:LONGPOLL:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:LONGPOLL:" fmt, ##arg)
#include <assert.h>
#include "longpoll-private.h"

// #define SEND_MULTIPART_BOUNDARIES

void memstore_fakeprocess_push(ngx_int_t slot);
void memstore_fakeprocess_push_random(void);
void memstore_fakeprocess_pop(void);
ngx_int_t memstore_slot(void);

static const subscriber_t new_longpoll_sub;

static void empty_handler() { }

static void sudden_abort_handler(subscriber_t *sub) {
#if FAKESHARD
  full_subscriber_t  *fsub = (full_subscriber_t  *)sub;
  memstore_fakeprocess_push(fsub->data.owner);
#endif
  sub->fn->dequeue(sub);
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}

//void verify_unique_response(ngx_str_t *uri, nchan_msg_id_t *msgid, nchan_msg_t *msg, subscriber_t *sub);

subscriber_t *longpoll_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id) {
  DBG("create for req %p", r);
  full_subscriber_t      *fsub;
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(r, nchan_module);
  //TODO: allocate from pool (but not the request's pool)
  if((fsub = ngx_alloc(sizeof(*fsub), ngx_cycle->log)) == NULL) {
    ERR("Unable to allocate");
    assert(0);
    return NULL;
  }
  ngx_memcpy(&fsub->sub, &new_longpoll_sub, sizeof(new_longpoll_sub));
  fsub->sub.request = r;
  fsub->data.cln = NULL;
  fsub->data.finalize_request = 1;
  fsub->data.holding = 0;
  fsub->data.act_as_intervalpoll = 0;
  fsub->sub.cf = ngx_http_get_module_loc_conf(r, nchan_module);
  ngx_memzero(&fsub->data.timeout_ev, sizeof(fsub->data.timeout_ev));
  fsub->data.timeout_handler = empty_handler;
  fsub->data.timeout_handler_data = NULL;
  fsub->data.dequeue_handler = empty_handler;
  fsub->data.dequeue_handler_data = NULL;
  fsub->data.already_responded = 0;
  fsub->data.awaiting_destruction = 0;
  fsub->sub.reserved = 0;
  fsub->sub.enqueued = 0;
  
  if(fsub->sub.cf->longpoll_multimsg) {
    fsub->sub.dequeue_after_response = 0;
  }
  
  fsub->data.multimsg_first = NULL;
  fsub->data.multimsg_last = NULL;
  
#if NCHAN_SUBSCRIBER_LEAK_DEBUG
  subscriber_debug_add(&fsub->sub);
  //set debug label
  fsub->sub.lbl = ngx_calloc(r->uri.len+1, ngx_cycle->log);
  ngx_memcpy(fsub->sub.lbl, r->uri.data, r->uri.len);
#endif
  
  if(msg_id) {
    nchan_copy_new_msg_id(&fsub->sub.last_msgid, msg_id);
  }
  else {
    fsub->sub.last_msgid.time = 0;
    fsub->sub.last_msgid.tag.fixed[0] = 0;
    fsub->sub.last_msgid.tagcount = 1;
  }
  
  ctx->prev_msg_id = fsub->sub.last_msgid;
  
  fsub->data.owner = memstore_slot();
  
  //http request sudden close cleanup
  if((fsub->data.cln = ngx_http_cleanup_add(r, 0)) == NULL) {
    ERR("Unable to add request cleanup for longpoll subscriber");
    assert(0);
    return NULL;
  }
  fsub->data.cln->data = fsub;
  fsub->data.cln->handler = (ngx_http_cleanup_pt )sudden_abort_handler;
  DBG("%p created for request %p", &fsub->sub, r);
  
  if(ctx) {
    ctx->sub = &fsub->sub;
    ctx->subscriber_type = fsub->sub.name;
  }
  
  return &fsub->sub;
}

ngx_int_t longpoll_subscriber_destroy(subscriber_t *sub) {
  full_subscriber_t   *fsub = (full_subscriber_t  *)sub;
  
  if(sub->reserved > 0) {
    DBG("%p not ready to destroy (reserved for %i) for req %p", sub, sub->reserved, fsub->sub.request);
    fsub->data.awaiting_destruction = 1;
  }
  else {
    DBG("%p destroy for req %p", sub, fsub->sub.request);
    nchan_free_msg_id(&fsub->sub.last_msgid);
#if NCHAN_SUBSCRIBER_LEAK_DEBUG
    subscriber_debug_remove(sub);
    ngx_free(sub->lbl);
    ngx_memset(fsub, 0xB9, sizeof(*fsub)); //debug
#endif
    ngx_free(fsub);
  }
  return NGX_OK;
}

static void ensure_request_hold(full_subscriber_t *fsub) {
  if(fsub->data.holding == 0) {
    DBG("hodl request %p", fsub->sub.request);
    fsub->data.holding = 1;
    fsub->sub.request->read_event_handler = ngx_http_test_reading;
    fsub->sub.request->write_event_handler = ngx_http_request_empty_handler;
    fsub->sub.request->main->count++; //this is the right way to hold and finalize the request... maybe
  }
}

static ngx_int_t longpoll_reserve(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  ensure_request_hold(fsub);
  self->reserved++;
  DBG("%p reserve for req %p, reservations: %i", self, fsub->sub.request, self->reserved);
  return NGX_OK;
}
static ngx_int_t longpoll_release(subscriber_t *self, uint8_t nodestroy) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  assert(self->reserved > 0);
  self->reserved--;
  DBG("%p release for req %p. reservations: %i", self, fsub->sub.request, self->reserved);
  if(nodestroy == 0 && fsub->data.awaiting_destruction == 1 && self->reserved == 0) {
    longpoll_subscriber_destroy(self);
    return NGX_ABORT;
  }
  else {
    return NGX_OK;
  }
}

static void timeout_ev_handler(ngx_event_t *ev) {
  full_subscriber_t *fsub = (full_subscriber_t *)ev->data;
#if FAKESHARD
  memstore_fakeprocess_push(fsub->data.owner);
#endif
  fsub->data.timeout_handler(&fsub->sub, fsub->data.timeout_handler_data);
  fsub->sub.dequeue_after_response = 1;
  fsub->sub.fn->respond_status(&fsub->sub, NGX_HTTP_NOT_MODIFIED, NULL);
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}

ngx_int_t longpoll_enqueue(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  assert(fsub->sub.enqueued == 0);
  DBG("%p enqueue", self);
  
  fsub->data.finalize_request = 1;
  
  fsub->sub.enqueued = 1;
  ensure_request_hold(fsub);
  if(self->cf->subscriber_timeout > 0) {
    //add timeout timer
    //nextsub->ev should be zeroed;
    fsub->data.timeout_ev.handler = timeout_ev_handler;
    fsub->data.timeout_ev.data = fsub;
    fsub->data.timeout_ev.log = ngx_cycle->log;
    ngx_add_timer(&fsub->data.timeout_ev, self->cf->subscriber_timeout * 1000);
  }
  
  return NGX_OK;
}

static ngx_int_t longpoll_dequeue(subscriber_t *self) {
  full_subscriber_t    *fsub = (full_subscriber_t  *)self;
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(fsub->sub.request, nchan_module);
  if(fsub->data.timeout_ev.timer_set) {
    ngx_del_timer(&fsub->data.timeout_ev);
  }
  DBG("%p dequeue", self);
  fsub->data.dequeue_handler(self, fsub->data.dequeue_handler_data);
  self->enqueued = 0;
  
  ctx->sub = NULL;
  
  if(fsub->data.finalize_request) {
    DBG("finalize request %p", fsub->sub.request);
    ngx_http_finalize_request(fsub->sub.request, NGX_OK);
  }
  
  if(self->destroy_after_dequeue) {
    longpoll_subscriber_destroy(self);
  }
  return NGX_OK;
}

static ngx_int_t dequeue_maybe(subscriber_t *self) {
  if(self->dequeue_after_response) {
    self->fn->dequeue(self);
  }
  return NGX_OK;
}

static ngx_int_t abort_response(subscriber_t *sub, char *errmsg) {
  ERR("abort! %s", errmsg ? errmsg : "unknown error");
  dequeue_maybe(sub);
  return NGX_ERROR;
}

static ngx_int_t longpoll_multimsg_add(full_subscriber_t *fsub, nchan_msg_t *msg, char **err) {
  
  nchan_longpoll_multimsg_t     *mmsg;
  
  if((mmsg = ngx_palloc(fsub->sub.request->pool, sizeof(*mmsg))) == NULL) {
    *err = "can't allocate multimsg";
    return NGX_ERROR;
  }
  
  mmsg->msg = msg;
  mmsg->next = NULL;
  if(fsub->data.multimsg_first == NULL) {
    fsub->data.multimsg_first = mmsg;
  }
  if(fsub->data.multimsg_last) {
    fsub->data.multimsg_last->next = mmsg;
  }
  fsub->data.multimsg_last = mmsg;
  
  return NGX_OK;
}

static ngx_int_t longpoll_respond_message(subscriber_t *self, nchan_msg_t *msg) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  ngx_int_t                  rc;
  char                      *err = NULL;
  ngx_http_request_t        *r = fsub->sub.request;
  nchan_request_ctx_t       *ctx = ngx_http_get_module_ctx(r, nchan_module);
  nchan_loc_conf_t          *cf = fsub->sub.cf;
  
  DBG("%p respond req %p msg %p", self, r, msg);
  
  ctx->prev_msg_id = self->last_msgid;
  update_subscriber_last_msg_id(self, msg);
  ctx->msg_id = self->last_msgid;
  
  //disable abort handler
  fsub->data.cln->handler = empty_handler;

  //verify_unique_response(&fsub->data.request->uri, &self->last_msgid, msg, self);
  
  if(!cf->longpoll_multimsg) {
    assert(fsub->data.already_responded != 1);
    fsub->data.already_responded = 1;
    if(ctx->request_origin_header.len > 0) {
      nchan_add_response_header(r, &NCHAN_HEADER_ALLOW_ORIGIN, &cf->allow_origin);
    }
    if((rc = nchan_respond_msg(r, msg, &self->last_msgid, 0, &err)) != NGX_OK) {
      return abort_response(self, err);
    }
  }
  else {
    if((rc = longpoll_multimsg_add(fsub, msg, &err)) != NGX_OK) {
      return abort_response(self, err);
    }
  }
  dequeue_maybe(self);
  return rc;
}

static ngx_int_t longpoll_multimsg_respond(full_subscriber_t *fsub) {
  ngx_http_request_t    *r = fsub->sub.request;
  nchan_request_ctx_t   *ctx = ngx_http_get_module_ctx(r, nchan_module);
  nchan_loc_conf_t      *cf = fsub->sub.cf;
  char                  *err;
  ngx_int_t              rc;
  
#ifdef SEND_MULTIPART_BOUNDARIES
  ngx_int_t              i;
  ngx_buf_t              newline_buf;

  ngx_buf_t              boundary[3];
  u_char                 char_boundary[50];
  u_char                *char_boundary_last;
  ngx_chain_t           *chains, *first_chain = NULL, *last_chain = NULL;
  ngx_str_t             *content_type;
  ngx_buf_t             *buf;
#endif
  size_t                 size = 0;
  
  nchan_longpoll_multimsg_t *first, *cur;
  
  fsub->sub.dequeue_after_response = 1;
  
  if(ctx->request_origin_header.len > 0) {
    nchan_add_response_header(r, &NCHAN_HEADER_ALLOW_ORIGIN, &cf->allow_origin);
  }
  
  if(fsub->data.multimsg_first == fsub->data.multimsg_last) {
    //just one message.
    if((rc = nchan_respond_msg(r, fsub->data.multimsg_first->msg, &fsub->sub.last_msgid, 0, &err)) != NGX_OK) {
      return abort_response(&fsub->sub, err);
    }
    return NGX_OK;
  }
  
#ifdef SEND_MULTIPART_BOUNDARIES
  //multi messages
  nchan_request_set_content_type_multipart_boundary_header(r, ctx);
  
  char_boundary_last = ngx_snprintf(char_boundary, 50, ("\r\n--%V--\r\n"), nchan_request_multipart_boundary(r, ctx));

  //set up the boundaries
  for(i=0; i<3; i++) {
    ngx_memzero(&boundary[i], sizeof(ngx_buf_t));
    boundary[i].memory = 1;
    if(i==0) {
      boundary[i].start = &char_boundary[2];
      boundary[i].end = &char_boundary_last[-4];
    }
    else if(i==1) {
      boundary[i].start = &char_boundary[0];
      boundary[i].end = &char_boundary_last[-4];
    }
    else if(i==2) {
      boundary[i].start = &char_boundary[0];
      boundary[i].end = char_boundary_last;
      boundary[i].last_buf = 1;
      boundary[i].last_in_chain = 1;
      boundary[i].flush = 1;
    }
    boundary[i].pos = boundary[i].start;
    boundary[i].last = boundary[i].end;
  }

  ngx_memzero(&newline_buf, sizeof(newline_buf));

  newline_buf.start = (u_char *)"\r\n\r\n";
  newline_buf.end = newline_buf.start + sizeof( newline_buf.start );
  newline_buf.pos = newline_buf.start;
  newline_buf.last = newline_buf.end;
  newline_buf.memory = 1;
  
  first = fsub->data.multimsg_first;
  
  for(cur = first; cur != NULL; cur = cur->next) 
  {
    chains = ngx_palloc(r->pool, sizeof(*chains)*4);
    
    if(last_chain) {
      last_chain->next = &chains[0];
    }
    if(!first_chain) {
      first_chain = &chains[0];
    }
    chains[0].buf = cur == first ? &boundary[0] : &boundary[1];
    chains[0].next = &chains[1];
    
    size += ngx_buf_size(chains[0].buf);
    
    content_type = &cur->msg->content_type;
    if (content_type->data != NULL) {
      buf = ngx_pcalloc(r->pool, sizeof(*buf) + content_type->len + 25);
      buf->memory = 1;
      buf->start = (u_char *)&buf[1];
      buf->end = ngx_snprintf(buf->start, content_type->len + 25, "\r\nContent-Type: %V\r\n\r\n", content_type);
      buf->pos = buf->start;
      buf->last = buf->end;
      chains[1].buf = buf;
    }
    else 
    {
      chains[1].buf = &newline_buf;
    }
    size += ngx_buf_size(chains[1].buf);
    
    if(ngx_buf_size(cur->msg->buf) > 0) {
      chains[1].next = &chains[2];
      
      buf = ngx_palloc(r->pool, sizeof(*buf));
      ngx_memcpy(buf, cur->msg->buf, sizeof(*buf));
      nchan_msg_buf_open_fd_if_needed(buf, NULL, r);
      chains[2].buf = buf;
      size += ngx_buf_size(chains[2].buf);
      
      last_chain = &chains[2];
    }
    else {
      last_chain = &chains[1];
    }
    
    if(cur->next == NULL) {
      last_chain->next = &chains[3];
      
      chains[3].buf = &boundary[2];
      chains[3].next = NULL;
      last_chain = &chains[3];
      size += ngx_buf_size(chains[3].buf);
    }
  }
  
  r->headers_out.status = NGX_HTTP_OK;
  r->headers_out.content_length_n = size;
  nchan_set_msgid_http_response_headers(r, &fsub->data.multimsg_last->msg->id);
  ngx_http_send_header(r);
  nchan_output_filter(r, first_chain);
  
  
  return NGX_OK;
#else

	first = fsub->data.multimsg_first;
  
	size = 0;
	for(cur = first; cur != NULL; cur = cur->next) 
		size += ngx_buf_size( cur->msg->buf );

	ngx_str_t str;
	str.data = ngx_pnalloc( r->pool, size );
	if ( !str.data )
		return abort_response(&fsub->sub, err);

	str.len = size;
	size_t curPos = 0;
	for(cur = first; cur != NULL; cur = cur->next) 
	{
		size_t curBufSize = ngx_buf_size( cur->msg->buf );
		ngx_memcpy( &str.data[curPos], cur->msg->buf->start, curBufSize );
		curPos += curBufSize;
	}

	if ( (rc = nchan_respond_string(r, NGX_HTTP_OK, NULL, &str, 0 ) ) != NGX_OK )
		return abort_response(&fsub->sub, err);

	return NGX_OK;
#endif

}

static ngx_int_t longpoll_respond_status(subscriber_t *self, ngx_int_t status_code, const ngx_str_t *status_line) {
  
  full_subscriber_t     *fsub = (full_subscriber_t *)self;
  ngx_http_request_t    *r = fsub->sub.request;
  nchan_request_ctx_t   *ctx = ngx_http_get_module_ctx(r, nchan_module);
  nchan_loc_conf_t      *cf = fsub->sub.cf;
  
  if(fsub->data.act_as_intervalpoll) {
    if(status_code == NGX_HTTP_NO_CONTENT || status_code == NGX_HTTP_NOT_MODIFIED || status_code == NGX_HTTP_NOT_FOUND ) {
      status_code = NGX_HTTP_NOT_MODIFIED;
    }
  }
  else if(status_code == NGX_HTTP_NO_CONTENT || status_code == NGX_HTTP_NOT_MODIFIED) {
    if(cf->longpoll_multimsg) {
      if(fsub->data.multimsg_first != NULL) {
        longpoll_multimsg_respond(fsub);
        dequeue_maybe(self);
      }
      return NGX_OK;
    }
    else { 
      //don't care, ignore
      return NGX_OK;
    }
  }
  
  DBG("%p respond req %p status %i", self, r, status_code);
  
  nchan_set_msgid_http_response_headers(r, &self->last_msgid);
  
  //disable abort handler
  fsub->data.cln->handler = empty_handler;
  
  if(ctx->request_origin_header.len > 0) {
    nchan_add_response_header(r, &NCHAN_HEADER_ALLOW_ORIGIN, &cf->allow_origin);
  }
  
  nchan_respond_status(r, status_code, status_line, 0);

  dequeue_maybe(self);
  return NGX_OK;
}

static ngx_int_t longpoll_set_timeout_callback(subscriber_t *self, subscriber_callback_pt cb, void *privdata) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  fsub->data.timeout_handler = cb;
  fsub->data.timeout_handler_data = privdata;
  return NGX_OK;
}

static void request_cleanup_handler(subscriber_t *sub) {

}


static ngx_int_t longpoll_set_dequeue_callback(subscriber_t *self, subscriber_callback_pt cb, void *privdata) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  if(fsub->data.cln == NULL) {
    fsub->data.cln = ngx_http_cleanup_add(fsub->sub.request, 0);
    fsub->data.cln->data = self;
    fsub->data.cln->handler = (ngx_http_cleanup_pt )request_cleanup_handler;
  }
  fsub->data.dequeue_handler = cb;
  fsub->data.dequeue_handler_data = privdata;
  return NGX_OK;
}

static const subscriber_fn_t longpoll_fn = {
  &longpoll_enqueue,
  &longpoll_dequeue,
  &longpoll_respond_message,
  &longpoll_respond_status,
  &longpoll_set_timeout_callback,
  &longpoll_set_dequeue_callback,
  &longpoll_reserve,
  &longpoll_release,
  NULL,
  &nchan_subscriber_authorize_subscribe
};

static ngx_str_t  sub_name = ngx_string("longpoll");

static const subscriber_t new_longpoll_sub = {
  &sub_name,
  LONGPOLL,
  &longpoll_fn,
  NCHAN_ZERO_MSGID,
  NULL,
  NULL,
  0, //reservations
  1, //deque after response
  1, //destroy after dequeue
  0, //enqueued
};






/*
#include <store/rbtree_util.h>
#include <execinfo.h>

typedef struct {
  ngx_str_t    id;
  nchan_msg_t  msg;
  ngx_str_t    smallmsg;
  subscriber_t sub;
  
} uniq_response_t;

static rbtree_seed_t  uniq_rsp_seed;
static rbtree_seed_t *urs = NULL;

void *urs_node_id(void *data) {
  return &((uniq_response_t *)data)->id;
}


void verify_unique_response(ngx_str_t *uri, nchan_msg_id_t *msgid, nchan_msg_t *msg, subscriber_t *sub) {
  
  ngx_rbtree_node_t  *node;
  uniq_response_t    *urnode;
  
  u_char              idbuf[1024];
  ngx_str_t           id; //response id
  
  id.data = idbuf;
  id.len = ngx_sprintf(idbuf, "%V | %V", uri, msgid_to_str(msgid)) - idbuf;
  
  if(urs == NULL) {
    urs = &uniq_rsp_seed;
    rbtree_init(urs, "unique response verifier", urs_node_id, NULL, NULL);
  }
  
  int msglen = 0;
  if(!msg->buf->in_file) {
    msglen = msg->buf->end - msg->buf->start;
  }
  
  if((node = rbtree_find_node(urs, &id)) != NULL) {
    urnode = rbtree_data_from_node(node);
    
    assert(msg->buf == urnode->msg.buf);
    assert(urnode->smallmsg.len == msglen);
    assert(ngx_strncmp(urnode->smallmsg.data, msg->buf->start, msglen) == 0);
    
    urnode->msg = *msg;
    urnode->sub = *sub;
  }
  else {
    
    node = rbtree_create_node(urs, sizeof(*urnode) + id.len + msglen);
    urnode = rbtree_data_from_node(node);
    urnode->id.data = (u_char *)&urnode[1];
    urnode->id.len = id.len;
    ngx_memcpy(urnode->id.data, id.data, id.len);
    
    if(!msg->buf->in_file) {
      urnode->smallmsg.len = msglen;
      urnode->smallmsg.data = urnode->id.data + id.len;
      ngx_memcpy(urnode->smallmsg.data, msg->buf->start, msglen);
      urnode->smallmsg.len = msglen;
    }
    else {
      urnode->smallmsg.len = 0;
      urnode->smallmsg.data = NULL;
    }
    
    urnode->msg = *msg;
    urnode->sub = *sub;
    
    rbtree_insert_node(urs, node);
  }
  
}
*/




