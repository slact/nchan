#include <ngx_http_push_module.h>
#include <ngx_crypt.h>
#include <ngx_sha1.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:WEBSOCKET:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:WEBSOCKET:" fmt, ##arg)
#include <assert.h>

#define WEBSOCKET_LAST_FRAME                0x8

#define WEBSOCKET_OPCODE_TEXT               0x1
#define WEBSOCKET_OPCODE_CLOSE              0x8
#define WEBSOCKET_OPCODE_PING               0x9
#define WEBSOCKET_OPCODE_PONG               0xA

#define WEBSOCKET_READ_START_STEP           0
#define WEBSOCKET_READ_GET_REAL_SIZE_STEP   1
#define WEBSOCKET_READ_GET_MASK_KEY_STEP    2
#define WEBSOCKET_READ_GET_PAYLOAD_STEP     3

//debugstuff
void memstore_fakeprocess_push(ngx_int_t slot);
void memstore_fakeprocess_push_random(void);
void memstore_fakeprocess_pop();
ngx_int_t memstore_slot();

static const subscriber_t new_websocket_sub;

typedef struct {
  u_char fin:1;
  u_char rsv1:1;
  u_char rsv2:1;
  u_char rsv3:1;
  u_char opcode:4;
  u_char mask:1;
  u_char mask_key[4];
  uint64_t payload_len;
  u_char  header[8];
  u_char *payload;
  u_char *last;
  ngx_uint_t step;
} ws_frame_t;

typedef struct {
  subscriber_t            sub;
  ngx_http_request_t     *request;
  ngx_http_cleanup_t     *cln;
  subscriber_callback_pt  dequeue_handler;
  void                   *dequeue_handler_data;
  ngx_event_t             timeout_ev;
  subscriber_callback_pt  timeout_handler;
  void                   *timeout_handler_data;
  ngx_int_t               owner;
  ws_frame_t              frame;
  unsigned                shook_hands:1;
  unsigned                finalize_request:1;
  unsigned                already_enqueued:1;
  unsigned                awaiting_destruction:1;
  unsigned                reserved;
} full_subscriber_t;

static void sudden_abort_handler(subscriber_t *sub) {
#if FAKESHARD
  full_subscriber_t  *fsub = (full_subscriber_t  *)sub;
  memstore_fakeprocess_push(fsub->data.owner);
#endif
  sub->dequeue(sub);
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}


static void empty_handler() { }

static void websocket_init_frame(ws_frame_t *frame) {
  ngx_memzero(frame, sizeof(*frame)); 
  frame->step = WEBSOCKET_READ_START_STEP;
  frame->last = NULL;
  frame->payload = NULL;
}

subscriber_t *websocket_subscriber_create(ngx_http_request_t *r) {
  DBG("create for req %p", r);
  full_subscriber_t  *fsub;
  //TODO: allocate from pool (but not the request's pool)
  if((fsub = ngx_alloc(sizeof(*fsub), ngx_cycle->log)) == NULL) {
    ERR("Unable to allocate");
    return NULL;
  }
  ngx_memcpy(&fsub->sub, &new_websocket_sub, sizeof(new_websocket_sub));
  fsub->request = r;
  fsub->cln = NULL;
  fsub->finalize_request = 0;
  fsub->shook_hands = 0;
  fsub->sub.cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  
  ngx_memzero(&fsub->timeout_ev, sizeof(fsub->timeout_ev));
  fsub->timeout_handler = empty_handler;
  fsub->timeout_handler_data = NULL;
  fsub->dequeue_handler = empty_handler;
  fsub->dequeue_handler_data = NULL;
  fsub->already_enqueued = 0;
  fsub->awaiting_destruction = 0;
  fsub->reserved = 0;
  
  websocket_init_frame(&fsub->frame);
  
  fsub->owner = memstore_slot();
  
  //http request sudden close cleanup
  if((fsub->cln = ngx_http_cleanup_add(r, 0)) == NULL) {
    ERR("Unable to add request cleanup for websocket subscriber");
    return NULL;
  }
  fsub->cln->data = fsub;
  fsub->cln->handler = (ngx_http_cleanup_pt )sudden_abort_handler;
  DBG("%p created for request %p", &fsub->sub, r);
  
  ngx_http_set_ctx(r, fsub, ngx_http_push_module); //gonna need this for recv
  
  return &fsub->sub;
}

ngx_int_t websocket_subscriber_destroy(subscriber_t *sub) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)sub;
  if(fsub->reserved > 0) {
    DBG("%p not ready to destroy (reserved for %i) for req %p", sub, fsub->reserved, fsub->request);
    fsub->awaiting_destruction = 1;
  }
  else {
    DBG("%p destroy for req %p", sub, fsub->request);
    ngx_free(fsub);
  }
  return NGX_OK;
}


static void websocket_perform_handshake(full_subscriber_t *fsub) {
  static ngx_str_t    magic = ngx_string("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
  ngx_str_t           ws_accept_key, sha1_str;
  u_char              buf_sha1[21];
  u_char              buf[255];
  ngx_str_t          *tmp, *ws_key;
  ngx_int_t           ws_version;
  ngx_http_request_t *r = fsub->request;
  
  ngx_sha1_t          sha1;
  
  ws_accept_key.data = buf;
  
  r->headers_out.content_length_n = 0;
  r->header_only = 1;  
  
  if((tmp = ngx_http_push_get_header_value(r, NGX_HTTP_PUSH_HEADER_SEC_WEBSOCKET_VERSION)) == NULL) {
    r->headers_out.status = NGX_HTTP_BAD_REQUEST;
    fsub->sub.dequeue_after_response=1;
  }
  ws_version=ngx_atoi(tmp->data, tmp->len);
  if(ws_version != 13) {
    r->headers_out.status = NGX_HTTP_BAD_REQUEST;
    fsub->sub.dequeue_after_response=1;
  }
  
  if((ws_key = ngx_http_push_get_header_value(r, NGX_HTTP_PUSH_HEADER_SEC_WEBSOCKET_KEY)) == NULL) {
    r->headers_out.status = NGX_HTTP_BAD_REQUEST;
    fsub->sub.dequeue_after_response=1;
  }
  
  if(r->headers_out.status != NGX_HTTP_BAD_REQUEST) {
    //generate accept key
    ngx_sha1_init(&sha1);
    ngx_sha1_update(&sha1, ws_key->data, ws_key->len);
    ngx_sha1_update(&sha1, magic.data, magic.len);
    ngx_sha1_final(buf_sha1, &sha1);
    sha1_str.len=20;
    sha1_str.data=buf_sha1;
    
    ws_accept_key.len=ngx_base64_encoded_length(sha1_str.len);
    assert(ws_accept_key.len < 255);
    ngx_encode_base64(&ws_accept_key, &sha1_str);
    
    
    ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_SEC_WEBSOCKET_ACCEPT, &ws_accept_key);
    ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_UPGRADE, &NGX_HTTP_PUSH_WEBSOCKET);
    ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_CONNECTION, &NGX_HTTP_PUSH_UPGRADE);
    r->headers_out.status_line = NGX_HTTP_PUSH_HTTP_STATUS_101;
    r->headers_out.status = NGX_HTTP_SWITCHING_PROTOCOLS;

    r->keepalive=0; //apparently, websocket must not use keepalive.
  }
  
  ngx_http_send_header(r);
}


static void websocket_reading(ngx_http_request_t *r);

static ngx_int_t ensure_handshake(full_subscriber_t *fsub) {
  if(fsub->shook_hands == 0) {
    fsub->request->read_event_handler = websocket_reading;
    fsub->request->write_event_handler = ngx_http_request_empty_handler;
    fsub->request->main->count++; //this is the right way to hold and finalize the
    websocket_perform_handshake(fsub);
    fsub->shook_hands = 1;
    return NGX_OK;
  }
  return NGX_DECLINED;
}

static ngx_int_t websocket_reserve(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  DBG("%p reserve for req %p", self, fsub->request);
  ensure_handshake(fsub);
  fsub->reserved++;
  return NGX_OK;
}
static ngx_int_t websocket_release(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  DBG("%p release for req %p", self, fsub->request);
  assert(fsub->reserved > 0);
  fsub->reserved--;
  if(fsub->awaiting_destruction == 1 && fsub->reserved == 0) {
    ngx_free(fsub);
    return NGX_ABORT;
  }
  else {
    return NGX_OK;
  }
}

static ngx_int_t websocket_enqueue(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  ensure_handshake(fsub);
  
  //TODO
  return NGX_OK;
}

static ngx_int_t websocket_dequeue(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)fsub;
  //TODO
  return NGX_OK;
}

static ngx_int_t websocket_respond_message(subscriber_t *self, ngx_http_push_msg_t *msg) {
  //TODO
  return NGX_OK;
}

static ngx_int_t websocket_respond_status(subscriber_t *self, ngx_int_t status_code, const ngx_str_t *status_line) {
  //TODO
  return NGX_OK;
}

static ngx_int_t websocket_set_timeout_callback(subscriber_t *self, subscriber_callback_pt cb, void *privdata) {
  //TODO
  return NGX_OK;
}

static ngx_int_t websocket_set_dequeue_callback(subscriber_t *self, subscriber_callback_pt cb, void *privdata) {
  //TODO
  return NGX_OK;
}

static const subscriber_t new_websocket_sub = {
  &websocket_enqueue,
  &websocket_dequeue,
  &websocket_respond_message,
  &websocket_respond_status,
  &websocket_set_timeout_callback,
  &websocket_set_dequeue_callback,
  &websocket_reserve,
  &websocket_release,
  "websocket",
  WEBSOCKET,
  0, //deque after response
  1, //destroy after dequeue
  NULL
};






static ngx_flag_t is_utf8(u_char *, size_t);
static void flush_pending_output(ngx_http_request_t *);




static ngx_int_t ws_recv(ngx_connection_t *c, ngx_event_t *rev, ngx_buf_t *buf, ssize_t len) {
  ssize_t         n;
  n = c->recv(c, buf->last, (ssize_t) len - (buf->last - buf->start));

  if (n == NGX_AGAIN) {
    return NGX_AGAIN;
  }

  if ((n == NGX_ERROR) || (n == 0)) {
    return NGX_ERROR;
  }
  buf->pos = buf->last;
  buf->last += n;

  if ((buf->last - buf->start) < len) {
    return NGX_AGAIN;
  }

  return NGX_OK;
}

static uint64_t ws_ntohll(uint64_t value) {
  int num = 42;
  if (*(char *)&num == 42) {
    uint32_t high_part = ntohl((uint32_t)(value >> 32));
    uint32_t low_part = ntohl((uint32_t)(value & 0xFFFFFFFFLL));
    return (((uint64_t)low_part) << 32) | high_part;
  } else {
    return value;
  }
}

static const u_char WEBSOCKET_CLOSE_LAST_FRAME_BYTE[] = {WEBSOCKET_OPCODE_CLOSE | (WEBSOCKET_LAST_FRAME << 4), 0x00};
static const u_char WEBSOCKET_PING_LAST_FRAME_BYTE[]  = {WEBSOCKET_OPCODE_PING  | (WEBSOCKET_LAST_FRAME << 4), 0x00};
static const u_char WEBSOCKET_PONG_LAST_FRAME_BYTE[]  = {WEBSOCKET_OPCODE_PONG  | (WEBSOCKET_LAST_FRAME << 4), 0x00};


static void set_buffer(ngx_buf_t *buf, u_char *start, u_char *last, ssize_t len) {
  ngx_memzero(buf, sizeof(*buf));
  buf->start = start;
  buf->pos = buf->start;
  buf->last = (last != NULL) ? last : start;
  buf->end = buf->start + len;
  buf->temporary = 0;
  buf->memory = 1;
}

/* based on code from push stream module, written by
 * Wandenberg Peixoto <wandenberg@gmail.com>, Rogério Carvalho Schneider <stockrt@gmail.com>
 * thanks, guys!
*/
static void websocket_reading(ngx_http_request_t *r) {
  full_subscriber_t          *fsub = ngx_http_get_module_ctx(r, ngx_http_push_module);
  ws_frame_t                 *frame = &fsub->frame;
  ngx_int_t                   rc = NGX_OK;
  ngx_event_t                *rev;
  ngx_connection_t           *c;
  uint64_t                    i;
  ngx_buf_t                   buf;
  ngx_str_t                   msg_in_str;
  ngx_pool_t                 *temp_pool = NULL;
  
  set_buffer(&buf, frame->header, frame->last, 8);

  ERR("fsub: %p, frame: %p", fsub, frame);
  
  c = r->connection;
  rev = c->read;

  for (;;) {
    if (c->error || c->timedout || c->close || c->destroyed || rev->closed || rev->eof) {
      goto finalize;
    }
    
    switch (frame->step) {
      case WEBSOCKET_READ_START_STEP:
        //reading frame header
        if ((rc = ws_recv(c, rev, &buf, 2)) != NGX_OK) {
          goto exit;
        }
        
        frame->fin  = (frame->header[0] >> 7) & 1;
        frame->rsv1 = (frame->header[0] >> 6) & 1;
        frame->rsv2 = (frame->header[0] >> 5) & 1;
        frame->rsv3 = (frame->header[0] >> 4) & 1;
        frame->opcode = frame->header[0] & 0xf;
        
        frame->mask = (frame->header[1] >> 7) & 1;
        frame->payload_len = frame->header[1] & 0x7f;
        
        frame->step = WEBSOCKET_READ_GET_REAL_SIZE_STEP;
        break;
      
      case WEBSOCKET_READ_GET_REAL_SIZE_STEP:
        switch(frame->payload_len) {
          uint64_t len;
          case 126:
            if ((rc = ws_recv(c, rev, &buf, 2)) != NGX_OK) {
              goto exit;
            }
            ngx_memcpy(&len, frame->header, 2);
            frame->payload_len = ntohs(len);
            break;
            
          case 127:
            if ((rc = ws_recv(c, rev, &buf, 8)) != NGX_OK) {
              goto exit;
            }
            ngx_memcpy(&len, frame->header, 8);
            frame->payload_len = ws_ntohll(len);
            break;
        }
        
        frame->step = WEBSOCKET_READ_GET_MASK_KEY_STEP;
        break;
      
      case WEBSOCKET_READ_GET_MASK_KEY_STEP:
        if (frame->mask) {
          if ((rc = ws_recv(c, rev, &buf, 4)) != NGX_OK) {
            goto exit;
          }
          ngx_memcpy(frame->mask_key, buf.start, 4);
        }
        
        frame->step = WEBSOCKET_READ_GET_PAYLOAD_STEP;
        break;
      
      case WEBSOCKET_READ_GET_PAYLOAD_STEP:
        if ((frame->opcode != WEBSOCKET_OPCODE_TEXT) && (frame->opcode != WEBSOCKET_OPCODE_CLOSE) && (frame->opcode != WEBSOCKET_OPCODE_PING) && (frame->opcode != WEBSOCKET_OPCODE_PONG)) {
          //TODO
          //rc = ngx_http_push_stream_send_response_text(r, NGX_HTTP_PUSH_STREAM_WEBSOCKET_CLOSE_LAST_FRAME_BYTE, sizeof(NGX_HTTP_PUSH_STREAM_WEBSOCKET_CLOSE_LAST_FRAME_BYTE), 1);
          goto finalize;
        }
        
        if (frame->payload_len > 0) {
          //create a temporary pool to allocate temporary elements
          if (temp_pool == NULL) {
            if ((temp_pool = ngx_create_pool(4096, r->connection->log)) == NULL) {
              ERR("unable to allocate memory for temporary pool");
              goto finalize;
            }
            if ((frame->payload = ngx_pcalloc(temp_pool, frame->payload_len)) == NULL) {
              ERR("unable to allocate memory for payload");
              goto finalize;
            }
            frame->last = frame->payload;
          }
          
          set_buffer(&buf, frame->payload, frame->last, frame->payload_len);
          
          if ((rc = ws_recv(c, rev, &buf, frame->payload_len)) != NGX_OK) {
            goto exit;
          }
          
          if (frame->mask) {
            //stupid overcomplicated websockets and their masks
            for (i = 0; i < frame->payload_len; i++) {
              frame->payload[i] = frame->payload[i] ^ frame->mask_key[i % 4];
            }
          }
          
          if (!is_utf8(frame->payload, frame->payload_len)) {
            goto finalize;
          }
          
          if (frame->opcode == WEBSOCKET_OPCODE_TEXT) {
            ERR("wtf is this shit");
            msg_in_str.data=frame->payload;
            msg_in_str.len=frame->payload_len;
            /*
            for (q = ngx_queue_head(&subscriber->subscriptions); q != ngx_queue_sentinel(&subscriber->subscriptions); q = ngx_queue_next(q)) {
              ngx_http_push_stream_subscription_t *subscription = ngx_queue_data(q, ngx_http_push_stream_subscription_t, queue);
              if (subscription->channel->for_events) {
                // skip events channel on publish by websocket connections
                continue;
              }
              if (ngx_http_push_stream_add_msg_to_channel(mcf, r->connection->log, subscription->channel, frame->payload, frame->payload_len, NULL, NULL, cf->store_messages, temp_pool) != NGX_OK) {
                goto finalize;
              }
            }
            */
          }
          
          if (temp_pool != NULL) {
            ngx_destroy_pool(temp_pool);
            temp_pool = NULL;
          }
        }

        frame->step = WEBSOCKET_READ_START_STEP;
        frame->last = NULL;

        if (frame->opcode == WEBSOCKET_OPCODE_PING) {
          //TODO
          //ngx_http_push_stream_send_response_text(r, WEBSOCKET_PONG_LAST_FRAME_BYTE, sizeof(WEBSOCKET_PONG_LAST_FRAME_BYTE), 1);
        }

        if (frame->opcode == WEBSOCKET_OPCODE_CLOSE) {
          //TODO
          //rc = ngx_http_push_stream_send_response_text(r, WEBSOCKET_CLOSE_LAST_FRAME_BYTE, sizeof(WEBSOCKET_CLOSE_LAST_FRAME_BYTE), 1);
          goto finalize;
        }
        return;

        break;

      default:
        ngx_log_debug(NGX_LOG_ERR, c->log, 0, "push stream module: unknown websocket step (%d)", frame->step);
        goto finalize;
        break;
    }
    
    set_buffer(&buf, frame->header, NULL, 8);
  }

exit:
  if (rc == NGX_AGAIN) {
    frame->last = buf.last;
    if (!c->read->ready) {
      if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
        ngx_log_error(NGX_LOG_INFO, c->log, ngx_socket_errno, "push stream module: failed to restore read events");
        goto finalize;
      }
    }
  }

  if (rc == NGX_ERROR) {
    rev->eof = 1;
    c->error = 1;
    ngx_log_error(NGX_LOG_INFO, c->log, ngx_socket_errno, "push stream module: client closed prematurely connection");
    goto finalize;
  }

  return;

finalize:

  //TODO
  //ngx_http_push_stream_run_cleanup_pool_handler(r->pool, (ngx_pool_cleanup_pt) ngx_http_push_stream_cleanup_request_context);
  
  ngx_http_finalize_request(r, c->error ? NGX_HTTP_CLIENT_CLOSED_REQUEST : NGX_OK);
  
}



static ngx_int_t output_filter(ngx_http_request_t *r, ngx_chain_t *in) {
  ngx_http_core_loc_conf_t               *clcf;
  //ngx_http_push_stream_module_ctx_t      *ctx = NULL;
  ngx_int_t                               rc;
  ngx_event_t                            *wev;
  ngx_connection_t                       *c;

  c = r->connection;
  wev = c->write;

  rc = ngx_http_output_filter(r, in);

  //if ((rc == NGX_OK) && (ctx = ngx_http_get_module_ctx(r, ngx_http_push_stream_module)) != NULL) {
  //    ngx_chain_update_chains(r->pool, &ctx->free, &ctx->busy, &in, (ngx_buf_tag_t) &ngx_http_push_stream_module);
  //}

  if (c->buffered & NGX_HTTP_LOWLEVEL_BUFFERED) {
    ERR("what's the deal with this NGX_HTTP_LOWLEVEL_BUFFERED thing?");
    clcf = ngx_http_get_module_loc_conf(r->main, ngx_http_core_module);
    r->write_event_handler = flush_pending_output;
    if (!wev->delayed) {
      ngx_add_timer(wev, clcf->send_timeout);
    }
    if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
      return NGX_ERROR;
    }
    return NGX_OK;
  } 
  else {
    if (wev->timer_set) {
      ngx_del_timer(wev);
    }
  }
  return rc;
}


static ngx_flag_t is_utf8(u_char *p, size_t n) {
  u_char  c, *last;
  size_t  len;
  
  last = p + n;
  
  for (len = 0; p < last; len++) {
    c = *p;
    
    if (c < 0x80) {
      p++;
      continue;
    }
    
    if (ngx_utf8_decode(&p, n) > 0x10ffff) {
      /* invalid UTF-8 */
      return 0;
    }
  }
  return 1;
}


static void flush_pending_output(ngx_http_request_t *r) {
  int                        rc;
  ngx_event_t               *wev;
  ngx_connection_t          *c;
  ngx_http_core_loc_conf_t  *clcf;
  
  c = r->connection;
  wev = c->write;
  
  //ngx_log_debug2(NGX_LOG_DEBUG_HTTP, wev->log, 0, "http writer handler: \"%V?%V\"", &r->uri, &r->args);

  clcf = ngx_http_get_module_loc_conf(r->main, ngx_http_core_module);

  if (wev->timedout) {
    if (!wev->delayed) {
      ngx_log_error(NGX_LOG_INFO, c->log, NGX_ETIMEDOUT, "websocket client timed out");
      c->timedout = 1;
      ngx_http_finalize_request(r, NGX_HTTP_REQUEST_TIME_OUT);
      return;
    }
    wev->timedout = 0;
    wev->delayed = 0;

    if (!wev->ready) {
      ngx_add_timer(wev, clcf->send_timeout);
      if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
        ngx_http_finalize_request(r, 0);
      }
      return;
    }
  }
  
  if (wev->delayed || r->aio) {
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, wev->log, 0, "websocket client http writer delayed");
    if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
      ngx_http_finalize_request(r, 0);
    }
    return;
  }
  
  rc = output_filter(r, NULL);

  //ngx_log_debug3(NGX_LOG_DEBUG_HTTP, c->log, 0, "http writer output filter: %d, \"%V?%V\"", rc, &r->uri, &r->args);

  if (rc == NGX_ERROR) {
    ngx_http_finalize_request(r, rc);
    return;
  }

  if (r->buffered || r->postponed || (r == r->main && c->buffered)) {
    if (!wev->delayed) {
      ngx_add_timer(wev, clcf->send_timeout);
    }
    if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
      ngx_http_finalize_request(r, 0);
    }
    return;
  }
  //ngx_log_debug2(NGX_LOG_DEBUG_HTTP, wev->log, 0, "http writer done: \"%V?%V\"", &r->uri, &r->args);
  r->write_event_handler = ngx_http_request_empty_handler;
}

