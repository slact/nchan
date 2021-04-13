#include <nchan_module.h>
#include <subscribers/common.h>
#include <util/nchan_subrequest.h>
#include <util/nchan_fake_request.h>
#include <util/nchan_util.h>
#if nginx_version >= 1000003
#include <ngx_crypt.h>
#endif
#include <ngx_sha1.h>
#include <nginx.h>


#if (NGX_ZLIB)
#include <zlib.h>
#endif


//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:WEBSOCKET:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:WEBSOCKET:" fmt, ##arg)
#include <assert.h>

#if 0 //debug WS optimized xor
  #define WEBSOCKET_OPTIMIZED_UNMASK 1
  #define WEBSOCKET_FAKEOPTIMIZED_UNMASK 1
  #define __vector_size_bytes 32
#elif __AVX2__
  #include <immintrin.h>                     // AVX2 intrinsics
  #define WEBSOCKET_OPTIMIZED_UNMASK 1
  
  #define __vector_size_bytes 32
  #define __vector_type __m256i
  #define __vector_load_intrinsic _mm256_load_si256
  #define __vector_xor_intrinsic _mm256_xor_si256
  #define __vector_store_intrinsic _mm256_store_si256
    
#elif __SSE2__
  #define WEBSOCKET_OPTIMIZED_UNMASK 1
  #include <emmintrin.h>                     // SSE2 intrinsics
  
  #define __vector_size_bytes 16
  #define __vector_type __m128i
  #define __vector_load_intrinsic _mm_load_si128
  #define __vector_xor_intrinsic _mm_xor_si128
  #define __vector_store_intrinsic _mm_store_si128
#endif

#define WEBSOCKET_LAST_FRAME                0x8
#define WEBSOCKET_LAST_FRAME_RSV1           0xC

#define WEBSOCKET_OPCODE_TEXT               0x1
#define WEBSOCKET_OPCODE_BINARY             0x2
#define WEBSOCKET_OPCODE_CLOSE              0x8
#define WEBSOCKET_OPCODE_PING               0x9
#define WEBSOCKET_OPCODE_PONG               0xA
  
#define WEBSOCKET_READ_START_STEP           0
#define WEBSOCKET_READ_GET_REAL_SIZE_STEP   1
#define WEBSOCKET_READ_GET_MASK_KEY_STEP    2
#define WEBSOCKET_READ_GET_PAYLOAD_STEP     3


#define WEBSOCKET_FRAME_HEADER_MAX_LENGTH   146 //144 + 2 for possible close status code

#define WEBSOCKET_CLOSING_TIMEOUT           250 //ms

#define DEFLATE_MAX_WINDOW_BITS             15
#define DEFLATE_MIN_WINDOW_BITS             9 //it should be 8, but zlib doesn't support window size of 8 these days
#define DEFLATE_DEFAULT_CLIENT_WINDOW_BITS  15

/**
 * here's what a websocket frame looks like
 * 
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-------+-+-------------+-------------------------------+
  |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
  |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
  |N|V|V|V|       |S|             |   (if payload len==126/127)   |
  | |1|2|3|       |K|             |                               |
  +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
  |     Extended payload length continued, if payload len == 127  |
  + - - - - - - - - - - - - - - - +-------------------------------+
  |                               |Masking-key, if MASK set to 1  |
  +-------------------------------+-------------------------------+
  | Masking-key (continued)       |          Payload Data         |
  +-------------------------------- - - - - - - - - - - - - - - - +
  :                     Payload Data continued ...                :
  + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
  |                     Payload Data continued ...                |
  +---------------------------------------------------------------+
 */

#define CLOSE_NORMAL                 1000
#define CLOSE_GOING_AWAY             1001
#define CLOSE_PROTOCOL_ERROR         1002
#define CLOSE_UNSUPPORTED_DATA       1003
#define CLOSE_INVALID_PAYLOAD        1007
#define CLOSE_POLICY_VIOLATION       1008
#define CLOSE_MESSAGE_TOO_BIG        1009
#define CLOSE_EXTENSION_MISSING      1010
#define CLOSE_INTERNAL_SERVER_ERROR  1011

#define REQUEST_PCALLOC(r, what) what = ngx_pcalloc((r)->pool, sizeof(*(what)))
#define REQUEST_PALLOC(r, what) what = ngx_palloc((r)->pool, sizeof(*(what)))

static const u_char WEBSOCKET_PAYLOAD_LEN_16_BYTE = 126;
static const u_char WEBSOCKET_PAYLOAD_LEN_64_BYTE = 127;

static const u_char WEBSOCKET_TEXT_LAST_FRAME_BYTE =  WEBSOCKET_OPCODE_TEXT  | (WEBSOCKET_LAST_FRAME << 4);
static const u_char WEBSOCKET_TEXT_DEFLATED_LAST_FRAME_BYTE = WEBSOCKET_OPCODE_TEXT | (WEBSOCKET_LAST_FRAME_RSV1 << 4);

static const u_char WEBSOCKET_BINARY_LAST_FRAME_BYTE =  WEBSOCKET_OPCODE_BINARY  | (WEBSOCKET_LAST_FRAME << 4);
static const u_char WEBSOCKET_BINARY_DEFLATED_LAST_FRAME_BYTE = WEBSOCKET_OPCODE_BINARY  | (WEBSOCKET_LAST_FRAME_RSV1 << 4);

static const u_char WEBSOCKET_CLOSE_LAST_FRAME_BYTE = WEBSOCKET_OPCODE_CLOSE | (WEBSOCKET_LAST_FRAME << 4);
static const u_char WEBSOCKET_PONG_LAST_FRAME_BYTE  = WEBSOCKET_OPCODE_PONG  | (WEBSOCKET_LAST_FRAME << 4);
static const u_char WEBSOCKET_PING_LAST_FRAME_BYTE  = WEBSOCKET_OPCODE_PING  | (WEBSOCKET_LAST_FRAME << 4);

static ngx_str_t   binary_mimetype = ngx_string("application/octet-stream");

#define NCHAN_WS_TMP_POOL_SIZE (4*1024)


typedef struct framebuf_s framebuf_t;
struct framebuf_s {
  u_char        chr[WEBSOCKET_FRAME_HEADER_MAX_LENGTH + 10]; // +10 for the reservoir tip. just to be safe.
  framebuf_t   *prev;
  framebuf_t   *next;
};


#if FAKESHARD
//debugstuff
void memstore_fakeprocess_push(ngx_int_t slot);
void memstore_fakeprocess_push_random(void);
void memstore_fakeprocess_pop();
ngx_int_t memstore_slot();
#endif

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

typedef struct full_subscriber_s full_subscriber_t;

typedef struct nchan_pub_upstream_request_data_s nchan_pub_upstream_request_data_t;
struct nchan_pub_upstream_request_data_s {
  ngx_http_request_t                 *sr;
  ngx_buf_t                           body_buf;
  full_subscriber_t                  *fsub;
  nchan_pub_upstream_request_data_t  *next;
  unsigned                            sent:1;
  unsigned                            binary:1;
};

typedef struct {
#if (NGX_ZLIB)
  z_stream               *zstream_in;
#endif
  int8_t                  server_max_window_bits;
  int8_t                  client_max_window_bits;
  unsigned                server_no_context_takeover:1;
  unsigned                client_no_context_takeover:1;
  unsigned                enabled:1;
} permessage_deflate_t;

struct full_subscriber_s {
  subscriber_t            sub;
  ngx_http_cleanup_t     *cln;
  nchan_request_ctx_t    *ctx;
  subscriber_callback_pt  enqueue_callback;
  void                   *enqueue_callback_data;
  subscriber_callback_pt  dequeue_callback;
  void                   *dequeue_callback_data;
  ngx_event_t             timeout_ev;
  ngx_event_t             closing_ev;
  ws_frame_t              frame;
  
  ngx_event_t             ping_ev;
  
  permessage_deflate_t    deflate;
  
  struct {
    ngx_str_t               *channel_id;
    ngx_str_t               *upstream_request_url;
    ngx_pool_t              *msg_pool;
    void                   (*intercept)(subscriber_t *, nchan_msg_t *);
  }                       publisher;
  
  unsigned                awaiting_pong:1;
  unsigned                ws_meta_subprotocol:1;
  unsigned                holding:1; //make sure the request doesn't close right away
  unsigned                shook_hands:1;
  unsigned                sent_close_frame:1;
  unsigned                received_close_frame:1;
  unsigned                finalize_request:1;
  unsigned                awaiting_destruction:1;
};// full_subscriber_t

static void empty_handler() { }

static ngx_int_t websocket_send_frame(full_subscriber_t *fsub, const u_char opcode, off_t len, ngx_chain_t *chain);
static void set_buf_to_str(ngx_buf_t *buf, const ngx_str_t *str);
static ngx_chain_t *websocket_frame_header_chain(full_subscriber_t *fsub, const u_char opcode, off_t len, ngx_chain_t *chain);
static ngx_flag_t is_utf8(ngx_buf_t *);
static ngx_chain_t *websocket_close_frame_chain(full_subscriber_t *fsub, uint16_t code, ngx_str_t *err);
static ngx_int_t websocket_send_close_frame(full_subscriber_t *fsub, uint16_t code, ngx_str_t *err);
static ngx_int_t websocket_send_close_frame_cstr(full_subscriber_t *fsub, uint16_t code, const char *err);
static ngx_int_t websocket_respond_status(subscriber_t *self, ngx_int_t status_code, const ngx_str_t *status_line, ngx_chain_t *status_body);

static ngx_int_t websocket_publish(full_subscriber_t *fsub, ngx_buf_t *buf, int binary);

static ngx_int_t websocket_reserve(subscriber_t *self);
static ngx_int_t websocket_release(subscriber_t *self, uint8_t nodestroy);

static void websocket_delete_timers(full_subscriber_t *fsub);
static ngx_chain_t *websocket_msg_frame_chain(full_subscriber_t *fsub, nchan_msg_t *msg);

/*
ngx_int_t ws_reserve_tmp_pool(full_subscriber_t *fsub) {
  if(!fsub->tmp_pool) {
    fsub->tmp_pool = ngx_create_pool(NCHAN_WS_TMP_POOL_SIZE, fsub->sub.request->connection->log);
  }
  if(!fsub->tmp_pool) {
    ERR("unable to create temp pool for websocket sub %p", fsub);
    return NGX_ERROR;
  }
  fsub->tmp_pool_use_count++;
  return NGX_OK;
}

ngx_int_t ws_release_tmp_pool(full_subscriber_t *fsub) {
  if(!fsub->tmp_pool) {
    ERR("attempted to release tmp_pool when it's NULL");
    return NGX_ERROR;
  }
  if(--fsub->tmp_pool_use_count == 0) {
    ngx_destroy_pool(fsub->tmp_pool);
    fsub->tmp_pool = NULL;
  } else {
  }

  return NGX_OK;
}
*/

ngx_pool_t *ws_get_msgpool(full_subscriber_t *fsub) {
  if(!fsub->publisher.msg_pool) {
    fsub->publisher.msg_pool = ngx_create_pool(NCHAN_WS_TMP_POOL_SIZE, fsub->sub.request->connection->log);
  }
  return fsub->publisher.msg_pool;
}
ngx_int_t ws_destroy_msgpool(full_subscriber_t *fsub) {
  if(fsub->publisher.msg_pool) {
    ngx_destroy_pool(fsub->publisher.msg_pool);
    fsub->publisher.msg_pool = NULL;
  }
  return NGX_OK;
}

static ngx_int_t websocket_finalize_request(full_subscriber_t *fsub) {
  subscriber_t       *sub = &fsub->sub;
  ngx_http_request_t *r = sub->request;
  
  if(fsub->cln) {
    fsub->cln->handler = (ngx_http_cleanup_pt )empty_handler;
  }
  
  if(sub->cf->unsubscribe_request_url && sub->enqueued) {
    nchan_subscriber_unsubscribe_request(sub);
  }
  nchan_subscriber_subrequest_cleanup(sub);
  
  sub->status = DEAD;
  if(sub->enqueued) {
    sub->fn->dequeue(sub);
  }
  nchan_http_finalize_request(r, NGX_HTTP_OK);
  return NGX_OK;
}

static void aborted_ws_close_request_rev_handler(ngx_http_request_t *r) {
  nchan_request_ctx_t        *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  full_subscriber_t          *fsub = (full_subscriber_t *)ctx->sub;
  if(fsub) {
    ctx->sub->status = DEAD;
    fsub->sub.request->headers_out.status = NGX_HTTP_CLIENT_CLOSED_REQUEST;
    websocket_finalize_request(fsub);
  }
}

static void sudden_abort_handler(subscriber_t *sub) {
  //DBG("sudden abort handler for sub %p request %p", sub, sub->request);
  if(!sub) {
    //DBG("%p already freed apparently?...", sub);
    return; //websocket subscriber already freed
  }
#if FAKESHARD
  full_subscriber_t  *fsub = (full_subscriber_t  *)sub;
  memstore_fakeprocess_push(fsub->sub.owner);
#endif
  sub->request->read_event_handler = aborted_ws_close_request_rev_handler;
  sub->fn->dequeue(sub);
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}

static void ws_request_empty_handler(ngx_http_request_t *r) {
  nchan_request_ctx_t        *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  full_subscriber_t          *fsub = (full_subscriber_t *)ctx->sub;  
  
  if(r->connection->read->eof) {
    //sudden disconnect
    nchan_add_oneshot_timer((void (*)(void *))sudden_abort_handler, &fsub->sub, 0);
  }
}

/*
static void sudden_upstream_request_abort_handler(full_subscriber_t *fsub) {
  
}
*/

static void init_buf(ngx_buf_t *buf, int8_t last);
static void init_msg_buf(ngx_buf_t *buf);

#if WEBSOCKET_OPTIMIZED_UNMASK
static void websocket_unmask_frame(ws_frame_t *frame) {
  //stupid overcomplicated websockets and their masks
  
  uint64_t   i, j, payload_len = frame->payload_len;
  u_char    *payload = frame->payload;
  u_char    *mask_key = frame->mask_key;
  uint64_t   preamble_len = payload_len <= __vector_size_bytes ? payload_len : (uintptr_t )payload % __vector_size_bytes;
  uint64_t   fastlen;
  u_char     extended_mask[__vector_size_bytes];  
  
  //preamble -- for alignment or msglen < __vector_size_bytes
  for (i = 0; i < preamble_len && i < payload_len; i++) {
    payload[i] ^= mask_key[i % 4];
  }
  //are we done?
  if(payload_len < __vector_size_bytes) {
    return;
  }
  
  if((uintptr_t )(&payload[i]) % __vector_size_bytes != 0) { //didn't get alignment right for some reason
    fastlen = 0;
  }
  else {
    fastlen = ((payload_len - i) / __vector_size_bytes) * __vector_size_bytes;
  }
    
#if WEBSOCKET_FAKEOPTIMIZED_UNMASK
  for (/*void*/; i < fastlen + preamble_len; i++) {
    payload[i] ^= mask_key[i % 4];
  }
#else
  __vector_type    w, w_mask;
  
  if (fastlen > 0) {
    for(j=0; j<__vector_size_bytes; j+=4) {
      ngx_memcpy(&extended_mask[j], mask_key, 4);
    }
    w_mask = __vector_load_intrinsic((__vector_type *)extended_mask);
    for (/*void*/; i < fastlen + preamble_len; i += __vector_size_bytes) {
      w = __vector_load_intrinsic((__vector_type *)&payload[i]);       // load [__vector_size_bytes] bytes
      w = __vector_xor_intrinsic(w, w_mask);           // XOR with mask
      __vector_store_intrinsic((__vector_type *)&payload[i], w);   // store [__vector_size_bytes] masked bytes
    }
  }
#endif
  
  //leftovers
  for (/*void*/; i < payload_len; i++) {
    payload[i] ^= mask_key[i % 4];
  }
  
}
#else
static void websocket_unmask_frame(ws_frame_t *frame) {
  //stupid overcomplicated websockets and their masks
  uint64_t   i, mpayload_len = frame->payload_len;
  u_char    *mpayload = frame->payload;
  u_char    *mask_key = frame->mask_key;
  
  for (i = 0; i < mpayload_len; i++) {
    mpayload[i] ^= mask_key[i % 4];
  }
}
#endif

static ngx_int_t ws_output_filter(full_subscriber_t *fsub, ngx_chain_t *chain) {
  /*if(fsub->publish_upstream && fsub->sub.request->pool == fsub->publish_upstream->temp_request_pool) {
    ngx_int_t rc;
    fsub->sub.request->pool = fsub->publish_upstream->real_request_pool;
    rc = nchan_output_filter(fsub->sub.request, chain);
    fsub->sub.request->pool = fsub->publish_upstream->temp_request_pool;
    return rc;
  }
  else {
    return nchan_output_filter(fsub->sub.request, chain);
  }*/
  //placate google's devious ngx_pagespeed by not using the header_only hack
  ngx_http_request_t *r = fsub->sub.request;
  r->header_only = 0;
  r->chunked = 0;
  return nchan_output_filter(r, chain);
}

static ngx_int_t ws_output_msg_filter(full_subscriber_t *fsub, nchan_msg_t *msg) {
  /*if(fsub->publish_upstream && fsub->sub.request->pool != fsub->publish_upstream->real_request_pool) {
    ngx_int_t rc;
    ngx_pool_t *fake_pool = fsub->sub.request->pool;
    fsub->sub.request->pool = fsub->publish_upstream->real_request_pool;
    rc = nchan_output_msg_filter(fsub->sub.request, msg, websocket_msg_frame_chain(fsub, msg));
    fsub->sub.request->pool = fake_pool;
    return rc;
  }
  else {
    return nchan_output_msg_filter(fsub->sub.request, msg, websocket_msg_frame_chain(fsub, msg));
  }*/
  return nchan_output_msg_filter(fsub->sub.request, msg, websocket_msg_frame_chain(fsub, msg));
}

typedef struct {
  full_subscriber_t *fsub;
  ngx_pool_t        *pool;
  ngx_buf_t         *msgbuf;
  nchan_fakereq_subrequest_data_t *subrequest;
  unsigned           binary:1;
  nchan_msg_t        msg;
} ws_publish_data_t;


static ngx_int_t websocket_publish_callback(ngx_int_t status, nchan_channel_t *ch, ws_publish_data_t *d) {
  time_t                 last_seen = 0;
  ngx_uint_t             subscribers = 0;
  ngx_uint_t             messages = 0;
  full_subscriber_t     *fsub = d->fsub;
  nchan_msg_id_t        *msgid = NULL;
  ngx_http_request_t    *r = fsub->sub.request;
  ngx_str_t             *accept_header = NULL;
  ngx_buf_t             *tmp_buf;
  nchan_buf_and_chain_t *bc;
  if(ch) {
    subscribers = ch->subscribers;
    last_seen = ch->last_seen;
    messages  = ch->messages;
    msgid = &ch->last_published_msg_id;
    fsub->ctx->channel_subscriber_last_seen = last_seen;
    fsub->ctx->channel_subscriber_count = subscribers;
    fsub->ctx->channel_message_count = messages;
  }
  if(d->subrequest) {
    nchan_requestmachine_request_cleanup_manual(d->subrequest);
  }
  else {
    ngx_destroy_pool(d->pool);
  }
  d = NULL;
  
  if(websocket_release(&fsub->sub, 0) == NGX_ABORT || fsub->sub.status == DEAD) {
    //zombie publisher
    //nothing more to do, we're finished here
    return NGX_OK;
  }
  
  switch(status) {
    case NCHAN_MESSAGE_QUEUED:
    case NCHAN_MESSAGE_RECEIVED:
      nchan_maybe_send_channel_event_message(fsub->sub.request, CHAN_PUBLISH);
      if(fsub->sub.cf->sub.websocket) {
        //don't reply with status info, this websocket is used for subscribing too,
        //so it should only be receiving messages
        return NGX_OK;
      }
      accept_header = nchan_get_accept_header_value(r);
      bc = nchan_bufchain_pool_reserve(fsub->ctx->bcp, 1);
      tmp_buf = nchan_channel_info_buf(accept_header, messages, subscribers, last_seen, msgid, NULL);
      ngx_memcpy(&bc->buf, tmp_buf, sizeof(*tmp_buf));
      bc->buf.last_buf=1;
      
      ws_output_filter(fsub, websocket_frame_header_chain(fsub, WEBSOCKET_TEXT_LAST_FRAME_BYTE, ngx_buf_size((&bc->buf)), &bc->chain));
      break;
    case NGX_HTTP_INSUFFICIENT_STORAGE:
      websocket_respond_status(&fsub->sub, NGX_HTTP_INSUFFICIENT_STORAGE, NULL, NULL);
      break;
    case NGX_ERROR:
    case NGX_HTTP_INTERNAL_SERVER_ERROR:
      websocket_respond_status(&fsub->sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, NULL);
      break;
  }
  return NGX_OK;
}

static void websocket_publish_continue(ws_publish_data_t *d) {
  nchan_msg_t             *msg = &d->msg;
  struct timeval           tv;
  full_subscriber_t       *fsub = d->fsub;
  ngx_http_request_t      *r = d->fsub->sub.request;
  
  ngx_memzero(msg, sizeof(*msg));
  
  msg->buf=*d->msgbuf;
  if(r->headers_in.content_type) {
    msg->content_type = &r->headers_in.content_type->value;
  }
  
  ngx_gettimeofday(&tv);
  msg->id.time = tv.tv_sec;
  msg->id.tag.fixed[0]=0;
  msg->id.tagcount=1;
  msg->id.tagactive=0;
  
  if(d->binary) {
    msg->content_type = &binary_mimetype;
  }
  //ERR("%p pool sz %d", fsub, r->pool->total_allocd);
  msg->storage = NCHAN_MSG_POOL;
  
  if(nchan_need_to_deflate_message(fsub->sub.cf)) {
    nchan_deflate_message_if_needed(msg, fsub->sub.cf, r, d->pool);
  }
  
  if(fsub->publisher.intercept) {
    fsub->publisher.intercept(&fsub->sub, msg);
    ngx_destroy_pool(d->pool);
  }
  else {
    websocket_reserve(&fsub->sub);
    fsub->sub.cf->storage_engine->publish(fsub->publisher.channel_id, msg, fsub->sub.cf, (callback_pt )websocket_publish_callback, d); 
    nchan_update_stub_status(total_published_messages, 1);
  }
  
}


static ngx_int_t websocket_heartbeat(full_subscriber_t *fsub, ngx_buf_t *buf) {
  ngx_str_t      str_in;
  if(!fsub->sub.cf->websocket_heartbeat.enabled) {
    return NGX_DECLINED;
  }
  
  str_in.data = buf->pos;
  str_in.len = buf->last - buf->pos;
  if(nchan_ngx_str_match(fsub->sub.cf->websocket_heartbeat.in, &str_in)) {
    nchan_buf_and_chain_t *bc = nchan_bufchain_pool_reserve(fsub->ctx->bcp, 1);
    init_buf(&bc->buf, 1);
    set_buf_to_str(&bc->buf, fsub->sub.cf->websocket_heartbeat.out);
    websocket_send_frame(fsub, WEBSOCKET_TEXT_LAST_FRAME_BYTE, fsub->sub.cf->websocket_heartbeat.out->len, &bc->chain);
    return NGX_OK;
  }
  else {
    return NGX_DECLINED;
  }
}

ngx_int_t websocket_publish_upstream_handler(ngx_int_t rc, ngx_http_request_t *sr, void *pd) {
  ws_publish_data_t       *d = pd;
  full_subscriber_t       *fsub = d->fsub;
  
  assert(d->subrequest);
  
  if(websocket_release(&fsub->sub, 0) == NGX_ABORT || rc == NGX_ABORT) {
    //websocket client disappered, or subrequest got canceled some other way
    nchan_requestmachine_request_cleanup_manual(d->subrequest);
    return NGX_OK;
  }
  
  if(rc == NGX_OK) {
    ngx_int_t                        code = sr->headers_out.status;
    ngx_int_t                        content_length;
    ngx_chain_t                     *request_chain;
    
    switch(code) {
      case NGX_HTTP_OK:
      case NGX_HTTP_CREATED:
      case NGX_HTTP_ACCEPTED:
        if(sr->upstream) {
          ngx_buf_t    *buf;
          
          content_length = nchan_subrequest_content_length(sr);
#if nginx_version >= 1013010
          request_chain = sr->out;
#else
          request_chain = sr->upstream->out_bufs;
#endif
          if(content_length > 0 && request_chain) {
            if (request_chain->next != NULL) {
              buf = nchan_chain_to_single_buffer(d->pool, request_chain, content_length);
            }
            else {
              buf = request_chain->buf;
              if(buf->memory) {
                buf->start = buf->pos;
                buf->end = buf->last;
                buf->last_in_chain = 1;
                buf->last_buf = 1;
              }
            }
          }
          else {
            buf = ngx_pcalloc(d->pool, sizeof(*buf));
            buf->memory=1;
            buf->last_in_chain=1;
            buf->last_buf=1;
          }
          d->msgbuf = buf;
          websocket_publish_continue(d);
        }
        else {
          request_chain = NULL;
          ERR("upstream missing from upstream subrequest");
        }
        
        break;
      
      case NGX_HTTP_NOT_MODIFIED:
        websocket_publish_continue(d);
        //cleaned up later
        break;
        
      case NGX_HTTP_NO_CONTENT:
        //cancel publishing
        nchan_requestmachine_request_cleanup_manual(d->subrequest);
        break;
      
      default:
        nchan_requestmachine_request_cleanup_manual(d->subrequest);
        websocket_respond_status(&fsub->sub, NGX_HTTP_FORBIDDEN, NULL, NULL);
        break;
    }
  }
  else {
    nchan_requestmachine_request_cleanup_manual(d->subrequest);
    websocket_respond_status(&fsub->sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, NULL);
  }
  
  
  
  return NGX_OK;
}
static ngx_int_t websocket_publish(full_subscriber_t *fsub, ngx_buf_t *buf, int binary) {
#if (NGX_DEBUG_POOL)
  ERR("ws request pool size: %V", ngx_http_debug_pool_str(fsub->sub.request->pool));
#endif
  
  ngx_int_t          rc = NGX_OK;
  ws_publish_data_t *d = ngx_palloc(ws_get_msgpool(fsub), sizeof(*d));
  if(d == NULL) {
    return NGX_ERROR;
  }
  d->fsub = fsub;
  d->binary = binary;
  //move the msg pool
  d->pool = fsub->publisher.msg_pool;
  d->msgbuf = buf;
  fsub->publisher.msg_pool = NULL;
  
  if(fsub->publisher.intercept || fsub->publisher.upstream_request_url == NULL) { // don't need to send request upstream
    d->subrequest = NULL;
    websocket_publish_continue(d);
  }
  else {
    nchan_requestmachine_request_params_t param;
    param.url.str = fsub->publisher.upstream_request_url;
    param.url_complex = 0;
    param.pool = d->pool;
    param.body = buf;
    param.response_headers_only = 0;
    param.cb = (callback_pt )websocket_publish_upstream_handler;
    param.pd = d;
    param.manual_cleanup = 1;
    
    websocket_reserve(&d->fsub->sub);
    
    d->subrequest = nchan_subscriber_subrequest(&fsub->sub, &param);
    
    rc = d->subrequest == NULL ? NGX_ERROR : NGX_OK;
  }
  
  return rc;
}

static void websocket_init_frame(ws_frame_t *frame) {
  ngx_memzero(frame, sizeof(*frame)); 
  frame->step = WEBSOCKET_READ_START_STEP;
  frame->last = NULL;
  frame->payload = NULL;
}

static void *framebuf_alloc(void *pd) {
  return ngx_palloc((ngx_pool_t *)pd, sizeof(framebuf_t));
}

static void closing_ev_handler(ngx_event_t *ev) {
  full_subscriber_t *fsub = (full_subscriber_t *)ev->data;
  DBG("closing_ev timer handler for %p, delayed", fsub);
  //fsub->sub.status = DEAD;
  websocket_finalize_request(fsub);
}

subscriber_t *websocket_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id) {
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  char                 *err;
  
  DBG("create for req %p", r);
  full_subscriber_t  *fsub = NULL;
  if((fsub = ngx_alloc(sizeof(*fsub), ngx_cycle->log)) == NULL) {
    err="Unable to allocate";
    goto fail;
  }
  
  nchan_subscriber_init(&fsub->sub, &new_websocket_sub, r, msg_id);
  fsub->cln = NULL;
  fsub->ctx = ctx;
  fsub->ws_meta_subprotocol = 0;
  fsub->finalize_request = 0;
  fsub->holding = 0;
  fsub->shook_hands = 0;
  fsub->awaiting_pong = 0;
  fsub->sent_close_frame = 0;
  fsub->received_close_frame = 0;
  ngx_memzero(&fsub->ping_ev, sizeof(fsub->ping_ev));
  
  nchan_subscriber_init_timeout_timer(&fsub->sub, &fsub->timeout_ev);
  
  ngx_memzero(&fsub->deflate, sizeof(fsub->deflate));
  
  fsub->enqueue_callback = empty_handler;
  fsub->enqueue_callback_data = NULL;
  
  fsub->dequeue_callback = empty_handler;
  fsub->dequeue_callback_data = NULL;
  fsub->awaiting_destruction = 0;
  
  ngx_memzero(&fsub->closing_ev, sizeof(fsub->closing_ev));
  nchan_init_timer(&fsub->closing_ev, closing_ev_handler, fsub);
  
  //what should the buffers look like?
  
  /*
  //mesage buf
  b->last_buf = 1;
  b->last_in_chain = 1;
  b->flush = 1;
  b->memory = 1;
  b->temporary = 0;
  */
  
  ngx_memzero(&fsub->publisher, sizeof(fsub->publisher));
  
  
  if(fsub->sub.cf->pub.websocket) {
    fsub->publisher.channel_id = nchan_get_channel_id(r, PUB, 0);
  }
  
  if(fsub->sub.cf->publisher_upstream_request_url) {
    ngx_str_t *url = ngx_palloc(r->pool, sizeof(*url));
    if(url == NULL) {
      err="Unable to allocate websocket upstream url";
      goto fail;
    }
    ngx_http_complex_value(r, fsub->sub.cf->publisher_upstream_request_url, url);
    fsub->publisher.upstream_request_url = url;
  }
  else {
    fsub->publisher.upstream_request_url = NULL;
  }
  
  websocket_init_frame(&fsub->frame);
  
  //http request sudden close cleanup
  if((fsub->cln = ngx_http_cleanup_add(r, 0)) == NULL) {
    err = "Unable to add request cleanup for websocket subscriber";
    goto fail;
  }
  fsub->cln->data = fsub;
  fsub->cln->handler = (ngx_http_cleanup_pt )sudden_abort_handler;
  DBG("%p created for request %p", &fsub->sub, r);
  
  assert(ctx != NULL);
  ctx->sub = &fsub->sub; //gonna need this for recv
  ctx->subscriber_type = fsub->sub.name;
  
  #if NCHAN_SUBSCRIBER_LEAK_DEBUG
    subscriber_debug_add(&fsub->sub);
  #endif

  //send-frame buffer
  ctx->output_str_queue = ngx_palloc(r->pool, sizeof(*ctx->output_str_queue));
  nchan_reuse_queue_init(ctx->output_str_queue, offsetof(framebuf_t, prev), offsetof(framebuf_t, next), framebuf_alloc, NULL, r->pool);
  
  //bufchain pool
  ctx->bcp = ngx_palloc(r->pool, sizeof(nchan_bufchain_pool_t));
  nchan_bufchain_pool_init(ctx->bcp, r->pool);
  
  return &fsub->sub;
  
  
fail: 
  if(fsub) {
    if(fsub->cln) {
      fsub->cln->data = NULL;
    }
    ngx_free(fsub);
  }
  ERR("%s", (u_char *)err);
  return NULL;
}

ngx_int_t websocket_subscriber_destroy(subscriber_t *sub) {
  full_subscriber_t            *fsub = (full_subscriber_t  *)sub;
  
  if(!fsub->awaiting_destruction) {
    fsub->ctx->sub = NULL;
  }
   
  if(sub->reserved > 0) {
    DBG("%p not ready to destroy (reserved for %i) for req %p", sub, sub->reserved, fsub->sub.request);
    fsub->awaiting_destruction = 1;
    sub->status = DEAD;
  }
  else {
    DBG("%p destroy for req %p", sub, fsub->sub.request);
#if NCHAN_SUBSCRIBER_LEAK_DEBUG
    subscriber_debug_remove(&fsub->sub);
#endif

    websocket_delete_timers(fsub);
    nchan_free_msg_id(&sub->last_msgid);
    //ngx_memset(fsub, 0x13, sizeof(*fsub));
    ws_destroy_msgpool(fsub);
    if(fsub->deflate.zstream_in) {
      inflateEnd(fsub->deflate.zstream_in);
      ngx_free(fsub->deflate.zstream_in);
      fsub->deflate.zstream_in = NULL;
    }
    
    nchan_subscriber_subrequest_cleanup(sub);
    ngx_free(fsub);
  }
  return NGX_OK;
}

static ngx_int_t extract_deflate_window_bits(full_subscriber_t *fsub, u_char *lcur, u_char *lend, const char* setting_name, int8_t *bits_out) {
  ngx_int_t bits;
  u_char    *ltmp;
  if((ltmp = ngx_strnstr(lcur, (char *)setting_name, lend - lcur)) != NULL) {
    ltmp += strlen(setting_name); //strlen
    if(*ltmp == '=') ltmp++;
    if(*ltmp == '"') ltmp++;
    u_char    *nend = ltmp;
    while(nend <= lend && *nend >='0' && *nend <='9') nend++;
    if(nend - ltmp == 0) {
      //no value, don't set
      return NGX_OK;
    }
    bits = ngx_atoi(ltmp, nend - ltmp);
    switch (bits) {
      case NGX_ERROR: //bad value
        return NGX_ERROR;
      case DEFLATE_MIN_WINDOW_BITS ... DEFLATE_MAX_WINDOW_BITS:
        *bits_out = bits;
        return NGX_OK;
      default:
        return NGX_ERROR; //out of range
    }
  }
  return NGX_OK;
}

static ngx_int_t respond_with_error(full_subscriber_t *fsub, const char* msg) {
  fsub->sub.dequeue_after_response = 1;
  fsub->sub.request->header_only = 0;
  nchan_respond_sprintf(fsub->sub.request, NGX_HTTP_BAD_REQUEST, &NCHAN_CONTENT_TYPE_TEXT_PLAIN, 1, "%s", msg);
  return NGX_ERROR;
}
  
static ngx_int_t websocket_perform_handshake(full_subscriber_t *fsub) {
  static ngx_str_t    magic = ngx_string("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
  ngx_str_t           ws_accept_key, sha1_str;
  u_char              buf_sha1[21];
  u_char              buf[255];
  u_char              permessage_deflate_buf[128];
  ngx_str_t          *tmp, *ws_key, *subprotocols;
  ngx_int_t           ws_version;
  ngx_http_request_t *r = fsub->sub.request;
  
  nchan_main_conf_t  *mcf = ngx_http_get_module_main_conf(r, ngx_nchan_module);
  int server_window_bits = mcf->zlib_params.windowBits;
  
  ngx_str_t          *which_deflate_extension = NULL;
  static ngx_str_t permessage_deflate = ngx_string("permessage-deflate");
  static ngx_str_t perframe_deflate = ngx_string("deflate-frame");
  static ngx_str_t webkit_perframe_deflate = ngx_string("x-webkit-deflate-frame");
  
  permessage_deflate_t pmd;
#if (NGX_ZLIB)
  pmd.zstream_in = NULL;
#endif
  pmd.server_max_window_bits = NGX_CONF_UNSET;
  pmd.client_max_window_bits = NGX_CONF_UNSET;
  pmd.server_no_context_takeover = 0;
  pmd.client_no_context_takeover = 0;
  pmd.enabled = 0;
  
  ngx_sha1_t          sha1;
  
  ws_accept_key.data = buf;
  
  r->headers_out.content_length_n = 0;
  r->chunked = 0;
  r->header_only = 1;
  
  if((tmp = nchan_get_header_value(r, NCHAN_HEADER_SEC_WEBSOCKET_VERSION)) == NULL) {
    fsub->sub.dequeue_after_response=1;
    r->header_only = 0;
    nchan_respond_cstring(r, NGX_HTTP_BAD_REQUEST, &NCHAN_CONTENT_TYPE_TEXT_PLAIN, "No Sec-Websocket-Version header present", 1);
    return NGX_ERROR;
  }
  else {
    ws_version=ngx_atoi(tmp->data, tmp->len);
    if(ws_version != 13) {
      //only websocket version 13 (RFC 6455) is supported
      fsub->sub.dequeue_after_response=1;
      r->header_only = 0;
      nchan_respond_cstring(r, NGX_HTTP_BAD_REQUEST, &NCHAN_CONTENT_TYPE_TEXT_PLAIN, "Unsupported websocket protocol version (only version 13 is supported)", 1);
      return NGX_ERROR;
    }
  }
  
  if((ws_key = nchan_get_header_value(r, NCHAN_HEADER_SEC_WEBSOCKET_KEY)) == NULL) {
    fsub->sub.dequeue_after_response=1;
    r->header_only = 0;
    nchan_respond_cstring(r, NGX_HTTP_BAD_REQUEST, &NCHAN_CONTENT_TYPE_TEXT_PLAIN, "No Sec-Websocket-Key header present", 1);
    return NGX_ERROR;
  }
  
  if((subprotocols = nchan_get_header_value(r, NCHAN_HEADERS_SEC_WEBSOCKET_PROTOCOL)) != NULL) {
    static ngx_str_t     ws_meta = ngx_string("ws+meta.nchan");
    if(subprotocols->len >= ws_meta.len && ngx_strncmp(subprotocols->data, ws_meta.data, ws_meta.len) == 0) {
      fsub->ws_meta_subprotocol = 1;
      nchan_add_response_header(r, &NCHAN_HEADERS_SEC_WEBSOCKET_PROTOCOL, &ws_meta);
      
      //init meta headers str reuse queue
      nchan_subscriber_init_msgid_reusepool(fsub->ctx, r->pool);
    }
    else {
      nchan_add_response_header(r, &NCHAN_HEADERS_SEC_WEBSOCKET_PROTOCOL, NULL);
    }
  }
  
  if((tmp = nchan_get_header_value(r, NCHAN_HEADER_SEC_WEBSOCKET_EXTENSIONS)) != NULL) {
    
    u_char      *cur = tmp->data, *end = tmp->data + tmp->len;
    u_char      *lcur, *lend;
    
    if(nchan_strscanstr(&cur, &permessage_deflate, end)) {
      which_deflate_extension = &permessage_deflate;
    }
    else if(nchan_strscanstr(&cur, &webkit_perframe_deflate, end)) {
      which_deflate_extension = &webkit_perframe_deflate;
    }
    else if(nchan_strscanstr(&cur, &perframe_deflate, end)) {
      which_deflate_extension = &perframe_deflate;
    }

    if(which_deflate_extension) {
      lcur = cur;
      lend = memchr(lcur, ',', end - lcur); 
      if(lend == NULL) lend = end;
      
      pmd.enabled = 1;
      
      pmd.client_no_context_takeover = ngx_strnstr(lcur, "client_no_context_takeover", lend - lcur) != NULL ? 1 : 0;
      pmd.server_no_context_takeover = ngx_strnstr(lcur, "server_no_context_takeover", lend - lcur) != NULL ? 1 : 0;
      
      if(which_deflate_extension == &permessage_deflate) {
        if(extract_deflate_window_bits(fsub, lcur, lend, "client_max_window_bits", &pmd.client_max_window_bits) != NGX_OK) {
          return respond_with_error(fsub, "invalid client_max_window_bits permessage-deflate setting");
        }
        if(extract_deflate_window_bits(fsub, lcur, lend, "server_max_window_bits", &pmd.server_max_window_bits) != NGX_OK) {
          return respond_with_error(fsub, "invalid server_max_window_bits permessage-deflate setting");
        }
      }
      else if(which_deflate_extension == &webkit_perframe_deflate || which_deflate_extension == &perframe_deflate) {
        if(extract_deflate_window_bits(fsub, lcur, lend, "max_window_bits", &pmd.server_max_window_bits) == NGX_OK) {
          pmd.client_max_window_bits = pmd.server_max_window_bits;
        }
        else {
          return respond_with_error(fsub, "invalid max_window_bits perframe-deflate setting");
        }
      }
    }
  }
  
  //generate permessage-deflate headers
  if(pmd.enabled) {
    u_char *ws_ext_end;
    ngx_str_t ws_extensions;
    ws_extensions.data = permessage_deflate_buf;
    
    if(which_deflate_extension == &webkit_perframe_deflate || which_deflate_extension == &perframe_deflate) {
      ws_ext_end = ngx_snprintf(permessage_deflate_buf, 128, "%V; ", which_deflate_extension);
      if (pmd.server_max_window_bits != NGX_CONF_UNSET) {
        if(pmd.server_max_window_bits < server_window_bits) {
          return respond_with_error(fsub, "max_window_bits perframe-deflate is too small");
        }
        pmd.server_max_window_bits = server_window_bits; //always the configured window bits
        ws_ext_end = ngx_snprintf(ws_ext_end, (permessage_deflate_buf + 128 - ws_ext_end),
                                  "max_window_bits=%i; ",
                                  pmd.server_max_window_bits);
      }
      else {
        pmd.server_max_window_bits = pmd.client_max_window_bits = server_window_bits;
      }
    }
    else {
      ws_ext_end = ngx_snprintf(permessage_deflate_buf, 128, "%V; %s%s", 
                                which_deflate_extension,
                                pmd.server_no_context_takeover ? "server_no_context_takeover; " : "",
                                pmd.client_no_context_takeover ? "client_no_context_takeover; " : "");
      if (pmd.server_max_window_bits != NGX_CONF_UNSET) {
        if(pmd.server_max_window_bits < server_window_bits) {
          return respond_with_error(fsub, "server_max_window_bits perframe-deflate is too small");
        }
        pmd.server_max_window_bits = server_window_bits; //always the configured window bits
        ws_ext_end = ngx_snprintf(ws_ext_end, (permessage_deflate_buf + 128 - ws_ext_end), "server_max_window_bits=%i; ", pmd.server_max_window_bits);
      }
      else {
        pmd.server_max_window_bits = server_window_bits;
      }
      
      if (pmd.client_max_window_bits != NGX_CONF_UNSET) {
        ws_ext_end = ngx_snprintf(ws_ext_end, (permessage_deflate_buf + 128 - ws_ext_end), "client_max_window_bits=%i; ", pmd.client_max_window_bits);
      }
      else {
        pmd.client_max_window_bits = DEFLATE_DEFAULT_CLIENT_WINDOW_BITS;
      }
    }
    ws_extensions.len = ws_ext_end - permessage_deflate_buf - 2; //-2 for the trailing "; "
    nchan_add_response_header(r, &NCHAN_HEADER_SEC_WEBSOCKET_EXTENSIONS, &ws_extensions);
  }
  fsub->deflate = pmd;

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
  
  nchan_include_access_control_if_needed(r, fsub->ctx);
  nchan_add_response_header(r, &NCHAN_HEADER_SEC_WEBSOCKET_ACCEPT, &ws_accept_key);
  nchan_add_response_header(r, &NCHAN_HEADER_UPGRADE, &NCHAN_WEBSOCKET);
#if nginx_version < 1003013
  nchan_add_response_header(r, &NCHAN_HEADER_CONNECTION, &NCHAN_UPGRADE);
#endif
  r->headers_out.status_line = NCHAN_HTTP_STATUS_101;
  r->headers_out.status = NGX_HTTP_SWITCHING_PROTOCOLS;
  
  r->keepalive=0; //apparently, websocket must not use keepalive.
  
  ngx_http_send_header(r);
  return NGX_OK;
}

static void websocket_reading(ngx_http_request_t *r);

static ngx_buf_t *websocket_inflate_message(full_subscriber_t *fsub, ngx_buf_t *msgbuf, ngx_pool_t *pool) {
#if (NGX_ZLIB)
  z_stream      *strm;
  int            rc;
  ngx_buf_t     *outbuf;
  
  if(fsub->deflate.zstream_in == NULL) {
    if((fsub->deflate.zstream_in = ngx_calloc(sizeof(*strm), ngx_cycle->log)) == NULL) {
      nchan_log_error("couldn't allocate deflate stream.");
      return NULL;
    }
    strm = fsub->deflate.zstream_in;
    
    strm->zalloc = Z_NULL;
    strm->zfree = Z_NULL;
    strm->opaque = Z_NULL;
    
    rc = inflateInit2(strm, -fsub->deflate.client_max_window_bits);
    
    if(rc != Z_OK) {
      nchan_log_error("couldn't initialize inflate stream.");
      ngx_free(strm);
      fsub->deflate.zstream_in = NULL;
      return NULL;
    }
  }
  
  strm = fsub->deflate.zstream_in;
  
  outbuf = nchan_inflate(strm, msgbuf, fsub->sub.request, pool);
  return outbuf;
#else
  return NULL;
#endif
}

static void ensure_request_hold(full_subscriber_t *fsub) {
  if(fsub->holding == 0) {
    fsub->sub.request->read_event_handler = websocket_reading;
    fsub->sub.request->write_event_handler = ws_request_empty_handler;
    fsub->sub.request->main->count++; //this is the right way to hold and finalize the
    fsub->holding = 1;
  }
}

static ngx_int_t ensure_handshake(full_subscriber_t *fsub) {
  if(fsub->shook_hands == 0) {
    ensure_request_hold(fsub);
    if(websocket_perform_handshake(fsub) == NGX_OK) {
      fsub->shook_hands = 1;
      return NGX_OK;
    }
    else {
      return NGX_ERROR;
    }
  }
  return NGX_OK;
}

static ngx_int_t websocket_reserve(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  ensure_request_hold(fsub);
  self->reserved++;
  //DBG("%p reserve for req %p. reservations: %i", self, fsub->sub.request, self->reserved);
  return NGX_OK;
}
static ngx_int_t websocket_release(subscriber_t *self, uint8_t nodestroy) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  //DBG("%p release for req %p, reservations: %i", self, fsub->sub.request, self->reserved);
  assert(self->reserved > 0);
  self->reserved--;
  if(nodestroy == 0 && fsub->awaiting_destruction == 1 && self->reserved == 0) {
    websocket_subscriber_destroy(self);
    return NGX_ABORT;
  }
  else {
    return NGX_OK;
  }
}

static void ping_ev_handler(ngx_event_t *ev) {
  full_subscriber_t *fsub = (full_subscriber_t *)ev->data;
  if(ev->timedout) {
    ev->timedout=0;
    if(fsub->awaiting_pong) {
      //never got a PONG back
      //NGX_HTTP_CLIENT_CLOSED_REQUEST?
      websocket_finalize_request(fsub);
    }
    else {
      fsub->awaiting_pong = 1;
      websocket_send_frame(fsub, WEBSOCKET_PING_LAST_FRAME_BYTE, 0, NULL); 
      ngx_add_timer(&fsub->ping_ev, fsub->sub.cf->websocket_ping_interval * 1000);
    }
  }
}

static ngx_int_t websocket_enqueue(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  ngx_int_t           rc;
  if((rc = ensure_handshake(fsub)) != NGX_OK) {
    return rc;
  }
  self->enqueued = 1;
  
  if(fsub->enqueue_callback) {
    fsub->enqueue_callback(&fsub->sub, fsub->enqueue_callback_data);
  }
  
  if(self->cf->websocket_ping_interval > 0) {
    //add timeout timer
    //nextsub->ev should be zeroed;
    nchan_init_timer(&fsub->ping_ev, ping_ev_handler, fsub);
    ngx_add_timer(&fsub->ping_ev, self->cf->websocket_ping_interval * 1000);
  }
  
  if(self->cf->subscriber_timeout > 0) {
    //add timeout timer
    ngx_add_timer(&fsub->timeout_ev, self->cf->subscriber_timeout * 1000);
  }
  
  return NGX_OK;
}

static void websocket_delete_timers(full_subscriber_t *fsub) {
  if(fsub->ping_ev.timer_set) {
    ngx_del_timer(&fsub->ping_ev);
  }
  
  if(fsub->closing_ev.timer_set) {
    ngx_del_timer(&fsub->closing_ev);
  }
  
  if(fsub->timeout_ev.timer_set) {
    ngx_del_timer(&fsub->timeout_ev);
  }
}

static ngx_int_t websocket_dequeue(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  DBG("%p dequeue", self);
  if(fsub->dequeue_callback) {
    fsub->dequeue_callback(&fsub->sub, fsub->dequeue_callback_data);
  }
  
  self->enqueued = 0;
  
  if(!fsub->sent_close_frame && fsub->shook_hands) {
    websocket_send_close_frame_cstr(fsub, CLOSE_NORMAL, "410 Gone");
  }
  
  websocket_delete_timers(fsub);
  
  if(self->destroy_after_dequeue) {
    websocket_subscriber_destroy(self);
  }
  return NGX_OK;
}

static ngx_int_t websocket_set_enqueue_callback(subscriber_t *self, subscriber_callback_pt cb, void *privdata) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  fsub->enqueue_callback = cb;
  fsub->enqueue_callback_data = privdata;
  return NGX_OK;
}

static ngx_int_t websocket_set_dequeue_callback(subscriber_t *self, subscriber_callback_pt cb, void *privdata) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  fsub->dequeue_callback = cb;
  fsub->dequeue_callback_data = privdata;
  return NGX_OK;
}

static ngx_int_t ws_recv(ngx_connection_t *c, ngx_event_t *rev, ngx_buf_t *buf, ssize_t len) {
  ssize_t         n;
  n = c->recv(c, buf->last, (ssize_t) len - (buf->last - buf->start));

  if (n == NGX_AGAIN) {
    return NGX_AGAIN;
  }
  else if (n == NGX_ERROR || rev->eof) {
    return NGX_ERROR;
  }
  else if (n == 0) {
    return NGX_DONE;
  }
  
  buf->pos = buf->last;
  buf->last += n;

  if ((buf->last - buf->start) < len) {
    return NGX_AGAIN;
  }

  return NGX_OK;
}

static void set_buffer(ngx_buf_t *buf, u_char *start, u_char *last, ssize_t len) {
  ngx_memzero(buf, sizeof(*buf));
  buf->start = start;
  buf->pos = buf->start;
  buf->last = (last != NULL) ? last : start;
  buf->end = buf->start + len;
  buf->temporary = 0;
  buf->memory = 1;
}

static void websocket_reading_finalize(ngx_http_request_t *r) {
  nchan_request_ctx_t        *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  full_subscriber_t          *fsub = (full_subscriber_t *)ctx->sub;
  
  //fsub->sub.status = DEAD;
  if(fsub) {
    websocket_delete_timers(fsub);
    websocket_finalize_request(fsub);
  }
}
  
/* based on code from push stream module, written by
 * Wandenberg Peixoto <wandenberg@gmail.com>, Rogério Carvalho Schneider <stockrt@gmail.com>
 * thanks, guys!
*/
static void websocket_reading(ngx_http_request_t *r) {
  nchan_request_ctx_t        *ctx;
  full_subscriber_t          *fsub;
  ws_frame_t                 *frame;
  ngx_int_t                   rc;
  ngx_event_t                *rev;
  ngx_connection_t           *c;
  ngx_buf_t                  *msgbuf, buf;
  //ngx_str_t                 msg_in_str;
retry:
  ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  fsub = (full_subscriber_t *)ctx->sub;
  frame = &fsub->frame;
  rc = NGX_OK;

  c = r->connection;
  rev = c->read;
  
  if(!fsub) {
    nchan_http_finalize_request(r, NGX_OK);
    return;
  }
  
  set_buffer(&buf, frame->header, frame->last, 8);

  //DBG("websocket_reading fsub: %p, frame: %p", fsub, frame);
  


  for (;;) {
    if (c->error || c->timedout || c->close || c->destroyed || rev->closed || rev->eof || rev->pending_eof) {
      //ERR("c->error %i c->timedout %i c->close %i c->destroyed %i rev->closed %i rev->eof %i", c->error, c->timedout, c->close, c->destroyed, rev->closed, rev->eof);
      fsub->sub.request->headers_out.status = NGX_HTTP_CLIENT_CLOSED_REQUEST;
      return websocket_reading_finalize(r);
    }
    
    switch (frame->step) {
      case WEBSOCKET_READ_START_STEP:
        //reading frame header
        if ((rc = ws_recv(c, rev, &buf, 2)) != NGX_OK) {
          fsub->sub.request->headers_out.status = NGX_HTTP_CLIENT_CLOSED_REQUEST;
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
            frame->payload_len = nchan_ntohll(len);
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
        
        switch(frame->opcode) {
          case WEBSOCKET_OPCODE_PING:
          case WEBSOCKET_OPCODE_PONG:
          case WEBSOCKET_OPCODE_CLOSE:
            { //block-scope these vars
              u_char                  payloadbuf[1024];
              int                     close_code;
              ngx_str_t               payload_str;
              nchan_buf_and_chain_t  *bc;
              
              if (frame->payload_len == 0) {
                frame->payload = NULL;
              }
              else if(frame->payload_len < 1024) {
                frame->payload = payloadbuf;
                frame->last = frame->payload;
                set_buffer(&buf, frame->payload, frame->last, frame->payload_len);
                if ((rc = ws_recv(c, rev, &buf, frame->payload_len)) != NGX_OK) {
                  ERR("ws_recv NOT OK when receiving payload");
                  goto exit;
                }
                if (frame->mask) {
                  websocket_unmask_frame(frame);
                }
              }
              else {
                //ERROR: frame too big
                websocket_send_close_frame(fsub, CLOSE_MESSAGE_TOO_BIG, NULL);
                return websocket_reading_finalize(r);
              }
              
              switch(frame->opcode) {
                case WEBSOCKET_OPCODE_PING:
                  bc = nchan_bufchain_pool_reserve(fsub->ctx->bcp, 1);
                  DBG("%p got pinged", fsub);
                  init_buf(&bc->buf, 1);
                  payload_str.data = frame->payload;
                  payload_str.len = frame->payload_len;
                  set_buf_to_str(&bc->buf, &payload_str);
                  websocket_send_frame(fsub, WEBSOCKET_PONG_LAST_FRAME_BYTE, frame->payload_len, &bc->chain);
                  break;
                
                case WEBSOCKET_OPCODE_PONG:
                  DBG("%p Got ponged", fsub);
                  if(fsub->awaiting_pong) {
                    fsub->awaiting_pong = 0;
                  }
                  // unsolicited pongs are ok too as per 
                  // https://tools.ietf.org/html/rfc6455#page-37
                  break;
                
                case WEBSOCKET_OPCODE_CLOSE:
                  fsub->received_close_frame = 1;
                  if(frame->payload_len >= 2) {
                    ngx_memcpy(&close_code, frame->payload, 2);
                    close_code = ntohs(close_code);
                    payload_str.data = frame->payload + 2;
                    payload_str.len = frame->payload_len - 2;
                  }
                  else {
                    close_code = 0;
                    payload_str.data = (u_char *)"";
                    payload_str.len = 0;
                  }
                  DBG("%p wants to close (code %i reason \"%V\")", fsub, close_code, &payload_str);
                  if(!fsub->sent_close_frame) {
                    websocket_send_close_frame(fsub, close_code, &payload_str);
                  }
                  return websocket_reading_finalize(r);
                  break; //good practice?
              }
            }
            break;
          case WEBSOCKET_OPCODE_TEXT:
          case WEBSOCKET_OPCODE_BINARY:
            
            if(!fsub->sub.cf->pub.websocket) {
              websocket_send_close_frame_cstr(fsub, CLOSE_POLICY_VIOLATION, "Publishing not allowed.");
              return websocket_reading_finalize(r);
            }
            
            //TODO: check max websocket message length
            if(frame->payload == NULL) {
              if(ws_get_msgpool(fsub) == NULL) {
                ERR("failed to get msgpool");
                websocket_send_close_frame(fsub, CLOSE_INTERNAL_SERVER_ERROR, NULL);
                return websocket_reading_finalize(r);
              }
              if((frame->payload = ngx_palloc(ws_get_msgpool(fsub), frame->payload_len == 0 ? 1 : frame->payload_len)) == NULL) {
                ERR("failed to reserve payload len in tmp pool");
                websocket_send_close_frame(fsub, CLOSE_INTERNAL_SERVER_ERROR, NULL);
                ws_destroy_msgpool(fsub);
                fsub->publisher.msg_pool = NULL;
                return websocket_reading_finalize(r);
              }
              frame->last = frame->payload;
            }
            
            set_buffer(&buf, frame->payload, frame->last, frame->payload_len);
            
            if (frame->payload_len > 0 && (rc = ws_recv(c, rev, &buf, frame->payload_len)) != NGX_OK) {
              DBG("ws_recv NOT OK when receiving payload, but that's ok");
              goto exit;
            }
            
            if (frame->mask) {
              websocket_unmask_frame(frame);
            }
            
            if((msgbuf = ngx_palloc(ws_get_msgpool(fsub), sizeof(*msgbuf))) == NULL) {
              ERR("failed to reserve payload msgbuf in tmp pool");
              websocket_send_close_frame(fsub, CLOSE_INTERNAL_SERVER_ERROR, NULL);
              ws_destroy_msgpool(fsub);
              fsub->publisher.msg_pool = NULL;
              return websocket_reading_finalize(r);
            }
            
            init_msg_buf(msgbuf);
            
            msgbuf->start = frame->payload;
            msgbuf->pos = msgbuf->start;
            msgbuf->end = msgbuf->start + frame->payload_len;
            msgbuf->last = msgbuf->end;
            
            //inflate message if needed
            if(fsub->deflate.enabled && frame->rsv1) {
              if((msgbuf = websocket_inflate_message(fsub, msgbuf, ws_get_msgpool(fsub))) == NULL) {
                websocket_send_close_frame_cstr(fsub, CLOSE_INVALID_PAYLOAD, "Invalid permessage-deflate data");
                ws_destroy_msgpool(fsub);
                return websocket_reading_finalize(r);
              }
            }
            
            if (frame->opcode == WEBSOCKET_OPCODE_TEXT && !is_utf8(msgbuf)) {
              ws_destroy_msgpool(fsub);
              websocket_send_close_frame_cstr(fsub, CLOSE_INVALID_PAYLOAD, "Invalid text frame (not UTF8).");
              return websocket_reading_finalize(r);
            }
            
            if(websocket_heartbeat(fsub, msgbuf) != NGX_OK) {
              websocket_publish(fsub, msgbuf, frame->opcode == WEBSOCKET_OPCODE_BINARY);
            }
            else {
              ws_destroy_msgpool(fsub);
            }
            break;
          default:
            //invalid frame or something
            websocket_send_frame(fsub, WEBSOCKET_CLOSE_LAST_FRAME_BYTE, 0, NULL); //TODO: more specific close frame plz
            return websocket_reading_finalize(r);
            break;
        }
        
        frame->step = WEBSOCKET_READ_START_STEP; //restart loop
        frame->last = NULL;
        frame->payload = NULL;
        goto retry; //I know, Dijkstra, I know.
        break;
        
      default:
        nchan_log(NGX_LOG_ERR, c->log, 0, "unknown websocket step (%d)", frame->step);
        return websocket_reading_finalize(r);
    }
    
    set_buffer(&buf, frame->header, NULL, 8);
  }

exit:
  
  if (rc == NGX_AGAIN) {
    frame->last = buf.last;
    if (!c->read->ready) {
      if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
        nchan_log(NGX_LOG_INFO, c->log, ngx_socket_errno, "websocket client: failed to restore read events");
        return websocket_reading_finalize(r);
      }
    }
  }

  if (rc == NGX_ERROR) {
    rev->eof = 1;
    c->error = 1;
    nchan_log(NGX_LOG_INFO, c->log, ngx_socket_errno, "websocket client prematurely closed connection");
    return websocket_reading_finalize(r);
  }
}


static ngx_flag_t is_utf8(ngx_buf_t *buf) {
  
  u_char *p;
  size_t n;
  
  u_char  c, *last;
  size_t  len;
  int     mmapped = 0;
  
  if(ngx_buf_in_memory(buf)) {
    n = ngx_buf_size(buf);
    p = buf->pos;
  }
  else {
    ngx_fd_t fd = buf->file->fd == NGX_INVALID_FILE ? nchan_fdcache_get(&buf->file->name) : buf->file->fd;
    n = buf->file_last - buf->file_pos;
    p = mmap(NULL, n, PROT_READ, MAP_SHARED, fd, buf->file_pos);
    if (p == MAP_FAILED) {
      return 0;
    }
    mmapped = 1;
  }
  
  last = p + n;
  
  for (len = 0; p < last; len++) {
    c = *p;
    
    if (c < 0x80) {
      p++;
      continue;
    }
    
    if (ngx_utf8_decode(&p, last - p) > 0x10ffff) {
      /* invalid UTF-8 */
      if(mmapped) {
        munmap(p, n);
      }
      return 0;
    }
  }
  if(mmapped) {
    munmap(p, n);
  }
  return 1;
}

static void init_buf(ngx_buf_t *buf, int8_t last){
  ngx_memzero(buf, sizeof(*buf));
  buf->memory = 1;
  if(last) {
    buf->last_buf = 1;
    buf->last_in_chain = 1;
    buf->flush = 1;
  }
}

static void init_header_buf(ngx_buf_t *buf) {
  init_buf(buf, 0);
}

static void init_msg_buf(ngx_buf_t *buf) {
  init_buf(buf, 1);
}

static ngx_int_t websocket_frame_header(full_subscriber_t *fsub, ngx_buf_t *buf, const u_char opcode, off_t len) {
  
  framebuf_t           *framebuf = nchan_reuse_queue_push(fsub->ctx->output_str_queue);
  u_char               *last = framebuf->chr;
  uint64_t              len_net;
  init_header_buf(buf);
  buf->start = last;
  *last = opcode;
  last++;
  if (len <= 125) {
    last = ngx_copy(last, &len, 1);
    buf->end++;
  }
  else if (len < (1 << 16)) {
    last = ngx_copy(last, &WEBSOCKET_PAYLOAD_LEN_16_BYTE, sizeof(WEBSOCKET_PAYLOAD_LEN_16_BYTE));
    len_net = htons(len);
    last = ngx_copy(last, &len_net, 2);
  }
  else {
    last = ngx_copy(last, &WEBSOCKET_PAYLOAD_LEN_64_BYTE, sizeof(WEBSOCKET_PAYLOAD_LEN_64_BYTE));
    len_net = nchan_htonll(len);
    last = ngx_copy(last, &len_net, 8);
  }
  buf->end=last;
  buf->last=last;
  buf->last_buf= len == 0;
  buf->pos=buf->start;
  return NGX_OK;
}

/*static ngx_int_t websocket_msg_frame_header(ngx_buf_t *buf, off_t len) {
  return websocket_frame_header(buf, WEBSOCKET_TEXT_LAST_FRAME_BYTE, len);
}
*/

static void set_buf_to_str(ngx_buf_t *buf, const ngx_str_t *str) {
  buf->start=str->data;
  buf->end=str->data + str->len;
  buf->pos = buf->start;
  buf->last = buf->end;
}

static ngx_chain_t *websocket_frame_header_chain(full_subscriber_t *fsub, const u_char opcode, off_t len, ngx_chain_t *msg_chain) {
  
  nchan_buf_and_chain_t   *bc = nchan_bufchain_pool_reserve(fsub->ctx->bcp, 1);

  init_header_buf(&bc->buf);
  
  websocket_frame_header(fsub, &bc->buf, opcode, len);
  
  if(len == 0) {
    bc->buf.last_buf=1;
  }
  else {
    bc->buf.last_buf=0;
    assert(msg_chain != NULL);
    bc->chain.next = msg_chain;
  }
  bc->buf.pos=bc->buf.start;

  return &bc->chain;
}

static ngx_int_t websocket_send_frame(full_subscriber_t *fsub, const u_char opcode, off_t len, ngx_chain_t *msg_chain) {
  return ws_output_filter(fsub, websocket_frame_header_chain(fsub, opcode, len, msg_chain));
}

static ngx_chain_t *websocket_msg_frame_chain(full_subscriber_t *fsub, nchan_msg_t *msg) {
  nchan_buf_and_chain_t *bc;
  ngx_file_t            *file_copy;
  ngx_buf_t             *chained_msgbuf = NULL;
  size_t                 sz;
  u_char                 frame_opcode;
  int                    compressed;
  ngx_buf_t             *msgbuf;
  compressed = fsub->deflate.enabled && msg->compressed && msg->compressed->compression == NCHAN_MSG_COMPRESSION_WEBSOCKET_PERMESSAGE_DEFLATE;

  msgbuf = compressed ? &msg->compressed->buf : &msg->buf;
  sz = ngx_buf_size(msgbuf);
  
  if(msg->content_type && nchan_ngx_str_match(msg->content_type, &binary_mimetype)) {
    frame_opcode = compressed ? WEBSOCKET_BINARY_DEFLATED_LAST_FRAME_BYTE : WEBSOCKET_BINARY_LAST_FRAME_BYTE;
  }
  else {
    frame_opcode = compressed ? WEBSOCKET_TEXT_DEFLATED_LAST_FRAME_BYTE : WEBSOCKET_TEXT_LAST_FRAME_BYTE;
  }
  
  if(fsub->ws_meta_subprotocol) {
    if(!compressed) {
      static ngx_str_t          id_line = ngx_string("id: ");
      static ngx_str_t          content_type_line = ngx_string("\ncontent-type: ");
      static ngx_str_t          two_newlines = ngx_string("\n\n");
      ngx_chain_t              *cur;
      ngx_str_t                 msgid;
      bc = nchan_bufchain_pool_reserve(fsub->ctx->bcp, 4 + (msg->content_type ? 2 : 0));
      cur = &bc->chain;
      
      // id: 
      ngx_init_set_membuf(cur->buf, id_line.data, id_line.data + id_line.len);
      sz += id_line.len;
      cur = cur->next;
      
      //msgid value
      msgid = nchan_subscriber_set_recyclable_msgid_str(fsub->ctx, &fsub->sub.last_msgid);
      ngx_init_set_membuf(cur->buf, msgid.data, msgid.data + msgid.len);
      sz += msgid.len;
      cur = cur->next;
      
      if(msg->content_type) {
        // content-type: 
        ngx_init_set_membuf(cur->buf, content_type_line.data, content_type_line.data + content_type_line.len);
        sz += content_type_line.len;
        cur = cur->next;
        
        //content-type value
        ngx_init_set_membuf(cur->buf, msg->content_type->data, msg->content_type->data + msg->content_type->len);
        sz += msg->content_type->len;
        cur = cur->next;
      }
      
      // \n\n
      ngx_init_set_membuf(cur->buf, two_newlines.data, two_newlines.data + two_newlines.len);
      sz += two_newlines.len;
    
    
      if(ngx_buf_size(msgbuf) > 0) {
        cur = cur->next;
        //now the message
        *cur->buf = *msgbuf;
        chained_msgbuf = cur->buf;
        assert(cur->next == NULL);
      }
      else {
        cur->next = NULL;
        cur->buf->last_in_chain = 1;
        cur->buf->last_buf = 1;
      }
    }
    else {
#if (NGX_ZLIB)
      u_char        ws_meta_header[512];
      static u_char ws_meta_header_deflated[512];
      ngx_chain_t  *cur;
      u_char       *end;
      ngx_str_t     ws_meta_header_str_in;
      ngx_str_t     ws_meta_header_str_out;
      
      ws_meta_header_str_out.data = ws_meta_header_deflated;
      ws_meta_header_str_out.len = 512;
      
      ngx_str_t     msgid = nchan_subscriber_set_recyclable_msgid_str(fsub->ctx, &fsub->sub.last_msgid);
      if(msg->content_type) {
        end = ngx_snprintf(ws_meta_header, 512, "id: %V\ncontent-type: %V\n\n", &msgid, msg->content_type);
      }
      else {
        end = ngx_snprintf(ws_meta_header, 512, "id: %V\n\n", &msgid);
      }
      
      ws_meta_header_str_in.data = ws_meta_header;
      ws_meta_header_str_in.len = end - ws_meta_header;
      
      bc = nchan_bufchain_pool_reserve(fsub->ctx->bcp, 2);
      cur = &bc->chain;
      
      nchan_common_simple_deflate_raw_block(&ws_meta_header_str_in, &ws_meta_header_str_out);
      
      ngx_init_set_membuf(cur->buf, ws_meta_header_str_out.data, ws_meta_header_str_out.data + ws_meta_header_str_out.len);
      sz += ws_meta_header_str_out.len;
      cur = cur->next;
      
      //now the message
      *cur->buf = *msgbuf;
      chained_msgbuf = cur->buf;
#endif
    }
  }
  else {
    bc = nchan_bufchain_pool_reserve(fsub->ctx->bcp, 1);
    //message first
    chained_msgbuf = &bc->buf;
    bc->buf = *msgbuf;
  }
  
  if(chained_msgbuf && msgbuf->file) {
    file_copy = nchan_bufchain_pool_reserve_file(fsub->ctx->bcp);
    nchan_msg_buf_open_fd_if_needed(chained_msgbuf, file_copy, NULL);
  }
  
  //DBG("opcode: %i, orig sz: %i compressed sz: %i", frame_opcode, ngx_buf_size((&msg->buf)), compressed ? ngx_buf_size(msgbuf) : 0);
  //now the header
  return websocket_frame_header_chain(fsub, frame_opcode, sz, &bc->chain);
}

static ngx_int_t websocket_send_close_frame_cstr(full_subscriber_t *fsub, uint16_t code, const char *err) {
  ngx_str_t errstr=ngx_string(err);
  return websocket_send_close_frame(fsub, code, &errstr);
}

static ngx_int_t websocket_send_close_frame(full_subscriber_t *fsub, uint16_t code, ngx_str_t *err) {
  if(fsub->sent_close_frame == 1) {
    DBG("%p already sent close frame");
    websocket_finalize_request(fsub);
  }
  else {
    ws_output_filter(fsub, websocket_close_frame_chain(fsub, code, err));
    fsub->sent_close_frame = 1;
    ngx_add_timer(&fsub->closing_ev, fsub->received_close_frame ? 0 : WEBSOCKET_CLOSING_TIMEOUT);
  }
  return NGX_OK;
}

static ngx_chain_t *websocket_close_frame_chain(full_subscriber_t *fsub, uint16_t code, ngx_str_t *err) {
  nchan_buf_and_chain_t *bc; 
  ngx_chain_t    *hdr_chain;
  ngx_buf_t      *code_buf, *msg_buf;
  ngx_str_t       alt_err;
  static uint16_t code_net;
  
  if(err) {
    alt_err.data=err->data;
    alt_err.len=err->len;
  }
  else {
    alt_err.data=NULL;
    alt_err.len=0;
  }
  err = &alt_err;
  
  if(code == 0) {
    return websocket_frame_header_chain(fsub, WEBSOCKET_CLOSE_LAST_FRAME_BYTE, 0, NULL);
  }
  
  if(code < 1000 || code == 1005 || code == 1006 || code >= 5000) {
    ERR("invalid websocket close status code %i", code);
    code=CLOSE_NORMAL;
  }  
  bc = nchan_bufchain_pool_reserve(fsub->ctx->bcp, err->len > 0 ? 2 : 1);
  
  code_buf = &bc->buf;
  
  init_buf(code_buf, err->len == 0);
  code_net=htons(code);
  code_buf->start = code_buf->pos = (u_char *)&code_net;
  code_buf->last = code_buf->end = ngx_copy(code_buf->pos, &code_net, 2);
  
  if(err->len > 0) {
    if(err->len > 123) {
      ERR("websocket close frame reason string is too long (length %i)", err->len);
      err->len = 123;
    }
    msg_buf = bc->chain.next->buf;
    init_msg_buf(msg_buf);
    set_buf_to_str(msg_buf, err);
  }

  
  hdr_chain = websocket_frame_header_chain(fsub, WEBSOCKET_CLOSE_LAST_FRAME_BYTE, err->len + 2, &bc->chain);
  return hdr_chain;
}


static ngx_int_t websocket_respond_message(subscriber_t *self, nchan_msg_t *msg) {
  ngx_int_t            rc;
  full_subscriber_t   *fsub = (full_subscriber_t *)self;
  if((rc = ensure_handshake(fsub)) != NGX_OK) {
    return rc;
  }
  ngx_http_request_t *r = self->request;
  r->header_only = 0;
  
  if(fsub->timeout_ev.timer_set) {
    ngx_del_timer(&fsub->timeout_ev);
    ngx_add_timer(&fsub->timeout_ev, fsub->sub.cf->subscriber_timeout * 1000);
  }
  
  fsub->ctx->prev_msg_id = self->last_msgid;
  update_subscriber_last_msg_id(self, msg);
  fsub->ctx->msg_id = self->last_msgid;
  
  rc = ws_output_msg_filter(fsub, msg);
  
  return rc;
}

static ngx_int_t websocket_respond_status(subscriber_t *self, ngx_int_t status_code, const ngx_str_t *status_line, ngx_chain_t *status_body) {
  static const ngx_str_t    STATUS_410=ngx_string("410 Channel Deleted");
  static const ngx_str_t    STATUS_403=ngx_string("403 Forbidden");
  static const ngx_str_t    STATUS_500=ngx_string("500 Internal Server Error");
  static const ngx_str_t    STATUS_507=ngx_string("507 Insufficient Storage");
  static const ngx_str_t    empty=ngx_string("");
  u_char                    msgbuf[50];
  ngx_str_t                 custom_close_msg;
  ngx_str_t                *close_msg = NULL;
  uint16_t                  close_code = 0;
  full_subscriber_t        *fsub = (full_subscriber_t *)self;
  
  if(status_code == NGX_HTTP_NO_CONTENT || (status_code == NGX_HTTP_NOT_MODIFIED && !status_line)) {
    //don't care, ignore
    return NGX_OK;
  }
  
  if(!fsub->shook_hands) {
    //still in HTTP land
    fsub->cln = NULL;
    return nchan_respond_status(fsub->sub.request, status_code, status_line, status_body, 1);
  }
  
  switch(status_code) {
    case 410:
      close_code = CLOSE_GOING_AWAY;
      fsub->sub.request->headers_out.status = NGX_HTTP_GONE;
      close_msg = (ngx_str_t *)(status_line ? status_line : &STATUS_410);
      break;
    case 403:
      close_code = CLOSE_POLICY_VIOLATION;
      fsub->sub.request->headers_out.status = NGX_HTTP_FORBIDDEN;
      close_msg = (ngx_str_t *)(status_line ? status_line : &STATUS_403);
      break;
    case 500: 
      close_code = CLOSE_INTERNAL_SERVER_ERROR;
      fsub->sub.request->headers_out.status = NGX_HTTP_INTERNAL_SERVER_ERROR;
      close_msg = (ngx_str_t *)(status_line ? status_line : &STATUS_500);
      break;
    case 507: 
      close_code = CLOSE_INTERNAL_SERVER_ERROR;
      fsub->sub.request->headers_out.status = NGX_HTTP_INSUFFICIENT_STORAGE;
      close_msg = (ngx_str_t *)(status_line ? status_line : &STATUS_507);
      break;
    default:
      if((status_code >= 400 && status_code < 600) || status_code == NGX_HTTP_NOT_MODIFIED) {
        custom_close_msg.data=msgbuf;
        fsub->sub.request->headers_out.status = status_code;
        custom_close_msg.len = ngx_sprintf(msgbuf,"%i %v", status_code, (status_line ? status_line : &empty)) - msgbuf;
        close_msg = &custom_close_msg;
        close_code = status_code >=500 && status_code <= 599 ? CLOSE_INTERNAL_SERVER_ERROR : CLOSE_NORMAL;
      }
      else {
        ERR("unhandled code %i, %v", status_code, (status_line ? status_line : &empty));
        assert(0);
      }
  }
  websocket_send_close_frame(fsub, close_code, close_msg);
  
  return NGX_OK;
}


ngx_int_t nchan_detect_websocket_request(ngx_http_request_t *r) {
  ngx_str_t       *tmp;
  
  if(r->method != NGX_HTTP_GET) {
    return 0;
  }
  
  if((tmp = nchan_get_header_value(r, NCHAN_HEADER_CONNECTION))) {
    if(ngx_strlcasestrn(tmp->data, tmp->data + tmp->len, NCHAN_UPGRADE.data, NCHAN_UPGRADE.len - 1) == NULL) return 0;
  }
  else return 0;
  
  if((tmp = nchan_get_header_value(r, NCHAN_HEADER_UPGRADE))) {
    if(tmp->len != NCHAN_WEBSOCKET.len || ngx_strncasecmp(tmp->data, NCHAN_WEBSOCKET.data, NCHAN_WEBSOCKET.len) != 0) return 0;
  }
  else return 0;

  return 1;
}

ngx_int_t websocket_intercept_published_message(subscriber_t *sub, void (*interceptor)(subscriber_t *, nchan_msg_t *)) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)sub;
  fsub->publisher.intercept = interceptor;
  return NGX_OK;
}

static const subscriber_fn_t websocket_fn = {
  &websocket_enqueue,
  &websocket_dequeue,
  &websocket_respond_message,
  &websocket_respond_status,
  &websocket_set_enqueue_callback,
  &websocket_set_dequeue_callback,
  &websocket_reserve,
  &websocket_release,
  &nchan_subscriber_receive_notice,
  &nchan_subscriber_authorize_subscribe_request
};

static ngx_str_t     sub_name = ngx_string("websocket");

static const subscriber_t new_websocket_sub = {
  &sub_name,
  WEBSOCKET,
  &websocket_fn,
  UNKNOWN,
  NCHAN_ZERO_MSGID,
  NULL,
  NULL,
  NULL,
  0, //reserved
  1, //enable sub/unsub callbacks
  0, //deque after response
  1, //destroy after dequeue
  0, //enqueued
#if NCHAN_SUBSCRIBER_LEAK_DEBUG
  NULL, NULL, NULL
#endif
};
