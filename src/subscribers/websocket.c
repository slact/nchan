#include <nchan_module.h>
#include <subscribers/common.h>
#include <util/nchan_subrequest.h>
#include <ngx_crypt.h>
#include <ngx_sha1.h>
#include <nginx.h>


//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:WEBSOCKET:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:WEBSOCKET:" fmt, ##arg)
#include <assert.h>

#if __AVX2__
  #include <immintrin.h>                     // AVX2 intrinsics
  #define WEBSOCKET_OPTIMIZED_UNMASK 1
  
  #define __vector_size_bytes 32
  #define __vector_type __m256i
  #define __vector_load_intrinsic _mm256_load_si256
  #define __vector_xor_intrinsic _mm256_xor_si256
  #define __vector_store_intrinsic _mm256_store_si256
    
#elif __SSE2__
  #define WEBSOCKET_OPTIMIZED_UNMASK 1
  #include <emmintrin.h>                     // SSE2 instrinsics
  
  #define __vector_size_bytes 16
  #define __vector_type __m128i
  #define __vector_load_intrinsic _mm_load_si128
  #define __vector_xor_intrinsic _mm_xor_si128
  #define __vector_store_intrinsic _mm_store_si128
#endif

#define WEBSOCKET_LAST_FRAME                0x8

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
static const u_char WEBSOCKET_CLOSE_LAST_FRAME_BYTE = WEBSOCKET_OPCODE_CLOSE | (WEBSOCKET_LAST_FRAME << 4);
static const u_char WEBSOCKET_PONG_LAST_FRAME_BYTE  = WEBSOCKET_OPCODE_PONG  | (WEBSOCKET_LAST_FRAME << 4);
static const u_char WEBSOCKET_PING_LAST_FRAME_BYTE  = WEBSOCKET_OPCODE_PING  | (WEBSOCKET_LAST_FRAME << 4);

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

typedef struct {
  ngx_pool_t                  *original_pool;
  ngx_http_cleanup_t          *original_cleanup;
  ngx_pool_t                  *tmp_pool;
  ngx_pool_t                  *framebuf_pool;
  ngx_http_cleanup_t          *cln;
  full_subscriber_t           *fsub;
  ngx_buf_t                    buf;
  //ngx_http_request_t        *sr;
  //ngx_http_posted_request_t *pr;
  //ngx_chain_t                link;
  ngx_str_t                    upstream_request_url;
} nchan_pub_upstream_data_t;

typedef struct nchan_pub_upstream_stuff_s nchan_pub_upstream_stuff_t;
struct nchan_pub_upstream_stuff_s {
  ngx_http_post_subrequest_t  psr;
  nchan_pub_upstream_data_t   psr_data;
  nchan_pub_upstream_stuff_t *prev;
  nchan_pub_upstream_stuff_t *next;
};

struct full_subscriber_s {
  subscriber_t            sub;
  ngx_http_cleanup_t     *cln;
  nchan_request_ctx_t    *ctx;
  subscriber_callback_pt  dequeue_handler;
  void                   *dequeue_handler_data;
  ngx_event_t             timeout_ev;
  ngx_event_t             closing_ev;
  ws_frame_t              frame;
  
  ngx_str_t              *publish_channel_id;
  nchan_pub_upstream_stuff_t  *upstream_stuff;
  
  ngx_event_t             ping_ev;
  unsigned                awaiting_pong:1;
  unsigned                ws_meta_subprotocol:1;
  unsigned                holding:1; //make sure the request doesn't close right away
  unsigned                shook_hands:1;
  unsigned                connected:1;
  unsigned                closing:1;
  unsigned                finalize_request:1;
  unsigned                awaiting_destruction:1;
};// full_subscriber_t


static ngx_int_t websocket_send_frame(full_subscriber_t *fsub, const u_char opcode, off_t len, ngx_chain_t *chain);
static void set_buf_to_str(ngx_buf_t *buf, const ngx_str_t *str);
static ngx_chain_t *websocket_frame_header_chain(full_subscriber_t *fsub, const u_char opcode, off_t len, ngx_chain_t *chain);
static ngx_flag_t is_utf8(u_char *, size_t);
static ngx_chain_t *websocket_close_frame_chain(full_subscriber_t *fsub, uint16_t code, ngx_str_t *err);
static ngx_int_t websocket_send_close_frame(full_subscriber_t *fsub, uint16_t code, ngx_str_t *err);
static ngx_int_t websocket_respond_status(subscriber_t *self, ngx_int_t status_code, const ngx_str_t *status_line);

static ngx_int_t websocket_publish(full_subscriber_t *fsub, ngx_buf_t *buf, ngx_pool_t *frame_pool);

static ngx_int_t websocket_reserve(subscriber_t *self);
static ngx_int_t websocket_release(subscriber_t *self, uint8_t nodestroy);

static void aborted_ws_close_request_rev_handler(ngx_http_request_t *r) {
  ngx_http_finalize_request(r, NGX_HTTP_CLIENT_CLOSED_REQUEST);
}


static void sudden_abort_handler(subscriber_t *sub) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)sub;
#if FAKESHARD
  memstore_fakeprocess_push(fsub->sub.owner);
#endif
  fsub->connected = 0;
  sub->request->read_event_handler = aborted_ws_close_request_rev_handler;
  sub->status = DEAD;
  sub->fn->dequeue(sub);
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}

/*
static void sudden_upstream_request_abort_handler(full_subscriber_t *fsub) {
  
}
*/

static void init_msg_buf(ngx_buf_t *buf);

#if WEBSOCKET_OPTIMIZED_UNMASK
static void websocket_unmask_frame(ws_frame_t *frame) {
  //stupid overcomplicated websockets and their masks
  
  uint64_t   i, j, payload_len = frame->payload_len;
  u_char    *payload = frame->payload;
  u_char    *mask_key = frame->mask_key;
  uint64_t   preamble_len = payload_len <= __vector_size_bytes ? payload_len : (uintptr_t )payload % __vector_size_bytes;
  uint64_t   fastlen;
  __vector_type    w, w_mask;
  u_char     extended_mask[__vector_size_bytes];  
  
  //preamble
  for (i = 0; i < preamble_len && i < payload_len; i++) {
    payload[i] ^= mask_key[i % 4];
  }
  //are we done?
  if(payload_len < __vector_size_bytes) {
    return;
  }
  
  assert((uintptr_t )(&payload[i]) % __vector_size_bytes == 0);
  
  for(j=0; j<__vector_size_bytes; j+=4) {
    ngx_memcpy(&extended_mask[j], mask_key, 4);
  }
  
  w_mask = __vector_load_intrinsic((__vector_type *)extended_mask);
  fastlen = payload_len - ((payload_len - i) % __vector_size_bytes);
  
  assert(fastlen % __vector_size_bytes == 0);
  
  for (/*void*/; i < fastlen; i += __vector_size_bytes) {           // note that N must be multiple of [__vector_size_bytes]
    w = __vector_load_intrinsic((__vector_type *)&payload[i]);       // load [__vector_size_bytes] bytes
    w = __vector_xor_intrinsic(w, w_mask);           // XOR with mask
    __vector_store_intrinsic((__vector_type *)&payload[i], w);   // store [__vector_size_bytes] masked bytes
  }
  
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

static ngx_int_t websocket_publish_callback(ngx_int_t status, nchan_channel_t *ch, full_subscriber_t *fsub) {
  time_t                 last_seen = 0;
  ngx_uint_t             subscribers = 0;
  ngx_uint_t             messages = 0;
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
  }
  
  if(websocket_release(&fsub->sub, 0) == NGX_ABORT) {
    //zombie publisher
    //nothing more to do, we're finished here
    return NGX_OK;
  }
  
  switch(status) {
    case NCHAN_MESSAGE_QUEUED:
    case NCHAN_MESSAGE_RECEIVED:
      if(fsub->sub.cf->sub.websocket) {
        //don't reply with status info, this websocket is used for subscribing too,
        //so it should only be recieving messages
        return NGX_OK;
      }
      if(r->headers_in.accept) {
        accept_header = &r->headers_in.accept->value;
      }
      bc = nchan_bufchain_pool_reserve(fsub->ctx->bcp, 1);
      tmp_buf = nchan_channel_info_buf(accept_header, messages, subscribers, last_seen, msgid, NULL);
      ngx_memcpy(&bc->buf, tmp_buf, sizeof(*tmp_buf));
      bc->buf.last_buf=1;
      
      nchan_output_filter(fsub->sub.request, websocket_frame_header_chain(fsub, WEBSOCKET_TEXT_LAST_FRAME_BYTE, ngx_buf_size((&bc->buf)), &bc->chain));
      break;
    case NGX_ERROR:
    case NGX_HTTP_INTERNAL_SERVER_ERROR:
      websocket_respond_status(&fsub->sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL);
      break;
  }
  return NGX_OK;
}

static void websocket_publish_continue(full_subscriber_t *fsub, ngx_buf_t *buf) {
  nchan_msg_t              msg;
  struct timeval           tv;
  ngx_http_request_t      *r = fsub->sub.request;
  ngx_memzero(&msg, sizeof(msg));
  
  msg.buf=buf;
  if(r->headers_in.content_type) {
    msg.content_type.data = r->headers_in.content_type->value.data;
    msg.content_type.len = r->headers_in.content_type->value.len;
  }
  
  ngx_gettimeofday(&tv);
  msg.id.time = tv.tv_sec;
  msg.id.tag.fixed[0]=0;
  msg.id.tagcount=1;
  msg.id.tagactive=0;
  
  websocket_reserve(&fsub->sub);
  fsub->sub.cf->storage_engine->publish(fsub->publish_channel_id, &msg, fsub->sub.cf, (callback_pt )websocket_publish_callback, fsub); 
  nchan_update_stub_status(total_published_messages, 1);
  
}

static void clean_upstream_sr_tmp_pools(nchan_pub_upstream_data_t *d) {
  ngx_destroy_pool(d->tmp_pool);
  ngx_destroy_pool(d->framebuf_pool);
}

static ngx_int_t websocket_publisher_upstream_handler(ngx_http_request_t *sr, void *data, ngx_int_t rc) {
  ngx_http_request_t         *r = sr->parent;
  nchan_pub_upstream_stuff_t *stuff = (nchan_pub_upstream_stuff_t *)data;
  nchan_pub_upstream_data_t  *d = &stuff->psr_data;
  full_subscriber_t          *fsub = d->fsub;
  ngx_http_cleanup_t         *cln;
  
#if nginx_version <= 1009004
  r->main->subrequests++; //avoid tripping up subrequest loop detection
#endif
  if(r->connection->data == sr) {
    r->connection->data = r;
  }
  if(r->postponed) {
    r->postponed = NULL;
  }
  
  r->pool = d->original_pool;
  //ERR("request %p orig pool %p", r, r->pool);
  
  //discard cleanup
  r->cleanup = d->original_cleanup;
  r->count--;
  //don't destroy the temp pool just yet, 
  
  if(rc == NGX_OK) {
    ngx_int_t                 code = sr->headers_out.status;
    ngx_int_t                 content_length;
    ngx_chain_t              *request_chain;
    
    switch(code) {
      case NGX_HTTP_OK:
      case NGX_HTTP_CREATED:
      case NGX_HTTP_ACCEPTED:
        if(sr->upstream) {
          ngx_buf_t    *buf;
          
          content_length = sr->upstream->headers_in.content_length_n > 0 ? sr->upstream->headers_in.content_length_n : 0;
          request_chain = sr->upstream->out_bufs;
          
          if (request_chain->next != NULL) {
            buf = nchan_chain_to_single_buffer(d->framebuf_pool, request_chain, content_length);
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
          
          websocket_publish_continue(fsub, buf);
        }
        else {
          //content_length = 0;
          request_chain = NULL;
          ERR("upstream missing from upstream subrequest");
        }
        
        break;
      
      case NGX_HTTP_NOT_MODIFIED:
        websocket_publish_continue(fsub, &d->buf);
        
        break;
        
      case NGX_HTTP_NO_CONTENT:
        //cancel publication
        break;
      
      default:
        websocket_respond_status(&fsub->sub, NGX_HTTP_FORBIDDEN, NULL);
        break;
    }
  }
  else {
    websocket_respond_status(&fsub->sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL);
  }
  
  if(fsub->upstream_stuff == stuff) {
    fsub->upstream_stuff = stuff->next;
  }
  if(stuff->next) {
    stuff->next->prev = stuff->prev;
  }  
  if(stuff->prev) {
    stuff->prev->next = stuff->next;
  }

  if((cln = ngx_http_cleanup_add(sr, 0)) == NULL) {
    ERR("Unable to add post-request cleanup for websocket upstream request");
    return NGX_ERROR;
  }
  cln->data = d;
  cln->handler = (ngx_http_cleanup_pt )clean_upstream_sr_tmp_pools;
  
  return NGX_OK;
}

static ngx_int_t websocket_publish(full_subscriber_t *fsub, ngx_buf_t *buf, ngx_pool_t *fb_pool) {
  static ngx_str_t         nopublishing = ngx_string("Publishing not allowed.");
  static ngx_str_t         POST_REQUEST_STRING = {4, (u_char *)"POST "};
  
  if(!fsub->sub.cf->pub.websocket) {
    return websocket_send_close_frame(fsub, CLOSE_POLICY_VIOLATION, &nopublishing);
  }
  
#if (NGX_DEBUG_POOL)
  ERR("ws request pool size: %V", ngx_http_debug_pool_str(fsub->sub.request->pool));
#endif
  
  ngx_http_complex_value_t        *publisher_upstream_request_url_ccv;
  
  publisher_upstream_request_url_ccv = fsub->sub.cf->publisher_upstream_request_url;
  if(publisher_upstream_request_url_ccv == NULL) {
    websocket_publish_continue(fsub, buf);
  }
  else {
    nchan_pub_upstream_stuff_t    *psr_stuff;
    ngx_http_post_subrequest_t    *psr;
    nchan_pub_upstream_data_t     *psrd;
    ngx_http_request_t            *r = fsub->sub.request;
    ngx_http_request_t            *sr;
    ngx_http_request_body_t       *fakebody;
    ngx_chain_t                   *fakebody_chain;
    ngx_buf_t                     *fakebody_buf;
    size_t                         sz;
    
    if(!fb_pool) {
      fb_pool = ngx_create_pool(1024, r->connection->log);
    }
    
    if((psr_stuff = ngx_palloc(fb_pool, sizeof(*psr_stuff))) == NULL) {
      ERR("can't allocate memory for publisher auth subrequest");
      websocket_respond_status(&fsub->sub, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL);
      return NGX_ERROR;
    }
    
    if(fsub->upstream_stuff) {
     fsub->upstream_stuff->prev = psr_stuff;
    }
    psr_stuff->next = fsub->upstream_stuff;
    psr_stuff->prev = NULL;
    fsub->upstream_stuff = psr_stuff;
    
    psr = &psr_stuff->psr;
    psr->data = psr_stuff;
    psr->handler = websocket_publisher_upstream_handler;
    psr_stuff->psr_data.fsub = fsub;
    
    psr_stuff->psr_data.tmp_pool = NULL;
    
    ngx_http_complex_value(r, publisher_upstream_request_url_ccv, &psr_stuff->psr_data.upstream_request_url); 
    
    psrd = &fsub->upstream_stuff->psr_data;
    psr = &fsub->upstream_stuff->psr;
    
    if(psrd->tmp_pool) {
      //previous upstream request's pool
      ngx_destroy_pool(psrd->tmp_pool);
      psrd->tmp_pool = NULL;
    }
    
    psrd->framebuf_pool = fb_pool;
    
    psrd->buf = *buf;
    psrd->original_pool = r->pool;
    psrd->original_cleanup = r->cleanup;
    r->cleanup = NULL;
    
    psrd->tmp_pool = ngx_create_pool(NCHAN_WS_TMP_POOL_SIZE, r->connection->log);
    
    r->pool = psrd->tmp_pool;
    //ERR("request %p tmp pool %p", r, r->pool);
    
    fakebody = ngx_pcalloc(r->pool, sizeof(*fakebody));
    if(ngx_buf_size(buf) > 0) {
      fakebody_chain = ngx_palloc(r->pool, sizeof(*fakebody_chain));
      fakebody_buf = ngx_palloc(r->pool, sizeof(*fakebody_buf));
      fakebody->bufs = fakebody_chain;
      fakebody_chain->next = NULL;
      fakebody_chain->buf = fakebody_buf;
      init_msg_buf(fakebody_buf);
      
      //just copy the buffer contents. it's inefficient but I don't care at the moment.
      //this can and should be optimized later
      sz = ngx_buf_size(buf);
      fakebody_buf->start = ngx_palloc(r->pool, sz); //huuh?
      ngx_memcpy(fakebody_buf->start, buf->start, sz);
      fakebody_buf->end = fakebody_buf->start + sz;
      fakebody_buf->pos = fakebody_buf->start;
      fakebody_buf->last = fakebody_buf->end;
    }
    else {
      sz = 0;
    }
    
    ngx_http_subrequest(r, &psrd->upstream_request_url, NULL, &sr, psr, NGX_HTTP_SUBREQUEST_IN_MEMORY);
    nchan_adjust_subrequest(sr, NGX_HTTP_POST, &POST_REQUEST_STRING, fakebody, sz, NULL);
    
    /*
    //http request sudden close cleanup
    if((psrd->cln = ngx_http_cleanup_add(sr, 0)) == NULL) {
      ERR("Unable to add request cleanup for websocket upstream request");
      return NGX_ERROR;
    }
    psrd->cln->data = fsub;
    psrd->cln->handler = (ngx_http_cleanup_pt )sudden_upstream_request_abort_handler;
    */
  }
  
  return NGX_OK;
}

static void empty_handler() { }

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
  ngx_http_finalize_request(fsub->sub.request, NGX_OK);
}

subscriber_t *websocket_subscriber_create(ngx_http_request_t *r, nchan_msg_id_t *msg_id) {
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  
  DBG("create for req %p", r);
  full_subscriber_t  *fsub;
  if((fsub = ngx_alloc(sizeof(*fsub), ngx_cycle->log)) == NULL) {
    ERR("Unable to allocate");
    return NULL;
  }
  
  nchan_subscriber_init(&fsub->sub, &new_websocket_sub, r, msg_id);
  fsub->cln = NULL;
  fsub->ctx = ctx;
  fsub->ws_meta_subprotocol = 0;
  fsub->finalize_request = 0;
  fsub->holding = 0;
  fsub->shook_hands = 0;
  fsub->connected = 0;
  fsub->awaiting_pong = 0;
  fsub->closing = 0;
  ngx_memzero(&fsub->ping_ev, sizeof(fsub->ping_ev));
  nchan_subscriber_init_timeout_timer(&fsub->sub, &fsub->timeout_ev);
  fsub->dequeue_handler = empty_handler;
  fsub->dequeue_handler_data = NULL;
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
  
  if(fsub->sub.cf->pub.websocket) {
    fsub->publish_channel_id = nchan_get_channel_id(r, PUB, 0);
  }
  
  fsub->upstream_stuff = NULL;
  
  websocket_init_frame(&fsub->frame);
  
  //http request sudden close cleanup
  if((fsub->cln = ngx_http_cleanup_add(r, 0)) == NULL) {
    ERR("Unable to add request cleanup for websocket subscriber");
    return NULL;
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
}

ngx_int_t websocket_subscriber_destroy(subscriber_t *sub) {
  full_subscriber_t            *fsub = (full_subscriber_t  *)sub;
  nchan_pub_upstream_stuff_t   *up_cur, *up_next;
  
  if(!fsub->awaiting_destruction) {
    fsub->ctx->sub = NULL;
  }
  
  for(up_cur = fsub->upstream_stuff; up_cur != NULL; up_cur = up_next) {
    up_next = up_cur->next;
    if(up_cur->psr_data.tmp_pool) {
      ngx_destroy_pool(up_cur->psr_data.tmp_pool);
    }
    
    if(up_cur->psr_data.framebuf_pool) {
      ngx_destroy_pool(up_cur->psr_data.framebuf_pool);
    }
  }
   
  if(sub->reserved > 0) {
    DBG("%p not ready to destroy (reserved for %i) for req %p", sub, sub->reserved, fsub->sub.request);
    fsub->awaiting_destruction = 1;
  }
  else {
    DBG("%p destroy for req %p", sub, fsub->sub.request);
#if NCHAN_SUBSCRIBER_LEAK_DEBUG
    subscriber_debug_remove(&fsub->sub);
#endif
    nchan_free_msg_id(&sub->last_msgid);
    //debug 
    ngx_memset(fsub, 0x13, sizeof(*fsub));
    ngx_free(fsub);
  }
  return NGX_OK;
}

static void websocket_perform_handshake(full_subscriber_t *fsub) {
  static ngx_str_t    magic = ngx_string("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
  ngx_str_t           ws_accept_key, sha1_str;
  u_char              buf_sha1[21];
  u_char              buf[255];
  ngx_str_t          *tmp, *ws_key, *subprotocols;
  ngx_int_t           ws_version;
  ngx_http_request_t *r = fsub->sub.request;
  
  ngx_sha1_t          sha1;
  
  ws_accept_key.data = buf;
  
  r->headers_out.content_length_n = 0;
  r->header_only = 1;  
  
  if((tmp = nchan_get_header_value(r, NCHAN_HEADER_SEC_WEBSOCKET_VERSION)) == NULL) {
    r->headers_out.status = NGX_HTTP_BAD_REQUEST;
    fsub->sub.dequeue_after_response=1;
  }
  else {
    ws_version=ngx_atoi(tmp->data, tmp->len);
    if(ws_version != 13) {
      r->headers_out.status = NGX_HTTP_BAD_REQUEST;
      fsub->sub.dequeue_after_response=1;
    }
  }
  
  if((ws_key = nchan_get_header_value(r, NCHAN_HEADER_SEC_WEBSOCKET_KEY)) == NULL) {
    r->headers_out.status = NGX_HTTP_BAD_REQUEST;
    fsub->sub.dequeue_after_response=1;
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
    
    nchan_include_access_control_if_needed(r, fsub->ctx);
    nchan_add_response_header(r, &NCHAN_HEADER_SEC_WEBSOCKET_ACCEPT, &ws_accept_key);
    nchan_add_response_header(r, &NCHAN_HEADER_UPGRADE, &NCHAN_WEBSOCKET);
#if nginx_version < 1003013
    nchan_add_response_header(r, &NCHAN_HEADER_CONNECTION, &NCHAN_UPGRADE);
#endif
    r->headers_out.status_line = NCHAN_HTTP_STATUS_101;
    r->headers_out.status = NGX_HTTP_SWITCHING_PROTOCOLS;
    
    r->keepalive=0; //apparently, websocket must not use keepalive.
  }
  
  ngx_http_send_header(r);
}

static void websocket_reading(ngx_http_request_t *r);


static void ensure_request_hold(full_subscriber_t *fsub) {
  if(fsub->holding == 0) {
    fsub->sub.request->read_event_handler = websocket_reading;
    fsub->sub.request->write_event_handler = ngx_http_request_empty_handler;
    fsub->sub.request->main->count++; //this is the right way to hold and finalize the
    fsub->holding = 1;
  }
}

static ngx_int_t ensure_handshake(full_subscriber_t *fsub) {
  if(fsub->shook_hands == 0) {
    ensure_request_hold(fsub);
    websocket_perform_handshake(fsub);
    fsub->shook_hands = 1;
    fsub->connected = 1;
    return NGX_OK;
  }
  return NGX_DECLINED;
}

static ngx_int_t websocket_reserve(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  ensure_request_hold(fsub);
  self->reserved++;
  DBG("%p reserve for req %p. reservations: %i", self, fsub->sub.request, self->reserved);
  return NGX_OK;
}
static ngx_int_t websocket_release(subscriber_t *self, uint8_t nodestroy) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  assert(self->reserved > 0);
  self->reserved--;
  DBG("%p release for req %p, reservations: %i", self, fsub->sub.request, self->reserved);
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
      ngx_http_finalize_request(fsub->sub.request, NGX_HTTP_CLIENT_CLOSED_REQUEST);
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
  ensure_handshake(fsub);
  self->enqueued = 1;
  
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

static ngx_int_t websocket_dequeue(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  DBG("%p dequeue", self);
  fsub->dequeue_handler(&fsub->sub, fsub->dequeue_handler_data);
  self->enqueued = 0;
  
  if(fsub->connected) {
    ngx_str_t          close_msg = ngx_string("410 Gone");
    websocket_send_close_frame(fsub, CLOSE_NORMAL, &close_msg);
  }
  
  if(fsub->ping_ev.timer_set) {
    ngx_del_timer(&fsub->ping_ev);
  }
  
  if(fsub->closing_ev.timer_set) {
    ngx_del_timer(&fsub->closing_ev);
  }
  
  if(fsub->timeout_ev.timer_set) {
    ngx_del_timer(&fsub->timeout_ev);
  }
  
  if(self->destroy_after_dequeue) {
    websocket_subscriber_destroy(self);
  }
  return NGX_OK;
}

static ngx_int_t websocket_set_dequeue_callback(subscriber_t *self, subscriber_callback_pt cb, void *privdata) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  fsub->dequeue_handler = cb;
  fsub->dequeue_handler_data = privdata;
  return NGX_OK;
}

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
  nchan_request_ctx_t        *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  full_subscriber_t          *fsub = (full_subscriber_t *)ctx->sub;
  ws_frame_t                 *frame = &fsub->frame;
  ngx_int_t                   rc = NGX_OK;
  ngx_event_t                *rev;
  ngx_connection_t           *c;
  ngx_buf_t                   buf;
  ngx_pool_t                 *temp_pool = NULL;
  int                         free_temp_pool = 1;
  ngx_buf_t                   msgbuf;
  int                         close_code;
  ngx_str_t                   close_reason;
  
  set_buffer(&buf, frame->header, frame->last, 8);

  //ERR("fsub: %p, frame: %p", fsub, frame);
  
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
          websocket_send_frame(fsub, WEBSOCKET_CLOSE_LAST_FRAME_BYTE, 0, NULL);
          goto finalize;
        }
        
        if (frame->payload_len > 0) {
          //create a temporary pool to allocate temporary elements
          if (temp_pool == NULL) {
            if ((temp_pool = ngx_create_pool(NCHAN_WS_TMP_POOL_SIZE, r->connection->log)) == NULL) {
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
            websocket_unmask_frame(frame);
          }
        }
        frame->step = WEBSOCKET_READ_START_STEP;
        frame->last = NULL;
        switch(frame->opcode) {
          case WEBSOCKET_OPCODE_TEXT:
            if (!is_utf8(frame->payload, frame->payload_len)) {
              goto finalize;
            }
            //intentional fall-through
          case WEBSOCKET_OPCODE_BINARY:
            init_msg_buf(&msgbuf);
            
            msgbuf.start = frame->payload;
            msgbuf.pos = msgbuf.start;
            msgbuf.end = msgbuf.start + frame->payload_len;
            msgbuf.last = msgbuf.end;
            
            //DBG("we got data %V", &msg_in_str);
            
            websocket_publish(fsub, &msgbuf, temp_pool);
            if(fsub->upstream_stuff) {
              free_temp_pool = 0;
            }
            break;
          
          case WEBSOCKET_OPCODE_PING:
            DBG("%p got pinged", fsub);
            websocket_send_frame(fsub, WEBSOCKET_PONG_LAST_FRAME_BYTE, 0, NULL);
            break;
            
          case WEBSOCKET_OPCODE_PONG:
            DBG("%p Got ponged", fsub);
            if(fsub->awaiting_pong) {
              fsub->awaiting_pong = 0;
            }
            else { //unexpected PONG. quit.
              websocket_send_frame(fsub, WEBSOCKET_CLOSE_LAST_FRAME_BYTE, 0, NULL);
              goto finalize;
            }
            break;
            
          case WEBSOCKET_OPCODE_CLOSE:
            if(frame->payload_len >= 2) {
              ngx_memcpy(&close_code, frame->payload, 2);
              close_code = ntohs(close_code);
              close_reason.data = frame->payload + 2;
              close_reason.len = frame->payload_len - 2;
            }
            else {
              close_code = 0;
              close_reason.data = (u_char *)"";
              close_reason.len = 0;
            }
            DBG("%p wants to close (code %i reason \"%V\")", fsub, close_code, &close_reason);
            websocket_send_close_frame(fsub, 0, NULL);
            goto finalize;
            break; //good practice?
        }
        if(free_temp_pool && temp_pool != NULL) {
          ngx_destroy_pool(temp_pool);
          temp_pool = NULL;
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
  
  if (temp_pool != NULL) {
    ngx_destroy_pool(temp_pool);
    temp_pool = NULL;
  }
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

  if (temp_pool != NULL) {
    ngx_destroy_pool(temp_pool);
    temp_pool = NULL;
  }
  
  ngx_http_finalize_request(r, c->error ? NGX_HTTP_CLIENT_CLOSED_REQUEST : NGX_OK);
  
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

uint64_t ws_htonll(uint64_t value) {
  int num = 42;
  if (*(char *)&num == 42) {
    uint32_t high_part = htonl((uint32_t)(value >> 32));
    uint32_t low_part = htonl((uint32_t)(value & 0xFFFFFFFFLL));
    return (((uint64_t)low_part) << 32) | high_part;
  } else {
    return value;
  }
}


static void init_header_buf(ngx_buf_t *buf) {
  ngx_memzero(buf, sizeof(*buf));
  buf->memory = 1;
}

static void init_msg_buf(ngx_buf_t *buf) {
  ngx_memzero(buf, sizeof(*buf));
  buf->last_buf = 1;
  buf->last_in_chain = 1;
  buf->flush = 1;
  buf->memory = 1;
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
    len_net = ws_htonll(len);
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
  return nchan_output_filter(fsub->sub.request, websocket_frame_header_chain(fsub, opcode, len, msg_chain));
}

static ngx_chain_t *websocket_msg_frame_chain(full_subscriber_t *fsub, nchan_msg_t *msg) {
  nchan_buf_and_chain_t *bc;
  size_t                 sz = ngx_buf_size((msg->buf));
  if(fsub->ws_meta_subprotocol) {
    static ngx_str_t          id_line = ngx_string("id: ");
    static ngx_str_t          content_type_line = ngx_string("\ncontent-type: ");
    static ngx_str_t          two_newlines = ngx_string("\n\n");
    ngx_chain_t              *cur;
    ngx_str_t                 msgid;
    bc = nchan_bufchain_pool_reserve(fsub->ctx->bcp, 4 + (msg->content_type.len > 0 ? 2 : 0));
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
    
    if(msg->content_type.len > 0) {
      // content-type: 
      ngx_init_set_membuf(cur->buf, content_type_line.data, content_type_line.data + content_type_line.len);
      sz += content_type_line.len;
      cur = cur->next;
      
      //content-type value
      ngx_init_set_membuf(cur->buf, msg->content_type.data, msg->content_type.data + msg->content_type.len);
      sz += msg->content_type.len;
      cur = cur->next;
    }
    
    // \n\n
    ngx_init_set_membuf(cur->buf, two_newlines.data, two_newlines.data + two_newlines.len);
    sz += two_newlines.len;
    cur = cur->next;
    
    //now the message
    *cur->buf = *msg->buf;
    assert(cur->next == NULL);
  }
  else {
    bc = nchan_bufchain_pool_reserve(fsub->ctx->bcp, 1);
    //message first
    bc->buf = *msg->buf;
  }
  
  if(msg->buf->file) {
    nchan_msg_buf_open_fd_if_needed(&bc->buf, nchan_bufchain_pool_reserve_file(fsub->ctx->bcp), NULL);
  }
  
  //now the header
  return websocket_frame_header_chain(fsub, WEBSOCKET_TEXT_LAST_FRAME_BYTE, sz, &bc->chain);
}

static ngx_int_t websocket_send_close_frame(full_subscriber_t *fsub, uint16_t code, ngx_str_t *err) {
  nchan_output_filter(fsub->sub.request, websocket_close_frame_chain(fsub, code, err));
  fsub->connected = 0;
  if(fsub->closing == 1) {
    DBG("%p already sent close frame");
    ngx_http_finalize_request(fsub->sub.request, NGX_OK);
  }
  else {
    fsub->closing = 1;
    ngx_add_timer(&fsub->closing_ev, WEBSOCKET_CLOSING_TIMEOUT);
  }
  return NGX_OK;
}

static ngx_chain_t *websocket_close_frame_chain(full_subscriber_t *fsub, uint16_t code, ngx_str_t *err) {
  nchan_buf_and_chain_t *bc; 
  ngx_chain_t   *hdr_chain;
  ngx_buf_t     *hdr_buf;
  ngx_buf_t     *msg_buf;
  ngx_str_t      alt_err;
  uint16_t       code_net;
  
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
  
  if(code < 1000 || code > 1011) {
    ERR("invalid websocket close status code %i", code);
    code=CLOSE_NORMAL;
  }
  bc = nchan_bufchain_pool_reserve(fsub->ctx->bcp, 1);
  msg_buf = &bc->buf;
  init_msg_buf(msg_buf);
  set_buf_to_str(msg_buf, err);
  hdr_chain = websocket_frame_header_chain(fsub, WEBSOCKET_CLOSE_LAST_FRAME_BYTE, err->len + 2, &bc->chain);
  hdr_buf = hdr_chain->buf;
  //there's definitely enough space at the end for 2 more bytes
  code_net=htons(code);
  hdr_buf->last = ngx_copy(hdr_buf->last, &code_net, 2);
  hdr_buf->end = hdr_buf->last;
  

  return hdr_chain;
}

static ngx_int_t websocket_respond_message(subscriber_t *self, nchan_msg_t *msg) {
  ngx_int_t        rc;
  full_subscriber_t         *fsub = (full_subscriber_t *)self;
  ensure_handshake(fsub);
  
  if(fsub->timeout_ev.timer_set) {
    ngx_del_timer(&fsub->timeout_ev);
    ngx_add_timer(&fsub->timeout_ev, fsub->sub.cf->subscriber_timeout * 1000);
  }
  
  fsub->ctx->prev_msg_id = self->last_msgid;
  update_subscriber_last_msg_id(self, msg);
  fsub->ctx->msg_id = self->last_msgid;
  
  rc = nchan_output_msg_filter(fsub->sub.request, msg, websocket_msg_frame_chain(fsub, msg));
  
  return rc;
}

static ngx_int_t websocket_respond_status(subscriber_t *self, ngx_int_t status_code, const ngx_str_t *status_line) {
  static const ngx_str_t    STATUS_410=ngx_string("410 Channel Deleted");
  static const ngx_str_t    STATUS_403=ngx_string("403 Forbidden");
  static const ngx_str_t    STATUS_500=ngx_string("500 Internal Server Error");
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
    return nchan_respond_status(fsub->sub.request, status_code, status_line, 0);
  }
  
  switch(status_code) {
    case 410:
      close_code = CLOSE_GOING_AWAY;
      close_msg = (ngx_str_t *)(status_line ? status_line : &STATUS_410);
      break;
    case 403:
      close_code = CLOSE_POLICY_VIOLATION;
      close_msg = (ngx_str_t *)(status_line ? status_line : &STATUS_403);
      break;
    case 500: 
      close_code = CLOSE_INTERNAL_SERVER_ERROR;
      close_msg = (ngx_str_t *)(status_line ? status_line : &STATUS_500);
      break;
    default:
      if((status_code >= 400 && status_code < 600) || status_code == NGX_HTTP_NOT_MODIFIED) {
        custom_close_msg.data=msgbuf;
        custom_close_msg.len = ngx_sprintf(msgbuf,"%i %v", status_code, (status_line ? status_line : &empty)) - msgbuf;
        close_msg = &custom_close_msg;
        close_code = CLOSE_NORMAL;
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

static const subscriber_fn_t websocket_fn = {
  &websocket_enqueue,
  &websocket_dequeue,
  &websocket_respond_message,
  &websocket_respond_status,
  &websocket_set_dequeue_callback,
  &websocket_reserve,
  &websocket_release,
  &nchan_subscriber_empty_notify,
  &nchan_subscriber_authorize_subscribe
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
  0, //reserved
  0, //deque after response
  1, //destroy after dequeue
  0, //enqueued
#if NCHAN_SUBSCRIBER_LEAK_DEBUG
  NULL,
  NULL
#endif
};
