#include <ngx_http_push_module.h>
#include <ngx_crypt.h>
#include <ngx_sha1.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SUB:WEBSOCKET:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SUB:WEBSOCKET:" fmt, ##arg)
#include <assert.h>

//debugstuff
void memstore_fakeprocess_push(ngx_int_t slot);
void memstore_fakeprocess_push_random(void);
void memstore_fakeprocess_pop();
ngx_int_t memstore_slot();

static const subscriber_t new_websocket_sub;

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
  
  fsub->owner = memstore_slot();
  
  //http request sudden close cleanup
  if((fsub->cln = ngx_http_cleanup_add(r, 0)) == NULL) {
    ERR("Unable to add request cleanup for websocket subscriber");
    return NULL;
  }
  fsub->cln->data = fsub;
  fsub->cln->handler = (ngx_http_cleanup_pt )sudden_abort_handler;
  DBG("%p created for request %p", &fsub->sub, r);
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

static ngx_int_t ensure_handshake(full_subscriber_t *fsub) {
  if(fsub->shook_hands == 0) {
    fsub->request->read_event_handler = ngx_http_test_reading;
    fsub->request->write_event_handler = ngx_http_request_empty_handler;
    fsub->request->main->count++; //this is the right way to hold and finalize the
    websocket_perform_handshake(fsub);
    fsub->shook_hands = 1;
  }
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
  //TODO
  return NGX_OK;
}

static ngx_int_t websocket_dequeue(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
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
