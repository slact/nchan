#include <nchan_module.h>
#include "subscribers/websocket.h"
#include <nchan_websocket_publisher.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "WEBSOCKET_PUBLISHER:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "WEBSOCKET_PUBLISHER:" fmt, ##arg)

static nchan_llist_timed_t  ws_pub_head;

void nchan_websocket_publisher_llist_init() {
  DBG("init WS publisher llist");
  ws_pub_head.prev = &ws_pub_head;
  ws_pub_head.next = &ws_pub_head;
  ws_pub_head.data = NULL;
  ws_pub_head.time = 0;
}


static nchan_llist_timed_t   *nchan_ws_llist_enqueue(subscriber_t *sub) {
  nchan_llist_timed_t    *cur, *last;
  if((cur = ngx_alloc(sizeof(*cur), ngx_cycle->log)) == NULL) {
    ERR("couldn't allocate llink for websocket publisher");
    return NULL;
  }
  last = ws_pub_head.prev;
  
  cur->prev = last;
  last->next = cur;
  
  cur->next = &ws_pub_head;
  ws_pub_head.prev = cur;
  
  cur->time = ngx_time();
  cur->data = (void *)sub;
  return cur;
}

static ngx_int_t nchan_ws_llink_destroy(nchan_llist_timed_t *cur) {
  nchan_llist_timed_t    *prev, *next;
  prev = cur->prev;
  next = cur->next;
  
  prev->next = next;
  next->prev = prev;
  
  ngx_memset(cur, 0xC4, sizeof(*cur)); //debugstuffs
  ngx_free(cur);
  return NGX_OK;
}

static void ws_publisher_dequeue_callback(subscriber_t *sub, void *privdata) {
  nchan_ws_llink_destroy((nchan_llist_timed_t *)privdata);
}

static ngx_str_t   pub_name = ngx_string("websocket");

ngx_int_t nchan_create_websocket_publisher(ngx_http_request_t  *r) {
  subscriber_t         *sub;
  nchan_llist_timed_t  *sub_link;
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  if(ctx) {
    ctx->publisher_type = &pub_name;
  }
  
  if((sub = websocket_subscriber_create(r, NULL)) == NULL) {
    ERR("couldn't create websocket publisher.");
    return NGX_ERROR;
  }
  
  if((sub_link = nchan_ws_llist_enqueue(sub)) == NULL) {
    websocket_subscriber_destroy(sub);
    ERR("couldn't create websocket publisher llink");
    return NGX_ERROR;
  }
  
  sub->fn->set_dequeue_callback(sub, ws_publisher_dequeue_callback, sub_link);
  sub->fn->enqueue(sub);
  return NGX_OK;
}
