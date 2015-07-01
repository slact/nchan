#include <ngx_http_push_module.h>
#define DEBUG_LEVEL NGX_LOG_INFO
#define DBG(...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, __VA_ARGS__)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)


static const subscriber_t new_longpoll_sub;

typedef struct {
  ngx_http_request_t      *request;
  ngx_http_cleanup_t      *cln;
  subscriber_callback_pt  dequeue_handler;
  void                   *dequeue_handler_data;
  ngx_event_t             timeout_ev;
  subscriber_callback_pt  timeout_handler;
  void                   *timeout_handler_data;
  unsigned                finalize_request:1;
} subscriber_data_t;

typedef struct {
  subscriber_t       sub;
  subscriber_data_t  data;
} full_subscriber_t;

static void empty_handler() { }

subscriber_t *longpoll_subscriber_create(ngx_http_request_t *r) {
  DBG("longpoll create for req %p", r);
  full_subscriber_t  *fsub;
  if((fsub = ngx_alloc(sizeof(*fsub), ngx_cycle->log)) == NULL) {
    ERR("Unable to allocate longpoll subscriber");
    return NULL;
  }
  ngx_memcpy(&fsub->sub, &new_longpoll_sub, sizeof(new_longpoll_sub));
  fsub->sub.data = &fsub->data;
  fsub->data.request = r;
  fsub->data.cln = NULL;
  fsub->data.finalize_request = 0;
  fsub->sub.cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
  fsub->sub.pool = r->pool;
  ngx_memzero(&fsub->data.timeout_ev, sizeof(fsub->data.timeout_ev));
  fsub->data.timeout_handler = empty_handler;
  fsub->data.timeout_handler_data = NULL;
  fsub->data.dequeue_handler = empty_handler;
  fsub->data.dequeue_handler_data = NULL;
  return &fsub->sub;
}

ngx_int_t longpoll_subscriber_destroy(subscriber_t *sub) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)sub;
  DBG("longpoll destroy %p for req %p", sub, fsub->data.request);
  ngx_free(sub);
  return NGX_OK;
}

static void timeout_ev_handler(ngx_event_t *ev) {
  full_subscriber_t *fsub = (full_subscriber_t *)ev->data;
  fsub->data.timeout_handler(&fsub->sub, fsub->data.timeout_handler_data);
  fsub->sub.dequeue_after_response = 1;
  fsub->sub.respond_status(&fsub->sub, NGX_HTTP_NOT_MODIFIED, NULL);
}

ngx_int_t longpoll_enqueue(subscriber_t *self) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  DBG("longpoll enqueue sub %p req %p", self, fsub->data.request);
  fsub->data.request->read_event_handler = ngx_http_test_reading;
  fsub->data.request->write_event_handler = ngx_http_request_empty_handler;
  fsub->data.request->main->count++; //this is the right way to hold and finalize the request... maybe
  fsub->data.finalize_request = 1;
  
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
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  if(fsub->data.timeout_ev.timer_set) {
    ngx_del_timer(&fsub->data.timeout_ev);
  }
  DBG("longpoll dequeue sub %p req %p", self, fsub->data.request);
  fsub->data.dequeue_handler(&fsub->sub, fsub->data.dequeue_handler_data);
  if(self->destroy_after_dequeue) {
    longpoll_subscriber_destroy(self);
  }
  return NGX_OK;
}

static ngx_int_t dequeue_maybe(subscriber_t *self) {
  if(self->dequeue_after_response) {
    self->dequeue(self);
  }
  return NGX_OK;
}

static ngx_int_t finalize_maybe(subscriber_t *self, ngx_int_t rc) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  if(((subscriber_data_t *)self->data)->finalize_request) {
    ngx_http_finalize_request(fsub->data.request, rc);
  }
  return NGX_OK;
}
static ngx_int_t abort_response(subscriber_t *sub, char *errmsg) {
  ERR(errmsg);
  finalize_maybe(sub, NGX_ERROR);
  dequeue_maybe(sub);
  return NGX_ERROR;
}

static ngx_int_t longpoll_respond_message(subscriber_t *self, ngx_http_push_msg_t *msg) {
  full_subscriber_t  *fsub = (full_subscriber_t  *)self;
  ngx_http_request_t        *r = fsub->data.request;
  ngx_chain_t               *rchain;
  ngx_buf_t                 *buffer = msg->buf;
  ngx_buf_t                 *rbuffer;
  ngx_str_t                 *etag;
  ngx_file_t                *rfile;
  ngx_pool_cleanup_t        *cln = NULL;
  ngx_pool_cleanup_file_t   *clnf = NULL;
  ngx_int_t                  rc;
  DBG("longpoll respond sub %p req %p msg %p", self, fsub->data.request, msg);
  if(buffer == NULL) {
    return abort_response(self, "attemtping to respond to subscriber with message with NULL buffer");
  }
  

  //message body
  rchain = ngx_pcalloc(r->pool, sizeof(*rchain));
  rbuffer = ngx_pcalloc(r->pool, sizeof(*rbuffer));
  rchain->next = NULL;
  rchain->buf = rbuffer;
  //rbuffer->recycled = 1; //do we need this? it's a fresh buffer with recycled data, so I'm guessing no.
  
  ngx_memcpy(rbuffer, buffer, sizeof(*buffer));
  //copied buffer will point to the original buffer's data
  
  rfile = rbuffer->file;
  if(rfile != NULL) {
    if((rfile = ngx_pcalloc(r->pool, sizeof(*rfile))) == NULL) {
      return abort_response(self, "couldn't allocate memory for file struct");
    }
    ngx_memcpy(rfile, buffer->file, sizeof(*rbuffer));
    rbuffer->file = rfile;
  }
  
  //ensure the file, if present, is open
  if((rfile = rbuffer->file) != NULL && rfile->fd == NGX_INVALID_FILE) {
    if(rfile->name.len == 0) {
      return abort_response(self, "longpoll subscriber given an invalid fd with no filename");
    }
    rfile->fd = ngx_open_file(rfile->name.data, NGX_FILE_RDONLY, NGX_FILE_OPEN, NGX_FILE_OWNER_ACCESS);
    if(rfile->fd == NGX_INVALID_FILE) {
      return abort_response(self, "can't create output chain, file in buffer won't open");
    }
    //and that it's closed when we're finished with it
    cln = ngx_pool_cleanup_add(r->pool, sizeof(*clnf));
    if(cln == NULL) {
      ngx_close_file(rfile->fd);
      rfile->fd=NGX_INVALID_FILE;
      return abort_response(self, "push module: can't create output chain file cleanup.");
    }
    cln->handler = ngx_pool_cleanup_file;
    clnf = cln->data;
    clnf->fd = rfile->fd;
    clnf->name = rfile->name.data;
    clnf->log = r->connection->log;
  }
  
  //now headers and stuff
  if (msg->content_type.data!=NULL) {
    r->headers_out.content_type.len=msg->content_type.len;
    r->headers_out.content_type.data = msg->content_type.data;
    r->headers_out.content_type_len = r->headers_out.content_type.len;
  }

  //message id: 2 parts, last-modified and etag headers.

  //last-modified
  r->headers_out.last_modified_time = msg->message_time;

  //etag
  if((etag = ngx_palloc(r->pool, sizeof(*etag) + NGX_INT_T_LEN))==NULL) {
    return abort_response(self, "unable to allocate memory for Etag header in subscriber's request pool");
  }
  etag->data = (u_char *)(etag+1);
  etag->len = ngx_sprintf(etag->data,"%ui", msg->message_tag)- etag->data;
  if ((ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ETAG, etag))==NULL) {
    return abort_response(self, "can't add etag header to response");
  }
  //Vary header needed for proper HTTP caching.
  ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_VARY, &NGX_HTTP_PUSH_VARY_HEADER_VALUE);
  
  r->headers_out.status=NGX_HTTP_OK;

  //now send that message

  //we know the entity length, and we're using just one buffer. so no chunking please.
  r->headers_out.content_length_n=ngx_buf_size(rbuffer);
  if((rc = ngx_http_send_header(r)) >= NGX_HTTP_SPECIAL_RESPONSE) {
    ERR("request %p, send_header response %i", r, rc);
    return abort_response(self, "WTF just happened to request?");
  }

  rc = ngx_http_output_filter(r, rchain);
  finalize_maybe(self, NGX_OK);
  dequeue_maybe(self);
  return NGX_OK;
}

static ngx_int_t longpoll_respond_status(subscriber_t *self, ngx_int_t status_code, const ngx_str_t *status_line) {
  ngx_http_request_t    *r = ((full_subscriber_t *)self)->data.request;
  DBG("longpoll respond sub %p req %p status %i", self, r, status_code);
  r->headers_out.status=status_code;
  if(status_line!=NULL) {
    r->headers_out.status_line.len =status_line->len;
    r->headers_out.status_line.data=status_line->data;
  }
  r->headers_out.content_length_n = 0;
  r->header_only = 1;
  ngx_http_send_header(r);
  finalize_maybe(self, NGX_OK);
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
    fsub->data.cln = ngx_http_cleanup_add(fsub->data.request, 0);
    fsub->data.cln->data = self;
    fsub->data.cln->handler = (ngx_http_cleanup_pt )request_cleanup_handler;
  }
  fsub->data.dequeue_handler = cb;
  fsub->data.dequeue_handler_data = privdata;
  return NGX_OK;
}

static const subscriber_t new_longpoll_sub = {
  &longpoll_enqueue,
  &longpoll_dequeue,
  &longpoll_respond_message,
  &longpoll_respond_status,
  &longpoll_set_timeout_callback,
  &longpoll_set_dequeue_callback,
  "longpoll",
  LONGPOLL,
  1, //deque after response
  1, //destroy after dequeue
  NULL
};