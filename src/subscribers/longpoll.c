#include <ngx_http_push_module.h>
#define DEBUG_LEVEL NGX_LOG_INFO
#define DBG(...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, __VA_ARGS__)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)


static const subscriber_t new_longpoll_sub;

typedef struct {
  unsigned         finalize_request:1;
} subscriber_data_t;

typedef struct {
  subscriber_t       sub;
  subscriber_data_t  data;
} full_subscriber_t;

subscriber_t *longpoll_subscriber_create(ngx_http_request_t *r) {
  DBG("longpoll create for req %p", r);
  full_subscriber_t  *fsub;
  if((fsub = ngx_alloc(sizeof(*fsub), ngx_cycle->log)) == NULL) {
    ERR("Unable to allocate longpoll subscriber");
    return NULL;
  }
  ngx_memcpy(&fsub->sub, &new_longpoll_sub, sizeof(new_longpoll_sub));
  fsub->sub.data = &fsub->data;
  fsub->sub.request = r;
  fsub->data.finalize_request = 0;
  return &fsub->sub;
}

ngx_int_t longpoll_subscriber_destroy(subscriber_t *sub) {
  DBG("longpoll destroy %p for req %p", sub, sub->request);
  ngx_free(sub);
  return NGX_OK;
}

ngx_int_t longpoll_enqueue(subscriber_t *self, ngx_int_t timeout) {
  DBG("longpoll enqueue sub %p req %p", self, self->request);
  self->request->read_event_handler = ngx_http_test_reading;
  self->request->write_event_handler = ngx_http_request_empty_handler;
  self->request->main->count++; //this is the right way to hold and finalize the request... maybe
  ((subscriber_data_t *)self->data)->finalize_request = 1;
  return NGX_OK;
}

ngx_int_t longpoll_dequeue(subscriber_t *self) {
  DBG("longpoll dequeue sub %p req %p", self, self->request);
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
  if(((subscriber_data_t *)self->data)->finalize_request) {
    ngx_http_finalize_request(self->request, rc);
  }
  return NGX_OK;
}
static ngx_int_t abort_response(subscriber_t *sub, char *errmsg) {
  ERR(errmsg);
  finalize_maybe(sub, NGX_ERROR);
  dequeue_maybe(sub);
  return NGX_ERROR;
}

ngx_int_t longpoll_respond_message(subscriber_t *self, ngx_http_push_msg_t *msg) {
  DBG("longpoll respond sub %p req %p msg %p", self, self->request, msg);
  ngx_http_request_t        *r = self->request;
  ngx_chain_t               *rchain;
  ngx_buf_t                 *buffer = msg->buf;
  ngx_buf_t                 *rbuffer;
  ngx_str_t                 *etag;
  ngx_file_t                *rfile;
  ngx_pool_cleanup_t        *cln = NULL;
  ngx_pool_cleanup_file_t   *clnf = NULL;
  ngx_int_t                  rc;
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

ngx_int_t longpoll_respond_status(subscriber_t *self, ngx_int_t status_code, const ngx_str_t *status_line) {
  DBG("longpoll respond sub %p req %p status %i", self, self->request, status_code);
  ngx_http_request_t    *r = self->request;
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

ngx_http_cleanup_t *longpoll_add_next_response_cleanup(subscriber_t *self, size_t privdata_size) {
  DBG("longpoll %p req %p add_response_cleanup", self, self->request);
  return ngx_http_cleanup_add(self->request, privdata_size);
}

static const subscriber_t new_longpoll_sub = {
  &longpoll_enqueue,
  &longpoll_dequeue,
  &longpoll_respond_message,
  &longpoll_respond_status,
  &longpoll_add_next_response_cleanup,
  "longpoll",
  LONGPOLL,
  1, //deque after response
  1, //destroy after dequeue
  NULL
};