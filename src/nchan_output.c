#include <ngx_http.h>
#include "nchan_output.h"
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "OUTPUT:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "OUTPUT:" fmt, ##arg)

#define REQUEST_PCALLOC(r, what) what = ngx_pcalloc((r)->pool, sizeof(*(what)))
#define REQUEST_PALLOC(r, what) what = ngx_palloc((r)->pool, sizeof(*(what)))

//general request-output functions and the iraq and the asian countries and dated references and the, uh, such

static void nchan_flush_pending_output(ngx_http_request_t *r) {
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
      ngx_log_error(NGX_LOG_INFO, c->log, NGX_ETIMEDOUT, "request timed out");
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
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, wev->log, 0, "http writer delayed");
    if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
      ngx_http_finalize_request(r, 0);
    }
    return;
  }
  
  rc = nchan_output_filter(r, NULL);

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

ngx_int_t nchan_output_filter(ngx_http_request_t *r, ngx_chain_t *in) {
/* from push stream module, written by
 * Wandenberg Peixoto <wandenberg@gmail.com>, Rogério Carvalho Schneider <stockrt@gmail.com>
 * thanks, guys!
*/
  ngx_http_core_loc_conf_t               *clcf;
  ngx_int_t                               rc;
  ngx_event_t                            *wev;
  ngx_connection_t                       *c;

  c = r->connection;
  wev = c->write;

  rc = ngx_http_output_filter(r, in);

  if (c->buffered & NGX_HTTP_LOWLEVEL_BUFFERED) {
    ERR("what's the deal with this NGX_HTTP_LOWLEVEL_BUFFERED thing?");
    clcf = ngx_http_get_module_loc_conf(r->main, ngx_http_core_module);
    r->write_event_handler = nchan_flush_pending_output;
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

ngx_int_t nchan_respond_status(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *status_line, ngx_int_t finalize) {
  ngx_int_t    rc = NGX_OK;
  r->headers_out.status=status_code;
  if(status_line!=NULL) {
    r->headers_out.status_line.len =status_line->len;
    r->headers_out.status_line.data=status_line->data;
  }
  r->headers_out.content_length_n = 0;
  r->header_only = 1;
    
  rc= ngx_http_send_header(r);
  if(finalize) {
    ngx_http_finalize_request(r, rc);
  }
  return rc;
}

ngx_int_t nchan_respond_cstring(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *content_type, char *body, ngx_int_t finalize) {
  ngx_str_t str;
  str.data = (u_char *)body;
  str.len=strlen(body);
  return nchan_respond_string(r, status_code, content_type, &str, finalize);
}

ngx_int_t nchan_respond_membuf(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *content_type, ngx_buf_t *body, ngx_int_t finalize) {
  ngx_str_t str;
  str.len = ngx_buf_size(body);
  str.data = body->start;
  return nchan_respond_string(r, status_code, content_type, &str, finalize);
}

ngx_int_t nchan_respond_msg(ngx_http_request_t *r, nchan_msg_t *msg, ngx_int_t finalize, char **err) {
  ngx_buf_t                 *buffer = msg->buf;
  nchan_buf_and_chain_t     *cb;
  ngx_str_t                 *etag;
  ngx_int_t                  rc;
  ngx_chain_t               *rchain;
  ngx_buf_t                 *rbuffer;
  ngx_file_t                *rfile;
  
  cb = ngx_palloc(r->pool, sizeof(*cb));
  rchain = &cb->chain;
  rbuffer = &cb->buf;
  
  rchain->next = NULL;
  rchain->buf = rbuffer;
  
  ngx_memcpy(rbuffer, buffer, sizeof(*buffer));
  
  rfile = rbuffer->file;
  if(rfile != NULL) {
    if((rfile = ngx_pcalloc(r->pool, sizeof(*rfile))) == NULL) {
      if(err) *err = "couldn't allocate memory for file struct";
      return NGX_ERROR;
    }
    ngx_memcpy(rfile, buffer->file, sizeof(*rbuffer));
    rbuffer->file = rfile;
  }
  
  if((rfile = rbuffer->file) != NULL && rfile->fd == NGX_INVALID_FILE) {
    ngx_pool_cleanup_t        *cln = NULL;
    ngx_pool_cleanup_file_t   *clnf = NULL;
    if(rfile->name.len == 0) {
      if(err) *err = "longpoll subscriber given an invalid fd with no filename";
      return NGX_ERROR;
    }
    rfile->fd = ngx_open_file(rfile->name.data, NGX_FILE_RDONLY, NGX_FILE_OPEN, NGX_FILE_OWNER_ACCESS);
    if(rfile->fd == NGX_INVALID_FILE) {
      if(err) *err = "can't create output chain, file in buffer won't open";
      return NGX_ERROR;
    }
    //and that it's closed when we're finished with it
    cln = ngx_pool_cleanup_add(r->pool, sizeof(*clnf));
    if(cln == NULL) {
      ngx_close_file(rfile->fd);
      rfile->fd=NGX_INVALID_FILE;
      if(err) *err = "nchan: can't create output chain file cleanup.";
      return NGX_ERROR;
    }
    cln->handler = ngx_pool_cleanup_file;
    clnf = cln->data;
    clnf->fd = rfile->fd;
    clnf->name = rfile->name.data;
    clnf->log = r->connection->log;
  }
  
  if (msg->content_type.data!=NULL) {
    r->headers_out.content_type.len=msg->content_type.len;
    r->headers_out.content_type.data = msg->content_type.data;
  }
  
  //last-modified
  r->headers_out.last_modified_time = msg->message_time;

  //etag
  if((etag = ngx_palloc(r->pool, sizeof(*etag) + NGX_INT_T_LEN))==NULL) {
    if(err) *err = "unable to allocate memory for Etag header in subscriber's request pool";
    return NGX_ERROR;
  }
  etag->data = (u_char *)(etag+1);
  etag->len = ngx_sprintf(etag->data,"%ui", msg->message_tag)- etag->data;
  if ((nchan_add_response_header(r, &NCHAN_HEADER_ETAG, etag))==NULL) {
    if(err) *err="can't add etag header to response";
    return NGX_ERROR;
  }
  //Vary header needed for proper HTTP caching.
  nchan_add_response_header(r, &NCHAN_HEADER_VARY, &NCHAN_VARY_HEADER_VALUE);
  
  r->headers_out.status=NGX_HTTP_OK;
  
  //now send that message

  //we know the entity length, and we're using just one buffer. so no chunking please.
  r->headers_out.content_length_n=ngx_buf_size(rbuffer);
  if((rc = ngx_http_send_header(r)) >= NGX_HTTP_SPECIAL_RESPONSE) {
    ERR("request %p, send_header response %i", r, rc);
    if(err) *err="WTF just happened to request?";
    return NGX_ERROR;
  }

  rc= nchan_output_filter(r, rchain);
  if(finalize) {
    ngx_http_finalize_request(r, rc);
  }
  return rc;
}

ngx_int_t nchan_respond_string(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *content_type, const ngx_str_t *body, ngx_int_t finalize) {
  ngx_int_t    rc = NGX_OK;
  ngx_buf_t   *b = REQUEST_PCALLOC(r, b);
  ngx_chain_t *chain = REQUEST_PALLOC(r, chain);
  
  //assume both were alloc'd fine
  
  r->headers_out.status=status_code;
  r->headers_out.content_length_n = body->len;
  
  if(content_type) {
    r->headers_out.content_type.len = content_type->len;
    r->headers_out.content_type.data = content_type->data;
  }
  
  chain->buf=b;
  chain->next=NULL;
  
  b->last_buf = 1;
  b->last_in_chain = 1;
  b->flush = 1; //flush just to be sure, although I should perhaps rethink this
  b->memory = 1;
  b->start = body->data;
  b->pos = body->data;
  b->end = body->data + body->len;
  b->last = b->end;
  
  ngx_http_send_header(r);
  rc= nchan_output_filter(r, chain);
  
  if(finalize) {
    ngx_http_finalize_request(r, rc);
  }
  return rc;
}

ngx_table_elt_t * nchan_add_response_header(ngx_http_request_t *r, const ngx_str_t *header_name, const ngx_str_t *header_value) {
  ngx_table_elt_t                *h = ngx_list_push(&r->headers_out.headers);
  if (h == NULL) {
    return NULL;
  }
  h->hash = 1;
  h->key.len = header_name->len;
  h->key.data = header_name->data;
  h->value.len = header_value->len;
  h->value.data = header_value->data;
  return h;
}

ngx_int_t nchan_OPTIONS_respond(ngx_http_request_t *r, const ngx_str_t *allow_origin, const ngx_str_t *allowed_headers, const ngx_str_t *allowed_methods) {
  static const  ngx_str_t ALLOW_HEADERS = ngx_string("Access-Control-Allow-Headers");
  static const  ngx_str_t ALLOW_METHODS = ngx_string("Access-Control-Allow-Methods");
  static const  ngx_str_t ALLOW_ORIGIN = ngx_string("Access-Control-Allow-Origin");
  
  nchan_add_response_header(r, &ALLOW_ORIGIN,  allow_origin);
  nchan_add_response_header(r, &ALLOW_HEADERS, allowed_headers);
  nchan_add_response_header(r, &ALLOW_METHODS, allowed_methods);
  return nchan_respond_status(r, NGX_HTTP_OK, NULL, 0);
}

/*

void nchan_copy_preallocated_buffer(ngx_buf_t *buf, ngx_buf_t *cbuf) {
  if (cbuf!=NULL) {
    ngx_memcpy(cbuf, buf, sizeof(*buf)); //overkill?
    if(buf->temporary || buf->memory) { //we don't want to copy mmpapped memory, so no ngx_buf_in_momory(buf)
      cbuf->pos = (u_char *) (cbuf+1);
      cbuf->last = cbuf->pos + ngx_buf_size(buf);
      cbuf->start=cbuf->pos;
      cbuf->end = cbuf->start + ngx_buf_size(buf);
      ngx_memcpy(cbuf->pos, buf->pos, ngx_buf_size(buf));
      cbuf->memory=ngx_buf_in_memory_only(buf) ? 1 : 0;
    }
    if (buf->file!=NULL) {
      cbuf->file = (ngx_file_t *) (cbuf+1) + ((buf->temporary || buf->memory) ? ngx_buf_size(buf) : 0);
      cbuf->file->fd=buf->file->fd;
      cbuf->file->log=ngx_cycle->log;
      cbuf->file->offset=buf->file->offset;
      cbuf->file->sys_offset=buf->file->sys_offset;
      cbuf->file->name.len=buf->file->name.len;
      cbuf->file->name.data=(u_char *) (cbuf->file+1);
      ngx_memcpy(cbuf->file->name.data, buf->file->name.data, buf->file->name.len);
    }
  }
}

#define NGX_HTTP_BUF_ALLOC_SIZE(buf)                                         \
(sizeof(*buf) +                                                              \
(((buf)->temporary || (buf)->memory) ? ngx_buf_size(buf) : 0) +              \
(((buf)->file!=NULL) ? (sizeof(*(buf)->file) + (buf)->file->name.len + 1) : 0))

//buffer is _copied_
ngx_chain_t * nchan_create_output_chain(ngx_buf_t *buf, ngx_pool_t *pool, ngx_log_t *log) {
  ngx_chain_t                    *out;
  ngx_file_t                     *file;
  ngx_pool_cleanup_t             *cln = NULL;
  ngx_pool_cleanup_file_t        *clnf = NULL;
  if((out = ngx_pcalloc(pool, sizeof(*out)))==NULL) {
    ngx_log_error(NGX_LOG_ERR, log, 0, "nchan: can't create output chain, can't allocate chain  in pool");
    return NULL;
  }
  ngx_buf_t                      *buf_copy;
  
  if((buf_copy = ngx_pcalloc(pool, NGX_HTTP_BUF_ALLOC_SIZE(buf)))==NULL) {
    //TODO: don't zero the whole thing!
    ngx_log_error(NGX_LOG_ERR, log, 0, "nchan: can't create output chain, can't allocate buffer copy in pool");
    return NULL;
  }
  nchan_copy_preallocated_buffer(buf, buf_copy);
  
  if (buf->file!=NULL) {
    if(buf->mmap) { //just the mmap, please
      buf->in_file=0;
      buf->file=NULL;
      buf->file_pos=0;
      buf->file_last=0;
    }
    else {
      file = buf_copy->file;
      file->log=log;
      if(file->fd==NGX_INVALID_FILE) {
        //ngx_log_error(NGX_LOG_ERR, log, 0, "opening invalid file at %s", file->name.data);
        file->fd=ngx_open_file(file->name.data, NGX_FILE_RDONLY, NGX_FILE_OPEN, NGX_FILE_OWNER_ACCESS);
      }
      if(file->fd==NGX_INVALID_FILE) {
        ngx_log_error(NGX_LOG_ERR, log, 0, "nchan: can't create output chain, file in buffer is invalid");
        return NULL;
      }
      else {
        //close file on cleanup
        if((cln = ngx_pool_cleanup_add(pool, sizeof(*clnf))) == NULL) {
          ngx_close_file(file->fd);
          file->fd=NGX_INVALID_FILE;
          ngx_log_error(NGX_LOG_ERR, log, 0, "nchan: can't create output chain file cleanup.");
          return NULL;
        }
        cln->handler = ngx_pool_cleanup_file;
        clnf = cln->data;
        clnf->fd = file->fd;
        clnf->name = file->name.data;
        clnf->log = pool->log;
      }
    }
  }
  
  
  
  buf_copy->last_buf = 1;
  out->buf = buf_copy;
  out->next = NULL;
  return out;
}
*/