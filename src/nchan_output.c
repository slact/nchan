#include <ngx_http.h>
#include <nchan_module.h>
#include "nchan_output.h"
#include <assert.h>
#include <nchan_thingcache.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "OUTPUT:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "OUTPUT:" fmt, ##arg)

#define REQUEST_PCALLOC(r, what) what = ngx_pcalloc((r)->pool, sizeof(*(what)))
#define REQUEST_PALLOC(r, what) what = ngx_palloc((r)->pool, sizeof(*(what)))




//file descriptor cache
static void *fd_cache = NULL;

static void *fd_open(ngx_str_t *filename) {
  static u_char   fn_buf[512];
  u_char         *fname, *last;
  ngx_fd_t        fd;
  off_t           len = filename->len;
  last = filename->data + len;
  if(*last == '\0' || (len > 0 && *(last - 1) == '\0')) {
    fname = filename->data;
  }
  else if(filename->len < 512) {
    DBG("non-null-terminated filename. gotta copy.");
    ngx_memcpy(&fn_buf, filename->data, filename->len);
    fn_buf[filename->len]='\0';
    fname = fn_buf;
  }
  else{
    DBG("filaname too long: %V", filename);
    return (void *)NGX_INVALID_FILE;
  }
  if(fname == NULL) {
    //static analyzer pointed this out. This is a damn unlikely condition, but
    //ALL HAIL CLANG'S STATIC ANALYZER
    return (void *)NGX_INVALID_FILE;
  }
  
  
  fd = ngx_open_file(fname, NGX_FILE_RDONLY, NGX_FILE_OPEN, NGX_FILE_OWNER_ACCESS);
  return (void *)(uintptr_t)fd;
}

static ngx_int_t fd_close(ngx_str_t *id, void *fdv) {
  ngx_fd_t     fd = (ngx_fd_t )(uintptr_t )fdv;
  
  DBG("fdcache close fd %i", fd);
  
  ngx_close_file(fd);
  return 1;
}

ngx_fd_t nchan_fdcache_get(ngx_str_t *filename) {
  ngx_fd_t    fd;
  fd = (ngx_fd_t)(uintptr_t )nchan_thingcache_get(fd_cache, filename);
  DBG("fdcache fd %i", fd);
  return fd;
}

void nchan_output_init(void) {
  fd_cache = nchan_thingcache_init("fd_cache", fd_open, fd_close, 5);
}

void nchan_output_shutdown(void) {
  nchan_thingcache_shutdown(fd_cache);
}


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
/*
static size_t etag_maxlen(nchan_msg_id_t *id) {
  return 7 * id->multi_count;
}
*/


static ngx_str_t msgtag_str;
static char msgtag_str_buf[10*NCHAN_MULTITAG_MAX + 30];

size_t msgtag_to_strptr(nchan_msg_id_t *id, char *ch) {
  int16_t  *t = id->tag;
  uint8_t   max = id->tagcount;
  uint8_t   i;
  char     *cur;
  
  static char* inactive="%i,";
  static char*  active="[%i],";
  if(max == 1) {
    return sprintf(ch, "%i", t[0]);
  }
  else {
    cur = ch;
    for(i=0; i < max; i++) {
      cur += sprintf(cur, id->tagactive != i ? inactive : active, t[i]);
    }
    cur[-1]='\0';
    return cur - ch - 1;
  }  
}

ngx_str_t *msgtag_to_str(nchan_msg_id_t *id) {
  size_t len;
  len = msgtag_to_strptr(id, msgtag_str_buf);
  msgtag_str.len = len;
  msgtag_str.data = (u_char *)msgtag_str_buf;
  return &msgtag_str;
}

ngx_str_t *msgid_to_str(nchan_msg_id_t *id) {
  int   l1, l2;
  char *cur;
  l1 = sprintf(msgtag_str_buf, "%li:", id->time);
  cur = &msgtag_str_buf[l1];
  l2 = msgtag_to_strptr(id, cur);
  msgtag_str.len = l1 + l2;
  msgtag_str.data = (u_char *)msgtag_str_buf;
  return &msgtag_str;
}

ngx_int_t nchan_set_msgid_http_response_headers(ngx_http_request_t *r, nchan_msg_id_t *msgid) {
  ngx_str_t                 *etag, *tmp_etag;
  nchan_loc_conf_t          *cf = ngx_http_get_module_loc_conf(r, nchan_module);
  
  
  if(!cf->msg_in_etag_only) {
    //last-modified
    r->headers_out.last_modified_time = msgid->time;
    tmp_etag = msgtag_to_str(msgid);
  }
  else {
    tmp_etag = msgid_to_str(msgid);
  }
  
  if((etag = ngx_palloc(r->pool, sizeof(*etag) + tmp_etag->len))==NULL) {
    return NGX_ERROR;
  }
  etag->data = (u_char *)(etag+1);
  etag->len = tmp_etag->len;
  ngx_memcpy(etag->data, tmp_etag->data, tmp_etag->len);
  if ((nchan_add_response_header(r, &NCHAN_HEADER_ETAG, etag))==NULL) {
    return NGX_ERROR;
  }
  
  //Vary header needed for proper HTTP caching.
  nchan_add_response_header(r, &NCHAN_HEADER_VARY, &NCHAN_VARY_HEADER_VALUE);
  return NGX_OK;
}

ngx_int_t nchan_respond_msg(ngx_http_request_t *r, nchan_msg_t *msg, nchan_msg_id_t *msgid, ngx_int_t finalize, char **err) {
  ngx_buf_t                 *buffer = msg->buf;
  nchan_buf_and_chain_t     *cb;
  ngx_int_t                  rc;
  ngx_chain_t               *rchain = NULL;
  ngx_buf_t                 *rbuffer;
  ngx_file_t                *rfile;
  
  if(ngx_buf_size(buffer) > 0) {
    cb = ngx_palloc(r->pool, sizeof(*cb));
    if (!cb) {
      if(err) *err = "couldn't allocate memory for buf-and-chain while responding with msg";
      return NGX_ERROR;
    }
    rchain = &cb->chain;
    rbuffer = &cb->buf;
    
    rchain->next = NULL;
    rchain->buf = rbuffer;
    
    ngx_memcpy(rbuffer, buffer, sizeof(*buffer));
    
    rfile = rbuffer->file;
    if(rfile != NULL) {
      if((rfile = ngx_pcalloc(r->pool, sizeof(*rfile))) == NULL) {
        if(err) *err = "couldn't allocate memory for file struct while responding with msg";
        return NGX_ERROR;
      }
      ngx_memcpy(rfile, buffer->file, sizeof(*rbuffer));
      rbuffer->file = rfile;
    }
    
    if((rfile = rbuffer->file) != NULL && rfile->fd == NGX_INVALID_FILE) {
      if(rfile->name.len == 0) {
        if(err) *err = "longpoll subscriber given an invalid fd with no filename";
        return NGX_ERROR;
      }
      rfile->fd = nchan_fdcache_get(&rfile->name);
      if(rfile->fd == NGX_INVALID_FILE) {
        if(err) *err = "can't create output chain, file in buffer won't open";
        return NGX_ERROR;
      }
    }
    
    r->headers_out.content_length_n=ngx_buf_size(rbuffer);
  }
  else {
    r->headers_out.content_length_n = 0;
    r->header_only = 1;
  }

  if (msg->content_type.data!=NULL) {
    r->headers_out.content_type.len=msg->content_type.len;
    r->headers_out.content_type.data = msg->content_type.data;
  }
  
  if(msgid == NULL) {
    msgid = &msg->id;
  }
  
  if(nchan_set_msgid_http_response_headers(r, msgid) != NGX_OK) {
    if(err) *err = "can't set msgid headers";
    return NGX_ERROR;
  }
  
  r->headers_out.status=NGX_HTTP_OK;
  
  //we know the entity length, and we're using just one buffer. so no chunking please.
  if((rc = ngx_http_send_header(r)) >= NGX_HTTP_SPECIAL_RESPONSE) {
    ERR("request %p, send_header response %i", r, rc);
    if(err) *err="WTF just happened to request?";
    return NGX_ERROR;
  }
  
  if(rchain) {
    rc= nchan_output_filter(r, rchain);
  }
  
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
  
  if ((!b) || (!chain)) {
    ERR("Couldn't allocate ngx buf or chain.");
    r->headers_out.status=NGX_HTTP_INTERNAL_SERVER_ERROR;
    r->headers_out.content_length_n = 0;
    r->header_only = 1;
    ngx_http_send_header(r);
    rc=NGX_ERROR;
  }
  else {
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
  }
  
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
