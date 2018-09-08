#include <ngx_http.h>
#include <nchan_module.h>
#include "nchan_output.h"
#include <util/nchan_thingcache.h>
#include <util/nchan_bufchainpool.h>
#include <store/memory/store.h>
#include <assert.h>

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

ngx_int_t nchan_msg_buf_open_fd_if_needed(ngx_buf_t *buf, ngx_file_t *file, ngx_http_request_t *r) {
  if(buf->in_file) {
    //open file fd if necessary
    if(!file) {
      if(r) {
        if((file = ngx_pcalloc(r->pool, sizeof(*file))) == NULL) {
          ERR("couldn't allocate memory for file struct while responding with msg");
          return NGX_ERROR;
        }
      }
      else {
        //no file given, can't allocate from NULL pool
        return NGX_ERROR;
      }
    }
    
    ngx_memcpy(file, buf->file, sizeof(*file));
    if(file->fd == NGX_INVALID_FILE) {
      file->fd = nchan_fdcache_get(&file->name);
      if(file->fd == NGX_INVALID_FILE) {
        ERR("can't create output chain, file in buffer won't open");
        return NGX_ERROR;
      }
    }
    buf->file = file;
  }
  return NGX_OK;
}


void nchan_output_init(void) {
  fd_cache = nchan_thingcache_init("fd_cache", fd_open, fd_close, 5);
  
}

void nchan_output_shutdown(void) {
  nchan_thingcache_shutdown(fd_cache);
}

typedef struct rsvmsg_queue_s rsvmsg_queue_t;
struct rsvmsg_queue_s {
  nchan_msg_t                  *msg;
  rsvmsg_queue_t               *prev;
  rsvmsg_queue_t               *next;
};

static void *rsvmsg_queue_palloc(void *pd) {
  return ngx_palloc(((ngx_http_request_t *)pd)->pool, sizeof(rsvmsg_queue_t));
}

static ngx_int_t rsvmsg_queue_release(void *pd, void *thing) {
  //ERR("release msg %p", ((rsvmsg_queue_t *)thing)->msg);
  msg_release(((rsvmsg_queue_t *)thing)->msg, "output reservation");
  return NGX_OK;
}

static void nchan_push_release_entire_message_queue(nchan_request_ctx_t *ctx) {
  if(ctx->reserved_msg_queue) {
    nchan_reuse_queue_flush(ctx->reserved_msg_queue);
  }
}

static void nchan_reserve_msg_cleanup(void *pd) {
  nchan_push_release_entire_message_queue((nchan_request_ctx_t *)pd);
}

static void nchan_output_reserve_message_queue(ngx_http_request_t *r, nchan_msg_t *msg) {
  nchan_request_ctx_t  *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  ngx_http_cleanup_t   *cln;
  
  if(msg->storage != NCHAN_MSG_SHARED) {
    if((msg = nchan_msg_derive_alloc(msg)) == NULL) {
      ERR("Coudln't alloc derived msg for output_reserve_message_queue");
      return;
    }
  }
  
  if(!ctx->reserved_msg_queue) {
    if((ctx->reserved_msg_queue = ngx_palloc(r->pool, sizeof(*ctx->reserved_msg_queue))) == NULL) {
      ERR("Coudln't palloc reserved_msg_queue");
      return;
    }
    nchan_reuse_queue_init(ctx->reserved_msg_queue, offsetof(rsvmsg_queue_t, prev), offsetof(rsvmsg_queue_t, next), rsvmsg_queue_palloc, rsvmsg_queue_release, r);
    
    if((cln = ngx_http_cleanup_add(r, 0)) == NULL) {
      ERR("Unable to add request cleanup for reserved_msg_queue queue");
      assert(0);
      return;
    }
    
    cln->data = ctx;
    cln->handler = nchan_reserve_msg_cleanup;
  }
  
  rsvmsg_queue_t   *qmsg = nchan_reuse_queue_push(ctx->reserved_msg_queue);
  qmsg->msg = msg;
  msg_reserve(msg, "output reservation");
}

//general request-output functions and the iraq and the asian countries and dated references and the, uh, such

void nchan_flush_pending_output(ngx_http_request_t *r) {
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
      nchan_http_finalize_request(r, NGX_HTTP_REQUEST_TIME_OUT);
      return;
    }
    wev->timedout = 0;
    wev->delayed = 0;

    if (!wev->ready) {
      ngx_add_timer(wev, clcf->send_timeout);
      if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
        nchan_http_finalize_request(r, 0);
      }
      return;
    }
  }
  
  if (wev->delayed || r->aio) {
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, wev->log, 0, "http writer delayed");
    if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
      nchan_http_finalize_request(r, 0);
    }
    return;
  }
  
  rc = nchan_output_filter(r, NULL);

  //ngx_log_debug3(NGX_LOG_DEBUG_HTTP, c->log, 0, "http writer output filter: %d, \"%V?%V\"", rc, &r->uri, &r->args);

  if (rc == NGX_ERROR) {
    nchan_http_finalize_request(r, rc);
    return;
  }

  if (r->buffered || r->postponed || (r == r->main && c->buffered)) {
    if (!wev->delayed) {
      ngx_add_timer(wev, clcf->send_timeout);
    }
    if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
      nchan_http_finalize_request(r, 0);
      return;
    }
  }
  //ngx_log_debug2(NGX_LOG_DEBUG_HTTP, wev->log, 0, "http writer done: \"%V?%V\"", &r->uri, &r->args);
  if(r->out == NULL) {
    r->write_event_handler = ngx_http_request_empty_handler;
  }
}

static void flush_all_the_reserved_things(nchan_request_ctx_t *ctx) {
  nchan_push_release_entire_message_queue(ctx);
  if(ctx->bcp) {
    nchan_bufchain_pool_flush(ctx->bcp);
  }
  if(ctx->output_str_queue) {
    nchan_reuse_queue_flush(ctx->output_str_queue);
  }
}

static ngx_int_t nchan_output_filter_generic(ngx_http_request_t *r, nchan_msg_t *msg, ngx_chain_t *in) {
/* from push stream module, written by
 * Wandenberg Peixoto <wandenberg@gmail.com>, Rogério Carvalho Schneider <stockrt@gmail.com>
 * thanks, guys!
 * modified to fit the needs of websockets and eventsources and multipartses and so on
*/
  ngx_http_core_loc_conf_t               *clcf;
  ngx_int_t                               rc;
  ngx_event_t                            *wev;
  ngx_connection_t                       *c;
  nchan_request_ctx_t                    *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  
  c = r->connection;
  wev = c->write;

  if(ctx && ctx->bcp) {
    nchan_bufchain_pool_refresh_files(ctx->bcp);
  }
  
  rc = ngx_http_output_filter(r, in);
  //ERR("outpuit filter plz");

  if (c->buffered & NGX_HTTP_LOWLEVEL_BUFFERED) {
    //ERR("what's the deal with this NGX_HTTP_LOWLEVEL_BUFFERED thing?");
    clcf = ngx_http_get_module_loc_conf(r->main, ngx_http_core_module);
    r->write_event_handler = nchan_flush_pending_output;
    if(msg) {
      nchan_output_reserve_message_queue(r, msg);
    }
    if (!wev->delayed) {
      //ERR("delay output by %ims", clcf->send_timeout);
      ngx_add_timer(wev, clcf->send_timeout);
    }
    if ((ngx_handle_write_event(wev, clcf->send_lowat)) != NGX_OK) {
      if(ctx) {
        flush_all_the_reserved_things(ctx);
      }
      return NGX_ERROR;
    }
    return NGX_OK;
  } 
  else {
    if (wev->timer_set) {
      //ERR("nevermind timer");
      ngx_del_timer(wev);
    }
  }
  
  if(r->out == NULL) {
    if(ctx) {
      flush_all_the_reserved_things(ctx);
    }
  }
  
  return rc;
}

ngx_int_t nchan_output_filter(ngx_http_request_t *r, ngx_chain_t *in) {
  return nchan_output_filter_generic(r, NULL, in);
}
ngx_int_t nchan_output_msg_filter(ngx_http_request_t *r, nchan_msg_t *msg, ngx_chain_t *in) {
  return nchan_output_filter_generic(r, msg, in);
}

void nchan_include_access_control_if_needed(ngx_http_request_t *r, nchan_request_ctx_t *ctx) {
  ngx_str_t          *origin;
  ngx_str_t          *allow_origin_val;
  static ngx_str_t    true_string = ngx_string("true");
  nchan_loc_conf_t   *cf;
  if(!ctx) {
    ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  }
  if(!ctx) {
    return;
  }
  
  if((origin = nchan_get_header_value_origin(r, ctx)) != NULL) {
    cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
    
    if(cf->allow_credentials) {
      nchan_add_response_header(r, &NCHAN_HEADER_ACCESS_CONTROL_ALLOW_CREDENTIALS, &true_string);
    }
    allow_origin_val = nchan_get_allow_origin_value(r, cf, ctx);
    if(allow_origin_val && allow_origin_val->len == 1 && allow_origin_val->data[0]=='*') {
      nchan_add_response_header(r, &NCHAN_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN, allow_origin_val);
    }
    else { //otherwise echo the origin
      nchan_add_response_header(r, &NCHAN_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }
  }
}

ngx_int_t nchan_respond_status(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *status_line, ngx_chain_t *status_body, ngx_int_t finalize) {
  ngx_int_t    rc = NGX_OK;
  r->headers_out.status=status_code;
  if(status_line!=NULL) {
    r->headers_out.status_line.len =status_line->len;
    r->headers_out.status_line.data=status_line->data;
  }
  if(status_body == NULL) {
    r->headers_out.content_length_n = 0;
    r->header_only = 1;
  }
  
  nchan_include_access_control_if_needed(r, NULL);
  
  rc= ngx_http_send_header(r);
  if(status_body) {
    rc = ngx_http_output_filter(r, status_body);
  }
  if(finalize) {
    nchan_http_finalize_request(r, rc);
  }
  return rc;
}

ngx_int_t nchan_respond_cstring(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *content_type, char *body, ngx_int_t finalize) {
  ngx_str_t str;
  str.data = (u_char *)body;
  str.len=strlen(body);
  return nchan_respond_string(r, status_code, content_type, &str, finalize);
}

ngx_int_t nchan_respond_sprintf(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *content_type, const ngx_int_t finalize, char *fmt, ...) {
  u_char *p;
  ngx_str_t str;
  va_list   args;
    
  str.len = 1024;
  str.data = ngx_palloc(r->pool, 1024);
  if(str.data == NULL) {
    return nchan_respond_status(r, status_code, NULL, NULL, finalize);
    return NGX_ERROR;
  }

  va_start(args, fmt);
  p = ngx_vslprintf(str.data, str.data + str.len, fmt, args);
  va_end(args);
  
  str.len = p - str.data;
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
static char msgtag_str_buf[10*255 + 30];

size_t msgtag_to_strptr(nchan_msg_id_t *id, char *ch) {

  uint8_t   max = id->tagcount;
  int16_t  *t = max <= NCHAN_FIXED_MULTITAG_MAX ? id->tag.fixed : id->tag.allocd;
  
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
      
      assert(t[i] >= -2);
      
      if(t[i] != -1) {
        cur += sprintf(cur, id->tagactive != i ? inactive : active, t[i]);
      }
      else { //shorthand
        assert(id->tagactive != i);
        *cur++='-';
        *cur++=',';
      }
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

ngx_int_t nchan_set_msgid_http_response_headers(ngx_http_request_t *r, nchan_request_ctx_t *ctx, nchan_msg_id_t *msgid) {
  ngx_str_t                 *etag, *tmp_etag;
  nchan_loc_conf_t          *cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  int8_t                     output_etag = 1;
  ngx_str_t                 *origin_header;
  
  if(!ctx) {
    ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  }
  origin_header = ctx ? nchan_get_header_value_origin(r, ctx) : NULL; 

  if(!cf->msg_in_etag_only) {
    //last-modified
    if(msgid->time > 0) {
      r->headers_out.last_modified_time = msgid->time;
    }
    else {
      output_etag = 0;
    }
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

  if(cf->custom_msgtag_header.len == 0) {
    if(output_etag) {
      nchan_add_response_header(r, &NCHAN_HEADER_ETAG, etag);
    }
    if(origin_header) {
      nchan_add_response_header(r, &NCHAN_HEADER_ACCESS_CONTROL_EXPOSE_HEADERS, &NCHAN_MSG_RESPONSE_ALLOWED_HEADERS);
    }
  }
  else {
    if(output_etag) {
      nchan_add_response_header(r, &cf->custom_msgtag_header, etag);
    }
    if(origin_header) {
      u_char        *cur = ngx_palloc(r->pool, 255);
      if(cur == NULL) {
        return NGX_ERROR;
      }
      ngx_str_t      allowed;
      allowed.data = cur;
      cur = ngx_snprintf(cur, 255, NCHAN_MSG_RESPONSE_ALLOWED_CUSTOM_ETAG_HEADERS_STRF, &cf->custom_msgtag_header);
      allowed.len = cur - allowed.data;
      nchan_add_response_header(r, &NCHAN_HEADER_ACCESS_CONTROL_EXPOSE_HEADERS, &allowed);
     }
  }
  
  //Vary header needed for proper HTTP caching.
  nchan_add_response_header(r, &NCHAN_HEADER_VARY, &NCHAN_VARY_HEADER_VALUE);
  return NGX_OK;
}

ngx_int_t nchan_respond_msg(ngx_http_request_t *r, nchan_msg_t *msg, nchan_msg_id_t *msgid, ngx_int_t finalize, char **err) {
  ngx_buf_t                 *buffer = &msg->buf;
  nchan_buf_and_chain_t     *cb;
  ngx_int_t                  rc;
  ngx_chain_t               *rchain = NULL;
  ngx_buf_t                 *rbuffer;
  nchan_request_ctx_t       *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  
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
    nchan_msg_buf_open_fd_if_needed(rbuffer, NULL, r);
    
    r->headers_out.content_length_n=ngx_buf_size(rbuffer);
  }
  else {
    r->headers_out.content_length_n = 0;
    r->header_only = 1;
  }

  if (msg->content_type) {
    r->headers_out.content_type = *msg->content_type;
  }
  
  if(msgid == NULL) {
    msgid = &msg->id;
  }
  
  if(nchan_set_msgid_http_response_headers(r, ctx, msgid) != NGX_OK) {
    if(err) *err = "can't set msgid headers";
    return NGX_ERROR;
  }
  
  r->headers_out.status=NGX_HTTP_OK;
  
  nchan_include_access_control_if_needed(r, ctx);
  
  //we know the entity length, and we're using just one buffer. so no chunking please.
  if((rc = ngx_http_send_header(r)) >= NGX_HTTP_SPECIAL_RESPONSE) {
    ERR("request %p, send_header response %i", r, rc);
    if(err) *err="WTF just happened to request?";
    return NGX_ERROR;
  }
  
  if(rchain) {
    rc= nchan_output_filter(r, rchain);
    if(rc != NGX_OK && err) *err="failed to write data to connection socket, probably because the connection got closed";
  }
  
  if(finalize) {
    nchan_http_finalize_request(r, rc);
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
  
  nchan_include_access_control_if_needed(r, NULL);
  
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
    
    if ((rc = ngx_http_send_header(r)) == NGX_OK) {
      rc= nchan_output_filter(r, chain);
    }
  }
  
  if(finalize) {
    nchan_http_finalize_request(r, rc);
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
  if(header_value) {
    h->value.len = header_value->len;
    h->value.data = header_value->data;
  }
  else {
    h->value.len = 0;
    h->value.data = NULL;
  }
  return h;
}

ngx_int_t nchan_OPTIONS_respond(ngx_http_request_t *r, const ngx_str_t *allowed_headers, const ngx_str_t *allowed_methods) {
  nchan_request_ctx_t      *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  
  nchan_add_response_header(r, &NCHAN_HEADER_ALLOW, allowed_methods);
  
  if(ctx && nchan_get_header_value_origin(r, ctx)) {
    //Access-Control-Allow-Origin is included later
    nchan_add_response_header(r, &NCHAN_HEADER_ACCESS_CONTROL_ALLOW_HEADERS, allowed_headers);
    nchan_add_response_header(r, &NCHAN_HEADER_ACCESS_CONTROL_ALLOW_METHODS, allowed_methods);
  }
  return nchan_respond_status(r, NGX_HTTP_OK, NULL, NULL, 0);
}

void nchan_http_finalize_request(ngx_http_request_t *r, ngx_int_t code) {
  if(r->connection && r->connection->write->error) {
    r->write_event_handler = NULL;
    //this will close the connection when request is finalized
    ngx_http_finalize_request(r, NGX_ERROR);
  }
  else {
    ngx_http_finalize_request(r, code);
  }
}
