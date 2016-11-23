#include <nchan_module.h>
#include <assert.h>

char *nchan_msgstatus_to_str(nchan_msg_status_t status) {
  switch(status) {
    case MSG_CHANNEL_NOTREADY:
      return "MSG_CHANNEL_NOTREADY";
    case MSG_NORESPONSE:
      return "MSG_NORESPONSE";
    case MSG_INVALID:
      return "MSG_INVALID";
    case MSG_PENDING:
      return "MSG_PENDING";
    case MSG_NOTFOUND:
      return "MSG_NOTFOUND";
    case MSG_FOUND:
      return "MSG_FOUND";
    case MSG_EXPECTED:
      return "MSG_EXPECTED";
    case MSG_EXPIRED:
      return "MSG_EXPIRED";
  }
  return "???";
}

int nchan_ngx_str_match(ngx_str_t *str1, ngx_str_t *str2) {
  if(str1->len != str2->len) {
    return 0;
  }
  return memcmp(str1->data, str2->data, str1->len) == 0;
}

ngx_int_t ngx_http_complex_value_noalloc(ngx_http_request_t *r, ngx_http_complex_value_t *val, ngx_str_t *value, size_t maxlen) {
  size_t                        len;
  ngx_http_script_code_pt       code;
  ngx_http_script_len_code_pt   lcode;
  ngx_http_script_engine_t      e;

  if (val->lengths == NULL) {
    *value = val->value;
    return NGX_OK;
  }

  ngx_http_script_flush_complex_value(r, val);

  ngx_memzero(&e, sizeof(ngx_http_script_engine_t));

  e.ip = val->lengths;
  e.request = r;
  e.flushed = 1;

  len = 0;

  while (*(uintptr_t *) e.ip) {
    lcode = *(ngx_http_script_len_code_pt *) e.ip;
    len += lcode(&e);
  }
  
  if(len > maxlen) {
    return NGX_ERROR;
  }
  
  value->len = len;

  e.ip = val->values;
  e.pos = value->data;
  e.buf = *value;

  while (*(uintptr_t *) e.ip) {
    code = *(ngx_http_script_code_pt *) e.ip;
    code((ngx_http_script_engine_t *) &e);
  }

  *value = e.buf;

  return NGX_OK;
}

u_char *nchan_strsplit(u_char **s1, ngx_str_t *sub, u_char *last_char) {
  u_char   *delim = sub->data;
  size_t    delim_sz = sub->len;
  u_char   *last = last_char - delim_sz;
  u_char   *cur;
  
  for(cur = *s1; cur < last; cur++) {
    if(ngx_strncmp(cur, delim, delim_sz) == 0) {
      *s1 = cur + delim_sz;
      return cur;
    }
  }
  *s1 = last_char;
  if(cur == last) {
    return last_char;
  }
  else if(cur > last) {
    return NULL;
  }
  assert(0);
  return NULL;
}

ngx_str_t *nchan_get_header_value(ngx_http_request_t * r, ngx_str_t header_name) {
  ngx_uint_t                       i;
  ngx_list_part_t                 *part = &r->headers_in.headers.part;
  ngx_table_elt_t                 *header= part->elts;
  
  for (i = 0; /* void */ ; i++) {
    if (i >= part->nelts) {
      if (part->next == NULL) {
        break;
      }
      part = part->next;
      header = part->elts;
      i = 0;
    }
    if (header[i].key.len == header_name.len
      && ngx_strncasecmp(header[i].key.data, header_name.data, header[i].key.len) == 0) {
      return &header[i].value;
      }
  }
  return NULL;
}

ngx_str_t *nchan_get_accept_header_value(ngx_http_request_t *r) {
#if (NGX_HTTP_HEADERS)  
  if(r->headers_in.accept == NULL) {
    return NULL;
  }
  else {
    return &r->headers_in.accept->value;
  }
#else
  ngx_str_t             accept_header_name = ngx_string("Accept");
  return nchan_get_header_value(r, accept_header_name);
#endif
}

static int nchan_strmatch_va_list(ngx_str_t *val, ngx_int_t n, va_list args) {
  u_char   *match;
  ngx_int_t i;
  for(i=0; i<n; i++) {
    match = va_arg(args, u_char *);
    if(ngx_strncasecmp(val->data, match, val->len)==0) {
      return 1;
    }
  }
  return 0;
}

int nchan_strmatch(ngx_str_t *val, ngx_int_t n, ...) {
  int      rc;
  va_list  args;
  va_start(args, n);  
  rc = nchan_strmatch_va_list(val, n, args);
  va_end(args);
  return rc;
}

int nchan_cstrmatch(char *cstr, ngx_int_t n, ...) {
  int       rc;
  va_list   args;
  ngx_str_t str;
  str.data = (u_char *)cstr;
  str.len = strlen(cstr);
  va_start(args, n);  
  rc = nchan_strmatch_va_list(&str, n, args);
  va_end(args);
  return rc;
}

int nchan_cstr_startswith(char *cstr, char *match) {
  for(/*void*/; *match != '\0'; cstr++, match++) {
    if(*cstr == '\0' || *cstr != *match)
      return 0;
  }
  return 1;
}

void nchan_scan_nearest_chr(u_char **cur, ngx_str_t *str, ngx_int_t n, ...) {
  u_char    chr;
  va_list   args;
  u_char   *shortest = NULL;
  
  u_char *tmp_cur;
  
  ngx_int_t i;
  
  for(tmp_cur = *cur; shortest == NULL && (tmp_cur == *cur || tmp_cur[-1] != '\0'); tmp_cur++) {
    va_start(args, n);
    for(i=0; shortest == NULL && i<n; i++) {
      chr = (u_char )va_arg(args, int);
      if(*tmp_cur == chr) {
        shortest = tmp_cur;
      }
    }
    va_end(args);
  }
  if(shortest) {
    str->data = (u_char *)*cur;
    str->len = shortest - *cur;
    *cur = shortest + 1;
  }
  else {
    str->data = NULL;
    str->len = 0;
  }
}

void nchan_scan_until_chr_on_line(ngx_str_t *line, ngx_str_t *str, u_char chr) {
  u_char     *cur;
  //ERR("rest_line: \"%V\"", line);
  cur = (u_char *)memchr(line->data, chr, line->len);
  if(!cur) {
    *str = *line;
    line->data += line->len;
    line->len = 0;
  }
  else {
    str->data = line->data;
    str->len = (cur - line->data);
    line->len -= str->len + 1;
    line->data += str->len + 1;
  }
  //ERR("str: \"%V\"", str);
}

void nchan_strcpy(ngx_str_t *dst, ngx_str_t *src, size_t maxlen) {
  size_t len = src->len > maxlen && maxlen > 0 ? maxlen : src->len;
  ngx_memcpy(dst->data, src->data, len);
  dst->len = len;
}

static ngx_buf_t *ensure_last_buf(ngx_pool_t *pool, ngx_buf_t *buf) {
  ngx_buf_t *cbuf;
  if(buf->last_buf == 1) {
    return buf;
  }
  else {
    cbuf = ngx_create_temp_buf(pool,sizeof(*cbuf));
    *cbuf = *buf;
    cbuf->last_buf = 1;
    return cbuf;
  }
}

// this function adapted from push stream module. thanks Wandenberg Peixoto <wandenberg@gmail.com> and Rogério Carvalho Schneider <stockrt@gmail.com>
ngx_buf_t * nchan_chain_to_single_buffer(ngx_pool_t *pool, ngx_chain_t *chain, size_t content_length) {
  ngx_buf_t *buf = NULL;
  ssize_t n;
  size_t len;

  if (chain->next == NULL) {
    return ensure_last_buf(pool, chain->buf);
  }
  //nchan_log_error("multiple buffers in request, need memcpy :(");
  if (chain->buf->in_file) {
    if (ngx_buf_in_memory(chain->buf)) {
      nchan_log_error("can't handle a buffer in a temp file and in memory ");
    }
    if (chain->next != NULL) {
      nchan_log_error("error reading request body with multiple ");
    }
    return ensure_last_buf(pool, chain->buf);
  }
  buf = ngx_create_temp_buf(pool, content_length + 1);
  if (buf != NULL) {
    ngx_memset(buf->start, '\0', content_length + 1);
    while ((chain != NULL) && (chain->buf != NULL)) {
      len = ngx_buf_size(chain->buf);
      // if buffer is equal to content length all the content is in this buffer
      if (len >= content_length) {
        buf->start = buf->pos;
        buf->last = buf->pos;
        len = content_length;
      }
      if (chain->buf->in_file) {
        n = ngx_read_file(chain->buf->file, buf->start, len, 0);
        if (n == NGX_FILE_ERROR) {
          nchan_log_error("cannot read file with request body");
          return NULL;
        }
        buf->last = buf->last + len;
        ngx_delete_file(chain->buf->file->name.data);
        chain->buf->file->fd = NGX_INVALID_FILE;
      } else {
        buf->last = ngx_copy(buf->start, chain->buf->pos, len);
      }
      
      chain = chain->next;
      buf->start = buf->last;
    }
    buf->last_buf = 1;
  }
  return buf;
}

ngx_int_t nchan_init_timer(ngx_event_t *ev, void (*cb)(ngx_event_t *), void *pd) {
#if nginx_version >= 1008000
  ev->cancelable = 1;
#endif
  ev->handler = cb;
  ev->data = pd;
  ev->log = ngx_cycle->log;
  return NGX_OK;
}


typedef struct {
  ngx_event_t    ev;
  void          (*cb)(void *pd);
} oneshot_timer_t;

void oneshot_timer_callback(ngx_event_t *ev) {
  oneshot_timer_t  *timer = container_of(ev, oneshot_timer_t, ev);
  timer->cb(ev->data);
  ngx_free(timer);
 }

ngx_int_t nchan_add_oneshot_timer(void (*cb)(void *), void *pd, ngx_msec_t delay) {
  oneshot_timer_t *timer = ngx_alloc(sizeof(*timer), ngx_cycle->log);
  ngx_memzero(&timer->ev, sizeof(timer->ev));
  timer->cb = cb;
  nchan_init_timer(&timer->ev, oneshot_timer_callback, pd);
  ngx_add_timer(&timer->ev, delay);
  return NGX_OK;
}

int nchan_ngx_str_char_substr(ngx_str_t *str, char *substr, size_t sz) {
  //naive non-null-terminated string matcher. don't use it in tight loops!
  char *cur;
  size_t len = str->len;
  
  for(cur = (char *)str->data, len = str->len; len >= sz; cur++, len--) {
    if(strncmp(cur, substr, sz) == 0) {
      return 1;
    }
  }
  return 0;
}

#if (NGX_DEBUG_POOL)
//Copyright (C) 2015 Alibaba Group Holding Limited
static ngx_str_t            debug_pool_str;
ngx_str_t *ngx_http_debug_pool_str(ngx_pool_t *pool) {
  u_char              *p, *unit;
  size_t               s, n, cn, ln;
  ngx_uint_t           i;
  ngx_pool_stat_t     *stat;
  static u_char        charbuf[512];
  
  debug_pool_str.len = 0;
  debug_pool_str.data = charbuf;
  
#define NGX_POOL_PID_SIZE       (NGX_TIME_T_LEN + sizeof("pid:\n") - 1)     /* sizeof pid_t equals time_t */
#define NGX_POOL_PID_FORMAT     "pid:%P\n"
#define NGX_POOL_ENTRY_SIZE     (48 /* func */ + 12 * 4 + sizeof("size: num: cnum: lnum: \n") - 1)
#define NGX_POOL_ENTRY_FORMAT   "size:%12z num:%12z cnum:%12z lnum:%12z %s\n"
#define NGX_POOL_SUMMARY_SIZE   (12 * 4 + sizeof("size: num: cnum: lnum: [SUMMARY]\n") - 1)
#define NGX_POOL_SUMMARY_FORMAT "size:%10z%2s num:%12z cnum:%12z lnum:%12z [SUMMARY]\n"

  p = charbuf;
  p = ngx_sprintf(p, NGX_POOL_PID_FORMAT, ngx_pid);

  /* lines of entry */

  s = n = cn = ln = 0;

  for (i = 0; i < NGX_POOL_STATS_MAX; i++) {
      for (stat = ngx_pool_stats[i]; stat != NULL; stat = stat->next) {
          p = ngx_snprintf(p, NGX_POOL_ENTRY_SIZE, NGX_POOL_ENTRY_FORMAT,
                            stat->size, stat->num, stat->cnum, stat->lnum,
                            stat->func);
          s += stat->size;
          n += stat->num;
          cn += stat->cnum;
          ln += stat->lnum;
      }
  }

  /* summary line */

  unit = (u_char *) " B";
  if (s > 1024 * 1024) {
    s = s / (1024 * 1024);
    unit = (u_char *) "MB";
  } else if (s > 1024) {
    s = s / 1024;
    unit = (u_char *) "KB";
  }

  p = ngx_snprintf(p, NGX_POOL_SUMMARY_SIZE, NGX_POOL_SUMMARY_FORMAT, s, unit, n, cn, ln);

  debug_pool_str.len = p - debug_pool_str.data;
  
  return &debug_pool_str;
}
#endif
