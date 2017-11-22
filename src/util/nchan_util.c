#include <nchan_module.h>
#include <assert.h>

#if (NGX_ZLIB)
#include <zlib.h>
#endif

int nchan_ngx_str_match(ngx_str_t *str1, ngx_str_t *str2) {
  if(str1->len != str2->len) {
    return 0;
  }
  return memcmp(str1->data, str2->data, str1->len) == 0;
}


ngx_int_t nchan_strscanstr(u_char **cur, ngx_str_t *find, u_char *last) {
  //inspired by ngx_strnstr
  char   *s2 = (char *)find->data;
  u_char *s1 = *cur;
  size_t  len = last - s1;
  u_char  c1, c2;
  size_t  n;
  c2 = *(u_char *) s2++;
  n = find->len - 1;
  do {
    do {
      if (len-- == 0) {
        return 0;
      }
      c1 = *s1++;
      if (c1 == 0) {
        return 0;
      }
    } while (c1 != c2);
    if (n > len) {
      return 0;
    }
  } while (ngx_strncmp(s1, (u_char *) s2, n) != 0);
  *cur = s1 + n;
  return 1;
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

ngx_str_t *nchan_get_header_value_origin(ngx_http_request_t *r, nchan_request_ctx_t *ctx) {
  ngx_str_t         *origin_header;
  static ngx_str_t   empty_str = ngx_string("");
  if(!ctx) {
    ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  }
  
  if(!ctx->request_origin_header) {
    if((origin_header = nchan_get_header_value(r, NCHAN_HEADER_ORIGIN)) != NULL) {
      ctx->request_origin_header = origin_header;
    }
    else {
      ctx->request_origin_header = &empty_str;
    }
  }
  
  return ctx->request_origin_header == &empty_str ? NULL : ctx->request_origin_header;
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

void nchan_scan_split_by_chr(u_char **cur, size_t max_len, ngx_str_t *str, u_char chr) {
  u_char   *shortest = NULL;
  u_char   *start = *cur;
  u_char   *tmp_cur;
  
  for(tmp_cur = *cur; shortest == NULL && (tmp_cur == *cur || tmp_cur - start < (ssize_t )max_len); tmp_cur++) {
    if(*tmp_cur == chr) {
      shortest = tmp_cur;
    }
  }
  if(shortest) {
    str->data = (u_char *)*cur;
    str->len = shortest - *cur;
    *cur = shortest + 1;
  }
  else if(tmp_cur - start == (ssize_t )max_len) {
    str->data = start;
    str->len = max_len;
    *cur = start + max_len;
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

ngx_str_t *nchan_get_allow_origin_value(ngx_http_request_t *r, nchan_loc_conf_t *cf, nchan_request_ctx_t *ctx) {
  if(!ctx) ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  if(!cf)  cf  = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  if(!ctx->allow_origin && cf->allow_origin) {
    ngx_str_t                 *allow_origin = ngx_palloc(r->pool, sizeof(*allow_origin));
    ngx_http_complex_value(r, cf->allow_origin, allow_origin);
    ctx->allow_origin = allow_origin;
  }
  
  return ctx->allow_origin;
}

int nchan_match_origin_header(ngx_http_request_t *r, nchan_loc_conf_t *cf, nchan_request_ctx_t *ctx) {
  ngx_str_t                 *origin_header;
  ngx_str_t                 *allow_origin;
  ngx_str_t                  curstr;
  u_char                    *cur, *end;
  
  if(cf->allow_origin == NULL) { //default is to always match
    return 1;
  }
  
  if((origin_header = nchan_get_header_value_origin(r, ctx)) == NULL) {
    return 1;
  }

  allow_origin = nchan_get_allow_origin_value(r, cf, ctx);
  
  cur = allow_origin->data;
  end = cur + allow_origin->len;
  
  while(cur < end) {
    nchan_scan_split_by_chr(&cur, end - cur, &curstr, ' ');
    if(curstr.len == 1 && curstr.data[0] == '*') {
      return 1;
    }
    if(nchan_ngx_str_match(&curstr, origin_header)) {
      return 1;
    }
  }
  
  return 0;
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


typedef struct {
  ngx_event_t    ev;
  ngx_msec_t     wait;
  int          (*cb)(void *pd);
} interval_timer_t;

void interval_timer_callback(ngx_event_t *ev) {
  interval_timer_t  *timer = container_of(ev, interval_timer_t, ev);
  int again = timer->cb(ev->data);
  if(again && ev->timedout) {
    ev->timedout=0;
    ngx_add_timer(&timer->ev, timer->wait);
  }
  else {
    ngx_free(timer);
  }
}

ngx_int_t nchan_add_interval_timer(int (*cb)(void *), void *pd, ngx_msec_t interval) {
  interval_timer_t *timer = ngx_alloc(sizeof(*timer), ngx_cycle->log);
  ngx_memzero(&timer->ev, sizeof(timer->ev));
  timer->cb = cb;
  timer->wait = interval;
  nchan_init_timer(&timer->ev, interval_timer_callback, pd);
  ngx_add_timer(&timer->ev, interval);
  return NGX_OK;
}

ngx_str_t *nchan_urldecode_str(ngx_http_request_t *r, ngx_str_t *str) {
  ngx_str_t   *out;
  u_char      *dst, *src;
  if(memchr(str->data, '%', str->len) == NULL) {
    return str;
  }
  
  out = ngx_palloc(r->pool, sizeof(*out) + str->len);
  out->data = (u_char *)&out[1];
  
  dst = out->data;
  src = str->data;
  
  ngx_unescape_uri(&dst, &src, str->len, 0);
  out->len = dst - out->data;
  
  return out;
}

int nchan_ngx_str_char_substr(ngx_str_t *str, char *substr, size_t sz) {
  //naive non-null-terminated string matcher. don't use it in tight loops!
  char   *cur = (char *)str->data;
  size_t  len;
  for(len = str->len; len >= sz; cur++, len--) {
    if(strncmp(cur, substr, sz) == 0) {
      return 1;
    }
  }
  return 0;
}

//converts string to positive double float
static double nchan_atof(u_char *line, ssize_t n) {
  ssize_t cutoff, cutlim;
  double  value = 0;
  
  u_char *decimal, *cur, *last = line + n;
  
  if (n == 0) {
    return NGX_ERROR;
  }

  cutoff = NGX_MAX_SIZE_T_VALUE / 10;
  cutlim = NGX_MAX_SIZE_T_VALUE % 10;
  
  decimal = memchr(line, '.', n);
  
  if(decimal == NULL) {
    decimal = line + n;
  }
  
  for (n = decimal - line; n-- > 0; line++) {
    if (*line < '0' || *line > '9') {
      return NGX_ERROR;
    }

    if (value >= cutoff && (value > cutoff || (*line - '0') > cutlim)) {
      return NGX_ERROR;
    }

    value = value * 10 + (*line - '0');
  }
  
  double decval = 0;
  
  
  
  for(cur = (decimal - last) > 10 ? decimal + 10 : last-1; cur > decimal && cur < last; cur--) {
    if (*cur < '0' || *cur > '9') {
      return NGX_ERROR;
    }
    decval = decval / 10 + (*cur - '0');
  }
  value = value + decval/10;
  
  return value;
}

ssize_t nchan_parse_size(ngx_str_t *line) {
  u_char   unit;
  size_t   len;
  ssize_t  size, scale, max;
  double   floaty;
  
  len = line->len;
  unit = line->data[len - 1];

  switch (unit) {
  case 'K':
  case 'k':
      len--;
      max = NGX_MAX_SIZE_T_VALUE / 1024;
      scale = 1024;
      break;

  case 'M':
  case 'm':
      len--;
      max = NGX_MAX_SIZE_T_VALUE / (1024 * 1024);
      scale = 1024 * 1024;
      break;
  
  case 'G':
  case 'g':
      len--;
      max = NGX_MAX_SIZE_T_VALUE / (1024 * 1024 * 1024);
      scale = 1024 * 1024 * 1024;
      break;

  default:
      max = NGX_MAX_SIZE_T_VALUE;
      scale = 1;
  }

  floaty = nchan_atof(line->data, len);
  
  if (floaty == NGX_ERROR || floaty > max) {
      return NGX_ERROR;
  }

  size = floaty * scale;

  return size;
}

char *nchan_conf_set_size_slot(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  char  *p = conf;

  size_t           *sp;
  ngx_str_t        *value;
  ngx_conf_post_t  *post;


  sp = (size_t *) (p + cmd->offset);
  if (*sp != NGX_CONF_UNSET_SIZE) {
      return "is duplicate";
  }

  value = cf->args->elts;

  *sp = nchan_parse_size(&value[1]);
  if (*sp == (size_t) NGX_ERROR) {
    return "invalid value";
  }

  if (cmd->post) {
    post = cmd->post;
    return post->post_handler(cf, post, sp);
  }

  return NGX_CONF_OK;
}

void ngx_init_set_membuf(ngx_buf_t *buf, u_char *start, u_char *end) {
  ngx_memzero(buf, sizeof(*buf));
  buf->start = start;
  buf->pos = start;
  buf->end = end;
  buf->last = end;
  buf->memory = 1;
}

void ngx_init_set_membuf_str(ngx_buf_t *buf, ngx_str_t *str) {
  ngx_memzero(buf, sizeof(*buf));
  buf->start = str->data;
  buf->pos = str->data;
  buf->end = str->data + str->len;
  buf->last = buf->end;
  buf->memory = 1;
}


#if (NGX_ZLIB)
static z_stream        *deflate_zstream = NULL;
static z_stream        *deflate_dummy_zstream = NULL;

static ngx_path_t      *message_temp_path = NULL;

ngx_int_t nchan_common_deflate_init(nchan_main_conf_t  *mcf) {
  int rc;
  
  message_temp_path = mcf->message_temp_path;
  
  if((deflate_zstream = ngx_calloc(sizeof(*deflate_zstream), ngx_cycle->log)) == NULL) {
    nchan_log_error("couldn't allocate deflate stream.");
    return NGX_ERROR;
  }
  
  deflate_zstream->zalloc = Z_NULL;
  deflate_zstream->zfree = Z_NULL;
  deflate_zstream->opaque = Z_NULL;
  
  rc = deflateInit2(deflate_zstream, (int) mcf->zlib_params.level, Z_DEFLATED, mcf->zlib_params.windowBits, mcf->zlib_params.memLevel, mcf->zlib_params.strategy);
  if(rc != Z_OK) {
    nchan_log_error("couldn't initialize deflate stream.");
    deflate_zstream = NULL;
    return NGX_ERROR;
  }
  
  if((deflate_dummy_zstream = ngx_calloc(sizeof(*deflate_dummy_zstream), ngx_cycle->log)) == NULL) {
    nchan_log_error("couldn't allocate dummy deflate stream.");
    return NGX_ERROR;
  }
  
  deflate_dummy_zstream->zalloc = Z_NULL;
  deflate_dummy_zstream->zfree = Z_NULL;
  deflate_dummy_zstream->opaque = Z_NULL;
  
  rc = deflateInit2(deflate_dummy_zstream, 0, Z_DEFLATED, -9, 1, Z_DEFAULT_STRATEGY);
  if(rc != Z_OK) {
    nchan_log_error("couldn't initialize deflate stream.");
    deflate_dummy_zstream = NULL;
    return NGX_ERROR;
  }
  
  return NGX_OK;
}

ngx_int_t nchan_common_deflate_shutdown(void) {
  if(deflate_zstream) {
    deflateEnd(deflate_zstream);
    ngx_free(deflate_zstream);
    deflate_zstream = NULL;
  }
  
  if(deflate_dummy_zstream) {
    deflateEnd(deflate_dummy_zstream);
    ngx_free(deflate_dummy_zstream);
    deflate_dummy_zstream = NULL;
  }
  return NGX_OK;
}

#define ZLIB_CHUNK 16384

static ngx_temp_file_t *make_temp_file(ngx_http_request_t *r, ngx_pool_t *pool) {
  ngx_temp_file_t           *tf;
  ngx_path_t                *temp_path;
  if(r) {
    ngx_http_core_loc_conf_t  *clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
    temp_path = clcf->client_body_temp_path;
  }
  else {
    temp_path = message_temp_path;
  }
  tf = ngx_pcalloc(pool, sizeof(*tf));
  if(!tf) {
    if(r) {
      nchan_log_request_error(r, "failed to allocate space for temp_file struct.");
    } else {
      nchan_log_error("failed to allocate space for temp_file struct.");
    }
    return NULL;
  }
  tf->file.fd = NGX_INVALID_FILE;
  if(r) {
    tf->file.log = r->connection->log;
  }
  else if(pool && pool->log) {
    tf->file.log = pool->log;
  }
  else {
    tf->file.log = ngx_cycle->log;
  }
  tf->path = temp_path;
  tf->pool = pool;
  tf->persistent = 1;
  tf->clean = 0; //this will close the file on pool cleanup
  tf->access = 0;
  if(ngx_create_temp_file(&tf->file, tf->path, tf->pool, tf->persistent, tf->clean, tf->access) != NGX_OK) {
    if(r) {
      nchan_log_request_error(r, "failed to create temp file for deflated message");
    } else {
      nchan_log_error("failed to create temp file for deflated message");
    }
    return NULL;
  }
  return tf;
}

ngx_buf_t *nchan_common_deflate(ngx_buf_t *in, ngx_http_request_t *r, ngx_pool_t *pool) {
  ngx_str_t           mm_instr;
  int                 mmapped = 0;
  ngx_temp_file_t    *tf = NULL;
  
  int                 rc;
  ngx_buf_t          *out = NULL;
  u_char              outbuf[ZLIB_CHUNK];
  unsigned            have = 0;
  off_t               written = 0;
  
  //input
  
  if(ngx_buf_in_memory(in)) {
    deflate_zstream->avail_in = ngx_buf_size(in);
    deflate_zstream->next_in = in->pos;
  }
  else {
    ngx_fd_t fd = in->file->fd == NGX_INVALID_FILE ? nchan_fdcache_get(&in->file->name) : in->file->fd;
    mm_instr.len = in->file_last - in->file_pos;
    mm_instr.data = mmap(NULL, mm_instr.len, PROT_READ, MAP_SHARED, fd, in->file_pos);
    if (mm_instr.data == MAP_FAILED) {
      nchan_log_request_error(r, "failed to mmap input file for deflated message");
      return NULL;
    }
    deflate_zstream->avail_in = mm_instr.len;
    deflate_zstream->next_in = mm_instr.data;
    mmapped = 1;
  }

  //output
  
  do {
    deflate_zstream->avail_out = ZLIB_CHUNK;
    deflate_zstream->next_out = outbuf;
    
    rc = deflate(deflate_zstream, Z_SYNC_FLUSH);
    assert(rc != Z_STREAM_ERROR);
    
    have = ZLIB_CHUNK - deflate_zstream->avail_out;
    
    if(deflate_zstream->avail_out == 0 && tf == NULL) {
      //if we filled up the buffer, let's start dumping to a file.
      tf = make_temp_file(r, pool);
      if(tf == NULL) { //couldn't make temp file for some reason
        nchan_log_request_error(r, "failed to allocate output buf for deflated message");
        deflateReset(deflate_zstream);
        return NULL;
      }
    }
    if(tf) {
      ngx_write_file(&tf->file, outbuf, have, written);
    }
    
    written += have;
  } while(rc != Z_BUF_ERROR);
  
  if(mmapped) {
    munmap(mm_instr.data, mm_instr.len);
  }
  
  if((out = ngx_palloc(pool, sizeof(*out))) == NULL) {
    nchan_log_request_error(r, "failed to allocate output buf for deflated message");
    deflateReset(deflate_zstream);
    return NULL;
  }
  
  if(written > 4) { //there will be a 00 00 FF FF chunk at the end. remove it as the permessage-deflate spec demands
    written -= 4;
  }
  
  if(tf) { //using a tempfile
    //thanks to tf->clean = 0, file will be closed on pool cleanup
    ngx_memzero(out, sizeof(*out));
    out->file_pos = 0;
    out->file_last = written;
    out->in_file = 1;
    out->file = &tf->file;
  }
  else {
    u_char  *outpooled = ngx_palloc(pool, written);
    if(!outpooled) {
      nchan_log_request_error(r, "failed to allocate output data for deflated message");
      deflateReset(deflate_zstream);
      return NULL;
    }
    ngx_memcpy(outpooled, outbuf, written);
    ngx_init_set_membuf(out, outpooled, outpooled + written);
  }
  out->last_buf = 1;
  
  deflateReset(deflate_zstream);
  return out;
}

ngx_buf_t *nchan_inflate(z_stream *stream, ngx_buf_t *in, ngx_http_request_t *r, ngx_pool_t *pool) {
  ngx_str_t           mm_instr;
  int                 mmapped = 0;
  ngx_temp_file_t    *tf = NULL;
  
  int                 rc;
  ngx_buf_t          *out = NULL;
  u_char              outbuf[ZLIB_CHUNK];
  unsigned            have = 0;
  off_t               written = 0;
  int                 trailer_appended = 0;
  
  //input
  if(ngx_buf_in_memory(in)) {
    stream->avail_in = ngx_buf_size(in);
    stream->next_in = in->pos;
  }
  else {
    ngx_fd_t fd = in->file->fd == NGX_INVALID_FILE ? nchan_fdcache_get(&in->file->name) : in->file->fd;
    mm_instr.len = in->file_last - in->file_pos;
    mm_instr.data = mmap(NULL, mm_instr.len, PROT_READ, MAP_SHARED, fd, in->file_pos);
    if (mm_instr.data == MAP_FAILED) {
      nchan_log_request_error(r, "failed to mmap input file for deflated message");
      return NULL;
    }
    stream->avail_in = mm_instr.len;
    stream->next_in = mm_instr.data;
    mmapped = 1;
  }

  //output
  
  do {
    stream->avail_out = ZLIB_CHUNK;
    stream->next_out = outbuf;
    
    if(stream->avail_in == 0 && !trailer_appended) {
      stream->avail_in = 4;
      stream->next_in = (u_char *)"\x00\x00\xFF\xFF";
      trailer_appended = 1;
    }
    rc = inflate(stream, trailer_appended ? Z_SYNC_FLUSH : Z_NO_FLUSH);
    assert(rc != Z_STREAM_ERROR);
    
    have = ZLIB_CHUNK - stream->avail_out;
    
    if(stream->avail_out == 0 && tf == NULL) {
      //if we filled up the buffer, let's start dumping to a file.
      tf = make_temp_file(r, pool);
    }
    if(tf) {
      ngx_write_file(&tf->file, outbuf, have, written);
    }
    written += have;
  } while(rc != Z_BUF_ERROR);
  
  if(mmapped) {
    munmap(mm_instr.data, mm_instr.len);
  }
  
  if((out = ngx_palloc(pool, sizeof(*out))) == NULL) {
    nchan_log_request_error(r, "failed to allocate output buf for deflated message");
    deflateReset(deflate_zstream);
    return NULL;
  }
  
  if(tf) { //using a tempfile
    //thanks to tf->clean = 0, file will be closed on pool cleanup
    ngx_memzero(out, sizeof(*out));
    out->file_pos = 0;
    out->file_last = written;
    out->in_file = 1;
    out->file = &tf->file;
  }
  else {
    u_char  *outpooled = ngx_palloc(pool, written);
    if(!outpooled) {
      nchan_log_request_error(r, "failed to allocate output data for deflated message");
      deflateReset(deflate_zstream);
      return NULL;
    }
    ngx_memcpy(outpooled, outbuf, written);
    ngx_init_set_membuf(out, outpooled, outpooled + written);
  }
  out->last_buf = 1;
  
  deflateReset(deflate_zstream);
  return out;
}


static ngx_int_t nchan_common_simple_deflate_internal(z_stream *strm, ngx_str_t *in, ngx_str_t *out) {
  int rc;
  strm->avail_in = in->len;
  strm->next_in = in->data;

  //output
  strm->avail_out = out->len;
  strm->next_out = out->data;

  rc = deflate(strm, Z_SYNC_FLUSH);
  if(rc != Z_STREAM_ERROR) {
    out->len = strm->total_out;
  }
  
  deflateReset(strm);
  
  return rc != Z_STREAM_ERROR ? NGX_OK : NGX_ERROR;
}

ngx_int_t nchan_common_simple_deflate(ngx_str_t *in, ngx_str_t *out) {
  return nchan_common_simple_deflate_internal(deflate_zstream, in, out);
}

ngx_int_t nchan_common_simple_deflate_raw_block(ngx_str_t *in, ngx_str_t *out) {
  return nchan_common_simple_deflate_internal(deflate_dummy_zstream, in, out);
}
  
#endif

ngx_flag_t nchan_need_to_deflate_message(nchan_loc_conf_t *cf) {
#if (NGX_ZLIB)
  if(cf->redis.enabled && cf->redis.storage_mode != REDIS_MODE_BACKUP) {
    //redis mode ns non-backup storage mode gets the messages deflated later on
    return 0;
  }  
  return cf->message_compression == NCHAN_MSG_COMPRESSION_WEBSOCKET_PERMESSAGE_DEFLATE;
#else
  return 0;
#endif
}

ngx_int_t nchan_deflate_message_if_needed(nchan_msg_t *msg, nchan_loc_conf_t *cf, ngx_http_request_t *r, ngx_pool_t  *pool) {
#if (NGX_ZLIB)
  if(nchan_need_to_deflate_message(cf)) {
    msg->compressed = ngx_pcalloc(pool, sizeof(*msg->compressed));
    if(!msg->compressed) {
      if(r) {
        nchan_log_request_error(r, "no memory to compress message");
      }
      else {
        nchan_log_error("no memory to compress message");
      }
    }
    else {
      ngx_buf_t  *compressed_buf = nchan_common_deflate(&msg->buf, r, pool);
      if(!compressed_buf) {
        if(r) {
          nchan_log_request_error(r, "failed to compress message");
        }
        else {
          nchan_log_error("failed to compress message");
        }
      }
      else {
        msg->compressed->compression = cf->message_compression;
        msg->compressed->buf = *compressed_buf;
      }
    }
  }
  return NGX_OK;
#else
  return NGX_DECLINED;
#endif
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
