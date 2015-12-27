#include <nchan_module.h>
#include <assert.h>

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

// this function adapted from push stream module. thanks Wandenberg Peixoto <wandenberg@gmail.com> and Rogério Carvalho Schneider <stockrt@gmail.com>
ngx_buf_t * nchan_chain_to_single_buffer(ngx_pool_t *pool, ngx_chain_t *chain, size_t content_length) {
  ngx_buf_t *buf = NULL;
  ssize_t n;
  size_t len;

  if (chain->next == NULL) {
    return chain->buf;
  }
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "nchan: multiple buffers in request, need memcpy :(");
  if (chain->buf->in_file) {
    if (ngx_buf_in_memory(chain->buf)) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan: can't handle a buffer in a temp file and in memory ");
    }
    if (chain->next != NULL) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan: error reading request body with multiple ");
    }
    return chain->buf;
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
          ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "nchan: cannot read file with request body");
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