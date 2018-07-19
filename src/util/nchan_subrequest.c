// Taken from agenzh's echo nginx module
// https://github.com/openresty/echo-nginx-module
// Thanks, agenzh!

/*
Copyright (C) 2009-2014, Yichun "agentzh" Zhang <agentzh@gmail.com>.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

    * Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include <nchan_module.h>

ngx_str_t  nchan_content_length_header_key = ngx_string("Content-Length");

static ngx_inline ngx_uint_t nchan_hash_str(u_char *src, size_t n) {
  ngx_uint_t  key;
  key = 0;
  while (n--) {
    key = ngx_hash(key, *src);
    src++;
  }
  return key;
}
#define nchan_hash_literal(s)                                        \
  nchan_hash_str((u_char *) s, sizeof(s) - 1)

ngx_int_t nchan_set_content_length_header(ngx_http_request_t *r, off_t len) {
  ngx_table_elt_t                 *h, *header;
  ngx_list_part_t                 *part;
  ngx_http_request_t              *pr;
  ngx_uint_t                       i;
  u_char                          *p;
  static ngx_uint_t                nchan_content_length_hash = 0;
  if(nchan_content_length_hash == 0) {
    nchan_content_length_hash = nchan_hash_literal("content-length");
  }
  
  r->headers_in.content_length_n = len;
  
  if (ngx_list_init(&r->headers_in.headers, r->pool, 20, sizeof(ngx_table_elt_t)) != NGX_OK) {
    return NGX_ERROR;
  }

  h = ngx_list_push(&r->headers_in.headers);
  if (h == NULL) {
    return NGX_ERROR;
  }

  h->key = nchan_content_length_header_key;
  h->lowcase_key= (u_char *)"content-length";

  r->headers_in.content_length = h;
  p = ngx_palloc(r->pool, NGX_OFF_T_LEN);
  if (p == NULL) {
    return NGX_ERROR;
  }

  h->value.data = p;
  h->value.len = ngx_sprintf(h->value.data, "%O", len) - h->value.data;

  h->hash = nchan_content_length_hash;

  pr = r->parent;

  if (pr == NULL) {
    return NGX_OK;
  }

  /* forward the parent request's all other request headers */

  part = &pr->headers_in.headers.part;
  header = part->elts;

  for (i = 0; /* void */; i++) {
    
    if (i >= part->nelts) {
      if (part->next == NULL) {
        break;
      }
      part = part->next;
      header = part->elts;
      i = 0;
    }
    if (header[i].key.len == sizeof("Content-Length") - 1 
        && ngx_strncasecmp(header[i].key.data, (u_char *) "Content-Length", sizeof("Content-Length") - 1) == 0) {
      continue;
    }
    h = ngx_list_push(&r->headers_in.headers);
    if (h == NULL) {
      return NGX_ERROR;
    }
    *h = header[i];
  }

  /* XXX maybe we should set those built-in header slot in
    * ngx_http_headers_in_t too? */

  return NGX_OK;
}

ngx_int_t nchan_adjust_subrequest(ngx_http_request_t *sr, ngx_uint_t method, ngx_str_t *method_name, ngx_http_request_body_t *request_body, size_t content_length_n) {
  //ngx_http_core_main_conf_t  *cmcf;
  ngx_http_request_t         *r;
  ngx_http_request_body_t    *body;
  ngx_int_t                   rc;

  sr->method = method;
  sr->method_name = *method_name;

  if (sr->method == NGX_HTTP_HEAD) {
    sr->header_only = 1;
  }
  r = sr->parent;

  sr->header_in = r->header_in;

  /* XXX work-around a bug in ngx_http_subrequest */
  if (r->headers_in.headers.last == &r->headers_in.headers.part) {
    sr->headers_in.headers.last = &sr->headers_in.headers.part;
  }

  
  /* we do not inherit the parent request's variables */
  //scratch that, let's inherit those vars
  //cmcf = ngx_http_get_module_main_conf(sr, ngx_http_core_module);
  //sr->variables = ngx_pcalloc(sr->pool, cmcf->variables.nelts * sizeof(ngx_http_variable_value_t));

  if (sr->variables == NULL) {
    return NGX_ERROR;
  }

  if ((body = request_body)!=NULL) {
    sr->request_body = body;

    rc = nchan_set_content_length_header(sr, content_length_n);
    
    if (rc != NGX_OK) {
      return NGX_ERROR;
    }
  }

  //dd("subrequest body: %p", sr->request_body);

  return NGX_OK;
}
#if (NGX_HTTP_V2) || (NGX_HTTP_SPDY)
static void nchan_recover_upstream_hacky_request_method(ngx_http_request_t *r) {
  ngx_uint_t           i;
  ngx_buf_t            request_buf;
  u_char              *str;
  ngx_int_t            len;
  
  static const struct {
    int8_t            len;
    const u_char      method[11];
    uint32_t          code;
  } tests[] = {
    { 3, "GET ",       NGX_HTTP_GET },
    { 4, "POST ",      NGX_HTTP_POST },
    { 4, "HEAD ",      NGX_HTTP_HEAD },
    { 7, "OPTIONS ",   NGX_HTTP_OPTIONS },
    { 8, "PROPFIND ",  NGX_HTTP_PROPFIND },
    { 3, "PUT ",       NGX_HTTP_PUT },
    { 5, "MKCOL ",     NGX_HTTP_MKCOL },
    { 6, "DELETE ",    NGX_HTTP_DELETE },
    { 4, "COPY ",      NGX_HTTP_COPY },
    { 4, "MOVE ",      NGX_HTTP_MOVE },
    { 9, "PROPPATCH ", NGX_HTTP_PROPPATCH },
    { 4, "LOCK ",      NGX_HTTP_LOCK },
    { 6, "UNLOCK ",    NGX_HTTP_UNLOCK },
    { 5, "PATCH ",     NGX_HTTP_PATCH },
    { 5, "TRACE ",     NGX_HTTP_TRACE }
  };
  
  request_buf = *r->upstream->request_bufs->buf;
  str= request_buf.start;
  len = request_buf.end - request_buf.start;
  
  for(i=0; i < sizeof(tests) / sizeof(tests[0]); i++) {
    if(len >= tests[i].len + 1 && ngx_strncmp(str, tests[i].method, tests[i].len + 1) == 0) {
      r->method = tests[i].code;
      r->method_name.len = tests[i].len;
      r->method_name.data = (u_char *)tests[i].method;
      break;
    }
  }
}
#endif


static void nchan_recover_http_request_method(ngx_http_request_t *r) {
  ngx_http_request_t       rdummy;
  ngx_buf_t                header_in_buf = *r->header_in;
  ngx_int_t                rc;
  
  header_in_buf.pos = header_in_buf.start;
  ngx_memzero(&rdummy, sizeof(rdummy));
  rdummy.request_line = r->request_line;
  
  rc = ngx_http_parse_request_line(&rdummy, &header_in_buf);

  if (rc == NGX_OK) {
    r->method_name.len = rdummy.method_end - rdummy.request_start + 1;
    r->method_name.data = rdummy.request_line.data;
    r->method = rdummy.method;
  }
}

ngx_int_t nchan_recover_x_accel_redirected_request_method(ngx_http_request_t *r) {
#if (NGX_HTTP_V2)
  if(r->stream) {
    nchan_recover_upstream_hacky_request_method(r);
  }
  else {
    nchan_recover_http_request_method(r);
  }
#elif (NGX_HTTP_SPDY)
  if(r->spdy_stream) {
    nchan_recover_upstream_hacky_request_method(r);
  }
  else {
    nchan_recover_http_request_method(r);
  }
#else
  nchan_recover_http_request_method(r);
#endif
  return NGX_OK;
}

size_t nchan_subrequest_content_length(ngx_http_request_t *sr) {
  size_t                            len = 0;
  ngx_http_upstream_headers_in_t   *headers_in = &sr->upstream->headers_in;
  ngx_chain_t                      *chain;
  ngx_chain_t                      *body_chain;
#if nginx_version >= 1013010
  body_chain = sr->out;
#else
  body_chain = sr->upstream->out_bufs;
#endif
  
  
  if(headers_in->chunked || headers_in->content_length_n == -1) {
    //count it
    for(chain = body_chain; chain != NULL; chain = chain->next) {
      len += ngx_buf_size((chain->buf));
    }
  }
  else {
    len = headers_in->content_length_n > 0 ? headers_in->content_length_n : 0;
  }
  
  return len;
}


ngx_http_request_t *nchan_create_subrequest(ngx_http_request_t *r, ngx_str_t *url, ngx_buf_t *body, ngx_http_post_subrequest_pt cb, void *pd) {
  ngx_http_post_subrequest_t    *psr = ngx_pcalloc(r->pool, sizeof(*psr));
  ngx_http_request_t            *sr;
  
  psr->handler = cb;
  psr->data = pd;
  
  ngx_http_subrequest(r, url, NULL, &sr, psr, NGX_HTTP_SUBREQUEST_IN_MEMORY);
  
  if((sr->request_body = ngx_pcalloc(r->pool, sizeof(*sr->request_body))) == NULL) { //dummy request body 
    return NULL;
  }
  
  if(body && ngx_buf_size(body) > 0) {
    static ngx_str_t                   POST_REQUEST_STRING = {4, (u_char *)"POST "};
    size_t                             sz;
    ngx_http_request_body_t           *sr_body = sr->request_body;
    ngx_chain_t                       *fakebody_chain;
    ngx_buf_t                         *fakebody_buf;
    
    fakebody_chain = ngx_palloc(r->pool, sizeof(*fakebody_chain));
    fakebody_buf = ngx_pcalloc(r->pool, sizeof(*fakebody_buf));
    sr_body->bufs = fakebody_chain;
    fakebody_chain->next = NULL;
    fakebody_chain->buf = fakebody_buf;
    fakebody_buf->last_buf = 1;
    fakebody_buf->last_in_chain = 1;
    fakebody_buf->flush = 1;
    fakebody_buf->memory = 1;
    
    //just copy the buffer contents. it's inefficient but I don't care at the moment.
    //this can and should be optimized later
    sz = ngx_buf_size(body);
    fakebody_buf->start = ngx_palloc(r->pool, sz); //huuh?
    ngx_memcpy(fakebody_buf->start, body->start, sz);
    fakebody_buf->end = fakebody_buf->start + sz;
    fakebody_buf->pos = fakebody_buf->start;
    fakebody_buf->last = fakebody_buf->end;
    
    nchan_adjust_subrequest(sr, NGX_HTTP_POST, &POST_REQUEST_STRING, sr_body, sz);
  }
  else {
    sr->header_only = 1;
  }
  sr->args = r->args;
  
  return sr;
}
