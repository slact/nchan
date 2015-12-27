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


static ngx_int_t ngx_http_echo_adjust_subrequest(ngx_http_request_t *sr, ngx_http_echo_subrequest_t *parsed_sr) {
  ngx_http_core_main_conf_t  *cmcf;
  ngx_http_request_t         *r;
  ngx_http_request_body_t    *body;
  ngx_int_t                   rc;

  sr->method = parsed_sr->method;
  sr->method_name = *(parsed_sr->method_name);

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

  body = parsed_sr->request_body;
  if (body) {
    sr->request_body = body;
    
    rc = ngx_http_echo_set_content_length_header(sr, body->buf ? ngx_buf_size(body->buf) : 0);
    
    if (rc != NGX_OK) {
      return NGX_ERROR;
    }
  }

  //dd("subrequest body: %p", sr->request_body);

  return NGX_OK;
}