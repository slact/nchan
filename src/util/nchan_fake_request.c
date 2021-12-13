#include "nchan_fake_request.h"
#include <util/nchan_subrequest.h>
#include <assert.h>
//fake request and connection code adapted from lua-nginx-module by agentzh

static void nchan_close_fake_request(ngx_http_request_t *r);
static void nchan_close_fake_connection(ngx_connection_t *c);

static ngx_connection_t *nchan_create_fake_connection(ngx_pool_t *pool) {
  ngx_log_t               *log;
  ngx_connection_t        *c;
  ngx_connection_t        *saved_c = NULL;
  assert(pool);

  /* (we temporarily use a valid fd (0) to make ngx_get_connection happy) */
  if (ngx_cycle->files) {
      saved_c = ngx_cycle->files[0];
  }

  c = ngx_get_connection(0, ngx_cycle->log);

  if (ngx_cycle->files) {
    ngx_cycle->files[0] = saved_c;
  }

  if (c == NULL) {
    return NULL;
  }

  c->fd = (ngx_socket_t) -1;
  c->number = ngx_atomic_fetch_add(ngx_connection_counter, 1);

  c->pool = pool;

  log = ngx_pcalloc(c->pool, sizeof(ngx_log_t));
  if (log == NULL) {
      goto failed;
  }

  c->write->active = 1;
  
  c->log = log;
  c->log->connection = c->number;
  c->log->action = NULL;
  c->log->data = NULL;
  c->log->data = ngx_pcalloc(c->pool, sizeof(ngx_http_log_ctx_t));
  if(c->log->data == NULL) {
    goto failed;
  }
  c->read->log = log;
  c->write->log = log;
#if nginx_version >= 1019009
  c->read->active = 1;
#endif  
  c->log_error = NGX_ERROR_INFO;

  c->error = 0;

  return c;

failed:

  nchan_close_fake_connection(c);
  return NULL;
}

static void nchan_close_fake_connection(ngx_connection_t *c) {
  ngx_pool_t          *pool;
  ngx_connection_t    *saved_c = NULL;

  ngx_log_debug1(NGX_LOG_DEBUG_HTTP, c->log, 0,
                  "http close fake http connection %p", c);

  c->destroyed = 1;

  pool = c->pool;

  if (c->read->timer_set) {
    ngx_del_timer(c->read);
  }

  if (c->write->timer_set) {
    ngx_del_timer(c->write);
  }

  c->read->closed = 1;
  c->write->active = 0;
  c->write->closed = 1;

  /* we temporarily use a valid fd (0) to make ngx_free_connection happy */

  c->fd = 0;

  if (ngx_cycle->files) {
    saved_c = ngx_cycle->files[0];
  }

  ngx_free_connection(c);

  c->fd = (ngx_socket_t) -1;

  if (ngx_cycle->files) {
    ngx_cycle->files[0] = saved_c;
  }

  if (pool) {
    ngx_destroy_pool(pool);
  }
}

static ngx_http_request_t *nchan_new_fake_request(ngx_connection_t *c) {
  ngx_http_request_t *r = ngx_palloc(c->pool, sizeof(ngx_http_request_t));
  assert(c->data == NULL);
  if (r == NULL) {
    return NULL;
  }

  c->requests++;

  c->data = r;
  return r;
}
/*
static ngx_int_t nchan_initialize_fake_request(ngx_http_request_t *r, ngx_connection_t *c) {
  ngx_memzero(r, sizeof(*r));
  r->pool = c->pool;
  assert(c->data == r);
  
  #if 0
  hc = ngx_pcalloc(c->pool, sizeof(ngx_http_connection_t));
  if (hc == NULL) {
      goto failed;
  }

  r->header_in = c->buffer;
  r->header_end = c->buffer->start;

  if (ngx_list_init(&r->headers_out.headers, r->pool, 0,
                    sizeof(ngx_table_elt_t))
      != NGX_OK)
  {
      goto failed;
  }

  if (ngx_list_init(&r->headers_in.headers, r->pool, 0,
                    sizeof(ngx_table_elt_t))
      != NGX_OK)
  {
      goto failed;
  }
#endif

  r->ctx = ngx_pcalloc(c->pool, sizeof(void *) * ngx_http_max_module);
  if (r->ctx == NULL) {
      return NGX_ERROR;
  }

#if 0
  cmcf = ngx_http_get_module_main_conf(r, ngx_http_core_module);

  r->variables = ngx_pcalloc(r->pool, cmcf->variables.nelts
                              * sizeof(ngx_http_variable_value_t));
  if (r->variables == NULL) {
      goto failed;
  }
#endif
  
  r->connection = c;

  r->headers_in.content_length_n = 0;
  c->data = r;
#if 0
  hc->request = r;
  r->http_connection = hc;
#endif
  r->signature = NGX_HTTP_MODULE;
  r->main = r;
  r->count = 1;

  r->method = NGX_HTTP_UNKNOWN;

  r->headers_in.keep_alive_n = -1;
  r->uri_changes = NGX_HTTP_MAX_URI_CHANGES + 1;
  r->subrequests = NGX_HTTP_MAX_SUBREQUESTS + 1;

  r->http_state = NGX_HTTP_PROCESS_REQUEST_STATE;
  r->discard_body = 1;
  return NGX_OK;
}
*/
void empty_handler(ngx_http_request_t *r) {
  //do nothing
}

ngx_http_request_t *nchan_create_derivative_fake_request(ngx_connection_t *c, ngx_http_request_t *rsrc) {
  ngx_http_request_t      *fr = nchan_new_fake_request(c);
  if(fr == NULL) {
    return NULL;
  }
  
  *fr = *rsrc;
  fr->read_event_handler = empty_handler;
  fr->write_event_handler = empty_handler;
  fr->connection = c;
  fr->main = fr;
  fr->pool = c->pool;
  fr->parent = NULL;
  fr->cleanup = NULL;
  fr->http_state = NGX_HTTP_PROCESS_REQUEST_STATE;
  fr->signature = NGX_HTTP_MODULE;
  fr->count = 1;
  
  return fr;
}

void nchan_finalize_fake_request(ngx_http_request_t *r, ngx_int_t rc) {
  ngx_connection_t          *c;
#if (NGX_HTTP_SSL)
  ngx_ssl_conn_t            *ssl_conn;
#endif

  c = r->connection;

  ngx_log_debug3(NGX_LOG_DEBUG_HTTP, c->log, 0,
                  "http finalize fake request: %d, a:%d, c:%d",
                  rc, r == c->data, r->main->count);

  if (rc == NGX_DONE) {
      nchan_close_fake_request(r);
      return;
  }

  if (rc == NGX_ERROR || rc >= NGX_HTTP_SPECIAL_RESPONSE) {

#if (NGX_HTTP_SSL)

    if (r->connection->ssl) {
        ssl_conn = r->connection->ssl->connection;
        if (ssl_conn) {
          c = ngx_ssl_get_connection(ssl_conn);
        }
    }

#endif

    nchan_close_fake_request(r);
    return;
  }

  if (c->read->timer_set) {
    ngx_del_timer(c->read);
  }

  if (c->write->timer_set) {
    c->write->delayed = 0;
    ngx_del_timer(c->write);
  }

  nchan_close_fake_request(r);
}

static void nchan_close_fake_request(ngx_http_request_t *r) {
  ngx_connection_t  *c;

  r = r->main;
  c = r->connection;

  ngx_log_debug1(NGX_LOG_DEBUG_HTTP, c->log, 0,
                  "http fake request count:%d", r->count);

  if (r->count == 0) {
    ngx_log_error(NGX_LOG_ALERT, c->log, 0, "http fake request count is zero");
  }

  r->count--;

  if (r->count) {
    return;
  }

  nchan_free_fake_request(r);
  nchan_close_fake_connection(c);
}

void nchan_free_fake_request(ngx_http_request_t *r) {
  ngx_log_t                 *log;
  ngx_http_cleanup_t        *cln;

  log = r->connection->log;

  ngx_log_debug0(NGX_LOG_DEBUG_HTTP, log, 0, "http close fake request");

  if (r->pool == NULL) {
    ngx_log_error(NGX_LOG_ALERT, log, 0, "http fake request already closed");
    return;
  }

  cln = r->cleanup;
  r->cleanup = NULL;

  while (cln) {
    if (cln->handler) {
      cln->handler(cln->data);
    }

    cln = cln->next;
  }

  r->request_line.len = 0;

  r->connection->destroyed = 1;
}



ngx_int_t nchan_requestmachine_initialize(nchan_requestmachine_t *rm, ngx_http_request_t *template_request) {
  rm->template_request = template_request;
  nchan_slist_init(&rm->request_queue, nchan_fakereq_subrequest_data_t, slist.prev, slist.next);
  return NGX_OK;
}

static ngx_int_t nchan_requestmachine_run(nchan_requestmachine_t *rm) {
  nchan_fakereq_subrequest_data_t *head = nchan_slist_first(&rm->request_queue);
  if(head && !head->running) {
    head->running = 1;
    ngx_http_run_posted_requests(head->r->connection);
  }
  return NGX_OK;
}

static ngx_int_t nchan_requestmachine_subrequest_handler(ngx_http_request_t *sr, void *pd, ngx_int_t code) {
  nchan_fakereq_subrequest_data_t *d = pd;
  d->running = 0;
  if(d->rm) {
    assert(d->rm->request_queue.head == d);
    if(d->cb) {
      d->cb(code, sr, d->pd);
    }
    if(d->rm) {
      nchan_slist_remove(&d->rm->request_queue, d);
      nchan_requestmachine_run(d->rm);
    }
  }
  else if(d->cb) {
    d->cb(NGX_ABORT, sr, d->pd);
  }
  if(!d->manual_cleanup && !d->cleanup_timer.timer_set) {
    ngx_add_timer(&d->cleanup_timer, 0);
  }
  return NGX_OK;
}

ngx_int_t nchan_requestmachine_request_cleanup_manual(nchan_fakereq_subrequest_data_t *d) {
  if(!d->cleanup_timer.timer_set) {
    ngx_add_timer(&d->cleanup_timer, 0);
  }
  return NGX_OK;
}

static void nchan_requestmachine_request_cleanup_on_request_finalize_cln_handler(void *d) {
  nchan_requestmachine_request_cleanup_manual(d);
}

ngx_int_t nchan_requestmachine_request_cleanup_on_request_finalize(nchan_fakereq_subrequest_data_t *d, ngx_http_request_t *r) {
  ngx_http_cleanup_t     *cln = ngx_http_cleanup_add(r, 0);
  if(cln == NULL) {
    return NGX_ERROR;
  }
  cln->data = d;
  cln->handler = nchan_requestmachine_request_cleanup_on_request_finalize_cln_handler;
  return NGX_OK;
}

static void fakerequest_cleanup_timer_handler(ngx_event_t *ev) {
  nchan_fakereq_subrequest_data_t *d = ev->data;
  d->r->main->count--;
  assert(d->r->main->count == 1);
  nchan_finalize_fake_request(d->r, NGX_OK);
}

nchan_fakereq_subrequest_data_t *nchan_requestmachine_request(nchan_requestmachine_t *rm, nchan_requestmachine_request_params_t *params) {
  nchan_fakereq_subrequest_data_t *d;
  ngx_pool_t *pool = params->pool;
  int         created_pool = 0;
  ngx_str_t url;
  
  if(!pool) {
    if((pool = ngx_create_pool(1024, ngx_cycle->log)) == NULL) {
      nchan_log_error("failed to create requestmachine pool");
      return NULL;
    }
    created_pool = 1;
  }
  
  if(params->url_complex) {
    if(ngx_http_complex_value_custom_pool(rm->template_request, params->url.cv, &url, pool) != NGX_OK) {
      if(created_pool) {
        ngx_destroy_pool(pool);
      }
      nchan_log_error("failed to create subrequest url");
      return NULL;
    }
    params->url.str = &url;
  }
  
  ngx_connection_t            *fc = nchan_create_fake_connection(pool);
  if(fc == NULL) {
    if(created_pool) {
      ngx_destroy_pool(pool);
    }
    return NULL;
  }
  ngx_http_request_t          *fr = nchan_create_derivative_fake_request(fc, rm->template_request);
  ngx_http_request_t          *sr;
  
  d = ngx_palloc(pool, sizeof(*d));
  ngx_http_post_subrequest_t  *psr = ngx_pcalloc(pool, sizeof(*psr));
  if(fr == NULL || d == NULL || psr == NULL) {
    if(created_pool) {
      ngx_destroy_pool(pool);
    }
    return NULL;
  }
  
  fr->main_conf = rm->template_request->main_conf;
  fr->srv_conf = rm->template_request->srv_conf;
  fr->loc_conf = rm->template_request->loc_conf;
  
  d->pd = params->pd;
  d->cb = params->cb;
  d->running = 0;
  d->r = fr;
  d->rm = rm;
  d->manual_cleanup = params->manual_cleanup;
  ngx_memzero(&d->cleanup_timer, sizeof(d->cleanup_timer));
  nchan_init_timer(&d->cleanup_timer, fakerequest_cleanup_timer_handler, d);
  
  fr->main->count++; //make sure the fake request doesn't auto-finalize on subrequest completion
  
  psr->handler = nchan_requestmachine_subrequest_handler;
  psr->data = d;
  
  ngx_http_subrequest(fr, params->url.str, NULL, &sr, psr, NGX_HTTP_SUBREQUEST_IN_MEMORY);
  if(sr == NULL) {
    if(created_pool) {
      ngx_destroy_pool(pool);
    }
    return NULL;
  }
  d->sr = sr;
  
  if((sr->request_body = ngx_pcalloc(pool, sizeof(*sr->request_body))) == NULL) { //dummy request body 
    if(created_pool) {
      ngx_destroy_pool(pool);
    }
    return NULL;
  }
  
  if(params->body && ngx_buf_size(params->body) > 0) {
    static ngx_str_t                   POST_REQUEST_STRING = {4, (u_char *)"POST "};
    size_t                             sz = ngx_buf_size(params->body);;
    ngx_http_request_body_t           *sr_body = sr->request_body;
    ngx_chain_t                       *fakebody_chain;
    ngx_buf_t                         *fakebody_buf;
    
    fakebody_chain = ngx_palloc(pool, sizeof(*fakebody_chain));
    fakebody_buf = ngx_pcalloc(pool, sizeof(*fakebody_buf));
    sr_body->bufs = fakebody_chain;
    fakebody_chain->next = NULL;
    fakebody_chain->buf = fakebody_buf;
    *fakebody_buf = *params->body;
    fakebody_buf->last_buf = 1;
    fakebody_buf->last_in_chain = 1;
    fakebody_buf->flush = 1;
    fakebody_buf->memory = 1;
    
    nchan_adjust_subrequest(sr, NGX_HTTP_POST, &POST_REQUEST_STRING, sr_body, sz);
  }
  else {
    nchan_set_content_length_header(sr, 0);
  }
  
  sr->header_only = params->response_headers_only;
  
  sr->args = fr->args;
  
  nchan_slist_append(&rm->request_queue, d);
  
  nchan_requestmachine_run(rm);
  return d;
}
ngx_int_t nchan_requestmachine_shutdown(nchan_requestmachine_t *rm) {
  return nchan_requestmachine_abort(rm);
}

ngx_int_t nchan_requestmachine_abort(nchan_requestmachine_t *rm) {
  nchan_fakereq_subrequest_data_t *cur;
  ngx_http_request_t              *r;
  ngx_http_core_main_conf_t       *cmcf;
  while((cur = nchan_slist_pop(&rm->request_queue)) != NULL) {
    cur->rm = NULL; //no requestmachine ref means the machine stopped.
    r = cur->r;
    //clear stuff that might be used after the upstream request finishes
    /*
    if (ngx_list_init(&r->headers_out.headers, r->pool, 0, sizeof(ngx_table_elt_t)) != NGX_OK) {
      nchan_log_error("couldn't create headers_out for requestmachine request");
    }
    if (ngx_list_init(&r->headers_in.headers, r->pool, 0,sizeof(ngx_table_elt_t)) != NGX_OK) {
      nchan_log_error("couldn't create headers_in for requestmachine request");
    }
    */

    r->ctx = ngx_pcalloc(r->pool, sizeof(void *) * ngx_http_max_module);
    if (r->ctx == NULL) {
      nchan_log_error("couldn't create ctx for requestmachine request");
    }
    cmcf = ngx_http_get_module_main_conf(r, ngx_http_core_module);
    r->variables = ngx_pcalloc(r->pool, cmcf->variables.nelts * sizeof(ngx_http_variable_value_t));
    if (r->variables == NULL) {
      nchan_log_error("couldn't create vars for requestmachine request");
    }
  }
  return NGX_OK;
}
