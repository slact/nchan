#include <nchan_module.h>
#include "nchan_fake_request.h"

//fake request and connection code adapted from lua-nginx-module by agentzh

static void nchan_close_fake_request(ngx_http_request_t *r);

ngx_connection_t *nchan_create_fake_connection(ngx_pool_t *pool) {
  ngx_log_t               *log;
  ngx_connection_t        *c;
  ngx_connection_t        *saved_c = NULL;

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

  if (pool) {
    c->pool = pool;
  } 
  else {
    c->pool = ngx_create_pool(128, c->log);
    if (c->pool == NULL) {
      goto failed;
    }
  }

  log = ngx_pcalloc(c->pool, sizeof(ngx_log_t));
  if (log == NULL) {
      goto failed;
  }

  c->log = log;
  c->log->connection = c->number;
  c->log->action = NULL;
  c->log->data = NULL;

  c->log_error = NGX_ERROR_INFO;

  c->error = 1;

  return c;

failed:

  nchan_close_fake_connection(c);
  return NULL;
}

void nchan_close_fake_connection(ngx_connection_t *c) {
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

ngx_http_request_t *nchan_create_fake_request(ngx_connection_t *c) {
  ngx_http_request_t      *r;

  r = ngx_pcalloc(c->pool, sizeof(ngx_http_request_t));
  if (r == NULL) {
    return NULL;
  }

  c->requests++;

  r->pool = c->pool;

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

  r->ctx = ngx_pcalloc(r->pool, sizeof(void *) * ngx_http_max_module);
  if (r->ctx == NULL) {
      return NULL;
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

  return r;
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
