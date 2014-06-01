#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <nginx.h>

#include <ngx_http_push_defs.h>
#include <ngx_http_push_types.h>
#include <ngx_http_push_module.h>

#include "legacy.h"
#include "rbtree_util.c"
#include <store/ngx_http_push_module_ipc.c>

//initialization
static ngx_int_t ngx_http_push_store_init_module(ngx_cycle_t *cycle) {
  ngx_core_conf_t                *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  return NGX_ERROR;
}
static ngx_int_t ngx_http_push_store_init_worker(ngx_cycle_t *cycle) {
  ngx_core_conf_t                *ccf = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
  return NGX_ERROR;
}
static ngx_int_t ngx_http_push_store_init_postconfig(ngx_conf_t *cf) {
  ngx_http_push_main_conf_t *conf = ngx_http_conf_get_module_main_conf(cf, ngx_http_push_module);
  return NGX_ERROR;
}
static void ngx_http_push_store_create_main_conf(ngx_conf_t *cf, ngx_http_push_main_conf_t *mcf) {
  
}

//exit
static void ngx_http_push_store_exit_worker(ngx_cycle_t *cycle) {
  ngx_http_push_ipc_exit_worker(cycle);
}
static void ngx_http_push_store_exit_master(ngx_cycle_t *cycle) {
  //destroy channel tree in shared memory
  ngx_http_push_walk_rbtree(ngx_http_push_movezig_channel_locked);
}


//channels
static ngx_http_push_channel_t * ngx_http_push_store_get_channel(ngx_str_t *id, time_t channel_timeout, ngx_log_t *log) {
  //get the channel and check channel authorization while we're at it.
  return NULL;
}
static ngx_http_push_channel_t * ngx_http_push_store_find_channel(ngx_str_t *id, time_t channel_timeout, ngx_log_t *log) {
  //get the channel and check channel authorization while we're at it.
  return NULL;
}
static ngx_int_t ngx_http_push_store_delete_channel(ngx_http_push_channel_t *channel, ngx_http_request_t *r) {
  return NGX_ERROR;
}

static ngx_int_t ngx_http_push_store_publish(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line, ngx_log_t *log) {
  return 0;
}
static ngx_http_push_subscriber_t * ngx_http_push_store_subscribe(ngx_http_push_channel_t *channel, ngx_http_request_t *r) {
  return NULL;
}
static ngx_int_t ngx_http_push_store_channel_subscribers(ngx_http_push_channel_t * channel) {
  return 0;
}
static ngx_int_t ngx_http_push_store_channel_worker_subscribers(ngx_http_push_subscriber_t * worker_sentinel) {
  return 0;
}

static ngx_http_push_msg_t * ngx_http_push_store_create_message(ngx_http_push_channel_t *channel, ngx_http_request_t *r) {
  return NULL;
}
static ngx_int_t ngx_http_push_delete_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t force) {
  return NGX_ERROR;
}
static ngx_http_push_msg_t * ngx_http_push_store_get_message(ngx_http_push_channel_t *channel, ngx_http_request_t *r, ngx_int_t                       *msg_search_outcome, ngx_http_push_loc_conf_t *cf, ngx_log_t *log) {
  return NULL;
}
static void ngx_http_push_store_reserve_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
}
static void ngx_http_push_store_release_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
}
static ngx_int_t ngx_http_push_store_enqueue_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_http_push_loc_conf_t *cf) {
  return NGX_ERROR;
}
static ngx_str_t * ngx_http_push_store_etag_from_message(ngx_http_push_msg_t *msg, ngx_pool_t *pool){
  ngx_str_t *etag;
  if(pool!=NULL && (etag = ngx_palloc(pool, sizeof(*etag) + NGX_INT_T_LEN))==NULL) {
    return NULL;
  }
  else if(pool==NULL && (etag = ngx_alloc(sizeof(*etag) + NGX_INT_T_LEN, ngx_cycle->log))==NULL) {
    return NULL;
  }
  etag->data = (u_char *)(etag+1);
  etag->len = ngx_sprintf(etag->data,"%ui", msg->message_tag)- etag->data;
  return etag;
}

static ngx_str_t * ngx_http_push_store_content_type_from_message(ngx_http_push_msg_t *msg, ngx_pool_t *pool){
  ngx_str_t *content_type;
  if(pool != NULL && (content_type = ngx_palloc(pool, sizeof(*content_type) + msg->content_type.len))==NULL) {
    return NULL;
  }
  else if(pool == NULL && (content_type = ngx_alloc(sizeof(*content_type) + msg->content_type.len, ngx_cycle->log))==NULL) {
    return NULL;
  }
  content_type->data = (u_char *)(content_type+1);
  content_type->len = msg->content_type.len;
  ngx_memcpy(content_type->data, msg->content_type.data, content_type->len);
  return content_type;
}







// this function adapted from push stream module. thanks Wandenberg Peixoto <wandenberg@gmail.com> and Rogério Carvalho Schneider <stockrt@gmail.com>
static ngx_buf_t * ngx_http_push_request_body_to_single_buffer(ngx_http_request_t *r) {
  ngx_buf_t *buf = NULL;
  ngx_chain_t *chain;
  ssize_t n;
  off_t len;

  chain = r->request_body->bufs;
  if (chain->next == NULL) {
    return chain->buf;
  }
  if (chain->buf->in_file) {
    if (ngx_buf_in_memory(chain->buf)) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: can't handle a buffer in a temp file and in memory ");
    }
    if (chain->next != NULL) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: error reading request body with multiple ");
    }
    return chain->buf;
  }
  buf = ngx_create_temp_buf(r->pool, r->headers_in.content_length_n + 1);
  if (buf != NULL) {
    ngx_memset(buf->start, '\0', r->headers_in.content_length_n + 1);
    while ((chain != NULL) && (chain->buf != NULL)) {
      len = ngx_buf_size(chain->buf);
      // if buffer is equal to content length all the content is in this buffer
      if (len >= r->headers_in.content_length_n) {
        buf->start = buf->pos;
        buf->last = buf->pos;
        len = r->headers_in.content_length_n;
      }
      if (chain->buf->in_file) {
        n = ngx_read_file(chain->buf->file, buf->start, len, 0);
        if (n == NGX_FILE_ERROR) {
          ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: cannot read file with request body");
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
  }
  return buf;
}


void ngx_http_push_store_do_nothing(void) {
}


ngx_http_push_store_t  ngx_http_push_store_legacy = {
    //init
    &ngx_http_push_store_init_module,
    &ngx_http_push_store_init_worker,
    &ngx_http_push_store_init_postconfig,
    &ngx_http_push_store_create_main_conf,
    
    //shutdown
    &ngx_http_push_store_exit_worker,
    &ngx_http_push_store_exit_master,
  
    //channel stuff
    &ngx_http_push_store_get_channel, //creates channel if not found
    &ngx_http_push_store_find_channel, //returns channel or NULL if not found
    &ngx_http_push_store_delete_channel,
    &ngx_http_push_store_get_message,
    &ngx_http_push_store_reserve_message,
    &ngx_http_push_store_release_message,
    
    //pub/sub
    &ngx_http_push_store_publish,
    &ngx_http_push_store_subscribe,
    
    //channel properties
    &ngx_http_push_store_channel_subscribers,
    &ngx_http_push_store_channel_worker_subscribers,
    
    &ngx_http_push_store_do_nothing, //legacy shared-memory store helpers
    &ngx_http_push_store_do_nothing, //legacy shared-memory store helpers
    
    //message stuff
    &ngx_http_push_store_create_message,
    &ngx_http_push_delete_message,
    &ngx_http_push_delete_message,
    &ngx_http_push_store_enqueue_message,
    &ngx_http_push_store_etag_from_message,
    &ngx_http_push_store_content_type_from_message
};
