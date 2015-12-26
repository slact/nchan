#include <nchan_module.h>
#include <util/nchan_output.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "VARIABLES:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "VARIABLES:" fmt, ##arg)

typedef struct {
  ngx_str_t                   name;
  ngx_http_get_variable_pt    handler;
  uintptr_t                   data;
} nchan_variable_t;

static nchan_request_ctx_t *get_main_request_ctx(ngx_http_request_t *r){
  nchan_request_ctx_t        *ctx;
  ngx_http_request_t        *rcur;
  
  //if this is an subrequest, get nearest parent existing ctx
  for(rcur = r; rcur != NULL; rcur = rcur->parent) {
    ctx = ngx_http_get_module_ctx(rcur, nchan_module);
    if(ctx) return ctx;
  }
  
  //no existing ctx found
  return NULL;
}

static void set_varval(ngx_http_variable_value_t *v, u_char *data, size_t len) {
  v->valid = 1;
  v->no_cacheable = 1;
  v->not_found = 0;
  v->len = len;
  v->data = data;
}

static ngx_int_t nchan_channel_event(ngx_http_request_t *r, ngx_http_variable_value_t *v, uintptr_t data) {
  nchan_request_ctx_t        *ctx = get_main_request_ctx(r);
  if(ctx == NULL || ctx->channel_event_name == NULL) {
    v->not_found = 1;
    return NGX_OK;
  }
  
  set_varval(v, ctx->channel_event_name->data, ctx->channel_event_name->len);
  
  return NGX_OK;
}

static ngx_int_t nchan_channel_id_variable(ngx_http_request_t *r, ngx_http_variable_value_t *v, uintptr_t data) {
  nchan_request_ctx_t        *ctx = get_main_request_ctx(r);
  if(ctx == NULL) {
    v->not_found = 1;
    return NGX_OK;
  }
  
  set_varval(v, ctx->channel_id[data].data, ctx->channel_id[data].len);
  
  return NGX_OK;
}

static ngx_int_t nchan_subscriber_type_variable(ngx_http_request_t *r, ngx_http_variable_value_t *v, uintptr_t data) {
  nchan_request_ctx_t        *ctx = get_main_request_ctx(r);
  if(ctx == NULL || ctx->subscriber_type == NULL) {
    v->not_found = 1;
    return NGX_OK;
  }
  
  set_varval(v, ctx->subscriber_type->data, ctx->subscriber_type->len);
  
  return NGX_OK;
}

static ngx_int_t nchan_publisher_type_variable(ngx_http_request_t *r, ngx_http_variable_value_t *v, uintptr_t data) {
  nchan_request_ctx_t        *ctx = get_main_request_ctx(r);
  if(ctx == NULL || ctx->publisher_type == NULL) {
    v->not_found = 1;
    return NGX_OK;
  }
  
  set_varval(v, ctx->publisher_type->data, ctx->publisher_type->len);
  
  return NGX_OK;
}

static ngx_int_t nchan_message_id_variable(ngx_http_request_t *r, ngx_http_variable_value_t *v, uintptr_t data) {
  static u_char        msgidbuf[100];
  ngx_str_t           *msgid;
  
  nchan_request_ctx_t        *ctx = get_main_request_ctx(r);
  if(ctx == NULL || (ctx->msg_id.time == 0 && ctx->msg_id.tagcount == 0)) {
    v->not_found = 1;
    return NGX_OK;
  }
  
  msgid = msgid_to_str(&ctx->msg_id);
  ngx_memcpy(msgidbuf, msgid->data, msgid->len);
  set_varval(v, msgidbuf, msgid->len);
  
  return NGX_OK;
}

static ngx_int_t nchan_prev_message_id_variable(ngx_http_request_t *r, ngx_http_variable_value_t *v, uintptr_t data) {
  static u_char        msgidbuf[100];
  ngx_str_t           *msgid;
  
  nchan_request_ctx_t        *ctx = get_main_request_ctx(r);
  if(ctx == NULL  || (ctx->prev_msg_id.time == 0 && ctx->prev_msg_id.tagcount == 0)) {
    v->not_found = 1;
    return NGX_OK;
  }
  
  msgid = msgid_to_str(&ctx->prev_msg_id);
  ngx_memcpy(msgidbuf, msgid->data, msgid->len);
  set_varval(v, msgidbuf, msgid->len);
  
  return NGX_OK;
}

/*
static ngx_int_t nchan_message_variable(ngx_http_request_t *r, ngx_http_variable_value_t *v, uintptr_t data) {
  
  
  v->not_found = 1;
  return NGX_OK;
}
*/

/*
static ngx_int_t nchan_message_alert_type_variable(ngx_http_request_t *r, ngx_http_variable_value_t *v, uintptr_t data) {
  
  
  v->not_found = 1;
  return NGX_OK;
}
*/







nchan_variable_t nchan_vars[] = {
  { ngx_string("nchan_channel_id"),         nchan_channel_id_variable, 0},
  { ngx_string("nchan_channel_id1"),        nchan_channel_id_variable, 0},
  { ngx_string("nchan_channel_id2"),        nchan_channel_id_variable, 1},
  { ngx_string("nchan_channel_id3"),        nchan_channel_id_variable, 2},
  { ngx_string("nchan_channel_id4"),        nchan_channel_id_variable, 3},
  { ngx_string("nchan_channel_event"),      nchan_channel_event, 0},
  { ngx_string("nchan_subscriber_type"),    nchan_subscriber_type_variable, 0},
  { ngx_string("nchan_publisher_type"),     nchan_publisher_type_variable, 0},
//  { ngx_string("nchan_message"),            nchan_message_variable, 0},
  { ngx_string("nchan_prev_message_id"),    nchan_prev_message_id_variable, 0},
  { ngx_string("nchan_message_id"),         nchan_message_id_variable, 0},
//  { ngx_string("nchan_message_alert_type"), nchan_message_alert_type_variable, 0},
  
  { ngx_null_string,                        NULL, 0 }
};


ngx_int_t nchan_add_variables(ngx_conf_t *cf) {
  nchan_variable_t              *var;
  ngx_http_variable_t           *v;

  for (var = nchan_vars; var->name.len; var++) {
    
    v = ngx_http_add_variable(cf, &var->name, NGX_HTTP_VAR_CHANGEABLE);
    if (v == NULL) {
      return NGX_ERROR;
    }
    
    v->get_handler = var->handler;
    v->data = var->data;
  }

  return NGX_OK;
}