#include <nchan_module.h>
#include "nchan_srtp.h"

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SRTP:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SRTP:" fmt, ##arg)


ngx_uint_t nchan_srtp_max_module;

static ngx_command_t  nchan_srtp_core_commands[] = {
  { ngx_string("srtp"),
    NGX_MAIN_CONF|NGX_CONF_BLOCK|NGX_CONF_NOARGS,
    nchan_srtp_block,
    0,
    0,
    NULL }
};

static ngx_core_module_t  nchan_srtp_core_module_ctx = {
  ngx_string("srtp"),
  NULL,
  NULL
};

ngx_module_t nchan_srtp_core_module = {
  NGX_MODULE_V1,
  &nchan_srtp_core_module_ctx,           /* module context */
  nchan_srtp_core_commands,              /* module directives */
  NGX_CORE_MODULE,                       /* module type */
  NULL,                                  /* init master */
  NULL,                                  /* init module */
  NULL,                                  /* init process */
  NULL,                                  /* init thread */
  NULL,                                  /* exit thread */
  NULL,                                  /* exit process */
  NULL,                                  /* exit master */
  NGX_MODULE_V1_PADDING
};

char *nchan_srtp_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  DBG("srtp block here");

  char                        *rv;
  ngx_uint_t                   m, mi;
  //ngx_uint_t                   s, i;
  ngx_conf_t                   pcf;
  nchan_srtp_core_module_t    *module;
  nchan_srtp_conf_ctx_t       *ctx;

  ctx = ngx_pcalloc(cf->pool, sizeof(nchan_srtp_conf_ctx_t));
  if (ctx == NULL) {
    return NGX_CONF_ERROR;
  }

  *(nchan_srtp_conf_ctx_t **) conf = ctx;

  /* count the number of the srtp modules and set up their indices */
  nchan_srtp_max_module = 0;
  for (m = 0; ngx_modules[m]; m++) {
    if (ngx_modules[m]->type != NCHAN_SRTP_MODULE) {
      continue;
    }
    ngx_modules[m]->ctx_index = nchan_srtp_max_module++;
  }

  /* the srtp main_conf context, it is the same in the all srtp contexts */
  ctx->main_conf = ngx_pcalloc(cf->pool, sizeof(void *) * nchan_srtp_max_module);
  if (ctx->main_conf == NULL) {
    return NGX_CONF_ERROR;
  }

  /*
   * the srtp null srv_conf context, it is used to merge
   * the server{}s' srv_conf's
   */
  ctx->srv_conf = ngx_pcalloc(cf->pool, sizeof(void *) * nchan_srtp_max_module);
  if (ctx->srv_conf == NULL) {
    return NGX_CONF_ERROR;
  }

  /*
   * create the main_conf's, the null srv_conf's, and the null app_conf's
   * of the all rtmp modules
   */
  for (m = 0; ngx_modules[m]; m++) {
    if (ngx_modules[m]->type != NCHAN_SRTP_MODULE) {
      continue;
    }
    
    module = ngx_modules[m]->ctx;
    mi = ngx_modules[m]->ctx_index;
    
    if (module->create_main_conf) {
      ctx->main_conf[mi] = module->create_main_conf(cf);
      if (ctx->main_conf[mi] == NULL) {
        return NGX_CONF_ERROR;
      }
    }
    
    if (module->create_srv_conf) {
      ctx->srv_conf[mi] = module->create_srv_conf(cf);
      if (ctx->srv_conf[mi] == NULL) {
        return NGX_CONF_ERROR;
      }
    }
  }

  pcf = *cf;
  cf->ctx = ctx;

  for (m = 0; ngx_modules[m]; m++) {
    if (ngx_modules[m]->type != NCHAN_SRTP_MODULE) {
      continue;
    }
    
    module = ngx_modules[m]->ctx;
    
    if (module->preconfiguration) {
      if (module->preconfiguration(cf) != NGX_OK) {
        return NGX_CONF_ERROR;
      }
    }
  }

  /* parse inside the rtmp{} block */
  cf->module_type = NCHAN_SRTP_MODULE;
  cf->cmd_type = NCHAN_SRTP_MAIN_CONF;
  rv = ngx_conf_parse(cf, NULL);

  if (rv != NGX_CONF_OK) {
    *cf = pcf;
    return rv;
  }
  
  
  
  
  //continued....
  
  
  return NGX_CONF_OK;
}