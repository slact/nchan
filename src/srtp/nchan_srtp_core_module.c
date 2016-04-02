#include <nchan_module.h>
#include "nchan_srtp.h"

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SRTP:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SRTP:" fmt, ##arg)


ngx_uint_t nchan_srtp_max_module;

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

