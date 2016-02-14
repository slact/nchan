#ifndef NCHAN_SRTP_H
#define NCHAN_SRTP_H

#include <nchan_types.h>

typedef struct {
  ngx_int_t   (*preconfiguration)(ngx_conf_t *cf);
  ngx_int_t   (*postconfiguration)(ngx_conf_t *cf);

  void       *(*create_main_conf)(ngx_conf_t *cf);
  char       *(*init_main_conf)(ngx_conf_t *cf, void *conf);

  void       *(*create_srv_conf)(ngx_conf_t *cf);
  char       *(*merge_srv_conf)(ngx_conf_t *cf, void *prev, void *conf);

  void       *(*create_loc_conf)(ngx_conf_t *cf);
  char       *(*merge_loc_conf)(ngx_conf_t *cf, void *prev, void *conf);
} nchan_srtp_module_t;

typedef struct {
  void        **main_conf;
  void        **srv_conf;
} nchan_srtp_conf_ctx_t;

typedef struct {
  ngx_array_t                servers;
  ngx_array_t                listen; 
  ngx_array_t               *ports;
} nchan_srtp_core_main_conf_t;

char *nchan_srtp_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);


typedef struct{
  /* array of the ngx_tcp_server_name_t, "server_name" directive */
  ngx_array_t              server_names;

  /* array of the ngx_tcp_core_loc_t, "location" directive */
  ngx_array_t              locations;

  ngx_msec_t               timeout;
  ngx_msec_t               resolver_timeout;

  ngx_flag_t               so_keepalive;
  ngx_flag_t               tcp_nodelay;

  ngx_str_t                server_name;

  u_char                  *file_name;
  ngx_int_t                line;

  ngx_resolver_t          *resolver;

  /*ACL rules*/
  ngx_array_t             *rules;

  /* server ctx */
  nchan_srtp_conf_ctx_t   *ctx;
} nchan_srtp_core_srv_conf_t;


typedef struct {
  u_char                  sockaddr[NGX_SOCKADDRLEN];
  socklen_t               socklen;

  /* server ctx */
  nchan_srtp_conf_ctx_t  *ctx;

  unsigned                default_port:1;
  unsigned                bind:1;
  unsigned                wildcard:1;
#if (NGX_HAVE_INET6 && defined IPV6_V6ONLY)
  unsigned                ipv6only:2;
#endif
  nchan_srtp_core_srv_conf_t *conf;
} nchan_srtp_listen_t;

typedef struct {
  int                      family;
  in_port_t                port;
  ngx_array_t              addrs;       /* array of ngx_tcp_conf_addr_t */
} nchan_srtp_conf_port_t;

typedef struct {
  struct sockaddr         *sockaddr;
  socklen_t                socklen;

  nchan_srtp_conf_ctx_t      *ctx;
  nchan_srtp_conf_ctx_t      *default_ctx;

  unsigned                 bind:1;
  unsigned                 wildcard:1;
#if (NGX_HAVE_INET6 && defined IPV6_V6ONLY)
  unsigned                 ipv6only:2;
#endif
} nchan_srtp_conf_addr_t;



#endif //NCHAN_SRTP_H