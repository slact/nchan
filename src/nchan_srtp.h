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

  ngx_uint_t                 server_names_hash_max_size;
  ngx_uint_t                 server_names_hash_bucket_size;

  ngx_uint_t                 variables_hash_max_size;
  ngx_uint_t                 variables_hash_bucket_size;

  ngx_hash_keys_arrays_t    *variables_keys;

  ngx_array_t               *ports;
} nchan_srtp_core_main_conf_t;

char *nchan_srtp_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

#endif //NCHAN_SRTP_H