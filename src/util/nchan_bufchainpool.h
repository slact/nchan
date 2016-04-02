#ifndef NCHAN_BUFCHAINPOOL_H
#define NCHAN_BUFCHAINPOOL_H
#include <nchan_module.h>

typedef struct {
  ngx_chain_t     chain;
  ngx_buf_t       buf;
} nchan_buf_and_chain_t;

typedef struct nchan_bufchain_link_s nchan_bufchain_link_t;
struct nchan_bufchain_link_s {
  nchan_bufchain_link_t    *next;
  nchan_buf_and_chain_t     bc;
};

typedef struct nchan_file_link_s nchan_file_link_t;
struct nchan_file_link_s{
  nchan_file_link_t        *next;
  ngx_file_t                file;
};

typedef struct {
  ngx_int_t                  bc_count;
  ngx_int_t                  file_count;
  ngx_int_t                  bc_recycle_count;
  ngx_int_t                  file_recycle_count;
  nchan_bufchain_link_t     *bc_head;
  nchan_bufchain_link_t     *bc_recycle_head;
  nchan_file_link_t         *file_head;
  nchan_file_link_t         *file_recycle_head;
  ngx_pool_t                *pool;
} nchan_bufchain_pool_t;

ngx_int_t nchan_bufchain_pool_init(nchan_bufchain_pool_t *bcp, ngx_pool_t *pool);
nchan_buf_and_chain_t *nchan_bufchain_pool_reserve(nchan_bufchain_pool_t *bcp, ngx_int_t count);
ngx_file_t *nchan_bufchain_pool_reserve_file(nchan_bufchain_pool_t *bcp);
void nchan_bufchain_pool_refresh_files(nchan_bufchain_pool_t *bcp);
void nchan_bufchain_pool_flush(nchan_bufchain_pool_t *bcp);
#endif //NCHAN_BUFCHAINPOOL_H
