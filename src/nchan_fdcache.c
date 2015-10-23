#include <nchan_module.h>
#include <uthash.h>
#define CHANNEL_HASH_FIND(id_buf, p)    HASH_FIND( hh, mpt->hash, (id_buf)->data, (id_buf)->len, p)
#define CHANNEL_HASH_ADD(chanhead)      HASH_ADD_KEYPTR( hh, mpt->hash, (chanhead->id).data, (chanhead->id).len, chanhead)
#define CHANNEL_HASH_DEL(chanhead)      HASH_DEL( mpt->hash, chanhead)

#undef uthash_malloc
#undef uthash_free
#define uthash_malloc(sz) ngx_alloc(sz, ngx_cycle->log)
#define uthash_free(ptr,sz) ngx_free(ptr)

ngx_int_t nchan_fdcache_init(void) { 
  return NGX_OK;
}

ngx_int_t nchan_fdcache_shutdown(void) {
  return NGX_OK;
}

ngx_fd_t nchan_fdcache_get(ngx_str_t *name) {
  return NGX_INVALID_FILE;
}