#include <ngx_http_push_module.h>
#include "shmem.h"
#define DEBUG_SHM_ALLOC 0

#define SHPOOL(shmem) ((ngx_slab_pool_t *)(shmem)->zone->shm.addr)

//shared memory
shmem_t *shm_create(ngx_str_t *name, ngx_conf_t *cf, size_t shm_size, ngx_int_t (*init)(ngx_shm_zone_t *, void *), void *privdata) {

  ngx_shm_zone_t    *zone;
  shmem_t           *shm;

  shm_size = ngx_align(shm_size, ngx_pagesize);
  if (shm_size < 8 * ngx_pagesize) {
        ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "The push_max_reserved_memory value must be at least %udKiB", (8 * ngx_pagesize) >> 10);
        shm_size = 8 * ngx_pagesize;
    }
  /*
  if(ngx_http_push_shm_zone && ngx_http_push_shm_zone->shm.size != shm_size) {
    ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "Cannot change memory area size without restart, ignoring change");
  }
  */
  ngx_conf_log_error(NGX_LOG_INFO, cf, 0, "Using %udKiB of shared memory for push module", shm_size >> 10);

  shm = ngx_alloc(sizeof(*shm), ngx_cycle->log);
  zone = ngx_shared_memory_add(cf, name, shm_size, &ngx_http_push_module);
  if (zone == NULL || shm == NULL) {
    return NULL;
  }
  shm->zone = zone;

  zone->init = init;
  zone->data = (void *) 1;
  return shm;
}

ngx_int_t shm_init(shmem_t *shm) {
  #if (DEBUG_SHM_ALLOC == 1)
  ngx_slab_pool_t    *shpool = SHPOOL(shm);
  ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "ngx_http_push_shpool start %p size %i", shpool->start, (u_char *)shpool->end - (u_char *)shpool->start);
  #endif
  return NGX_OK;
}

ngx_int_t shm_destroy(shmem_t *shm) {
  ngx_free(shm);
  return NGX_OK;
}
void *shm_alloc(shmem_t *shm, size_t size, const char *label) {
  void         *p;
#if FAKESHARD
  if((p = ngx_alloc(size, ngx_cycle->log))==NULL) {
#else
  if((p = ngx_slab_alloc(SHPOOL(shm), size))==NULL) {
#endif
  
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "shpool alloc failed");
  }
  #if (DEBUG_SHM_ALLOC == 1)
  if (p != NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "shpool alloc addr %p size %ui label %s", p, size, label == NULL ? "none" : label);
  }
  #endif
  return p;
}

void *shm_calloc(shmem_t *shm, size_t size, const char *label) {
  void *p = shm_alloc(shm, size, label);
  if(p != NULL) {
    ngx_memzero(p, size);
  }
  return p;
}
void shm_free(shmem_t *shm, void *p) {
#if FAKESHARD
  ngx_free(p);
#else
  ngx_slab_free(SHPOOL(shm), p);
#endif
  #if (DEBUG_SHM_ALLOC == 1)
  ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "shpool free addr %p", p);
  #endif
}

ngx_str_t *shm_copy_string(shmem_t *shm, ngx_str_t *str_in) {
  ngx_str_t *str;
  if((str = shm_alloc(shm, sizeof(*str) + str_in->len, "string")) == NULL) {
    return NULL;
  }
  str->len=str_in->len;
  str->data=(u_char *)&str[1];
  ngx_memcpy(str->data, str_in->data, str->len);
  return str;
}