#include <nchan_module.h>
#include "shmem.h"
#include "assert.h"

#if nginx_version <= 1011006
//old ugly slab-hack to track alloc'd pages
#include <util/ngx_nchan_hacked_slab.h>
#endif

#define DEBUG_SHM_ALLOC 0

#define SHPOOL(shmem) ((ngx_slab_pool_t *)(shmem)->zone->shm.addr)

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "SHMEM(%i):" fmt, memstore_slot(), ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "SHMEM(%i):" fmt, memstore_slot(), ##args)

//#include <valgrind/memcheck.h>

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
  if(nchan_shm_zone && nchan_shm_zone->shm.size != shm_size) {
    ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "Cannot change memory area size without restart, ignoring change");
  }
  */
  ngx_conf_log_error(NGX_LOG_INFO, cf, 0, "Using %udKiB of shared memory for nchan", shm_size >> 10);

  shm = ngx_alloc(sizeof(*shm), ngx_cycle->log);
  zone = ngx_shared_memory_add(cf, name, shm_size, &ngx_nchan_module);
  if (zone == NULL || shm == NULL) {
    return NULL;
  }
  shm->zone = zone;

  zone->init = init;
  zone->data = (void *) 1;
  return shm;
}

#if nginx_version <= 1011006
void shm_set_allocd_pages_tracker(shmem_t *shm, ngx_atomic_uint_t *ptr) {
  nchan_slab_set_reserved_pages_tracker(SHPOOL(shm), ptr);
}
#else
size_t shm_used_pages(shmem_t *shm) {
  ngx_slab_pool_t    *shpool = SHPOOL(shm);
  ngx_slab_page_t    *slots = (ngx_slab_page_t *) ((u_char *) (shpool) + sizeof(ngx_slab_pool_t));
  size_t              size = shpool->end - (u_char *) slots;
  
  ngx_uint_t          max = (ngx_uint_t) (size / (ngx_pagesize + sizeof(ngx_slab_page_t)));
  return max - shpool->pfree;
}
#endif

ngx_int_t shm_init(shmem_t *shm) {
  ngx_slab_pool_t    *shpool = SHPOOL(shm);
  #if (DEBUG_SHM_ALLOC == 1)
  ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "nchan_shpool start %p size %i", shpool->start, (u_char *)shpool->end - (u_char *)shpool->start);
  #endif
#if nginx_version <= 1011006
  nchan_slab_init(shpool);
#else
  ngx_slab_init(shpool);
#endif  
  
  return NGX_OK;
}

ngx_int_t shm_reinit(shmem_t *shm) {
  ngx_slab_pool_t    *shpool = SHPOOL(shm);
#if nginx_version <= 1011006
  nchan_slab_init(shpool);
#else
  ngx_slab_init(shpool);
#endif  
  
  return NGX_OK;
}

void shmtx_lock(shmem_t *shm) {
  ngx_shmtx_lock(&SHPOOL(shm)->mutex);
}
void shmtx_unlock(shmem_t *shm) {
  ngx_shmtx_unlock(&SHPOOL(shm)->mutex);
}

ngx_int_t shm_destroy(shmem_t *shm) {
  //VALGRIND_DESTROY_MEMPOOL(SHPOOL(shm));
  ngx_free(shm);
  
  return NGX_OK;
}
void *shm_alloc(shmem_t *shm, size_t size, const char *label) {
  void         *p;
#if (FAKESHARD || FAKE_SHMEM)
  p = ngx_alloc(size, ngx_cycle->log);
#else
  #if nginx_version <= 1011006
  p = nchan_slab_alloc(SHPOOL(shm), size);  
  #else
  p = ngx_slab_alloc(SHPOOL(shm), size);
  #endif
#endif
  if(p == NULL) {
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
#if (FAKESHARD || FAKE_SHMEM)
  ngx_free(p);
#else
  #if nginx_version <= 1011006
  nchan_slab_free(SHPOOL(shm), p);
  #else
  ngx_slab_free(SHPOOL(shm), p);
  #endif
#endif
#if (DEBUG_SHM_ALLOC == 1)
  ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "shpool free addr %p", p);
#endif
}



//copypasta because these should only be used for debugging

void *shm_locked_alloc(shmem_t *shm, size_t size, const char *label) {
  void         *p;
#if (FAKESHARD || FAKE_SHMEM)
  p = ngx_alloc(size, ngx_cycle->log);
#else
  #if nginx_version <= 1011006
  p = nchan_slab_alloc_locked(SHPOOL(shm), size);
  #else 
  p = ngx_slab_alloc_locked(SHPOOL(shm), size);
  #endif
#endif
  if(p == NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "shpool alloc failed");
  }

  #if (DEBUG_SHM_ALLOC == 1)
  if (p != NULL) {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "shpool alloc addr %p size %ui label %s", p, size, label == NULL ? "none" : label);
  }
  #endif
  return p;
}

void *shm_locked_calloc(shmem_t *shm, size_t size, const char *label) {
  void *p = shm_locked_alloc(shm, size, label);
  if(p != NULL) {
    ngx_memzero(p, size);
  }
  return p;
}

void shm_locked_free(shmem_t *shm, void *p) {
#if (FAKESHARD || FAKE_SHMEM)
  ngx_free(p);
#else
  #if nginx_version <= 1011006
  nchan_slab_free_locked(SHPOOL(shm), p);
  #else
  ngx_slab_free_locked(SHPOOL(shm), p);
  #endif
#endif
#if (DEBUG_SHM_ALLOC == 1)
  ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "shpool free addr %p", p);
#endif
}

void shm_verify_immutable_string(shmem_t *shm, ngx_str_t *str) {
 /* u_char    *pt=(u_char *)str-1;
  assert(pt[0]=='<');
  pt=str->data;
  assert(pt[str->len]=='>');
  */
}

void shm_free_immutable_string(shmem_t *shm, ngx_str_t *str) {
  shm_free(shm, (void *)str);
}

ngx_str_t *shm_copy_immutable_string(shmem_t *shm, ngx_str_t *str_in) {
  ngx_str_t    *str;
  size_t        sz = sizeof(*str) + str_in->len;
  if((str = shm_alloc(shm, sz, "string")) == NULL) {
    return NULL;
  }
  str->data=(u_char *)&str[1];
  str->len=str_in->len;
  ngx_memcpy(str->data, str_in->data, str_in->len);
  return str;
}
