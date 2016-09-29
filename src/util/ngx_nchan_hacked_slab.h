
/*
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 */


#ifndef _NCHAN_HACKED_SLAB_H_INCLUDED_
#define _NCHAN_HACKED_SLAB_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>


void nchan_slab_init(ngx_slab_pool_t *pool);
void *nchan_slab_alloc(ngx_slab_pool_t *pool, size_t size);
void *nchan_slab_alloc_locked(ngx_slab_pool_t *pool, size_t size);
void *nchan_slab_calloc(ngx_slab_pool_t *pool, size_t size);
void *nchan_slab_calloc_locked(ngx_slab_pool_t *pool, size_t size);
void nchan_slab_free(ngx_slab_pool_t *pool, void *p);
void nchan_slab_free_locked(ngx_slab_pool_t *pool, void *p);

void nchan_slab_set_reserved_pages_tracker(ngx_slab_pool_t *pool, ngx_atomic_uint_t *ref);

#endif /* _NCHAN_HACKED_SLAB_H_INCLUDED_ */
