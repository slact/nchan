#include <nchan_module.h>
#include <util/shmem.h>
#include <store/memory/store.h>
#include <store/memory/store-private.h>
#include <store/memory/ipc.h>


static shmem_t                        *shm = NULL;
static nchan_stats_t                  *shstats = NULL;
static int                             stats_enabled = 0;

static ngx_int_t initialize_shm(ngx_shm_zone_t *zone, void *data);

ngx_int_t nchan_stats_init_postconfig(ngx_conf_t *cf, int enable) {
  ngx_str_t              name = ngx_string("nchan_worker_stats");
  shm = shm_create(&name, cf, sizeof(*shstats)*2, initialize_shm, &ngx_nchan_module);
  stats_enabled = enable;
  return NGX_OK;
}

ngx_int_t nchan_stats_init_worker(ngx_cycle_t *cycle) {
  nchan_stats_worker_t *wstats = &shstats->worker[ngx_process_slot];
  
  ngx_memzero(wstats, sizeof(*wstats));
  
  return NGX_OK;
}

void nchan_stats_exit_worker(ngx_cycle_t *cycle) {
  shm_destroy(shm); //just for this worker...
}

void nchan_stats_exit_master(ngx_cycle_t *cycle) {
  shm_destroy(shm);
}

static ngx_int_t initialize_shm(ngx_shm_zone_t *zone, void *data) {
  if(data) { //zone being passed after restart
    zone->data = data;
    shstats = zone->data;
  }
  else {
    shm_init(shm);
    
    if((shstats = shm_calloc(shm, sizeof(*shstats), "root shared data")) == NULL) {
      return NGX_ERROR;
    }
  }
  
  return NGX_OK;
}

void __nchan_stats_global_incr(off_t offset, int count) {
  if(!stats_enabled || !shstats) {
    return;
  }
  ngx_atomic_fetch_add((ngx_atomic_uint_t *)((char *)&shstats->global + offset), count);
}
void __nchan_stats_worker_incr(off_t offset, int count) {
  if(!stats_enabled || !shstats) {
    return;
  }
  ngx_atomic_fetch_add((ngx_atomic_uint_t *)((char *)&shstats->worker[ngx_process_slot] + offset), count);
}

ngx_int_t nchan_stats_get_all(nchan_stats_worker_t *worker, nchan_stats_global_t *global) {
  if(!stats_enabled) {
    if(worker) {
      ngx_memzero(worker, sizeof(*worker));
    }
    if(global) {
      ngx_memzero(global, sizeof(*global));
    }
    return NGX_OK;
  }
  
  ipc_t *ipc = nchan_memstore_get_ipc();
  if(!ipc) {
    return NGX_ERROR;
  }
  
  if(worker) {
    ngx_memzero(worker, sizeof(*worker));
    ngx_atomic_uint_t *worker_sum_stats_array = (ngx_atomic_uint_t *)worker;
    
    const ngx_int_t *slots;
    size_t worker_count = ipc_worker_slots(ipc, &slots);
    unsigned i;
    for(i=0; i<worker_count; i++) {
      ngx_atomic_uint_t *worker_stats_array = (ngx_atomic_uint_t *)&shstats->worker[slots[i]];
      unsigned j;
      for(j=0; j<sizeof(nchan_stats_worker_t)/sizeof(ngx_atomic_uint_t); j++) {
        worker_sum_stats_array[j]+=worker_stats_array[j];
      }
    }
  }
  
  if(global) {
    *global = shstats->global;
  }
  return NGX_OK;
}
