#include <nchan_module.h>
#include "ngx_rwlock.h"

#define NGX_RWLOCK_SPIN         2048
#define NGX_RWLOCK_WRITE        -1

#define DISABLE_RWLOCK 0  //just use a regular mutex for everything
#define DEBUG_NGX_RWLOCK 1

void ngx_rwlock_init(ngx_rwlock_t *lock) {
  lock->mutex=1;
  lock->lock=0;
  lock->write_pid=0;
  lock->mutex=0;
}

static void rwl_lock_mutex(ngx_rwlock_t *lock) {
  #if (NGX_HAVE_ATOMIC_OPS)
  ngx_atomic_t  *mutex = &lock->mutex;
  ngx_uint_t i, n;
  for(;;) {
    if(*mutex == 0 && ngx_atomic_cmp_set(mutex, 0, ngx_pid)) {
      return;
    }
    if(ngx_ncpu > 1) {
      for(n = 1; n < NGX_RWLOCK_SPIN; n <<= 1) {
        for(i = 0; i < n; i++) {
          ngx_cpu_pause();
        }
        #if (DEBUG_NGX_RWLOCK)
        ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "rwlock %p mutex wait", lock);
        #endif
        if(*mutex == 0 && ngx_atomic_cmp_set(mutex, 0, ngx_pid)) {
          return;
        }
      }
    }
    ngx_sched_yield();
  }
  #else
  #if (NGX_THREADS)
  #error ngx_spinlock() or ngx_atomic_cmp_set() are not defined !
  #endif
  #endif
}
static void rwl_unlock_mutex(ngx_rwlock_t *lock) {
  #if (NGX_HAVE_ATOMIC_OPS)
  ngx_atomic_cmp_set(&lock->mutex, ngx_pid, 0);
  #else
  #if (NGX_THREADS)
  #error ngx_spinlock() or ngx_atomic_cmp_set() are not defined !
  #endif
  #endif
}

#define NGX_RWLOCK_MUTEX_COND(lock, cond, stmt)   \
if(cond) {                                          \
  rwl_lock_mutex(lock);                             \
  if(cond) {                                        \
    stmt;                                           \
    rwl_unlock_mutex(lock);                         \
    return;                                         \
  }                                                 \
  rwl_unlock_mutex(lock);                           \
}

void ngx_rwlock_reserve_read(ngx_rwlock_t *lock)
{
  #if (NGX_HAVE_ATOMIC_OPS)
  ngx_uint_t i, n;
  
  #if (DISABLE_RWLOCK == 1)
  rwl_lock_mutex(lock);
  return;
  #endif
  
  for(;;) {
    NGX_RWLOCK_MUTEX_COND(lock, (lock->lock != NGX_RWLOCK_WRITE), lock->lock++)
    #if (DEBUG_NGX_RWLOCK == 1)
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "rwlock %p reserve read read (%i)", lock, lock->lock);
    #endif
    if(ngx_ncpu > 1) {
      for(n = 1; n < NGX_RWLOCK_SPIN; n <<= 1) {
        for(i = 0; i < n; i++) {
          ngx_cpu_pause();
        }
        #if (DEBUG_NGX_RWLOCK == 1)
        ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "rwlock %p read lock wait", lock);
        #endif
        NGX_RWLOCK_MUTEX_COND(lock, (lock->lock != NGX_RWLOCK_WRITE), lock->lock++)
      }
    }
    ngx_sched_yield();
  }
  #else
  #if (NGX_THREADS)
  #error ngx_spinlock() or ngx_atomic_cmp_set() are not defined !
  #endif
  #endif
}
void ngx_rwlock_release_read(ngx_rwlock_t *lock) {
  #if (DISABLE_RWLOCK == 1)
  rwl_unlock_mutex(lock);
  #else
  NGX_RWLOCK_MUTEX_COND(lock, lock->lock != NGX_RWLOCK_WRITE && lock->lock != 0, lock->lock--)
  #endif
}

static int ngx_rwlock_write_check(ngx_rwlock_t *lock) {
  if(lock->lock==0) {
    rwl_lock_mutex(lock);
    if(lock->lock==0) {
      lock->lock=NGX_RWLOCK_WRITE;
      lock->write_pid=ngx_pid;
      rwl_unlock_mutex(lock);
      return 1;
    }
    rwl_unlock_mutex(lock);
  }
  return 0;
}

void ngx_rwlock_reserve_write(ngx_rwlock_t * lock) {
  #if (NGX_HAVE_ATOMIC_OPS)
  ngx_uint_t i, n;
  
  #if (DISABLE_RWLOCK == 1)
  rwl_lock_mutex(lock);
  return;
  #endif
  
  for(;;) {
    if(ngx_rwlock_write_check(lock))
      return;
    if(ngx_ncpu > 1) {
      for(n = 1; n < NGX_RWLOCK_SPIN; n <<= 1) {
        for(i = 0; i < n; i++) {
          ngx_cpu_pause();
        }
        #if (DEBUG_NGX_RWLOCK == 1)
        ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "rwlock %p write lock wait (reserved by %ui)", lock, lock->write_pid);
        #endif
        if(ngx_rwlock_write_check(lock))
          return;
      }
    }
    ngx_sched_yield();
  }
  #else
  #if (NGX_THREADS)
  #error ngx_spinlock() or ngx_atomic_cmp_set() are not defined !
  #endif
  #endif
}

void ngx_rwlock_release_write(ngx_rwlock_t *lock) {
  #if (DISABLE_RWLOCK == 1)
  rwl_unlock_mutex(lock);
  return;
  #endif
  
  if(lock->lock == NGX_RWLOCK_WRITE) {
    rwl_lock_mutex(lock);
    if(lock->lock == NGX_RWLOCK_WRITE) {
      lock->lock=0;
      if(lock->write_pid != ngx_pid) {
        ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "rwlock %p releasing someone else's (pid %ui) write lock.", lock, lock->write_pid);
      }
      lock->write_pid = 0;
      rwl_unlock_mutex(lock);
      return;
    }
    rwl_unlock_mutex(lock);
  }
  else {
    ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "rwlock %p tried to release nonexistent write lock, lock=%i.", lock, lock->lock);
  }
}
