#include <ngx_http_push_module.h>
#include "ngx_rwlock.h"

#define NGX_RWLOCK_SPIN         2048
#define NGX_RWLOCK_WRITE        -1

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
  }                                                 \
  rwl_unlock_mutex(lock);                           \
}                                                   \

void ngx_rwlock_reserve_read(ngx_rwlock_t *lock)
{
  #if (NGX_HAVE_ATOMIC_OPS)
  ngx_uint_t i, n;
  for(;;) {
    NGX_RWLOCK_MUTEX_COND(lock, (lock->lock != NGX_RWLOCK_WRITE), lock->lock++)
    if(ngx_ncpu > 1) {
      for(n = 1; n < NGX_RWLOCK_SPIN; n <<= 1) {
        for(i = 0; i < n; i++) {
          ngx_cpu_pause();
        }
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
  NGX_RWLOCK_MUTEX_COND(lock, lock->lock != NGX_RWLOCK_WRITE && lock->lock != 0, lock->lock--)
}


void ngx_rwlock_reserve_write(ngx_rwlock_t * lock)
{
  #if (NGX_HAVE_ATOMIC_OPS)
  ngx_uint_t i, n;
  for(;;) {
    NGX_RWLOCK_MUTEX_COND(lock, (lock->lock!=NGX_RWLOCK_WRITE), lock->lock=NGX_RWLOCK_WRITE)
    if(ngx_ncpu > 1) {
      for(n = 1; n < NGX_RWLOCK_SPIN; n <<= 1) {
        for(i = 0; i < n; i++) {
          ngx_cpu_pause();
        }
        NGX_RWLOCK_MUTEX_COND(lock, (lock->lock != NGX_RWLOCK_WRITE), lock->lock=NGX_RWLOCK_WRITE)
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
  NGX_RWLOCK_MUTEX_COND(lock, lock->lock == NGX_RWLOCK_WRITE, lock->lock=0)
}