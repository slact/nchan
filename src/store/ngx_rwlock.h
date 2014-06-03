typedef struct {
  ngx_atomic_int_t  lock;
  ngx_atomic_t      mutex;
  ngx_int_t         write_pid;
} ngx_rwlock_t;


void ngx_rwlock_release_write(ngx_rwlock_t *lock);
void ngx_rwlock_reserve_write(ngx_rwlock_t *lock);
void ngx_rwlock_release_read(ngx_rwlock_t *lock);
void ngx_rwlock_reserve_read(ngx_rwlock_t *lock);