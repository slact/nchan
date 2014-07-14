void ngx_rwlock_init(ngx_rwlock_t *lock);
void ngx_rwlock_release_write(ngx_rwlock_t *lock);
void ngx_rwlock_reserve_write(ngx_rwlock_t *lock);
void ngx_rwlock_release_read(ngx_rwlock_t *lock);
void ngx_rwlock_reserve_read(ngx_rwlock_t *lock);