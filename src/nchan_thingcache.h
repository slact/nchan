void *nchan_thingcache_init(char *name, void *(*create)(ngx_str_t *), ngx_int_t(*destroy)(ngx_str_t *, void *), ngx_uint_t ttl);
ngx_int_t nchan_thingcache_shutdown(void *tcv);
void *nchan_thingcache_get(void *tcv, ngx_str_t *id);
void *nchan_thingcache_find(void *tcv, ngx_str_t *id);