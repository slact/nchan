#undef uthash_malloc
#undef uthash_free
#define uthash_malloc(sz) ngx_alloc(sz, ngx_cycle->log)
#define uthash_free(ptr,sz) ngx_free(ptr)

#define STR(buf) (buf)->data, (buf)->len
#define BUF(buf) (buf)->pos, ((buf)->last - (buf)->pos)

#define NCHAN_DEFAULT_SUBSCRIBER_POOL_SIZE (5 * 1024)
#define NCHAN_DEFAULT_CHANHEAD_CLEANUP_INTERVAL 1000

void nchan_exit_notice_about_remaining_things(char *thing, char *where, ngx_int_t num);
