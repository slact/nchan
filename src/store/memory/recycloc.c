#include <nchan_module.h>

#define DBG(...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, __VA_ARGS__)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)

typedef struct recycloc_trash_s recycloc_trash_t;
typedef struct recycloc_s recycloc_t;

struct recycloc_llist_s {
  void             *data;
  struct recycloc_page_t  *prev;
  struct recycloc_page_t  *next;
};

struct recycloc_s {
  const char       *name;
  recycloc_llist_s *head;
  recycloc_llist_s *tail;
  recycloc_llist_s *prealloc_start;
  recycloc_llist_s *prealloc_end;
}

recycloc_t *recycloc_init(const char *name, recycloc_t *ptr, size_t data_size, size_t prealloc_count) {
  recycloc_t rec = NULL;
  if(page_size > ngx_pagesize) {
    ERR("can't create recycloc: page_size %i > ngx_pagesize %i", page_size, ngx_pagesize);
    return NULL;
  }
  if(!(rec = ngx_alloc(sizeof(recycloc_t) + page_size))){
    ERR("can't alloc enough (%i) memory for recycloc", sizeof(recycloc_t) + page_size);
    return NULL;
  }
  rec->name=name;
  rec->data_size = data_size;
  return rec;
  
  
}

void * recycloc_alloc(recycloc_t *self);



recycloc_t *recycloc_destroy(recycloc_t *self) {
  
  
}
