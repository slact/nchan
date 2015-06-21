#include <ngx_http_push_module.h>

#define DBG(...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, __VA_ARGS__)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)

typedef struct recycloc_trash_s recycloc_trash_t;
typedef struct recycloc_s recycloc_t;

struct recycloc_trash_s {
  size_t            size;
  void             *data;
  struct recycloc_page_t  *prev;
  struct recycloc_page_t  *next;
};

struct recycloc_s {
  
  
}

recycloc_t *recycloc_create(const char *name, size_t data_size, size_t page_size) {
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
  rec->data_size = data_size
  
}

recycloc_destroy