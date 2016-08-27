#ifndef NCHAN_LIST_H
#define NCHAN_LIST_H

#define NCHAN_LIST_DEBUG 1

#include <nginx.h>
#include <ngx_http.h>

typedef struct nchan_list_el_s nchan_list_el_t;

struct nchan_list_el_s {
  nchan_list_el_t  *prev;
  nchan_list_el_t  *next;
};

typedef struct {
#if NCHAN_LIST_DEBUG
  char            *name;
#endif
  nchan_list_el_t *head;
  nchan_list_el_t *tail;
  ngx_uint_t       n;
  size_t           data_sz;
  ngx_pool_t      *pool;
  size_t           pool_sz;
} nchan_list_t;

ngx_int_t nchan_list_init(nchan_list_t *list, size_t data_sz, char *name);
ngx_int_t nchan_list_pool_init(nchan_list_t *list, size_t data_sz, size_t pool_sz, char *name);
ngx_pool_t *nchan_list_get_pool(nchan_list_t *list);

void *nchan_list_append(nchan_list_t *list);
void *nchan_list_prepend(nchan_list_t *list);

void *nchan_list_prepend_sized(nchan_list_t *list, size_t sz);
void *nchan_list_append_sized(nchan_list_t *list, size_t sz);

ngx_int_t nchan_list_remove(nchan_list_t *list, void *el_data);
ngx_int_t nchan_list_empty(nchan_list_t *list);
ngx_int_t nchan_list_traverse_and_empty(nchan_list_t *list, void (*)(void *data, void *pd), void *pd);

#define nchan_list_el_from_data(el_data) \
  ((nchan_list_el_t *)el_data - 1)
#define nchan_list_data_from_el(el) \
  ((void *)&el[1])

#endif //NCHAN_LIST_H
