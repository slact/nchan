#ifndef NCHAN_LIST_H
#define NCHAN_LIST_H

#include <nginx.h>
#include <ngx_http.h>

typedef struct nchan_list_el_s nchan_list_el_t;

struct nchan_list_el_s {
  nchan_list_el_t  *prev;
  nchan_list_el_t  *next;
};

typedef struct {
  nchan_list_el_t *head;
  nchan_list_el_t *tail;
  size_t           data_sz;
} nchan_list_t;


ngx_int_t nchan_list_init(nchan_list_t *list, size_t data_sz);
void *nchan_list_append(nchan_list_t *list);
void *nchan_list_prepend(nchan_list_t *list);
ngx_int_t nchan_list_remove(nchan_list_t *list, void *el_data);
ngx_int_t nchan_list_empty(nchan_list_t *list);

#endif //NCHAN_LIST_H
