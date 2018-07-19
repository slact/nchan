#ifndef NCHAN_SLIST_H
#define NCHAN_SLIST_H

#define NCHAN_SLIST_DEBUG 0

#include <nginx.h>
#include <ngx_http.h>

typedef struct {
  off_t           prev;
  off_t           next;
} nchan_slist_offsets_t;

typedef struct {
  void                 *head;
  void                 *tail;
  ngx_uint_t            n;
  nchan_slist_offsets_t offset;
} nchan_slist_t;



#define nchan_slist_init(slist, data_type, prev_field, next_field) \
  __nchan_slist_init(slist, offsetof(data_type, prev_field), offsetof(data_type, next_field))
ngx_int_t __nchan_slist_init(nchan_slist_t *list, off_t offset_prev, off_t offset_next);
void *nchan_slist_first(nchan_slist_t *list);
void *nchan_slist_last(nchan_slist_t *list);
void *nchan_slist_next(nchan_slist_t *list, void *el);
void *nchan_slist_prev(nchan_slist_t *list, void *el);
ngx_int_t nchan_slist_append(nchan_slist_t *list, void *el);
ngx_int_t nchan_slist_prepend(nchan_slist_t *list, void *el);
ngx_int_t nchan_slist_remove(nchan_slist_t *list, void *el);

ngx_int_t nchan_slist_reset(nchan_slist_t *list);
ngx_int_t nchan_slist_transfer(nchan_slist_t *dst, nchan_slist_t *src);
void *nchan_slist_pop(nchan_slist_t *list);
void *nchan_slist_shift(nchan_slist_t *list);

int nchan_slist_is_empty(nchan_slist_t *list);

#endif /*NCHAN_SLIST_H*/
