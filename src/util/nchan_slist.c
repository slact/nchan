#include "nchan_slist.h"
#include <assert.h>

 /*
  * Boring non-allocating doubly-linked "static" list.
  *uses preset offsets for the prev and next fields on pre-allocated data
  */
 
ngx_int_t __nchan_slist_init(nchan_slist_t *list, off_t offset_prev, off_t offset_next) {
  list->offset.prev = offset_prev;
  list->offset.next = offset_next;
  return nchan_slist_reset(list);
}

ngx_int_t nchan_slist_reset(nchan_slist_t *list) {
  list->head = NULL;
  list->tail = NULL;
  list->n = 0;
  return NGX_OK;
}

void *nchan_slist_first(nchan_slist_t *list) {
  return list->head;
}
void *nchan_slist_last(nchan_slist_t *list) {
  return list->tail;
}
static void **nchan_slist_next_ptr(nchan_slist_t *list, void *el) {
  return (void **)&(((char *)el)[list->offset.next]);
}
static void **nchan_slist_prev_ptr(nchan_slist_t *list, void *el) {
  return (void **)&(((char *)el)[list->offset.prev]);
}

void *nchan_slist_next(nchan_slist_t *list, void *el) {
  void **ptr = nchan_slist_next_ptr(list, el);
  return *ptr;
}
void *nchan_slist_prev(nchan_slist_t *list, void *el) {
  void **ptr = nchan_slist_prev_ptr(list, el);
  return *ptr;
}

ngx_int_t nchan_slist_append(nchan_slist_t *list, void *el) {
  void **el_prev_ptr = nchan_slist_prev_ptr(list, el);
  void **el_next_ptr = nchan_slist_next_ptr(list, el);
  if(list->head == NULL) {
    list->head = el;
  }
  if(list->tail == NULL) {
    *el_prev_ptr = NULL;
  }
  else {
    void **tail_next = nchan_slist_next_ptr(list, list->tail);
    *el_prev_ptr = list->tail;
    *tail_next = el;
  }
  list->tail = el;
  *el_next_ptr = NULL;
  list->n++;
  return NGX_OK;
}
ngx_int_t nchan_slist_prepend(nchan_slist_t *list, void *el) {
  void **el_prev_ptr = nchan_slist_prev_ptr(list, el);
  void **el_next_ptr = nchan_slist_next_ptr(list, el);
  if(list->tail == NULL) {
    list->tail = el;
  }
  if(list->head == NULL) {
    *el_next_ptr = NULL;
  }
  else {
    void **head_prev_ptr = nchan_slist_prev_ptr(list, list->head);
    *el_next_ptr = list->head;
    *head_prev_ptr = el;
  }
  list->head = el;
  *el_prev_ptr = NULL;
  list->n++;
  return NGX_OK;
}
ngx_int_t nchan_slist_remove(nchan_slist_t *list, void *el) {
  void **el_prev_ptr = nchan_slist_prev_ptr(list, el);
  void **el_next_ptr = nchan_slist_next_ptr(list, el);
  void *el_prev = *el_prev_ptr, *el_next = *el_next_ptr;
  if(list->head == el) {
    list->head = el_next;
  }
  if(list->tail == el) {
    list->tail = el_prev;
  }
  if(el_prev) {
    void **el_prev_next_ptr = nchan_slist_next_ptr(list, el_prev);
    *el_prev_next_ptr = el_next;
  }
  if(el_next) {
    void **el_next_prev_ptr = nchan_slist_prev_ptr(list, el_next);
    *el_next_prev_ptr = el_prev;
  }
  list->n--;
  *el_prev_ptr = NULL;
  *el_next_ptr = NULL;
  return NGX_OK;
}

ngx_int_t nchan_slist_transfer(nchan_slist_t *dst, nchan_slist_t *src) {
  assert(dst->offset.prev == src->offset.prev); //same-ish slists
  assert(dst->offset.next == src->offset.next); //same-ish slists
  void **src_head_prev_ptr = nchan_slist_prev_ptr(src, src->head);
  void **dst_tail_next_ptr = nchan_slist_next_ptr(dst, src->tail);
  
  if(src->n == 0) {
    assert(src->head == NULL);
    assert(src->tail == NULL);
    return NGX_OK; //nothing to do
  }
  
  *src_head_prev_ptr = dst->tail;
  if(dst->tail) {
    *dst_tail_next_ptr = src->head;
  }
  dst->tail = src->tail;
  
  if(dst->head == NULL) {
    dst->head = src->head;
  }
  dst->tail = src->tail;
  dst->n += src->n;
  
  src->n = 0;
  src->head = NULL;
  src->tail = NULL;
  
  return NGX_OK;
}

void *nchan_slist_shift(nchan_slist_t *list) {
  void *head = list->head;
  if(head) {
    nchan_slist_remove(list, head);
  }
  return head;
}

void *nchan_slist_pop(nchan_slist_t *list) {
  void *tail = list->tail;
  if(tail) {
    nchan_slist_remove(list, tail);
  }
  return tail;
}

int nchan_slist_is_empty(nchan_slist_t *list) {
  if(list->n == 0) {
    assert(list->head == NULL);
    assert(list->tail == NULL);
    return 1;
  }
  else {
    assert(list->head != NULL);
    assert(list->tail != NULL);
    return 0;
  }
}
