#include "nchan_list.h"

 /*
  * Regular boring unoptimized doubly linked list.
  * Allocates elements from heap.
  * Should not be used in tight loops.
  */

ngx_int_t nchan_list_init(nchan_list_t *list, size_t data_sz) {
  list->head = NULL;
  list->tail = NULL;
  list->data_sz = data_sz;
  list->n = 0;
  return NGX_OK;
}

void *nchan_list_append(nchan_list_t *list) {
  nchan_list_el_t  *el, *tail = list->tail;
  el = ngx_alloc(sizeof(*el) + list->data_sz, ngx_cycle->log);
  if(tail)
    tail->next = el;
  
  el->prev = tail;
  el->next = NULL;
  
  if(!list->head)
    list->head = el;
  
  list->tail = el;
  list->n++;
  return (void *)&el[1];
}


void *nchan_list_prepend(nchan_list_t *list) {
nchan_list_el_t  *el, *head = list->head;
  el = ngx_alloc(sizeof(*el) + list->data_sz, ngx_cycle->log);
  if(head)
    head->prev = el;
  
  el->prev = NULL;
  el->next = head;
  
  list->head = el;
  
  if(!list->tail)
    list->head = el;
  list->n++;
  return (void *)&el[1];
}

ngx_int_t nchan_list_remove(nchan_list_t *list, void *el_data) {
  
  nchan_list_el_t  *el = (nchan_list_el_t *)el_data - 1;
  
  if(el->prev) 
    el->prev->next = el->next;
  
  if(el->next)
    el->next->prev = el->prev;
  
  if(list->head == el)
    list->head = el->next;
  
  if(list->tail == el)
    list->tail = el->prev;
  list->n--;
  
  return NGX_OK;
}

ngx_int_t nchan_list_empty(nchan_list_t *list) {
  nchan_list_el_t  *el, *next;
  
  for(el = list->head; el != NULL; el = next) {
    next = el->next;
    ngx_free(el);
  }
  list->head = NULL;
  list->tail = NULL;
  list->n = 0;
  return NGX_OK;
}
