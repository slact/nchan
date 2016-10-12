#include "nchan_list.h"

 /*
  * Regular boring unoptimized doubly linked list.
  * Allocates elements from heap.
  * Should not be used in tight loops.
  */

ngx_int_t nchan_list_init(nchan_list_t *list, size_t data_sz, char *name) {
  return nchan_list_pool_init(list, data_sz, 0, name);
}

ngx_int_t nchan_list_pool_init(nchan_list_t *list, size_t data_sz, size_t pool_sz, char *name) {
#if NCHAN_LIST_DEBUG
  list->name = name;
#endif
  list->head = NULL;
  list->tail = NULL;
  list->data_sz = data_sz;
  list->n = 0;
  list->pool = NULL;
  list->pool_sz = pool_sz;
  return NGX_OK;
}

ngx_pool_t *nchan_list_get_pool(nchan_list_t *list) {
  if(!list->pool && list->pool_sz) {
    list->pool = ngx_create_pool(list->pool_sz, ngx_cycle->log);
  }
  return list->pool;
}

void *nchan_list_append(nchan_list_t *list) {
 return nchan_list_append_sized(list, list->data_sz);
}

void *nchan_list_append_sized(nchan_list_t *list, size_t sz) {
  nchan_list_el_t  *el, *tail = list->tail;
  if(list->pool_sz > 0) {
    el = ngx_palloc(nchan_list_get_pool(list), sizeof(*el) + sz);
  }
  else {
    el = ngx_alloc(sizeof(*el) + sz, ngx_cycle->log);
  }
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
  return nchan_list_prepend_sized(list, list->data_sz);
}

void *nchan_list_prepend_sized(nchan_list_t *list, size_t sz) {
nchan_list_el_t  *el, *head = list->head;
  el = ngx_alloc(sizeof(*el) + sz, ngx_cycle->log);
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
  
  if(list->pool)
    ngx_pfree(list->pool, el);
  else
    ngx_free(el);
  
  return NGX_OK;
}

ngx_int_t nchan_list_empty(nchan_list_t *list) {
  nchan_list_el_t  *el, *next;
  if(list->pool) {
    ngx_destroy_pool(list->pool);
    list->pool = NULL;
  }
  else {
    for(el = list->head; el != NULL; el = next) {
      next = el->next;
      ngx_free(el);
    }
  }
  list->head = NULL;
  list->tail = NULL;
  list->n = 0;
  return NGX_OK;
}


ngx_int_t nchan_list_traverse_and_empty(nchan_list_t *list, void (*cb)(void *data, void *pd), void *pd) {
  nchan_list_el_t  *head = list->head, *el, *next;
  ngx_pool_t       *pool = list->pool;
  
  list->head = NULL;
  list->tail = NULL;
  list->n = 0;
  list->pool = NULL;
  
  for(el = head; el != NULL; el = next) {
    cb(nchan_list_data_from_el(el), pd);
    next = el->next;
    if(!pool) {
      ngx_free(el);
    }
  }
  
  if(pool)
    ngx_destroy_pool(pool);
  
  return NGX_OK;
}
