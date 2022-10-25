#include <nchan_module.h>
#include <stddef.h>
#include "nchan_timequeue.h"
#include <assert.h>

static void timequeue_page_reset(nchan_timequeue_page_t *page) {
  page->next = NULL;
  page->start = 0;
  page->end =  0;
}

static nchan_timequeue_page_t *timequeue_page_alloc(nchan_timequeue_t *pq) {
  nchan_timequeue_page_t *page = ngx_alloc(sizeof(*page) + sizeof(nchan_timequeue_time_t) * pq->items_per_page - 1, ngx_cycle->log);
  if(!page) {
    return NULL;
  }
  timequeue_page_reset(page);
  return page;
}

static nchan_timequeue_page_t *timequeue_page_enqueue(nchan_timequeue_t *pq) {
  nchan_timequeue_page_t *nextpage;
  if(pq->standby) {
    nextpage = pq->standby;
    pq->standby = nextpage->next;
  }
  else {
    nextpage = timequeue_page_alloc(pq);
    if(!nextpage) {
      return NULL;
    }
  }
  if(pq->last) {
    pq->last->next = nextpage;
    nextpage->next = NULL;
  }
  if(!pq->first) {
    pq->first = nextpage;
  }
  pq->last = nextpage;
  
  return nextpage;
}

int nchan_timequeue_init(nchan_timequeue_t *pq, size_t items_per_page, int anytag) {
  pq->items_per_page = items_per_page;
  
  pq->first = timequeue_page_alloc(pq);
  if(pq->first == NULL) {
    return 0;
  }
  pq->last = pq->first;
  pq->standby = NULL;
  pq->anytag = anytag;
  
  return 1;
}

static nchan_timequeue_page_t *timequeue_get_nonfull_page(nchan_timequeue_t *pq) {
  nchan_timequeue_page_t *page = pq->last;
  if(page->end >= pq->items_per_page) {
    return timequeue_page_enqueue(pq);
  }
  else {
    return page;
  }
}

int nchan_timequeue_queue(nchan_timequeue_t *pq, int tag) {
  nchan_timequeue_page_t *page = timequeue_get_nonfull_page(pq);
  if(!page) {
    nchan_log_error("timequque %p ENQUEUE tag %d: ERROR: can't get page", pq, tag);
    return 0;
  }
  nchan_timequeue_time_t    *data = &page->data[page->end];
  data->tag = tag;
  data->time_start = ngx_current_msec;
  page->end++;
  return 1;
}

int nchan_timequeue_dequeue(nchan_timequeue_t *pq, int expected_tag, nchan_timequeue_time_t *dequeued) {
  nchan_timequeue_page_t *page = pq->first;
  if(!page ||
    (page->start == page->end && page->start == 0)) {
    if(dequeued) {
      dequeued->time_start = 0;
      dequeued->tag = pq->anytag;
    }
    return 0;
  }
  nchan_timequeue_time_t    *data = &page->data[page->start];
  
  if(dequeued) {
    *dequeued = *data;
  }
  
  if(expected_tag != data->tag && expected_tag != pq->anytag) {
    return 0;
  }
  
  page->start++;
  
  if(page->start >= page->end) {
    if(pq->last == page) {
      assert(page->next == NULL);
      assert(pq->first == page);
      timequeue_page_reset(page);
    }
    else {
      assert(page->next != NULL);
      assert(pq->last != page);
      pq->first = page->next;
      timequeue_page_reset(page);
      //add to standby list
      page->next = pq->standby;
      pq->standby = page;
    }
  }
  
  return 1;
}

void nchan_timequeue_destroy(nchan_timequeue_t *pq) {
  nchan_timequeue_page_t *cur, *next;
  for(cur = pq->first; cur != NULL; cur = next) {
    next = cur->next;
    ngx_free(cur);
  }
  for(cur = pq->standby; cur != NULL; cur = next) {
    next = cur->next;
    ngx_free(cur);
  }
  pq->first = NULL;
  pq->last = NULL;
  pq->standby = NULL;
}
