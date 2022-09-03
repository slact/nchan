#ifndef NCHAN_TIMEQUEUE_H
#define NCHAN_TIMEQUEUE_H

#include <stddef.h>
#include <nginx.h>

typedef struct {
  ngx_msec_t  time_start;
  int         tag;
} nchan_timequeue_time_t;

struct nchan_timequeue_page_s {
  struct nchan_timequeue_page_s *next;
  uint16_t                       start;
  uint16_t                       end;
  nchan_timequeue_time_t         data[1];
};

typedef struct nchan_timequeue_page_s nchan_timequeue_page_t;

typedef struct {
  size_t items_per_page;
  
  nchan_timequeue_page_t *first;
  nchan_timequeue_page_t *last;
  nchan_timequeue_page_t *standby;
  
} nchan_timequeue_t;



int nchan_timequeue_init(nchan_timequeue_t *pq, size_t items_per_page);
int nchan_timequeue_queue(nchan_timequeue_t *pq, int tag);
int nchan_timequeue_dequeue(nchan_timequeue_t *pq, int tag, ngx_msec_t *start_time);
void nchan_timequeue_destroy(nchan_timequeue_t *pq);

#endif //NCHAN_TIMEQUEUE_H
