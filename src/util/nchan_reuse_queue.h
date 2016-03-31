#ifndef NCHAN_REUSE_QUEUE_H
#define NCHAN_REUSE_QUEUE_H

typedef struct {
  int                        size;
  int                        next_ptr_offset;
  int                        prev_ptr_offset;
  void                      *first;
  void                      *last;
  void                      *reserve;
  void                      *(*alloc)(void *privdata);
  ngx_int_t                  (*free)(void *privdata, void *thing);
  void                      *pd;
} nchan_reuse_queue_t;

ngx_int_t nchan_reuse_queue_init(nchan_reuse_queue_t *rq, int prev, int next, void *(*alloc)(void *), ngx_int_t (*free)(void *, void*), void *privdata);
ngx_int_t nchan_reuse_queue_shutdown(nchan_reuse_queue_t *rq);
ngx_int_t nchan_reuse_queue_flush(nchan_reuse_queue_t *rq);
void nchan_reuse_queue_each(nchan_reuse_queue_t *rq, void (*calback)(void *));
void *nchan_reuse_queue_first(nchan_reuse_queue_t *rq);
void *nchan_reuse_queue_push(nchan_reuse_queue_t *rq);
ngx_int_t nchan_reuse_queue_pop(nchan_reuse_queue_t *rq);

#endif /*NCHAN_REUSE_QUEUE_H*/
