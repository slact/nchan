#ifndef NCHAN_REAPER_H
#define NCHAN_REAPER_H

typedef enum {RESCAN, ROTATE, KEEP_PLACE} nchan_reaper_strategy_t;

typedef struct {
  char                      *name;
  ngx_int_t                  count;
  int                        next_ptr_offset;
  int                        prev_ptr_offset;
  void                      *last;
  void                      *first;
  ngx_int_t                  (*ready)(void *, uint8_t force); //ready to be reaped?
  void                       (*reap)(void *); //reap it
  ngx_event_t                timer;
  int                        tick_usec;
  nchan_reaper_strategy_t    strategy;
  float                      max_notready_ratio;
  void                      *position;

} nchan_reaper_t;

ngx_int_t nchan_reaper_start(nchan_reaper_t *rp, char *name, int prev, int next, ngx_int_t (*ready)(void *, uint8_t force), void (*reap)(void *), int tick_sec);

ngx_int_t nchan_reaper_flush(nchan_reaper_t *rp);
ngx_int_t nchan_reaper_stop(nchan_reaper_t *rp);
void nchan_reaper_each(nchan_reaper_t *rp, void (*cb)(void *thing, void *pd), void *pd);


ngx_int_t nchan_reaper_add(nchan_reaper_t *rp, void *thing);
ngx_int_t nchan_reaper_withdraw(nchan_reaper_t *rp, void *thing);

#endif /*NCHAN_REAPER_H*/
