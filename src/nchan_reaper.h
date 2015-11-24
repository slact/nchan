#ifndef NCHAN_REAPER_H
#define NCHAN_REAPER_H


typedef struct {
  char           *name;
  int             count;
  int             next_ptr_offset;
  int             prev_ptr_offset;
  void           *last;
  void           *first;
  ngx_event_t     timer;
  int             tick_usec;
  unsigned        rotate:1;
  uint16_t        max_notready;
  ngx_int_t       (*ready)(void *); //ready to be reaped?
  void            (*reap)(void *); //reap it
} nchan_reaper_t;

ngx_int_t nchan_reaper_start(nchan_reaper_t *rp, char *name, int prev, int next, ngx_int_t (*ready)(void *), void (*reap)(void *), int tick_sec);

ngx_int_t nchan_reaper_stop(nchan_reaper_t *rp);

ngx_int_t nchan_reaper_add(nchan_reaper_t *rp, void *thing);
ngx_int_t nchan_reaper_withdraw(nchan_reaper_t *rp, void *thing);

#endif /*NCHAN_REAPER_H*/