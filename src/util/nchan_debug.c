#include "nchan_debug.h"
#include <assert.h>

#define STRINGIFY_ENUM(val) \
  case val:                 \
    return #val;

char *nchan_msg_status_to_cstr(nchan_msg_status_t status) {
  switch(status) {
    STRINGIFY_ENUM(MSG_CHANNEL_NOTREADY)
    STRINGIFY_ENUM(MSG_INVALID)
    STRINGIFY_ENUM(MSG_PENDING)
    STRINGIFY_ENUM(MSG_NOTFOUND)
    STRINGIFY_ENUM(MSG_FOUND)
    STRINGIFY_ENUM(MSG_EXPECTED)
    STRINGIFY_ENUM(MSG_EXPIRED)
    STRINGIFY_ENUM(MSG_ERROR)
  }
  return "???";
}

#if NCHAN_SUBSCRIBER_LEAK_DEBUG

subscriber_t *subdebug_head = NULL;

void subscriber_debug_add(subscriber_t *sub) {
  if(subdebug_head == NULL) {
    sub->dbg_next = NULL;
    sub->dbg_prev = NULL;
  }
  else {
    sub->dbg_next = subdebug_head;
    sub->dbg_prev = NULL;
    assert(subdebug_head->dbg_prev == NULL);
    subdebug_head->dbg_prev = sub;
  }
  if(sub->request) {
    sub->lbl = ngx_calloc(sub->request->uri.len+1, ngx_cycle->log);
    ngx_memcpy(sub->lbl, sub->request->uri.data, sub->request->uri.len);
  }
  else {
    sub->lbl = NULL;
  }
  
  subdebug_head = sub;
}
void subscriber_debug_remove(subscriber_t *sub) {
  subscriber_t *prev, *next;
  prev = sub->dbg_prev;
  next = sub->dbg_next;
  if(subdebug_head == sub) {
    assert(sub->dbg_prev == NULL);
    if(next) {
      next->dbg_prev = NULL;
    }
    subdebug_head = next;
  }
  else {
    if(prev) {
      prev->dbg_next = next;
    }
    if(next) {
      next->dbg_prev = prev;
    }
  }
  
  sub->dbg_next = NULL;
  sub->dbg_prev = NULL;
  ngx_free(sub->lbl);
  sub->lbl = NULL;
}
void subscriber_debug_assert_isempty(void) {
  if(subdebug_head != NULL) {
    subscriber_t *cur;
    for(cur = subdebug_head; cur != NULL; cur = cur->dbg_next) {
      nchan_log_error("  LEFTOVER: %V sub (req. %p (pool %p) cf %p rsv: %d) lbl %s", cur->name, cur->request, cur->request ? cur->request->pool : NULL, cur->cf, cur->reserved, cur->lbl ? cur->lbl : (u_char *)"nolbl");
    }
    raise(SIGABRT);
  }
}
#endif
