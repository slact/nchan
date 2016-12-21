#include "nchan_debug.h"
#include <assert.h>

char *nchan_msg_status_to_cstr(nchan_msg_status_t status) {
  switch(status) {
    case MSG_CHANNEL_NOTREADY:
      return "MSG_CHANNEL_NOTREADY";
    case  MSG_NORESPONSE:
      return "MSG_NORESPONSE";
    case MSG_INVALID:
      return "MSG_INVALID";
    case MSG_PENDING:
      return "MSG_PENDING";
    case MSG_NOTFOUND:
      return "MSG_NOTFOUND";
    case MSG_FOUND:
      return "MSG_FOUND";
    case MSG_EXPECTED:
      return "MSG_EXPECTED";
    case MSG_EXPIRED:
      return "MSG_EXPIRED";
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
      nchan_log_error("  LEFTOVER: %V sub (req. %p (pool %p) cf %p) lbl %s", cur->name, cur->request, cur->request ? cur->request->pool : NULL, cur->cf, cur->lbl ? cur->lbl : (u_char *)"nolbl");
    }
    raise(SIGABRT);
  }
}
#endif
