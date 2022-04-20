#include <nchan_module.h>
#include "store_common.h"

void nchan_exit_notice_about_remaining_things(char *thing, char *where, ngx_int_t num) {
  if(num > 0) {
    nchan_log_notice("%i %s%s remain%s %sat exit", num, thing, num == 1 ? "" : "s", num == 1 ? "s" : "", where == NULL ? "" : where);
  }
}
