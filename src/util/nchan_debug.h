#ifndef NCHAN_DEBUG_H
#define NCHAN_DEBUG_H
#include <nchan_module.h>

char *nchan_msg_status_to_cstr(nchan_msg_status_t status);

#if NCHAN_SUBSCRIBER_LEAK_DEBUG
void subscriber_debug_add(subscriber_t *);
void subscriber_debug_remove(subscriber_t *);
void subscriber_debug_assert_isempty(void);
#endif

#endif //NCHAN_DEBUG_H
