#include "memrchr.h"
void *memrchr(const void *s, int c, size_t n) {
  const char *start = s;
  const char *end = &start[n]; //one too much on purpose
  while(--end >= start) { //and this is why
    if(c == *end) {
      return (void *)end;
    }
  }
  return NULL;
}
