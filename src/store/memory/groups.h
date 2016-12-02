 #ifndef MEMSTORE_GROUPS_HEADER
#define MEMSTORE_GROUPS_HEADER
#include <nchan_module.h>
#include <util/nchan_rbtree.h>

typedef struct {
  rbtree_seed_t           tree;
} memstore_groups_t;

ngx_int_t memstore_groups_init(memstore_groups_t *);
ngx_int_t memstore_groups_shutdown(memstore_groups_t *);


ngx_int_t memstore_group_find(memstore_groups_t *gp, ngx_str_t *name, void (*cb)(nchan_group_t *, void *), void *pd);

#endif //MEMSTORE_GROUPS_HEADER
