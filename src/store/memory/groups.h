 #ifndef MEMSTORE_GROUPS_HEADER
#define MEMSTORE_GROUPS_HEADER
#include <nchan_module.h>
#include <util/nchan_rbtree.h>

typedef struct {
  rbtree_seed_t           tree;
} memstore_groups_t;

ngx_int_t memstore_groups_init(memstore_groups_t *);
ngx_int_t memstore_groups_shutdown(memstore_groups_t *);


ngx_int_t memstore_group_find(memstore_groups_t *gp, ngx_str_t *name, callback_pt cb, void *pd);
nchan_group_t *memstore_group_owner_find(memstore_groups_t *gp, ngx_str_t *name);
ngx_int_t memstore_group_receive(memstore_groups_t *gp, nchan_group_t *shm_group);

#endif //MEMSTORE_GROUPS_HEADER
