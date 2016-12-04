 #ifndef MEMSTORE_GROUPS_HEADER
#define MEMSTORE_GROUPS_HEADER
#include <nchan_module.h>
#include <util/nchan_rbtree.h>

typedef struct {
  rbtree_seed_t           tree;
} memstore_groups_t;

typedef struct group_tree_node_s group_tree_node_t ;

#include "store-private.h"

typedef struct group_callback_s group_callback_t ;
struct group_callback_s {
  callback_pt         cb;
  void               *pd;
  group_callback_t   *next;
  unsigned            want_node:1;
};

struct group_tree_node_s {
  ngx_str_t                 name; //local memory copy of group name
  nchan_group_t            *group;
  group_callback_t         *when_ready_head;
  group_callback_t         *when_ready_tail;
  memstore_channel_head_t  *owned_chanhead_head;
};

ngx_int_t memstore_groups_init(memstore_groups_t *);
ngx_int_t memstore_groups_shutdown(memstore_groups_t *);


ngx_int_t memstore_group_find(memstore_groups_t *gp, ngx_str_t *name, callback_pt cb, void *pd);
ngx_int_t memstore_groupnode_find(memstore_groups_t *gp, ngx_str_t *name, callback_pt cb, void *pd);
group_tree_node_t *memstore_groupnode_find_now(memstore_groups_t *gp, ngx_str_t *name);
nchan_group_t *memstore_group_owner_find(memstore_groups_t *gp, ngx_str_t *name);
ngx_int_t memstore_group_receive(memstore_groups_t *gp, nchan_group_t *shm_group);

#endif //MEMSTORE_GROUPS_HEADER
