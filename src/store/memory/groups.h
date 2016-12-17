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
  char               *label;
};

struct group_tree_node_s {
  ngx_str_t                 name; //local memory copy of group name
  nchan_group_t            *group;
  group_callback_t         *when_ready_head;
  group_callback_t         *when_ready_tail;
  memstore_channel_head_t  *owned_chanhead_head;
  time_t                    getting_group;
};

ngx_int_t memstore_groups_init(memstore_groups_t *);
ngx_int_t memstore_groups_shutdown(memstore_groups_t *);


ngx_int_t memstore_group_find(memstore_groups_t *gp, ngx_str_t *name, callback_pt cb, void *pd);
ngx_int_t memstore_group_find_from_groupnode(memstore_groups_t *gp, group_tree_node_t *gtn, callback_pt cb, void *pd);


group_tree_node_t *memstore_groupnode_get(memstore_groups_t *gp, ngx_str_t *name);
nchan_group_t *memstore_group_owner_find(memstore_groups_t *gp, ngx_str_t *name, int *group_just_created);
ngx_int_t memstore_group_receive(memstore_groups_t *gp, nchan_group_t *shm_group);
ngx_int_t memstore_group_delete(memstore_groups_t *gp, ngx_str_t *name, callback_pt cb, void *pd);
ngx_int_t memstore_group_receive_delete(memstore_groups_t *gp, nchan_group_t *shm_group);

ngx_int_t memstore_group_add_channel(memstore_channel_head_t *ch);
ngx_int_t memstore_group_remove_channel(memstore_channel_head_t *ch);

void memstore_group_associate_own_channel(memstore_channel_head_t *ch);
void memstore_group_dissociate_own_channel(memstore_channel_head_t *ch);

ngx_int_t memstore_group_add_message(group_tree_node_t *gtn, nchan_msg_t *msg);
ngx_int_t memstore_group_remove_message(group_tree_node_t *gtn, nchan_msg_t *msg);

ngx_int_t memstore_group_add_subscribers(group_tree_node_t *gtn, int count);
#endif //MEMSTORE_GROUPS_HEADER
