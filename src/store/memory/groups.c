#include "groups.h"
#include "store-private.h"
#include <assert.h>
#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "MEMSTORE:GROUPS: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "MEMSTORE:GROUPS: " fmt, ##args)

typedef struct group_callback_s group_callback_t ;
struct group_callback_s {
  void              (*cb)(nchan_group_t *, void *);
  void               *pd;
  group_callback_t   *next;
};

typedef struct {
  ngx_str_t          name; //local memory copy of group name
  nchan_group_t     *group;
  group_callback_t  *when_ready;
} group_tree_node_t;

static void *group_id(void *d) {
  return &((group_tree_node_t *)d)->name;
}

ngx_int_t memstore_groups_init(memstore_groups_t *gp) {
  return rbtree_init(&gp->tree, "memstore groups", group_id, NULL, NULL);
}


ngx_int_t shutdown_walker(rbtree_seed_t *seed, void *node_data, void *privdata) {
  group_tree_node_t *node = (group_tree_node_t *)node_data;
  shmem_t *          shm = nchan_memstore_get_shm();
  ngx_int_t          myslot = memstore_slot();
  if(memstore_str_owner(&node->name) == myslot) {
    shm_free(shm, node->group);
  }
  return NGX_OK;
}

ngx_int_t memstore_groups_shutdown(memstore_groups_t *gp) {
  return rbtree_empty(&gp->tree, shutdown_walker, NULL);
}

static group_tree_node_t *group_owner_create_node(memstore_groups_t *gp, ngx_str_t *name) {
  //ASSUMES group name is owned by current worker, AND node does not yet exist
  ngx_rbtree_node_t      *node;
  group_tree_node_t      *gtn;
  
  if((node = rbtree_create_node(&gp->tree, sizeof(group_tree_node_t) + name->len)) == NULL) {
    ERR("couldn't alloc rbtree node for group %V", name);
    return NULL;
  }
  gtn = rbtree_data_from_node(node);
  gtn->name.len = name->len;
  gtn->name.data = (u_char *)(&gtn[1]);
  ngx_memcpy(gtn->name.data, name->data, name->len);
  
  gtn->group = shm_calloc(nchan_memstore_get_shm(), sizeof(*(gtn->group)) + name->len, "group");
  if(gtn->group == NULL) {
    ERR("couldn't alloc shmem for group %V", name);
    rbtree_destroy_node(&gp->tree, node);
    return NULL;
  }
  
  gtn->group->name.len = name->len;
  gtn->group->name.data = (u_char *)(&gtn->group[1]);
  ngx_memcpy(gtn->group->name.data, name->data, name->len);
  
  rbtree_insert_node(&gp->tree, node);
  
  return gtn;
}

nchan_group_t *memstore_group_owner_find(memstore_groups_t *gp, ngx_str_t *name) {
  ngx_rbtree_node_t      *node;
  group_tree_node_t      *gtn;
  assert(memstore_str_owner(name) == memstore_slot());
  if((node = rbtree_find_node(&gp->tree, name)) != NULL) {
    gtn = rbtree_data_from_node(node);
  }
  else {
    gtn = group_owner_create_node(gp, name);
  }
  
  return gtn ? gtn->group : NULL;
}

ngx_int_t memstore_group_find(memstore_groups_t *gp, ngx_str_t *name, void (*cb)(nchan_group_t *, void *), void *pd) {
  ngx_rbtree_node_t      *node;
  group_tree_node_t      *gtn;
  if((node = rbtree_find_node(&gp->tree, name)) != NULL) {
    gtn = rbtree_data_from_node(node);
  }
  else if(memstore_str_owner(name) == memstore_slot()) {
    gtn = group_owner_create_node(gp, name);
    cb(gtn ? gtn->group : NULL, pd);
    
    return NGX_OK;
  }
  else {
    gtn = rbtree_data_from_node(node);
    
    gtn->name.len = name->len;
    gtn->name.data = (u_char *)(&gtn[1]);
    ngx_memcpy(gtn->name.data, name->data, name->len);
    
    
  }
  return NGX_OK;
}

ngx_int_t memstore_group_receive(memstore_groups_t *gp, nchan_group_t *shm_group) {
  ngx_rbtree_node_t      *node;
  group_tree_node_t      *gtn;
  assert(memstore_str_owner(&shm_group->name) != memstore_slot());
  if((node = rbtree_find_node(&gp->tree, &shm_group->name)) != NULL) {
    gtn = rbtree_data_from_node(node);
    ERR("node for group %V already exists, prev shm data: %p, new %p", &shm_group->name, gtn->group, shm_group);
  }
  else {
    node = rbtree_create_node(&gp->tree, sizeof(group_tree_node_t) + shm_group->name.len);
  }
  
  if(node == NULL) {
    ERR("couldn't create node for group %V", &shm_group->name);
    return NGX_ERROR;
  }
  
  gtn = rbtree_data_from_node(node);
  gtn->name.len = shm_group->name.len;
  gtn->name.data = (u_char *)(&gtn[1]);
  ngx_memcpy(gtn->name.data, shm_group->name.data, shm_group->name.len);
  
  gtn->group = shm_group;
  
  rbtree_insert_node(&gp->tree, node);
  return NGX_OK;
}
