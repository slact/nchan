#include "groups.h"
#include "store-private.h"
#include "ipc-handlers.h"
#include <assert.h>

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "MEMSTORE:GROUPS: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "MEMSTORE:GROUPS: " fmt, ##args)

typedef struct group_callback_s group_callback_t ;
struct group_callback_s {
  callback_pt         cb;
  void               *pd;
  group_callback_t   *next;
};

typedef struct {
  ngx_str_t          name; //local memory copy of group name
  nchan_group_t     *group;
  group_callback_t  *when_ready_head;
  group_callback_t  *when_ready_tail;
} group_tree_node_t;

static void *group_id(void *d) {
  return &((group_tree_node_t *)d)->name;
}

ngx_int_t memstore_groups_init(memstore_groups_t *gp) {
  return rbtree_init(&gp->tree, "memstore groups", group_id, NULL, NULL);
}


ngx_int_t shutdown_walker(rbtree_seed_t *seed, void *node_data, void *privdata) {
  group_tree_node_t *gtn = (group_tree_node_t *)node_data;
  shmem_t *          shm = nchan_memstore_get_shm();
  ngx_int_t          myslot = memstore_slot();
  if(memstore_str_owner(&gtn->name) == myslot) {
    shm_free(shm, gtn->group);
  }
  return NGX_OK;
}

ngx_int_t memstore_groups_shutdown(memstore_groups_t *gp) {
  rbtree_empty(&gp->tree, shutdown_walker, NULL);
  return NGX_OK;
}

static group_tree_node_t *group_create_node(memstore_groups_t *gp, ngx_str_t *name, nchan_group_t *shm_group) {
  //assumes node does not yet exist
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
  
  gtn->group = shm_group;
  
  gtn->when_ready_head = NULL;
  gtn->when_ready_tail = NULL;
  
  rbtree_insert_node(&gp->tree, node);
  
  return gtn;
}

static group_tree_node_t *group_owner_create_node(memstore_groups_t *gp, ngx_str_t *name) {
  //ASSUMES group name is owned by current worker, AND node does not yet exist
  group_tree_node_t      *gtn;
  nchan_group_t          *group;
  group = shm_calloc(nchan_memstore_get_shm(), sizeof(*group) + name->len, "group");
  if(group == NULL) {
    ERR("couldn't alloc shmem for group %V", name);
    return NULL;
  }
  
  group->name.len = name->len;
  group->name.data = (u_char *)(&group[1]);
  ngx_memcpy(group->name.data, name->data, name->len);
  
  if((gtn = group_create_node(gp, name, group)) == NULL) {
    shm_free(nchan_memstore_get_shm(), group);
    return NULL;
  }
  
  memstore_ipc_broadcast_group(group);
  
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


nchan_group_t *memstore_group_find_now(memstore_groups_t *gp, ngx_str_t *name) {
  ngx_rbtree_node_t      *node;
  group_tree_node_t      *gtn = NULL;
  if((node = rbtree_find_node(&gp->tree, name)) != NULL) {
    gtn = rbtree_data_from_node(node);
  }
  else if(memstore_str_owner(name) == memstore_slot()) {
    gtn = group_owner_create_node(gp, name);
  }
  return gtn ? gtn->group : NULL;
}

ngx_int_t memstore_group_find(memstore_groups_t *gp, ngx_str_t *name, callback_pt cb, void *pd) {
  ngx_rbtree_node_t      *node;
  group_tree_node_t      *gtn = NULL;
  if((node = rbtree_find_node(&gp->tree, name)) != NULL) {
    gtn = rbtree_data_from_node(node);
  }
  else if(memstore_str_owner(name) == memstore_slot()) {
    if((gtn = group_owner_create_node(gp, name)) != NULL) {
      cb(NGX_OK, gtn->group, pd);
      return NGX_OK;
    }
    else {
      cb(NGX_ERROR, NULL, pd);
      return NGX_ERROR;
    }
  }
  else {
    if((gtn = group_create_node(gp, name, NULL)) == NULL) {
      cb(NGX_ERROR, NULL, pd);
      return NGX_ERROR;
    }
  }
  
  if (!gtn->group) {
    
    memstore_ipc_send_get_group(memstore_str_owner(name), name);
    
    //not ready yet, queue up the callback
    group_callback_t  *gcb;
    if((gcb = ngx_alloc(sizeof(*gcb), ngx_cycle->log)) == NULL) {
      ERR("couldn't allocate callback link for group %V", name);
      cb(NGX_ERROR, NULL, pd);
      return NGX_ERROR;
    }
    
    gcb->cb = cb;
    gcb->pd = pd;
    gcb->next = NULL;
    
    if(gtn->when_ready_tail) {
      gtn->when_ready_tail->next = gcb;
    }
    
    if(!gtn->when_ready_head) {
      gtn->when_ready_head = gcb;
    }
    
    gtn->when_ready_tail = gcb;
  }
  return NGX_OK;
}

ngx_int_t memstore_group_receive(memstore_groups_t *gp, nchan_group_t *shm_group) {
  ngx_rbtree_node_t      *node;
  group_tree_node_t      *gtn;
  group_callback_t       *gcb, *next_gcb;
  
  assert(memstore_str_owner(&shm_group->name) != memstore_slot());
  
  if((node = rbtree_find_node(&gp->tree, &shm_group->name)) != NULL) {
    gtn = rbtree_data_from_node(node);  
    for(gcb = gtn->when_ready_head; gcb != NULL; gcb =  next_gcb) {
      next_gcb = gcb->next;
      gcb->cb(NGX_OK, shm_group, gcb->pd);
      ngx_free(gcb);
    }
    gtn->when_ready_head = NULL;
    gtn->when_ready_tail = NULL;
    
  }
  else {
    gtn = group_create_node(gp, &shm_group->name, shm_group);
  }
  
  if(gtn) {
    gtn->group = shm_group;
    return NGX_OK;
  }
  else {
    return NGX_ERROR;
  }
}
