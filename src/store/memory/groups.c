#include "groups.h"
#include "store-private.h"
#include "ipc-handlers.h"
#include <assert.h>

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "MEMSTORE:GROUPS: " fmt, ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "MEMSTORE:GROUPS: " fmt, ##args)

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
  DBG("shutdown_walker %V group %p", &gtn->name, gtn->group);
  if(memstore_str_owner(&gtn->name) == myslot) {
    shm_free(shm, gtn->group);
  }
  return NGX_OK;
}

ngx_int_t memstore_groups_shutdown(memstore_groups_t *gp) {
  rbtree_empty(&gp->tree, shutdown_walker, NULL);
  DBG("empties rbtree");
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
  
  gtn->owned_chanhead_head = NULL;
  
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
  
  ERR("created group %p", group);
  
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


group_tree_node_t *memstore_groupnode_find_now(memstore_groups_t *gp, ngx_str_t *name) {
  ngx_rbtree_node_t      *node;
  group_tree_node_t      *gtn = NULL;
  if((node = rbtree_find_node(&gp->tree, name)) != NULL) {
    gtn = rbtree_data_from_node(node);
  }
  else if(memstore_str_owner(name) == memstore_slot()) {
    gtn = group_owner_create_node(gp, name);
  }
  return gtn;
}

static ngx_int_t memstore_group_find_generic(memstore_groups_t *gp, ngx_str_t *name, int want_whole_node, callback_pt cb, void *pd) {
  ngx_rbtree_node_t      *node;
  group_tree_node_t      *gtn = NULL;
  if((node = rbtree_find_node(&gp->tree, name)) != NULL) {
    gtn = rbtree_data_from_node(node);
  }
  else if(memstore_str_owner(name) == memstore_slot()) {
    if((gtn = group_owner_create_node(gp, name)) != NULL) {
      cb(NGX_OK, want_whole_node ? (void *)gtn : (void *)gtn->group, pd);
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
  
  if (gtn->group) {
    cb(NGX_OK, want_whole_node ? (void *)gtn : (void *)gtn->group, pd);
  }
  else {
    
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
    gcb->want_node = want_whole_node;
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

ngx_int_t memstore_group_find(memstore_groups_t *gp, ngx_str_t *name, callback_pt cb, void *pd) {
  return memstore_group_find_generic(gp, name, 0, cb, pd);
}

ngx_int_t memstore_groupnode_find(memstore_groups_t *gp, ngx_str_t *name, callback_pt cb, void *pd) {
  return memstore_group_find_generic(gp, name, 1, cb, pd);
}

ngx_int_t memstore_group_receive(memstore_groups_t *gp, nchan_group_t *shm_group) {
  ngx_rbtree_node_t      *node;
  group_tree_node_t      *gtn = NULL;
  group_callback_t       *gcb, *next_gcb;
  
  assert(memstore_str_owner(&shm_group->name) != memstore_slot());
  
  if((node = rbtree_find_node(&gp->tree, &shm_group->name)) != NULL) {
    gtn = rbtree_data_from_node(node);  
    for(gcb = gtn->when_ready_head; gcb != NULL; gcb =  next_gcb) {
      next_gcb = gcb->next;
      gcb->cb(NGX_OK, gcb->want_node ? (void *)gtn : (void *)shm_group , gcb->pd);
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

ngx_int_t memstore_group_receive_delete(memstore_groups_t *gp, nchan_group_t *shm_group) {
  memstore_channel_head_t    *cur;
  group_tree_node_t          *gtn;
  
  gtn = memstore_groupnode_find_now(gp, &shm_group->name);
  if(gtn) {
    while((cur = gtn->owned_chanhead_head) != NULL) {
      nchan_memstore_force_delete_channel(&cur->id, NULL, NULL);
    }
  }
  
  return NGX_OK;
}

nchan_group_t *memstore_group_delete(memstore_groups_t *gp, ngx_str_t *name) {
  static nchan_group_t        group;
  group_tree_node_t          *gtn;
  memstore_channel_head_t    *cur;
  
  gtn = memstore_groupnode_find_now(gp, name);
  
  if(gtn) {
    group = *gtn->group;
    while((cur = gtn->owned_chanhead_head) != NULL) {
      nchan_memstore_force_delete_channel(&cur->id, NULL, NULL);
    }
    
    memstore_ipc_broadcast_group_delete(gtn->group);
    return &group;
  }
  else {
    return NULL;
  }

}
void memstore_group_add_channel(memstore_channel_head_t *ch, group_tree_node_t *gnd) {
  assert(ch->groupnode == NULL);
  ch->groupnode = gnd;
  if(ch->multi) {
    ngx_atomic_fetch_add(&gnd->group->multiplexed_channels, 1);
  }
  else if (ch->owner == memstore_slot()) {
    ngx_atomic_fetch_add(&gnd->group->channels, 1);
  }
}

void memstore_group_remove_channel(memstore_channel_head_t *ch) {
  assert(ch->groupnode != NULL);

  if(ch->multi) {
    ngx_atomic_fetch_add(&ch->groupnode->group->multiplexed_channels, -1);
  }
  else if (ch->owner == memstore_slot()) {
    ngx_atomic_fetch_add(&ch->groupnode->group->channels, -1);
  }
  ch->groupnode = NULL;
}


void memstore_group_associate_own_channel(memstore_channel_head_t *ch) {
  group_tree_node_t *gnd = ch->groupnode;
  assert(gnd->group);
  assert(ch);
  
  if(!ch->multi && ch->owner == memstore_slot()) {
    ch->groupnode_next = gnd->owned_chanhead_head;
    if(gnd->owned_chanhead_head) {
      gnd->owned_chanhead_head->groupnode_prev = ch;
    }
    gnd->owned_chanhead_head = ch;
    
  }
  verify_gnd(ch);
}

void memstore_group_dissociate_own_channel(memstore_channel_head_t *ch) {
  if(!ch->multi && ch->owner == memstore_slot()) {
    if(ch->groupnode->owned_chanhead_head == ch) {
      ch->groupnode->owned_chanhead_head = ch->groupnode_next;
    }
    if(ch->groupnode_prev) {
      assert(ch->groupnode_prev->groupnode_next == ch);
      ch->groupnode_prev->groupnode_next = ch->groupnode_next;
    }
    if(ch->groupnode_next) {
      assert(ch->groupnode_next->groupnode_prev == ch);
      ch->groupnode_next->groupnode_prev = ch->groupnode_prev;
    }
    
    ch->groupnode_prev = NULL;
    ch->groupnode_next = NULL;
  }
  assert(ch->groupnode->owned_chanhead_head != ch);
}

