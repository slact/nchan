#include "groups.h"
#include "store.h"
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
  
  ERR("created group %p %V", group, &group->name);
  
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


static ngx_int_t add_whenready_callback(group_tree_node_t *gtn, char *lbl, callback_pt cb, void *pd) {
  //not ready yet, queue up the callback
  group_callback_t  *gcb;
  DBG("add to %p whenready %s for group %V", gtn, lbl, &gtn->name);
  if((gcb = ngx_alloc(sizeof(*gcb), ngx_cycle->log)) == NULL) {
    ERR("couldn't allocate callback link for group %V", &gtn->name);
    cb(NGX_ERROR, NULL, pd);
    return NGX_ERROR;
  }
  
  gcb->cb = cb;
  gcb->pd = pd;
  gcb->label = lbl;
  gcb->next = NULL;
  
  if(gtn->when_ready_tail) {
    gtn->when_ready_tail->next = gcb;
  }
  
  if(!gtn->when_ready_head) {
    gtn->when_ready_head = gcb;
  }
  
  
  gtn->when_ready_tail = gcb;
  
  for(gcb = gtn->when_ready_head; gcb != NULL; gcb = gcb->next){
    DBG("  whenready %s", gcb->label);
  }
  
  return NGX_OK;
}

static void call_whenready_callbacks(group_tree_node_t *gtn, nchan_group_t *shm_group) {
  group_callback_t       *gcb, *next_gcb;
  
  for(gcb = gtn->when_ready_head; gcb != NULL; gcb = next_gcb) {
    DBG("whenready for %p callback %s for group %V", gtn, gcb->label, &gtn->name);
    next_gcb = gcb->next;
    gcb->cb(shm_group ? NGX_OK : NGX_ERROR, shm_group , gcb->pd);
    ngx_free(gcb);
  }
  gtn->when_ready_head = NULL;
  gtn->when_ready_tail = NULL;
}

ngx_int_t memstore_group_find_from_groupnode(memstore_groups_t *gp, group_tree_node_t *gtn, callback_pt cb, void *pd) {
  if(!gtn) {
    cb(NGX_ERROR, NULL, pd);
    return NGX_ERROR;
  }
  if(gtn->group) {
    cb(NGX_OK, gtn->group, pd);
  }
  else {
    add_whenready_callback(gtn, "group find", cb, pd);
  }
  return NGX_OK;
}

ngx_int_t memstore_group_find(memstore_groups_t *gp, ngx_str_t *name, callback_pt cb, void *pd) {
  group_tree_node_t  *gtn = memstore_groupnode_get(gp, name);
  return memstore_group_find_from_groupnode(gp, gtn, cb, pd);
}

group_tree_node_t *memstore_groupnode_get(memstore_groups_t *gp, ngx_str_t *name) {
  ngx_rbtree_node_t      *node;
  group_tree_node_t      *gtn = NULL;
  ngx_int_t               owner;
  if((node = rbtree_find_node(&gp->tree, name)) != NULL) {
    gtn = rbtree_data_from_node(node);
  }
  else {
    owner = memstore_str_owner(name);
    if(owner == memstore_slot()) {
      gtn = group_owner_create_node(gp, name);
    }
    else {
      if((gtn = group_create_node(gp, name, NULL))!=NULL) {
        memstore_ipc_send_get_group(memstore_str_owner(name), name);
      }
    }
    if(!gtn) {
      ERR("couldn't create groupnode for group %V", name);
    }
  }
  return gtn;
}

ngx_int_t memstore_group_receive(memstore_groups_t *gp, nchan_group_t *shm_group) {
  ngx_rbtree_node_t      *node;
  group_tree_node_t      *gtn = NULL;
  
  assert(memstore_str_owner(&shm_group->name) != memstore_slot());
  DBG("memstore group receive %V", &shm_group->name);
  if((node = rbtree_find_node(&gp->tree, &shm_group->name)) != NULL) {
    gtn = rbtree_data_from_node(node);  
    gtn->group = shm_group;
    
    call_whenready_callbacks(gtn, shm_group);
    
  }
  else {
    gtn = group_create_node(gp, &shm_group->name, shm_group);
    DBG("created node %p", gtn);
  }
  
  return NGX_OK;
}

ngx_int_t memstore_group_receive_delete(memstore_groups_t *gp, nchan_group_t *shm_group) {
  memstore_channel_head_t    *cur;
  group_tree_node_t          *gtn = NULL;
  ngx_rbtree_node_t          *node;
  DBG("receive GROUP DELETE for %V", &shm_group->name);
  if((node = rbtree_find_node(&gp->tree, &shm_group->name)) != NULL) {
    gtn = rbtree_data_from_node(node);
  }
  DBG("gtn is %V", gtn);
  if(gtn) {
    
    call_whenready_callbacks(gtn, NULL);
    
    while((cur = gtn->owned_chanhead_head) != NULL) {
      memstore_group_dissociate_own_channel(cur);
      nchan_store_memory.delete_channel(&cur->id, cur->cf, NULL, NULL);
    }
  }
  
  return NGX_OK;
}

typedef struct {
  int       n;
  void     *data;
  unsigned  allocd:1;
} group_callback_data_t;

typedef struct {
  callback_pt        cb;
  void              *pd;
  memstore_groups_t *gp;
  unsigned           owned;
} group_delete_callback_data_t;
  


static ngx_int_t group_delete_callback(ngx_int_t rc, nchan_group_t *shm_group, group_delete_callback_data_t *d) {
  static nchan_group_t  group;
  if(shm_group) {
    DBG("GROUP DELETE find_group callback for %V", &shm_group->name);
    group = *shm_group;
    if(d->owned) {
      memstore_group_receive_delete(d->gp, shm_group);
    }
    memstore_ipc_broadcast_group_delete(shm_group);
  }
  else {
    ERR("group for delete callback is NULL");
    ngx_memzero(&group, sizeof(group));
  }
  d->cb(rc, &group, d->pd);
  ngx_free(d);
  return NGX_OK;
}

 ngx_int_t memstore_group_delete(memstore_groups_t *gp, ngx_str_t *name, callback_pt cb, void *pd) {
  group_tree_node_t            *gtn = NULL;
  ngx_int_t                     owner = memstore_str_owner(name);
  group_delete_callback_data_t *d;
  
  if((gtn = memstore_groupnode_get(gp, name)) == NULL) {
    ERR("couldn't get groupnode for deletion");
    cb(NGX_ERROR, NULL, pd);
    return NGX_ERROR;
  }
  
  if((d = ngx_alloc(sizeof(*d), ngx_cycle->log)) == NULL) {
    ERR("couldn't alloc callback data for group deletion");
    cb(NGX_ERROR, NULL, pd);
    return NGX_ERROR;
  }
  
  d->cb = cb;
  d->pd = pd;
  d->gp = gp;
  d->owned = owner == memstore_slot();
  DBG("start DELETE GROUP %V", &gtn->name);
  return memstore_group_find(gp, &gtn->name, (callback_pt )group_delete_callback, d);
}


static void verify_gnd(memstore_channel_head_t *ch) {
  /*
  memstore_channel_head_t *cur;
  int n=0, n2=0;
  for(cur = ch; cur != NULL; cur = cur->groupnode_next) {
    if(cur->groupnode_next) {
      assert(cur->groupnode_next->groupnode_prev == cur);
      n++;
    }
  }
  for(cur = ch; cur != NULL; cur = cur->groupnode_prev) {
    if(cur->groupnode_prev) {
      assert(cur->groupnode_prev->groupnode_next == cur);
      n++;
    }
  }

  if(ch->groupnode) {
    int prevnulls = 0;
    int nextnulls = 0;
    for(cur = ch->groupnode->owned_chanhead_head; cur != NULL; cur = cur->groupnode_next) {
      n2++;
      if(!cur->groupnode_prev) {
        assert(++prevnulls <= 1);
      }
      else {
        assert(cur->groupnode_prev->groupnode_next == cur);
      }
      
      if(!cur->groupnode_next) {
        assert(++nextnulls <= 1);
      }
      else {
        assert(cur->groupnode_next->groupnode_prev == cur);
      }
    }
    if(n>0) {
      assert(n2 - 1 == n);
    }
  }
  */
}

static void verify_gnd_ch_absent(memstore_channel_head_t *ch) {
  /*memstore_channel_head_t *cur;
  for(cur = ch->groupnode->owned_chanhead_head; cur != NULL; cur = cur->groupnode_next) {
    assert(cur != ch);
    if(cur->groupnode_prev) {      
      assert(cur->groupnode_prev != ch);
    }
  }
  */
  /*memstore_channel_head_t *tmp;
  HASH_ITER(hh, mpt->hash, cur, tmp) {
    assert(cur->groupnode_prev != ch);
    assert(cur->groupnode_next != ch);
  }*/
}


void memstore_group_associate_own_channel(memstore_channel_head_t *ch) {
  group_tree_node_t *gtn = ch->groupnode;
  
  verify_gnd(ch);
  verify_gnd_ch_absent(ch);
  
  assert(ch->owner == memstore_slot());
  
  if(!ch->multi) {
    ch->groupnode_next = gtn->owned_chanhead_head;
    if(gtn->owned_chanhead_head) {
      gtn->owned_chanhead_head->groupnode_prev = ch;
    }
    gtn->owned_chanhead_head = ch;
    
  }
  
  verify_gnd(ch);
}

void memstore_group_dissociate_own_channel(memstore_channel_head_t *ch) {
  verify_gnd(ch);
  assert(ch->owner == memstore_slot());
  if(!ch->multi) {
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
  verify_gnd(ch);
  verify_gnd_ch_absent(ch);
}





static ngx_int_t group_add_channel_callback(ngx_int_t rc, nchan_group_t *shm_group, group_callback_data_t *d) {
  memstore_channel_head_t   *ch = d->data;
  if(shm_group) {
    if(ch->multi) {
      ngx_atomic_fetch_add(&shm_group->multiplexed_channels, d->n);
    }
    else if (ch->owner == memstore_slot()) {
      ngx_atomic_fetch_add(&shm_group->channels, d->n);
    }
  }
  if(d->allocd) {
    memstore_chanhead_release(ch, "group_addchannel");
    ngx_free(d);
  }
  return NGX_OK;
}

static ngx_int_t memstore_group_add_channel_generic(memstore_channel_head_t *ch, int n) {
  if(ch->groupnode->group) {
    group_callback_data_t d = {n, ch, 0};
    group_add_channel_callback(NGX_OK, ch->groupnode->group, &d);
  }
  else {
    group_callback_data_t *d = ngx_alloc(sizeof(*d), ngx_cycle->log);
    if(!d) {
      ERR("Couldn't allocate group_add_channel data");
      return NGX_ERROR;
    }
    else {
      d->n = n;
      d->data = ch;
      d->allocd = 1;
      memstore_chanhead_reserve(ch, "group_addchannel");
      add_whenready_callback(ch->groupnode, "add channel", (callback_pt )group_add_channel_callback, d);
    }
  }
  return NGX_OK;
}

ngx_int_t memstore_group_add_channel(memstore_channel_head_t *ch) {
  return memstore_group_add_channel_generic(ch, 1);
}

ngx_int_t memstore_group_remove_channel(memstore_channel_head_t *ch) {
  return memstore_group_add_channel_generic(ch, -1);
}

static ngx_int_t group_add_message_callback(ngx_int_t rc, nchan_group_t *shm_group, group_callback_data_t *d) {
  
  nchan_msg_t   *msg = d->data;
  if(shm_group) {
    ngx_atomic_fetch_add(&shm_group->messages, d->n);
    if(ngx_buf_in_memory_only((msg->buf))) {
      ngx_atomic_fetch_add(&shm_group->messages_shmem_bytes, d->n * ngx_buf_size((msg->buf)));
    }
    else {
      assert(msg->buf->in_file);
      ngx_atomic_fetch_add(&shm_group->messages_file_bytes, d->n * ngx_buf_size((msg->buf)));
    }
  }
  if(d->allocd) {
    ngx_free(d);
  }
  return NGX_OK;
}

static ngx_int_t memstore_group_add_message_generic(group_tree_node_t *gtn, nchan_msg_t *msg, int n) {
  if(gtn->group) {
    group_callback_data_t d = {n, msg, 0};
    group_add_message_callback(NGX_OK, gtn->group, &d);
  }
  else {
    group_callback_data_t *d = ngx_alloc(sizeof(*d), ngx_cycle->log);
    if(!d) {
      ERR("Couldn't allocate group_add_message data");
      return NGX_ERROR;
    }
    else {
      d->n = n;
      d->data = msg;
      d->allocd = 1;
      add_whenready_callback(gtn, "add message", (callback_pt )group_add_message_callback, d);
    }
  }
  return NGX_OK;
}

ngx_int_t memstore_group_add_message(group_tree_node_t *gtn, nchan_msg_t *msg) {
  return memstore_group_add_message_generic(gtn, msg, 1);
}

ngx_int_t memstore_group_remove_message(group_tree_node_t *gtn, nchan_msg_t *msg) {
  return memstore_group_add_message_generic(gtn, msg, -1);
}




static ngx_int_t group_add_subscribers_callback(ngx_int_t rc, nchan_group_t *shm_group, group_callback_data_t *d) {
  
  if(shm_group) {
    ngx_atomic_fetch_add(&shm_group->subscribers, d->n);
  }
  
  if(d->allocd) {
    ngx_free(d);
  }
  return NGX_OK;
}

ngx_int_t memstore_group_add_subscribers(group_tree_node_t *gtn, int count) {
  if(gtn->group) {
    group_callback_data_t d = {count, NULL, 0};
    group_add_subscribers_callback(NGX_OK, gtn->group, &d);
  }
  else {
    group_callback_data_t *d = ngx_alloc(sizeof(*d), ngx_cycle->log);
    if(!d) {
      ERR("Couldn't allocate group_add_subscribers data");
      return NGX_ERROR;
    }
    else {
      ERR("didn't add subscriber yet");
      d->n = count;
      d->data = NULL;
      d->allocd = 1;
      add_whenready_callback(gtn, "add subscribers", (callback_pt )group_add_subscribers_callback, d);
    }
  }
  return NGX_OK;
}
