#include <nchan_module.h>
#include "nchan_rbtree.h"
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "RBTREE:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "RBTREE:" fmt, ##arg)

static uint32_t rbtree_hash_crc32(void *str) {
  return ngx_crc32_short(((ngx_str_t *)str)->data, ((ngx_str_t *)str)->len);
}
static ngx_int_t rbtree_compare_str(void *id1, void *id2) {
  return ngx_memn2cmp(((ngx_str_t *)id1)->data, ((ngx_str_t *)id2)->data, ((ngx_str_t *)id1)->len, ((ngx_str_t *)id2)->len);
}

/*
static ngx_int_t rbtree_validate_node(rbtree_seed_t *seed, ngx_rbtree_node_t *node) {
  if (node == &seed->sentinel) {
    return 1;
  }
  
  ngx_int_t    i, max, valid;
  ngx_rbtree_node_t *cur;
  valid = 0;
  max = sizeof(seed->actives)/sizeof(cur);
  
  for(i=0; i<max; i++){
    if(seed->actives[i] == node) {
      valid = 1;
      break;
    }
  }
  return valid;
  
}
*/

/*
static ngx_int_t rbtree_validate_nodes_reachable(rbtree_seed_t *seed) {
  ngx_int_t    i, max;
  ngx_rbtree_node_t *cur, *match;
  max = sizeof(seed->actives)/sizeof(cur);
  
  for(i=0; i<max; i++){
    cur = seed->actives[i];
    if(cur != NULL) {
      match = rbtree_find_node(seed, seed->id(rbtree_data_from_node(cur)));
      assert(match == cur);
    }
  }
  return 1;
}
*/

static ngx_rbtree_node_t * rbtree_find_node_generic(rbtree_seed_t *seed, void *id, uint32_t hash, ngx_rbtree_node_t **last_parent, ngx_int_t *last_compare) {
  ngx_rbtree_node_t              *root = seed->tree.root;
  ngx_rbtree_node_t              *node = root;
  ngx_rbtree_node_t              *sentinel = seed->tree.sentinel;
  ngx_int_t                       rc;

  while (node != sentinel) {
    if (hash < node->key) {
      node = node->left;
      continue;
    }
    if (hash > node->key) {
      node = node->right;
      continue;
    }

    /* hash == node->key */
    rc = seed->compare(id, seed->id(rbtree_data_from_node(node)));
    if (rc == 0) {
      return node;
    }
    node = (rc < 0) ? node->left : node->right;
  }
  /* not found */

  return NULL;
}

ngx_rbtree_node_t *rbtree_find_node(rbtree_seed_t *seed, void *id) {
  ngx_rbtree_node_t *found;
  found = rbtree_find_node_generic(seed, id, seed->hash(id), NULL, NULL);
  if(found) {
    DBG("found node %p", found);
  }
  else {
    DBG("node not found");
  }
  return found;
}

static void rbtree_insert_generic(ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel) {
  ngx_int_t         offset = offsetof(rbtree_seed_t, sentinel);
  rbtree_seed_t    *seed = (rbtree_seed_t *)((char *)sentinel - offset);
  ngx_int_t         rc;
  void             *id = seed->id(rbtree_data_from_node(node));
  
  ngx_rbtree_node_t  **p;
 
  for ( ;; ) {
    
    if (node->key != temp->key) {
      p = (node->key < temp->key) ? &temp->left : &temp->right;
    }
    else {
      rc = seed->compare(id, seed->id(rbtree_data_from_node(temp)));
      p = (rc < 0) ? &temp->left : &temp->right;
    }
    
    if (*p == sentinel) {
      break;
    }
    
    temp = *p;
  }

  *p = node;
  node->parent = temp;
  node->left = sentinel;
  node->right = sentinel;
  ngx_rbt_red(node);
}

ngx_rbtree_node_t *rbtree_create_node(rbtree_seed_t *seed, size_t data) {
  ngx_rbtree_node_t *node = ngx_alloc(sizeof(ngx_rbtree_node_t) + data, ngx_cycle->log);
  if(node) {
    node->left = NULL;
    node->right = NULL;
    node->parent = NULL;
    seed->allocd_nodes++;
  }
  DBG("created node %p", node);
  return node;
}

ngx_int_t rbtree_destroy_node(rbtree_seed_t *seed, ngx_rbtree_node_t *node) {
  seed->allocd_nodes--;
#if NCHAN_RBTREE_DBG
  ngx_memset(node, 0x67, sizeof(*node));
#endif
  DBG("Destroyed node %p", node);
  ngx_free(node);
  
  return NGX_OK;
}

ngx_int_t rbtree_insert_node(rbtree_seed_t *seed, ngx_rbtree_node_t *node) {
  void  *id = seed->id(rbtree_data_from_node(node));
  node->key = seed->hash(id);
  ngx_rbtree_insert(&seed->tree, node);
  seed->active_nodes++;
  
#if NCHAN_RBTREE_DBG
  
  //assert(rbtree_find_node(seed, seed->id(rbtree_data_from_node(node))) == node);
  
  //super-heavy debugging
  /*
  ngx_int_t    i, max, inserted = 0;
  ngx_rbtree_node_t *cur;
  max = sizeof(seed->actives)/sizeof(cur);
  
  assert(seed->active_nodes < max);
  
  for(i=0; i<max; i++){
    if(seed->actives[i] == NULL) {
      seed->actives[i]=node;
      inserted = 1;
      break;
    }
  }
  assert(inserted);
  */
  
#endif
  DBG("inserted node %p", node);
  return NGX_OK;
}

ngx_int_t rbtree_remove_node(rbtree_seed_t *seed, ngx_rbtree_node_t *node) {
  
  ngx_rbtree_delete(&seed->tree, node);
  DBG("Removed node %p", node);
  seed->active_nodes--;
  
#if NCHAN_RBTREE_DBG
  //assert(rbtree_find_node(seed, seed->id(rbtree_data_from_node(node))) == NULL);
  ngx_memset(node, 0x65, sizeof(*node));
  
  
  //super-heavy debugging
  /*
  ngx_int_t    i, max, removed = 0;
  ngx_rbtree_node_t *cur;
  max = sizeof(seed->actives)/sizeof(cur);
  
  assert(seed->active_nodes < max);
  
  for(i=0; i<max; i++){
    if(seed->actives[i] == node) {
      seed->actives[i]=NULL;
      removed = 1;
      break;
    }
  }
  assert(removed);
  
  */
  
#endif
  return NGX_OK;
}

static ngx_inline void rbtree_walk_real(rbtree_seed_t *seed, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel, rbtree_walk_callback_pt callback, void *data) {
  ngx_rbtree_node_t  *left, *right;
  if(node == sentinel || node == NULL) {
    return;
  }
  left = node->left;
  right = node->right;
  rbtree_walk_real(seed, left, sentinel, callback, data);
  rbtree_walk_real(seed, right, sentinel, callback, data);
  callback(seed, rbtree_data_from_node(node), data);
}

static ngx_inline void rbtree_walk_ordered_incr_real(rbtree_seed_t *seed, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel, rbtree_walk_callback_pt callback, void *data) {
  ngx_rbtree_node_t  *left, *right;
  if(node == sentinel || node == NULL) {
    return;
  }
  left = node->left;
  right = node->right;
  rbtree_walk_real(seed, left, sentinel, callback, data);
  callback(seed, rbtree_data_from_node(node), data);
  rbtree_walk_real(seed, right, sentinel, callback, data);
}

static ngx_inline void rbtree_walk_ordered_decr_real(rbtree_seed_t *seed, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel, rbtree_walk_callback_pt callback, void *data) {
  ngx_rbtree_node_t  *left, *right;
  if(node == sentinel || node == NULL) {
    return;
  }
  left = node->left;
  right = node->right;
  rbtree_walk_real(seed, right, sentinel, callback, data);
  callback(seed, rbtree_data_from_node(node), data);
  rbtree_walk_real(seed, left, sentinel, callback, data);
}

ngx_int_t rbtree_walk(rbtree_seed_t *seed, rbtree_walk_callback_pt callback, void *data) {
  rbtree_walk_real(seed, seed->tree.root, seed->tree.sentinel, callback, data);
  return NGX_OK;
}

ngx_int_t rbtree_walk_incr(rbtree_seed_t *seed, rbtree_walk_callback_pt callback, void *data) {
  rbtree_walk_ordered_incr_real(seed, seed->tree.root, seed->tree.sentinel, callback, data);
  return NGX_OK;
}
ngx_int_t rbtree_walk_decr(rbtree_seed_t *seed, rbtree_walk_callback_pt callback, void *data) {
  rbtree_walk_ordered_decr_real(seed, seed->tree.root, seed->tree.sentinel, callback, data);
  return NGX_OK;
}

typedef struct {
  void                **els;
  int                 (*include)(void *);
  int                   n;
} rbtree_walk_writesafe_data_t;

static ngx_int_t rbtree_walk_writesafe_callback(rbtree_seed_t *seed, void *node_data, rbtree_walk_writesafe_data_t *d) {
  if(d->include(node_data)) {
    d->els[d->n++]=node_data;
  }
  return NGX_OK;
}

ngx_int_t rbtree_walk_writesafe(rbtree_seed_t *seed, int (*include)(void *), rbtree_walk_callback_pt callback, void *data) {
  void                         *els_static[32];
  int                           allocd;
  int                           i;
  rbtree_walk_writesafe_data_t  d;
  if(seed->active_nodes > 32) {
    d.els = ngx_alloc(sizeof(void *) * seed->active_nodes, ngx_cycle->log);
    allocd = 1;
  }
  else {
    d.els = els_static;
    allocd = 0;
  }
  d.include = include;
  d.n = 0;
  rbtree_walk(seed, (rbtree_walk_callback_pt )rbtree_walk_writesafe_callback, &d);
  
  for(i=0; i<d.n; i++) {
    callback(seed, d.els[i], data);
  }
  
  if(allocd) ngx_free(d.els);
  return NGX_OK;
}

unsigned rbtree_empty(rbtree_seed_t *seed, rbtree_walk_callback_pt callback, void *data) {
  ngx_rbtree_t         *tree = &seed->tree;
  ngx_rbtree_node_t    *cur, *sentinel = tree->sentinel;
  unsigned int          n = 0;
  
  for(cur = tree->root; cur != NULL && cur != sentinel; cur = tree->root) {
    if(callback) {
      callback(seed, rbtree_data_from_node(cur), data);
    }
    rbtree_remove_node(seed, cur);
    rbtree_destroy_node(seed, cur);
    n++;
  }
  return n;
}


static ngx_inline void rbtree_conditional_walk_real(rbtree_seed_t *seed, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel, rbtree_walk_conditional_callback_pt callback, void *data) {
  rbtree_walk_direction_t    direction;
  
  if(node == sentinel || node == NULL) {
    return;
  }
  
  direction = callback(seed, rbtree_data_from_node(node), data);
  switch(direction) {
    case RBTREE_WALK_LEFT:
      rbtree_conditional_walk_real(seed, node->left, sentinel, callback, data);
      break;
    
    case RBTREE_WALK_RIGHT:
      rbtree_conditional_walk_real(seed, node->right, sentinel, callback, data);
      break;
    
    case RBTREE_WALK_LEFT_RIGHT:
      rbtree_conditional_walk_real(seed, node->left, sentinel, callback, data);
      rbtree_conditional_walk_real(seed, node->right, sentinel, callback, data);
      break;
    
    case RBTREE_WALK_STOP:
      //no more
      break;
  }
}

ngx_int_t rbtree_conditional_walk(rbtree_seed_t *seed, rbtree_walk_conditional_callback_pt callback, void *data) {
  rbtree_conditional_walk_real(seed, seed->tree.root, seed->tree.sentinel, callback, data);
  return NGX_OK;
}

ngx_int_t rbtree_init(rbtree_seed_t *seed, char *name, void *(*id)(void *), uint32_t (*hash)(void *), ngx_int_t (*compare)(void *, void *)) {
  seed->name=name;
  assert(id != NULL);
  if(hash==NULL) {
    hash = &rbtree_hash_crc32;
  }
  if(compare == NULL) {
    compare = &rbtree_compare_str;
  }
  seed->id = id;
  seed->hash = hash;
  seed->compare = compare;
  seed->active_nodes = 0;
  seed->allocd_nodes = 0;
#if NCHAN_RBTREE_DBG
  /*
  //super-heavy debugging setup
  ngx_int_t max = sizeof(seed->actives);
  ngx_memzero(seed->actives, sizeof(seed->actives));
  */
#endif
  ngx_rbtree_init(&seed->tree, &seed->sentinel, &rbtree_insert_generic);
  return NGX_OK;
}
