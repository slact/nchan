#include <nchan_module.h>
#include "rbtree_util.h"
#include <assert.h>

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "RBTREE:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "RBTREE:" fmt, ##arg)

static uint32_t rbtree_hash_crc32(void *str) {
  return ngx_crc32_short(((ngx_str_t *)str)->data, ((ngx_str_t *)str)->len);
}
static ngx_int_t rbtree_compare_str(void *id1, void *id2) {
  return ngx_memn2cmp(((ngx_str_t *)id1)->data, ((ngx_str_t *)id2)->data, ((ngx_str_t *)id1)->len, ((ngx_str_t *)id2)->len);
}

static ngx_rbtree_node_t * rbtree_find_node_generic(rbtree_seed_t *seed, void *id, uint32_t hash, ngx_rbtree_node_t **last_parent) {
  ngx_rbtree_node_t              *node = seed->tree.root;
  ngx_rbtree_node_t              *sentinel = seed->tree.sentinel;
  ngx_int_t                       rc;
  if(last_parent) {
    *last_parent = node;
  }
  while (node != sentinel) {
    if(last_parent && node->parent != NULL){
      *last_parent = node->parent;
    }
    if (hash < node->key) {
      node = node->left;
      continue;
    }
    if (hash > node->key) {
      node = node->right;
      continue;
    }
    /* find_hash == node->key */
    do {
      rc = seed->compare(id, seed->id(rbtree_data_from_node(node)));
      if (rc == 0) {
        return node;
      }
      node = (rc < 0) ? node->left : node->right;
    } while (node != sentinel && hash == node->key);
    break;
  }
  //not found
  return NULL;
}

ngx_rbtree_node_t *rbtree_find_node(rbtree_seed_t *seed, void *id) {
  ngx_rbtree_node_t *found;
  found = rbtree_find_node_generic(seed, id, seed->hash(id), NULL);
  if(found) {
    DBG("found node %p", found);
  }
  else {
    DBG("node not found");
  }
  return found;
}

static void rbtree_insert_generic(ngx_rbtree_node_t *root, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel) {
  ngx_int_t         offset = offsetof(rbtree_seed_t, sentinel);
  rbtree_seed_t    *seed = (rbtree_seed_t *)((char *)sentinel - offset); //is this right?
  
  ngx_rbtree_node_t         *p = NULL;
  ngx_rbtree_node_t         *f;
  void                      *id = seed->id(rbtree_data_from_node(node));
  
  DBG("insert node %p", node);
  
  f = rbtree_find_node_generic(seed, id, seed->hash(id), &p);
  if(f) {
    //node already exists
    return;
  }
  if(p) {
    //insert left or right of here
    //node's hash should be computed.
    node->right = sentinel;
    node->left = sentinel;
    if(p->key > node->key) {
      p->left = node;
    }
    else if(p->key < node->key) {
      p->right = node;
    }
    else if(p->key == node->key) {
      ngx_int_t rc = seed->compare(seed->id(rbtree_data_from_node(p)), seed->id(rbtree_data_from_node(node)));
      if(rc > 0){
        p->left = node;
      }
      else {
        p->right = node;
      }
    }
    node->parent = p;
    ngx_rbt_red(node);
  }
  else {
    //no parent?...
    return;
  }
  return;
}

ngx_rbtree_node_t *rbtree_create_node(rbtree_seed_t *seed, size_t data) {
  ngx_rbtree_node_t *node = ngx_alloc(sizeof(ngx_rbtree_node_t) + data, ngx_cycle->log);
  if(node) {
    node->left = NULL;
    node->right = NULL;
    node->parent = NULL;
  }
#if NCHAN_RBTREE_DBG
  seed->allocd_nodes++;
#endif
  DBG("created node %p", node);
  return node;
}

ngx_int_t rbtree_destroy_node(rbtree_seed_t *seed, ngx_rbtree_node_t *node) {
#if NCHAN_RBTREE_DBG
  ngx_memset(node, 0x67, sizeof(*node));
  seed->allocd_nodes--;
#endif
  ngx_free(node);
  DBG("Destroyed node %p", node);
  
  return NGX_OK;
}

ngx_int_t rbtree_insert_node(rbtree_seed_t *seed, ngx_rbtree_node_t *node) {
  void  *id = seed->id(rbtree_data_from_node(node));
#if NCHAN_RBTREE_DBG
  assert(rbtree_find_node(seed, id) == NULL);
#endif
  node->key = seed->hash(id);
  ngx_rbtree_insert(&seed->tree, node);
#if NCHAN_RBTREE_DBG
  seed->active_nodes++;
#endif
  DBG("inserted node %p", node);
  return NGX_OK;
}

ngx_int_t rbtree_remove_node(rbtree_seed_t *seed, ngx_rbtree_node_t *node) {
  ngx_rbtree_delete(&seed->tree, node);
  DBG("Removed node %p", node);
#if NCHAN_RBTREE_DBG
  assert(rbtree_find_node(seed, seed->id(rbtree_data_from_node(node))) == NULL);
  ngx_memset(node, 0x65, sizeof(*node));
  seed->active_nodes--;
#endif
  return NGX_OK;
}

static void rbtree_walk_real(rbtree_seed_t *seed, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel, rbtree_walk_callback_pt callback, void *data) {
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

ngx_int_t rbtree_walk(rbtree_seed_t *seed, rbtree_walk_callback_pt callback, void *data) {
  rbtree_walk_real(seed, seed->tree.root, seed->tree.sentinel, callback, data);
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
#if NCHAN_RBTREE_DBG
  seed->allocd_nodes = 0;
  seed->active_nodes = 0;
#endif
  ngx_rbtree_init(&seed->tree, &seed->sentinel, &rbtree_insert_generic);
  return NGX_OK;
}