#include <nchan_module.h>
#include "rbtree_util.h"
#include <assert.h>

#define DEBUG_LEVEL NGX_LOG_WARN
//#define DEBUG_LEVEL NGX_LOG_DEBUG
#define DBG(...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, __VA_ARGS__)
#define ERR(...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, __VA_ARGS__)

static uint32_t rbtree_hash_crc32(ngx_str_t *str) {
  return ngx_crc32_short(str->data, str->len);
}
static ngx_int_t rbtree_compare_str(ngx_str_t *id1, ngx_str_t *id2) {
  return ngx_memn2cmp(id1->data, id2->data, id1->len, id2->len);
}

static ngx_rbtree_node_t * rbtree_find_node_generic(rbtree_seed_t *seed, ngx_str_t *id, uint32_t hash, ngx_rbtree_node_t **last_parent) {
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
      rc = seed->compare(id, seed->id(node));
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

ngx_rbtree_node_t *rbtree_find_node(rbtree_seed_t *seed, ngx_str_t *id) {
  return rbtree_find_node_generic(seed, id, seed->hash(id), NULL);
}

ngx_rbtree_node_t   *rbtree_node_from_data(void *data)  {
  ngx_int_t         offset = offsetof(ngx_rbtree_node_t, data);
  return (ngx_rbtree_node_t *)((char *)data - offset); //return 
}

static void rbtree_insert_generic(ngx_rbtree_node_t *root, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel) {
  ngx_int_t         offset = offsetof(rbtree_seed_t, sentinel);
  rbtree_seed_t    *seed = (rbtree_seed_t *)((char *)sentinel - offset); //is this right?

  ngx_rbtree_node_t         *p = NULL;
  ngx_rbtree_node_t         *f;
  ngx_str_t                 *id = seed->id(node);
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
      ngx_int_t rc = seed->compare(seed->id(p), seed->id(node));
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
  ngx_rbtree_node_t *node = ngx_alloc(sizeof(ngx_rbtree_node_t) + data - sizeof(u_char), ngx_cycle->log);
  if(node) {
    node->left = NULL;
    node->right = NULL;
    node->parent = NULL;
  }
  return node;
}

ngx_int_t rbtree_destroy_node(rbtree_seed_t *seed, ngx_rbtree_node_t *node) {
  DBG("Destroy node %V", seed->id(node));
  ngx_free(node);
  return NGX_OK;
}

ngx_int_t rbtree_insert_node(rbtree_seed_t *seed, ngx_rbtree_node_t *node) {
  node->key = seed->hash(seed->id(node));
  ngx_rbtree_insert(&seed->tree, node);
  return NGX_OK;
}

ngx_int_t rbtree_remove_node(rbtree_seed_t *seed, ngx_rbtree_node_t *node) {
  ngx_rbtree_delete(&seed->tree, node);
  return NGX_OK;
}

static void rbtree_walk_real(rbtree_seed_t *seed, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel, ngx_int_t (*callback)(rbtree_seed_t *, ngx_rbtree_node_t *)) {
  ngx_rbtree_node_t  *left, *right;
  if(node == sentinel || node == NULL) {
    return;
  }
  left = node->left;
  right = node->right;
  rbtree_walk_real(seed, left, sentinel, callback);
  rbtree_walk_real(seed, right, sentinel, callback);
  callback(seed, node);
}

ngx_int_t rbtree_walk(rbtree_seed_t *seed, ngx_int_t (*callback)(rbtree_seed_t *seed, ngx_rbtree_node_t *)) {
  ngx_rbtree_node_t         *sentinel = seed->tree.sentinel;
  rbtree_walk_real(seed, seed->tree.root, sentinel, callback);
  return NGX_OK;
}

ngx_int_t rbtree_init(rbtree_seed_t *seed, char *name,   ngx_str_t *(*id)(void *), uint32_t (*hash)(ngx_str_t *), ngx_int_t (*compare)(ngx_str_t *, ngx_str_t *)) {
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
  ngx_rbtree_init(&seed->tree, &seed->sentinel, &rbtree_insert_generic);
  return NGX_OK;
}