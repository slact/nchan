#ifndef NCHAN_RBTREE_UTIL_HEADER
#define NCHAN_RBTREE_UTIL_HEADER

typedef struct {
  char              *name;
  ngx_rbtree_t       tree;
  ngx_rbtree_node_t  sentinel;
  ngx_str_t         *(*id)(void *node);
  uint32_t           (*hash)(ngx_str_t *str);
  ngx_int_t          (*compare)(ngx_str_t *id1, ngx_str_t *id2);
} rbtree_seed_t;


ngx_int_t            rbtree_init(rbtree_seed_t *, char *, ngx_str_t *(*id)(void *), uint32_t (*hash)(ngx_str_t *), ngx_int_t (*compare)(ngx_str_t *, ngx_str_t *));
ngx_int_t            rbtree_shutdown(rbtree_seed_t *);

ngx_rbtree_node_t   *rbtree_create_node(rbtree_seed_t *, size_t);
ngx_int_t            rbtree_insert_node(rbtree_seed_t *, ngx_rbtree_node_t *);
ngx_int_t            rbtree_remove_node(rbtree_seed_t *, ngx_rbtree_node_t *);
ngx_int_t            rbtree_destroy_node(rbtree_seed_t *, ngx_rbtree_node_t *);

ngx_rbtree_node_t   *rbtree_find_node(rbtree_seed_t *, ngx_str_t *);
ngx_int_t            rbtree_walk(rbtree_seed_t *seed, ngx_int_t (*callback)(rbtree_seed_t *seed, ngx_rbtree_node_t *, void *data), void *data);

ngx_rbtree_node_t   *rbtree_node_from_data(void *);

#endif /*NCHAN_RBTREE_UTIL_HEADER*/