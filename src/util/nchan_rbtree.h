#ifndef NCHAN_RBTREE_UTIL_HEADER
#define NCHAN_RBTREE_UTIL_HEADER

#define NCHAN_RBTREE_DBG 0

typedef struct {
  char              *name;
  ngx_rbtree_t       tree;
  ngx_rbtree_node_t  sentinel;
  ngx_uint_t         allocd_nodes;
  ngx_uint_t         active_nodes;
  void              *(*id)(void *node);
  uint32_t           (*hash)(void *);
  ngx_int_t          (*compare)(void *, void *);
  #if NCHAN_RBTREE_DBG   
  //ngx_rbtree_node_t *actives[4096]; //super-heavy debugging
#endif
} rbtree_seed_t;


#if NCHAN_RBTREE_DBG 
typedef struct ngx_rbtree_debug_node_s ngx_rbtree_debug_node_t;

typedef struct {
  struct ngx_rbtree_debug_node_s  *prev;
  struct ngx_rbtree_debug_node_s  *next;
} ngx_rbtree_debug_node_link_t;

struct ngx_rbtree_debug_node_s {
  ngx_rbtree_node_t              node;
  ngx_rbtree_debug_node_link_t   allocd;
  ngx_rbtree_debug_node_link_t   active;
}; //ngx_rbtree_debug_node_t;
#endif

typedef enum {RBTREE_WALK_LEFT, RBTREE_WALK_RIGHT, RBTREE_WALK_LEFT_RIGHT, RBTREE_WALK_STOP} rbtree_walk_direction_t;

typedef ngx_int_t (*rbtree_walk_callback_pt)(rbtree_seed_t *, void *node_data, void *privdata);
typedef rbtree_walk_direction_t (*rbtree_walk_conditional_callback_pt)(rbtree_seed_t *, void *, void *);

ngx_int_t rbtree_init(rbtree_seed_t *seed, char *name, void *(*id)(void *), uint32_t (*hash)(void *), ngx_int_t (*compare)(void *, void *));
unsigned             rbtree_empty(rbtree_seed_t *, rbtree_walk_callback_pt, void *data);

ngx_rbtree_node_t   *rbtree_create_node(rbtree_seed_t *, size_t data_size);
ngx_int_t            rbtree_insert_node(rbtree_seed_t *, ngx_rbtree_node_t *);
ngx_int_t            rbtree_remove_node(rbtree_seed_t *, ngx_rbtree_node_t *);
ngx_int_t            rbtree_destroy_node(rbtree_seed_t *, ngx_rbtree_node_t *);

ngx_rbtree_node_t   *rbtree_find_node(rbtree_seed_t *, void *id);

ngx_int_t            rbtree_walk(rbtree_seed_t *seed, rbtree_walk_callback_pt, void *data);
ngx_int_t            rbtree_walk_incr(rbtree_seed_t *seed, rbtree_walk_callback_pt callback, void *data);
ngx_int_t            rbtree_walk_decr(rbtree_seed_t *seed, rbtree_walk_callback_pt callback, void *data);
ngx_int_t            rbtree_walk_writesafe(rbtree_seed_t *seed, int (*include)(void *), rbtree_walk_callback_pt callback, void *data);

ngx_int_t            rbtree_conditional_walk(rbtree_seed_t *seed, rbtree_walk_conditional_callback_pt, void *data);

ngx_rbtree_node_t   *rbtree_node_from_data(void *);


#define rbtree_data_from_node(node) ((void *)(&node[1]))
#define rbtree_node_from_data(data) (ngx_rbtree_node_t *)((u_char *)data - sizeof(ngx_rbtree_node_t))

#endif /*NCHAN_RBTREE_UTIL_HEADER*/
