void ngx_http_push_rbtree_insert(ngx_rbtree_node_t *temp,ngx_rbtree_node_t *node,ngx_rbtree_node_t *sentinel);
void ngx_http_push_rbtree_walker(ngx_rbtree_t *tree,ngx_int_t(*apply)(ngx_http_push_channel_t *channel),ngx_rbtree_node_t *node);
void ngx_rbtree_generic_insert(ngx_rbtree_node_t *temp,ngx_rbtree_node_t *node,ngx_rbtree_node_t *sentinel,int(*compare)(const ngx_rbtree_node_t *left,const ngx_rbtree_node_t *right));
ngx_http_push_channel_t *ngx_http_push_get_channel(ngx_str_t *id,time_t timeout,ngx_shm_zone_t *shm_zoneg);
ngx_http_push_channel_t *ngx_http_push_find_channel(ngx_str_t *id,time_t timeout,ngx_shm_zone_t *shm_zone);
ngx_int_t ngx_http_push_delete_channel_locked(ngx_http_push_channel_t *trash,ngx_shm_zone_t *shm_zone);
ngx_http_push_channel_t *ngx_http_push_clean_channel_locked(ngx_http_push_channel_t *channel);
#define ngx_http_push_walk_rbtree(apply, shm_zone)                                            \
ngx_http_push_rbtree_walker(&((ngx_http_push_shm_data_t *) shm_zone->data)->tree, apply, ((ngx_http_push_shm_data_t *) shm_zone->data)->tree.root)
