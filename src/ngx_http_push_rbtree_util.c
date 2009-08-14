#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

static ngx_http_push_node_t * get_node(ngx_str_t * id, ngx_http_push_ctx_t * ctx, ngx_slab_pool_t * shpool, ngx_log_t * log);
static ngx_http_push_node_t * find_node(ngx_str_t * id, ngx_http_push_ctx_t * ctx, ngx_slab_pool_t * shpool, ngx_log_t * log);
static void ngx_rbtree_generic_insert(	ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel, int (*compare)(const ngx_rbtree_node_t *left, const ngx_rbtree_node_t *right));
static void ngx_http_push_rbtree_insert(ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel);
static int ngx_http_push_compare_rbtree_node(const ngx_rbtree_node_t *v_left, const ngx_rbtree_node_t *v_right);

static ngx_http_push_node_t * 
find_node(	ngx_str_t 			* id, 
			ngx_http_push_ctx_t	* ctx, 
			ngx_slab_pool_t		* shpool, 
			ngx_log_t 			* log)
{
    uint32_t                         hash;
    ngx_rbtree_node_t               *node, *sentinel;
    ngx_int_t                        rc;
    ngx_http_push_node_t  			*up;
	ngx_http_push_node_t			*trash = NULL;

    hash = ngx_crc32_short(id->data, id->len);

    node = ctx->rbtree->root;
    sentinel = ctx->rbtree->sentinel;

    while (node != sentinel) {

        if (hash < node->key) {
            node = node->left;
            continue;
        }

        if (hash > node->key) {
            node = node->right;
            continue;
        }

		//every search is responsible for deleting one empty node, if it comes across one
		if (trash==NULL) {
			if(ngx_queue_empty(& ((ngx_http_push_node_t *) node)->message_queue->queue)
				&& ((ngx_http_push_node_t *) node)->request==NULL )
			{
				trash = (ngx_http_push_node_t *) node;
			}
		}
		
        /* hash == node->key */

        do {
            up = (ngx_http_push_node_t *) node;

            rc = ngx_memn2cmp(id->data, up->id.data, id->len, up->id.len);

            if (rc == 0) {
				if(trash !=NULL && trash != up){ //take out the trash
					ngx_rbtree_delete(ctx->rbtree, (ngx_rbtree_node_t *) trash);
					ngx_slab_free_locked(shpool, trash);
				}				
				return up;
            }

            node = (rc < 0) ? node->left : node->right;

        } while (node != sentinel && hash == node->key);

        break;
    }
	//not found
	
	if(trash != NULL && up!=trash){ //take out your trash
		ngx_rbtree_delete(ctx->rbtree, (ngx_rbtree_node_t *) trash);
		ngx_slab_free_locked(shpool, trash);
	}
	return NULL;
}

//find a node. if node not found, make one, insert it, and return that.
 static ngx_http_push_node_t * 
get_node(	ngx_str_t 			* id, 
			ngx_http_push_ctx_t	* ctx, 
			ngx_slab_pool_t		* shpool, 
			ngx_log_t 			* log)
{
	ngx_http_push_node_t  			*up=NULL;
	up = find_node(id, ctx, shpool, log);
	if(up != NULL) { //we found our node
		return up;
	}
	up = ngx_slab_alloc_locked(shpool, sizeof(ngx_http_push_node_t) + id->len); //dirty dirty
	if (up == NULL) {
		//a failed malloc ain't the end of the world. take out the trash anyway
		return NULL;
	}
	
	
	up->id.data = (u_char *) up + sizeof(ngx_http_push_node_t); //contiguous piggy
	up->id.len = (u_char) id->len;
	ngx_memcpy(up->id.data, id->data, up->id.len);
	up->key = ngx_crc32_short(id->data, id->len);
	ngx_rbtree_insert(ctx->rbtree, (ngx_rbtree_node_t *) up);

	up->request=NULL;
	//initialize queues
	up->message_queue = ngx_slab_alloc_locked (shpool, sizeof(ngx_http_push_msg_t));
	ngx_queue_init(&up->message_queue->queue);
	return up;
}


static void 
ngx_rbtree_generic_insert(	ngx_rbtree_node_t *temp, 
							ngx_rbtree_node_t *node, 
							ngx_rbtree_node_t *sentinel, 
							int (*compare)(const ngx_rbtree_node_t *left, const ngx_rbtree_node_t *right))
{
    for ( ;; ) {
        if (node->key < temp->key) {

            if (temp->left == sentinel) {
                temp->left = node;
                break;
            }

            temp = temp->left;

        } else if (node->key > temp->key) {

            if (temp->right == sentinel) {
                temp->right = node;
                break;
            }

            temp = temp->right;

        } else { /* node->key == temp->key */
            if (compare(node, temp) < 0) {

                if (temp->left == sentinel) {
                    temp->left = node;
                    break;
                }

                temp = temp->left;

            } else {

                if (temp->right == sentinel) {
                    temp->right = node;
                    break;
                }

                temp = temp->right;
            }
        }
    }

    node->parent = temp;
    node->left = sentinel;
    node->right = sentinel;
    ngx_rbt_red(node);
}


static void
ngx_http_push_rbtree_insert(	ngx_rbtree_node_t *temp, 
										ngx_rbtree_node_t *node, 
										ngx_rbtree_node_t *sentinel) 
{
    ngx_rbtree_generic_insert(temp, node, sentinel, ngx_http_push_compare_rbtree_node);
}

static int 
ngx_http_push_compare_rbtree_node(	const ngx_rbtree_node_t *v_left,
									const ngx_rbtree_node_t *v_right)
{
    ngx_http_push_node_t *left, *right;
    left = (ngx_http_push_node_t *) v_left;
    right = (ngx_http_push_node_t *) v_right;
	
	return ngx_memn2cmp(left->id.data, right->id.data, left->id.len, right->id.len);
}