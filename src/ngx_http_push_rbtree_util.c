#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

static ngx_http_push_node_t * get_node(ngx_str_t * id, ngx_rbtree_t * tree, ngx_slab_pool_t * shpool, ngx_log_t * log);
static ngx_http_push_node_t * find_node(ngx_str_t * id, ngx_rbtree_t * tree, ngx_slab_pool_t * shpool, ngx_log_t * log);
static void     ngx_rbtree_generic_insert(	ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel, int (*compare)(const ngx_rbtree_node_t *left, const ngx_rbtree_node_t *right));
static void     ngx_http_push_rbtree_insert(ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel);
static int      ngx_http_push_compare_rbtree_node(const ngx_rbtree_node_t *v_left, const ngx_rbtree_node_t *v_right);
static ngx_int_t ngx_http_push_delete_node(ngx_rbtree_t *tree, ngx_rbtree_node_t *trash, ngx_slab_pool_t *shpool);

static ngx_http_push_node_t *	clean_node(ngx_http_push_node_t * node, ngx_slab_pool_t * shpool) {
	ngx_queue_t                 *sentinel = &node->message_queue->queue;
	time_t                       now = ngx_time();
	ngx_http_push_msg_t			*msg=NULL;
	while(!ngx_queue_empty(sentinel)){
		msg = ngx_queue_data(ngx_queue_head(sentinel), ngx_http_push_msg_t, queue);
		if (msg!=NULL && msg->expires != 0 && now > msg->expires) {
			ngx_queue_remove((&msg->queue));
			ngx_slab_free_locked(shpool, msg);
		}
		else { //definitely a message left to send
			return NULL;
		}
	}
	//at this point, the queue is empty
	return node->request==NULL ? node : NULL; //if no request, return this node to be deleted
}
static ngx_int_t ngx_http_push_delete_node(ngx_rbtree_t *tree, ngx_rbtree_node_t *trash, ngx_slab_pool_t *shpool) {
//assume the shm zone is already locked
	if(trash != NULL){ //take out the trash		
		ngx_rbtree_delete(tree, trash);
		ngx_slab_free_locked(shpool, trash);
		return NGX_OK;
	}
	return NGX_DECLINED;
}

static ngx_http_push_node_t *	find_node(
			ngx_str_t              *id, 
			ngx_rbtree_t           *tree, 
			ngx_slab_pool_t        *shpool, 
			ngx_log_t              *log)
{
    uint32_t                        hash;
    ngx_rbtree_node_t              *node, *sentinel;
    ngx_int_t                       rc;
    ngx_http_push_node_t           *up;
	ngx_http_push_node_t           *trash = NULL;
	if (tree==NULL) {
		return NULL;
	}
	
    hash = ngx_crc32_short(id->data, id->len);
	
    node = tree->root;
    sentinel = tree->sentinel;

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
			trash=clean_node((ngx_http_push_node_t *) node, shpool);
		}
		
        /* hash == node->key */

        do {
            up = (ngx_http_push_node_t *) node;

            rc = ngx_memn2cmp(id->data, up->id.data, id->len, up->id.len);

            if (rc == 0) {
				if(trash != up){ //take out the trash
					ngx_http_push_delete_node(tree, (ngx_rbtree_node_t *) trash, shpool);
				}
				clean_node(up, shpool);
				return up;
            }

            node = (rc < 0) ? node->left : node->right;

        } while (node != sentinel && hash == node->key);

        break;
    }
	//not found	
	if(trash != up){ //take out the trash
		ngx_http_push_delete_node(tree, (ngx_rbtree_node_t *) trash, shpool);
	}
	return NULL;
}

//find a node. if node not found, make one, insert it, and return that.
 static ngx_http_push_node_t *	get_node(
			ngx_str_t              *id, 
			ngx_rbtree_t           *tree, 
			ngx_slab_pool_t        *shpool, 
			ngx_log_t              *log)
{
	ngx_http_push_node_t           *up=find_node(id, tree, shpool, log);
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
	up->node.key = ngx_crc32_short(id->data, id->len);
	ngx_rbtree_insert(tree, (ngx_rbtree_node_t *) up);

	up->request=NULL;
	//initialize queues
	up->message_queue = ngx_slab_alloc_locked (shpool, sizeof(ngx_http_push_msg_t));
	if (up->message_queue==NULL) {
		ngx_slab_free_locked(shpool, up);
		return NULL;
	}
	ngx_queue_init(&up->message_queue->queue);
	return up;
}


static void	ngx_rbtree_generic_insert(
				ngx_rbtree_node_t *temp, 
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


static void	ngx_http_push_rbtree_insert(
				ngx_rbtree_node_t *temp, 
				ngx_rbtree_node_t *node, 
				ngx_rbtree_node_t *sentinel) 
{
    ngx_rbtree_generic_insert(temp, node, sentinel, ngx_http_push_compare_rbtree_node);
}

static int ngx_http_push_compare_rbtree_node(
				const ngx_rbtree_node_t *v_left,
				const ngx_rbtree_node_t *v_right)
{
    ngx_http_push_node_t *left, *right;
    left = (ngx_http_push_node_t *) v_left;
    right = (ngx_http_push_node_t *) v_right;
	
	return ngx_memn2cmp(left->id.data, right->id.data, left->id.len, right->id.len);
}