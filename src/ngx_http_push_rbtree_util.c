#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>


static ngx_http_push_channel_t * ngx_http_push_get_channel(ngx_str_t * id, ngx_log_t * log, time_t timeout);
static ngx_http_push_channel_t * ngx_http_push_find_channel(ngx_str_t * id, ngx_log_t * log);
static ngx_int_t ngx_http_push_delete_channel_locked(ngx_http_push_channel_t *trash);

static ngx_http_push_channel_t *	ngx_http_push_clean_channel_locked(ngx_http_push_channel_t * channel);

static void     ngx_rbtree_generic_insert(	ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel, int (*compare)(const ngx_rbtree_node_t *left, const ngx_rbtree_node_t *right));
static void     ngx_http_push_rbtree_insert(ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel);
static int      ngx_http_push_compare_rbtree_node(const ngx_rbtree_node_t *v_left, const ngx_rbtree_node_t *v_right);
static ngx_int_t ngx_http_push_delete_node_locked(ngx_rbtree_t *tree, ngx_rbtree_node_t *trash, ngx_slab_pool_t *shpool);

static ngx_http_push_channel_t *	ngx_http_push_clean_channel_locked(ngx_http_push_channel_t * channel) {
	ngx_queue_t                 *sentinel = &channel->message_queue->queue;
	time_t                       now = ngx_time();
	ngx_http_push_msg_t         *msg=NULL;
	while(!ngx_queue_empty(sentinel)){
		msg = ngx_queue_data(ngx_queue_head(sentinel), ngx_http_push_msg_t, queue);
		if (msg!=NULL && msg->expires != 0 && now > msg->expires) {
			ngx_http_push_delete_message_locked(channel, msg, ngx_http_push_shpool);
		}
		else { //definitely a message left to send
			return NULL;
		}
	}
	//at this point, the queue is empty
	return (channel->subscribers==0 && (channel->expires <= now)) ? channel : NULL; //if no waiting requests and channel expired, return this channel to be deleted
}

static ngx_int_t ngx_http_push_delete_channel_locked(ngx_http_push_channel_t *trash) {
	ngx_int_t                      res;
	res = ngx_http_push_delete_node_locked(&((ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data)->tree, (ngx_rbtree_node_t *)trash, ngx_http_push_shpool);
	if(res==NGX_OK) {
		((ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data)->channels--;
		return NGX_OK;
	}
	return res;
	
}

static ngx_int_t ngx_http_push_delete_node_locked(ngx_rbtree_t *tree, ngx_rbtree_node_t *trash, ngx_slab_pool_t *shpool) {
//assume the shm zone is already locked
	if(trash != NULL){ //take out the trash		
		ngx_rbtree_delete(tree, trash);
		
		//delete the worker-subscriber queue
		ngx_queue_t                *sentinel = (ngx_queue_t *)(&((ngx_http_push_channel_t *)trash)->workers_with_subscribers);
		ngx_queue_t                *cur = ngx_queue_head(sentinel);
		ngx_queue_t                *next;
		while(cur!=sentinel) {
			next = ngx_queue_next(cur);
			ngx_slab_free_locked(shpool, cur);
			cur = next;
		}
		
		ngx_slab_free_locked(shpool, trash);
		return NGX_OK;
	}
	return NGX_DECLINED;
}

static ngx_http_push_channel_t * ngx_http_push_find_channel(ngx_str_t *id, ngx_log_t *log) {
	ngx_rbtree_t                   *tree = &((ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data)->tree;
	uint32_t                        hash;
	ngx_rbtree_node_t              *node, *sentinel;
	ngx_int_t                       rc;
	ngx_http_push_channel_t        *up = NULL;
	ngx_http_push_channel_t        *trash[] = { NULL, NULL, NULL };
	
	ngx_uint_t                      i, trashed=0;
	if (tree==NULL) {
		return NULL;
	}
	
	hash = ngx_crc32_short(id->data, id->len);

	node = tree->root;
	sentinel = tree->sentinel;

	while (node != sentinel) {
		
		//every search is responsible for deleting a couple of empty, if it comes across them
		if (trashed < (sizeof(trash) / sizeof(*trash))) {
			if((trash[trashed]=ngx_http_push_clean_channel_locked((ngx_http_push_channel_t *) node))!=NULL) {
				trashed++;
			}
		}
		
		if (hash < node->key) {
			node = node->left;
			continue;
		}

		if (hash > node->key) {
			node = node->right;
			continue;
		}
		
		/* hash == node->key */

		do {
			up = (ngx_http_push_channel_t *) node;

			rc = ngx_memn2cmp(id->data, up->id.data, id->len, up->id.len);

			if (rc == 0) {
				//found
				for(i=0; i<trashed; i++) {
					if(trash[i] != up){ //take out the trash
						ngx_http_push_delete_channel_locked(trash[i]);
					}
				}
				ngx_http_push_clean_channel_locked(up);
				return up;
			}

			node = (rc < 0) ? node->left : node->right;

		} while (node != sentinel && hash == node->key);

		break;
	}
	//not found	
	for(i=0; i<trashed; i++) {
		ngx_http_push_delete_channel_locked(trash[i]);
	}
	return NULL;
}

//find a channel by id. if channel not found, make one, insert it, and return that.
 static ngx_http_push_channel_t *	ngx_http_push_get_channel(ngx_str_t *id, ngx_log_t *log, time_t timeout) {
	ngx_rbtree_t                   *tree;
	ngx_http_push_channel_t        *up=ngx_http_push_find_channel(id, log);
	
	if(up != NULL) { //we found our channel
		return up;
	}
	tree = &((ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data)->tree;
	if((up = ngx_http_push_slab_alloc_locked(sizeof(*up) + id->len + sizeof(ngx_http_push_msg_t)))==NULL) {
		return NULL;
	}
	up->id.data = (u_char *) (up+1); //contiguous piggy
	up->message_queue = (ngx_http_push_msg_t *) (up->id.data + id->len);
	
	up->id.len = (u_char) id->len;
	ngx_memcpy(up->id.data, id->data, up->id.len);
	up->node.key = ngx_crc32_short(id->data, id->len);
	ngx_rbtree_insert(tree, (ngx_rbtree_node_t *) up);

	//initialize queues
	ngx_queue_init(&up->message_queue->queue);
	up->messages=0;

	ngx_queue_init(&up->workers_with_subscribers.queue);
	up->subscribers=0;

	up->expires = ngx_time() + timeout;
	
	((ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data)->channels++;
	
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

#define ngx_http_push_walk_rbtree(apply)                                            \
	ngx_http_push_rbtree_walker(&((ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data)->tree, (ngx_slab_pool_t *)ngx_http_push_shm_zone->shm.addr, apply, ((ngx_http_push_shm_data_t *) ngx_http_push_shm_zone->data)->tree.root)

static void ngx_http_push_rbtree_walker(ngx_rbtree_t *tree, ngx_slab_pool_t *shpool, ngx_int_t (*apply)(ngx_http_push_channel_t * channel, ngx_slab_pool_t * shpool), ngx_rbtree_node_t *node) {
	ngx_rbtree_node_t              *sentinel = tree->sentinel;
	
	if(node!=sentinel) {
		apply((ngx_http_push_channel_t *)node, shpool);
		if(node->left!=NULL) {
			ngx_http_push_rbtree_walker(tree, shpool, apply, node->left);
		}
		if(node->right!=NULL) {
			ngx_http_push_rbtree_walker(tree, shpool, apply, node->right);
		}
	}
}

static void	ngx_http_push_rbtree_insert(ngx_rbtree_node_t *temp,  ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel) 
{
	ngx_rbtree_generic_insert(temp, node, sentinel, ngx_http_push_compare_rbtree_node);
}

static int ngx_http_push_compare_rbtree_node(const ngx_rbtree_node_t *v_left, const ngx_rbtree_node_t *v_right)
{
	ngx_http_push_channel_t *left = (ngx_http_push_channel_t *) v_left, *right = (ngx_http_push_channel_t *) v_right;
	return ngx_memn2cmp(left->id.data, right->id.data, left->id.len, right->id.len);
}
