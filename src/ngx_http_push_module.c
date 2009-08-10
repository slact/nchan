#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

//with the declarations

static char *ngx_http_push_source(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_push_source_handler(ngx_http_request_t * r);
static void ngx_http_push_source_body_handler(ngx_http_request_t * r);

static char *ngx_http_push_destination(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_push_destination_handler(ngx_http_request_t * r);

typedef struct {
	ngx_str_t			 id;
	ngx_shm_zone_t		 *shm_zone;
} ngx_http_push_loc_conf_t;

//message queue
typedef struct {
    ngx_queue_t				queue;
	unsigned				is_file:1;
	ngx_str_t				str;
} ngx_http_push_msg_t;

typedef struct {
	ngx_queue_t				 queue;
	ngx_http_request_t		*request;
} ngx_http_push_request_t;

typedef struct ngx_http_push_node_s ngx_http_push_node_t;
struct ngx_http_push_node_s {
	ngx_rbtree_key_t       			 key;
	ngx_rbtree_node_t				*left;
	ngx_rbtree_node_t				*right;
	ngx_rbtree_node_t     			*parent;
    ngx_http_push_msg_t				*message_queue;
	ngx_http_push_request_t			*request_queue;
	ngx_str_t						 id;
};

typedef struct {
    ngx_rbtree_t                   *rbtree;
} ngx_http_push_ctx_t;

//request cleanup stuff
typedef struct{
	ngx_http_push_request_t		*req;
	ngx_slab_pool_t				*shpool;
} ngx_http_push_request_cleanup_t;
static void ngx_http_push_cleanup_destination_request(ngx_http_push_request_cleanup_t * req);

static void * ngx_http_push_create_loc_conf(ngx_conf_t *cf);
static ngx_int_t ngx_http_push_send_to_destination(ngx_http_push_node_t * node, ngx_slab_pool_t *shpool);
static ngx_int_t ngx_http_push_init_shm_zone(ngx_shm_zone_t * shm_zone, void * data);
static char * ngx_http_push_id(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static ngx_command_t  ngx_http_push_commands[] = {

    { ngx_string("push_source"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_push_source,
      0,
      0,
      NULL },
	
    { ngx_string("push_destination"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_push_destination,
      0,
      0,
      NULL },
	  
	{ ngx_string("push_id"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_push_id,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

      ngx_null_command
};
static char * ngx_http_push_id(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
	ngx_http_compile_complex_value_t   ccv;
	ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));
	if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
}
//missing in nginx < 0.7.?
#ifndef ngx_queue_insert_tail
#define ngx_queue_insert_tail(h, x)                                           \
    (x)->prev = (h)->prev;                                                    \
    (x)->prev->next = x;                                                      \
    (x)->next = h;                                                            \
    (h)->prev = x
#endif

/********************************
 **** Red-black tree stuff
 *******************************/
static ngx_http_push_node_t * get_node(ngx_str_t * id, ngx_http_push_ctx_t * ctx, ngx_slab_pool_t * shpool, ngx_log_t * log);
static void ngx_rbtree_generic_insert(	ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel, int (*compare)(const ngx_rbtree_node_t *left, const ngx_rbtree_node_t *right));
static void ngx_http_push_rbtree_insert(ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel);
static int ngx_http_push_compare_rbtree_node(const ngx_rbtree_node_t *v_left, const ngx_rbtree_node_t *v_right);
 
 static ngx_http_push_node_t * 
get_node(	ngx_str_t 			* id, 
			ngx_http_push_ctx_t	* ctx, 
			ngx_slab_pool_t		* shpool, 
			ngx_log_t 			* log)
{
    uint32_t                         hash;
    ngx_rbtree_node_t               *node, *sentinel;
    ngx_int_t                        rc;
    ngx_http_push_node_t  			*up, *trash;
	u_char							 nodes_collected=0;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, log, 0, "push module: find_node %V", id);

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
		if (nodes_collected<1) {
			up = (ngx_http_push_node_t *) node;
			if(ngx_queue_empty(&up->message_queue->queue) && ngx_queue_empty(&up->request_queue->queue)  ) {
				ngx_log_debug1(NGX_LOG_DEBUG_HTTP, log, 0, "push module: found an empty node to trash");
				trash = (ngx_http_push_node_t *) node;
				nodes_collected++;
			}
		}
		
        /* hash == node->key */

        do {
            up = (ngx_http_push_node_t *) node;

            rc = ngx_memn2cmp(id->data, up->id.data, id->len, up->id.len);

            if (rc == 0) {
                ngx_log_debug0(NGX_LOG_DEBUG_HTTP, log, 0, "push module: found node");
				if(nodes_collected>0 && trash != up){ //take out the trash
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
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, log, 0, "push module: can't find node. gonna make one");

	up = ngx_slab_alloc_locked(shpool, sizeof(ngx_http_push_node_t) + id->len); //dirty dirty
	if (up == NULL) {
		//a failed malloc ain't the end of the world. take out the trash anyway
		if(nodes_collected>0){
			ngx_rbtree_delete(ctx->rbtree, (ngx_rbtree_node_t *) trash);
			ngx_slab_free_locked(shpool, trash);
		}
		return NULL;
	}
	up->id.data = (u_char *) up + sizeof(ngx_http_push_node_t); //contiguous piggy
	up->id.len = (u_char) id->len;
	ngx_memcpy(up->id.data, id->data, up->id.len);
	up->key = hash;
	ngx_rbtree_insert(ctx->rbtree, (ngx_rbtree_node_t *) up);

	//initialize queues
	up->request_queue = ngx_slab_alloc_locked (shpool, sizeof(ngx_http_push_request_t));
	up->message_queue = ngx_slab_alloc_locked (shpool, sizeof(ngx_http_push_msg_t));
	ngx_queue_init(&up->request_queue->queue);
	ngx_queue_init(&up->message_queue->queue);

	if(nodes_collected>0 && up!=trash){ //take out your trash
		ngx_rbtree_delete(ctx->rbtree, (ngx_rbtree_node_t *) trash);
		ngx_slab_free_locked(shpool, trash);
	}
	
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

/***********************************
 **** end Red-black tree stuff 
 ***********************************/

//enough declarations for now

static char *ngx_http_push_source(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t  *clcf;
    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_push_source_handler;
    return NGX_CONF_OK;
}

static char *ngx_http_push_destination(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t  *clcf;
    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_push_destination_handler;
    return NGX_CONF_OK;
}



static ngx_http_module_t  ngx_http_push_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,				         		   /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_http_push_create_loc_conf,         /* create location configuration */
    NULL                                   /* merge location configuration */
};

ngx_module_t  ngx_http_push_module = {
    NGX_MODULE_V1,
    &ngx_http_push_module_ctx,             /* module context */
    ngx_http_push_commands,                /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,					               /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};

ngx_str_t shm_name = ngx_string("push_module");
static void * ngx_http_push_create_loc_conf(ngx_conf_t *cf) {
	//create shared memory
	ngx_http_push_loc_conf_t 	*lcf;
	ngx_http_push_ctx_t 		*ctx;
	lcf = ngx_pcalloc(cf->pool, sizeof(ngx_http_push_loc_conf_t));
	if(lcf == NULL) {
		return NGX_CONF_ERROR;
	}
	
	lcf->shm_zone = ngx_shared_memory_add(cf, &shm_name, 8 * ngx_pagesize, &ngx_http_push_module);
    if (lcf->shm_zone == NULL) {
        return NGX_CONF_ERROR;
    }
	
	if(lcf->shm_zone->data==NULL) //first time
	{
		ctx = ngx_pcalloc(cf->pool, sizeof(ngx_http_push_ctx_t));
		if (ctx == NULL) {
			return NGX_CONF_ERROR;
		}
		lcf->shm_zone->data = ctx;
		lcf->shm_zone->init = ngx_http_push_init_shm_zone;
	}
	return lcf;
}

// shared memory zone initializer
static ngx_int_t ngx_http_push_init_shm_zone(ngx_shm_zone_t * shm_zone, void *data)
{
	if (data) { /* we're being reloaded, propagate the data "cookie" */
		shm_zone->data = data;
		return NGX_OK;
	}

    ngx_slab_pool_t                 *shpool;
    ngx_rbtree_node_t               *sentinel;
    ngx_http_push_ctx_t   			*ctx;

    ctx = shm_zone->data;

    shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;

    ctx->rbtree = ngx_slab_alloc(shpool, sizeof(ngx_rbtree_t));
    if (ctx->rbtree == NULL) {
        return NGX_ERROR;
    }

    sentinel = ngx_slab_alloc(shpool, sizeof(ngx_rbtree_node_t));
    if (sentinel == NULL) {
        return NGX_ERROR;
    }

    ngx_rbtree_sentinel_init(sentinel);

    ctx->rbtree->root = sentinel;
    ctx->rbtree->sentinel = sentinel;
    ctx->rbtree->insert = ngx_http_push_rbtree_insert;

    return NGX_OK;
}

// here go the handlers

static ngx_int_t ngx_http_push_destination_handler(ngx_http_request_t *r)
{
    ngx_str_t                        id;
    ngx_slab_pool_t                 *shpool;
    ngx_http_push_loc_conf_t		*cf;
    ngx_http_push_ctx_t 			*ctx;
    ngx_http_push_node_t  			*pushed;
	ngx_http_push_request_t			*pending_request;
	
	//TODO: r->method _= NGX_HTTP_DELETE

    cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
    if (&cf->id == NULL) {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "push module: no id found in POST upload req");
        return NGX_ERROR;
    }
	id = cf->id;

    if (cf->shm_zone == NULL) {
        ngx_log_error(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "push module: no shm zone found");
        return NGX_ERROR;
    }

    ctx = cf->shm_zone->data;

    shpool = (ngx_slab_pool_t *) cf->shm_zone->shm.addr;

    ngx_shmtx_lock(&shpool->mutex);
	pushed = get_node(&id, ctx, shpool, r->connection->log);
	ngx_shmtx_unlock(&shpool->mutex);
	
    if (pushed == NULL) { //node exists
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "push module: unable to allocate new node");
		return NGX_ERROR;
    } 
	
	//now insert it into the request list
	ngx_shmtx_lock(&shpool->mutex);
	pending_request = ngx_slab_alloc_locked(shpool, sizeof(ngx_http_push_request_t)); 
	pending_request->request = r;
	ngx_queue_insert_tail(&pushed->request_queue->queue, &pending_request->queue);
	ngx_shmtx_unlock(&shpool->mutex);
	
	//set up request cleanup callbacks
	ngx_http_cleanup_t * cln = (ngx_http_cleanup_t *) ngx_http_cleanup_add(r, sizeof(ngx_http_push_request_cleanup_t));
	ngx_http_push_request_cleanup_t * cleanup_data = (ngx_http_push_request_cleanup_t *) cln->data;
	cleanup_data->req=pending_request;
	cleanup_data->shpool=shpool;
	
	cln->handler=(ngx_http_cleanup_pt) ngx_http_push_cleanup_destination_request;
	
	return ngx_http_push_send_to_destination(pushed, shpool);
}

static void ngx_http_push_cleanup_destination_request(ngx_http_push_request_cleanup_t * cln) {
	if (cln->req->queue.next != NULL) {
		ngx_queue_remove(&cln->req->queue);
		ngx_slab_free(cln->shpool, cln->req);
		//TODO: delete from the tree if no more requests.
	}
}

//check if there are any reqyests to send messages to
static ngx_int_t ngx_http_push_send_to_destination(ngx_http_push_node_t * node, ngx_slab_pool_t * shpool) {
	
	ngx_queue_t *req_sentinel = &node->request_queue->queue;
	if(ngx_queue_empty(req_sentinel) || ngx_queue_empty(&node->message_queue->queue)) {
		return NGX_DONE;
	}
	
	ngx_queue_t 			*qmsg = ngx_queue_head(&node->message_queue->queue);
	ngx_http_push_msg_t		*msg  = ngx_queue_data(qmsg, ngx_http_push_msg_t, queue);
	
	ngx_queue_remove(qmsg);
	
	ngx_queue_t 				*qr; //request queue thinger
	ngx_http_push_request_t		*req_item; //request queue
	ngx_http_request_t			*r; //request queue
	ngx_buf_t         			*b;
	ngx_chain_t        			 out;
	do {
		qr = ngx_queue_head(req_sentinel);
		req_item = ngx_queue_data(qr, ngx_http_push_request_t, queue);
		r=req_item->request;
		ngx_queue_remove(qr);
		ngx_slab_free(shpool, req_item);
		
		//write the request headers
		r->headers_out.status=NGX_HTTP_OK;
		r->headers_out.content_type.len = sizeof("text/plain") - 1;
		r->headers_out.content_type.data = (u_char *) "text/plain";
		ngx_http_send_header(r);
		
		b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
		if (b == NULL) {
			ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
				"Failed to allocate response buffer.");
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
		
		//now the body...
		if (!msg->is_file) {
			ngx_str_t	* body = ngx_pcalloc(r->pool, sizeof(ngx_str_t) + msg->str.len);
			body->len = msg->str.len;
			body->data = (u_char *) body + sizeof(ngx_str_t);
			ngx_memcpy(body->data, msg->str.data, body->len);
			b->pos = body->data;
			b->last = body->data + body->len;
			b->memory=1; //read-only
			b->last_buf=1;
		}
		else {
			ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "Reading from file not yet implemented");
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
		out.buf = b;
		out.next = NULL;
		ngx_http_output_filter(r, &out);
		
	} while(!ngx_queue_empty(req_sentinel));
	ngx_slab_free(shpool, msg);
	return NGX_OK;
}
ngx_str_t all_is_well = ngx_string("yes.");
static void ngx_http_push_source_body_handler(ngx_http_request_t * r) {
    ngx_str_t                       id;
    ngx_slab_pool_t                 *shpool;
    ngx_http_push_loc_conf_t		*cf;
    ngx_http_push_ctx_t 			*ctx;
    ngx_http_push_node_t  			*pushed;
	
	ngx_http_request_body_t			*body;
    /* Is it a POST connection */
    if (r->method != NGX_HTTP_POST) {
        ngx_http_finalize_request(r, NGX_HTTP_NOT_ALLOWED);
		return;
    }
	
	//TODO: r->method _= NGX_HTTP_DELETE

    cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
	if (&cf->id==NULL) {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "push module: no id found in POST upload req");
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
    }
	id = cf->id;

    if (cf->shm_zone == NULL) {
        ngx_log_error(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "push module: no shm zone found");
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
    }

    ctx = cf->shm_zone->data;

    shpool = (ngx_slab_pool_t *) cf->shm_zone->shm.addr;

    ngx_shmtx_lock(&shpool->mutex);
	pushed = get_node(&id, ctx, shpool, r->connection->log);
	ngx_shmtx_unlock(&shpool->mutex);
	
    if (pushed == NULL) { //node exists
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "push module: unable to allocate new node");
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
    } 
	
	//save the freaking body
	body = r->request_body;
	if (body!=NULL) {
		u_char 					len, *offset;
		unsigned				is_file;
		ngx_http_push_msg_t		*msg;
		
		if (body->temp_file) {
			len = body->temp_file->file.name.len;
			offset = body->temp_file->file.name.data;
			is_file=1;
		}
		else if (body->buf) {
			len = body->buf->last - body->buf->pos;
			offset = body->buf->pos;
			is_file=0;
		}
		else {
			//not yet
			return;
		}

		//create a new message
		ngx_shmtx_lock(&shpool->mutex);
		msg = ngx_slab_alloc_locked(shpool, sizeof(ngx_http_push_msg_t) + len);
		if(msg == NULL) {
			ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
			return;
		}
		msg->is_file=is_file;
		msg->str.len = len;
		msg->str.data = (u_char *) msg + sizeof(ngx_http_push_msg_t);
		ngx_memcpy(msg->str.data, offset, len);
		
		//now insert it into the message queue
		ngx_queue_insert_tail(&pushed->message_queue->queue, &msg->queue);
		
		ngx_shmtx_unlock(&shpool->mutex);
	}
	
	ngx_int_t		rc = ngx_http_push_send_to_destination(pushed, shpool);
	if(rc==NGX_OK) {
		r->headers_out.status=NGX_HTTP_CREATED;
		
	}
	else if (rc==NGX_DONE){
		r->headers_out.status=NGX_HTTP_OK;
		r->headers_out.status_line.len =sizeof("202 Accepted")- 1;
		r->headers_out.status_line.data=(u_char *) "202 Accepted";
	}
	else { //error
		r->headers_out.status=rc; //probably a 500 error
	}
	//now a response is in order
	ngx_http_discard_request_body(r); //don't care about the rest of the request
	
	r->headers_out.content_length_n = 0;
	r->header_only = 1;
	
	ngx_http_finalize_request(r, ngx_http_send_header(r));
	return;
}

static ngx_int_t
ngx_http_push_source_handler(ngx_http_request_t * r)
{
	ngx_int_t				rc;
	
	/* Instruct ngx_http_read_client_request_body to store the request
       body entirely in a memory buffer or in a file */
    r->request_body_in_single_buf = 1;
    r->request_body_in_persistent_file = 1;
    r->request_body_in_clean_file = 1;
    r->request_body_file_log_level = 0;

	rc = ngx_http_read_client_request_body(r, ngx_http_push_source_body_handler);
	if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
		return rc;
	}
	return NGX_DONE;
}
