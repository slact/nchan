#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#include <ngx_http_push_module.h>
#include <ngx_http_push_rbtree_util.c>

static ngx_command_t  ngx_http_push_commands[] = {

    { ngx_string("push_source"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_push_source,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },
	
    { ngx_string("push_destination"),
      NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_push_destination,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

      ngx_null_command
};


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

static ngx_http_push_msg_t * ngx_http_push_dequeue_message(ngx_http_push_node_t * node) //does NOT free associated memory.
{
	ngx_queue_t *sentinel = &node->message_queue->queue; 
	if(ngx_queue_empty(sentinel)) {
		return NULL;
	}
	ngx_queue_t 			*qmsg = ngx_queue_head(sentinel);
	ngx_queue_remove(qmsg);
	return ngx_queue_data(qmsg, ngx_http_push_msg_t, queue);
}

static ngx_str_t  ngx_http_push_id = ngx_string("push_id"); //id variable name
static char *ngx_http_push_source(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_core_loc_conf_t  *clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module); 
	ngx_http_push_loc_conf_t *plcf = conf;                                    
    clcf->handler = ngx_http_push_source_handler;                                       
	plcf->index = ngx_http_get_variable_index(cf, &ngx_http_push_id);         
    if (plcf->index == NGX_ERROR) {                                           
        return NGX_CONF_ERROR;                                                
    }                                                                         
    return NGX_CONF_OK;
}

static char *ngx_http_push_destination(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_core_loc_conf_t  *clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module); 
	ngx_http_push_loc_conf_t *plcf = conf;                                    
    clcf->handler = ngx_http_push_destination_handler;                                       
	plcf->index = ngx_http_get_variable_index(cf, &ngx_http_push_id);         
    if (plcf->index == NGX_ERROR) {                                           
        return NGX_CONF_ERROR;                                                
    }                                                                         
    return NGX_CONF_OK;
}




static ngx_str_t shm_name = ngx_string("push_module"); //shared memory segment name
static void * ngx_http_push_create_loc_conf(ngx_conf_t *cf) {
	//create shared memory
	ngx_http_push_loc_conf_t 	*lcf;
	ngx_rbtree_t		 		*tree;
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
		tree = ngx_pcalloc(cf->pool, sizeof(ngx_rbtree_t));
		if (tree == NULL) {
			return NGX_CONF_ERROR;
		}
		lcf->shm_zone->data = tree;
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
    ngx_rbtree_t   					*tree;

    tree = shm_zone->data;

    shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;

    tree = ngx_slab_alloc(shpool, sizeof(ngx_rbtree_t));
    if (tree == NULL) {
        return NGX_ERROR;
    }

    sentinel = ngx_slab_alloc(shpool, sizeof(ngx_rbtree_node_t));
    if (sentinel == NULL) {
        return NGX_ERROR;
    }

    ngx_rbtree_sentinel_init(sentinel);

    tree->root = sentinel;
    tree->sentinel = sentinel;
    tree->insert = ngx_http_push_rbtree_insert;

    return NGX_OK;
}

// here go the handlers
#define ngx_http_push_set_id(id, vv, r, cf, log, ret_err)                     \
    (vv) = ngx_http_get_indexed_variable(r, (cf)->index);                     \
    if ((vv) == NULL || (vv)->not_found || (vv)->len == 0) {                  \
        ngx_log_error(NGX_LOG_ERR, log, 0,                                    \
                      "the \"$push_id\" variable is not set");                \
        return ret_err;                                                       \
    }                                                                         \
	id.data=vv->data;\
	id.len=vv->len

static ngx_int_t ngx_http_push_destination_handler(ngx_http_request_t *r)
{
	ngx_http_variable_value_t		*vv;
    ngx_http_push_loc_conf_t		*cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
    ngx_slab_pool_t                 *shpool = (ngx_slab_pool_t *) cf->shm_zone->shm.addr;
    ngx_rbtree_t 					*tree = cf->shm_zone->data;
	ngx_str_t                        id;
    ngx_http_push_node_t  			*node;
	
	ngx_http_push_set_id(id, vv, r, cf, r->connection->log, NGX_ERROR);

    ngx_shmtx_lock(&shpool->mutex);
	node = get_node(&id, tree, shpool, r->connection->log);
	ngx_shmtx_unlock(&shpool->mutex);
	
    if (node == NULL) { //unable to allocate node
		return NGX_ERROR;
    }
	ngx_http_discard_request_body(r); //don't care about the rest of the request
	
	if (node->request!=NULL) { //oh shit, someone's already waiting for a message on this id.
		//TODO: add settings for this sort of thing.
		ngx_http_finalize_request(node->request, NGX_HTTP_CONFLICT); //bump the old request. 
		node->request=NULL; //finalize_request probably set node->request to NULL on cleanup anyway, but just to be sure.
	}
	
	ngx_http_push_msg_t				*msg = ngx_http_push_dequeue_message(node);
	if (msg==NULL) { //message queue is empty
		//this means we must wait for a message.
		
		//clean that shit afterwards
		ngx_pool_cleanup_t              *cln = ngx_pool_cleanup_add(r->pool, sizeof(u_char));
		if (cln == NULL) { //make sure we can.
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
		cln->handler= (ngx_pool_cleanup_pt) ngx_http_push_destination_request_cleanup;
		cln->data = node;
		
		ngx_shmtx_lock(&shpool->mutex);
		node->request = r;
		ngx_shmtx_unlock(&shpool->mutex);
		r->read_event_handler = ngx_http_test_reading; //definitely test to see if the connection got closed or something	
		
		return NGX_DONE; //and wait.
	}
	else {
		//output the message
		ngx_int_t		rc = ngx_http_push_send_message_to_destination_request(r, msg);
		ngx_slab_free(shpool, msg);
		return rc;
	}
}

static void ngx_http_push_source_body_handler(ngx_http_request_t * r) {
    ngx_str_t                       id;
    ngx_http_push_loc_conf_t		*cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
	ngx_slab_pool_t                 *shpool =  (ngx_slab_pool_t *) cf->shm_zone->shm.addr;
	ngx_http_variable_value_t		*vv;
	ngx_http_request_body_t			*body;
    /* Is it a POST connection */
    if (r->method != NGX_HTTP_POST) {
        ngx_http_finalize_request(r, NGX_HTTP_NOT_ALLOWED);
		return;
    }
	
	//TODO: r->method _= NGX_HTTP_DELETE
	ngx_http_push_set_id(id, vv, r, cf, r->connection->log, );

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
		msg = ngx_slab_alloc_locked(shpool, sizeof(ngx_http_push_msg_t) + len + (r->headers_in.content_type==NULL ? 0 : r->headers_in.content_type->value.len));
		if(msg == NULL) {
			ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
			return;
		}
		//store the body
		msg->is_file=is_file;
		msg->str.len = len;
		msg->str.data = (u_char *) msg + sizeof(ngx_http_push_msg_t);
		ngx_memcpy(msg->str.data, offset, len);
		
		//store the content-type
		if(r->headers_in.content_type!=NULL) {
			msg->content_type.len=r->headers_in.content_type->value.len;
			msg->content_type.data=(u_char *) msg + sizeof(ngx_http_push_msg_t) + len;
			ngx_memcpy(msg->content_type.data, r->headers_in.content_type->value.data, msg->content_type.len);
		}
		else {
			msg->content_type.len=0;
			msg->content_type.data=NULL;
		}
		
		//get the node while we're at it.
		ngx_http_push_node_t  			*node = get_node(&id, (ngx_rbtree_t *) cf->shm_zone->data, shpool, r->connection->log);
		ngx_shmtx_unlock(&shpool->mutex);
		
		if (node == NULL) {
			ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "push module: unable to allocate new node");
			ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
			return;
		}
		
		if(node->request != NULL)
		{ //we've got some requests to take care of.
			ngx_http_finalize_request(node->request, ngx_http_push_send_message_to_destination_request(node->request, msg));
			ngx_slab_free(shpool, msg);
			r->headers_out.status=NGX_HTTP_CREATED;
		}
		else { //no requests waiting. queue it up!
			ngx_queue_insert_tail(&node->message_queue->queue, &msg->queue);
			r->headers_out.status=NGX_HTTP_OK;
			r->headers_out.status_line.len =sizeof("202 Accepted")- 1;
			r->headers_out.status_line.data=(u_char *) "202 Accepted";
		}
	}
	
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

static ngx_int_t ngx_http_push_send_message_to_destination_request(ngx_http_request_t *r, ngx_http_push_msg_t * msg)
{
	ngx_buf_t         			*b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
	ngx_chain_t        			 out;
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
	
	//set the content-type, if present
	if (msg->content_type.data!=NULL) {
		r->headers_out.content_type.len=msg->content_type.len;
		r->headers_out.content_type.data = ngx_palloc(r->pool, msg->content_type.len);
		ngx_memcpy(r->headers_out.content_type.data, msg->content_type.data, msg->content_type.len);
	}
	
	ngx_http_send_header(r);
	return ngx_http_output_filter(r, &out);	
}

static void ngx_http_push_destination_request_cleanup(ngx_http_push_node_t * node) {
	if (node!=NULL) {
		node->request=NULL;
	}
}
