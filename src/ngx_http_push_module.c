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
	ngx_http_core_loc_conf_t	*clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module); 
	ngx_http_push_loc_conf_t	*plcf = conf;                                    
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
	
	lcf->shm_zone = ngx_shared_memory_add(cf, &shm_name, 128 * ngx_pagesize, &ngx_http_push_module);
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

    shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;

    shm_zone->data = ngx_slab_alloc(shpool, sizeof(ngx_rbtree_t));
	tree = shm_zone->data;
    if (tree == NULL) {
        return NGX_ERROR;
    }

    sentinel = ngx_slab_alloc(shpool, sizeof(ngx_rbtree_node_t));
    if (sentinel == NULL) {
        return NGX_ERROR;
    }

	ngx_rbtree_init(tree, sentinel, ngx_http_push_rbtree_insert);

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

	
#define ngx_http_push_create_buf_copy(buf, cbuf, pool, pool_alloc)			  \
	(cbuf) = pool_alloc((pool), sizeof(ngx_buf_t) + (ngx_buf_in_memory((buf)) ? ngx_buf_size((buf)) : 0));\
	if ((cbuf)!=NULL) {														  \
		if(ngx_buf_in_memory((buf))) {										  \
			(cbuf)->pos = ((u_char *) (cbuf)) + sizeof(ngx_buf_t);			  \
			(cbuf)->last = (cbuf)->pos + ngx_buf_size((buf));				  \
			(cbuf)->start=(cbuf)->pos; 										  \
			(cbuf)->end = (cbuf)->start + ngx_buf_size((buf));				  \
			ngx_memcpy((cbuf)->pos, (buf)->pos, ngx_buf_size((buf))); 		  \
			(cbuf)->memory=ngx_buf_in_memory_only((buf)) ? 1 : 0;			  \
		}																	  \
		if ((buf)->in_file) {												  \
			cbuf->file_pos = (buf)->file_pos;								  \
			cbuf->file_pos = (buf)->file_pos;								  \
			cbuf->temp_file = (buf)->temp_file;								  \
			cbuf->file = (buf)->file; 										  \
		}																	  \
	}
	
static ngx_int_t ngx_http_push_destination_handler(ngx_http_request_t *r)
{
	ngx_http_variable_value_t		*vv;
    ngx_http_push_loc_conf_t		*cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
    ngx_slab_pool_t                 *shpool = (ngx_slab_pool_t *) cf->shm_zone->shm.addr;
	ngx_str_t                        id;
    ngx_http_push_node_t  			*node;
	ngx_http_push_msg_t				*msg;
	ngx_http_request_t				*existing_request;
	
	ngx_http_push_set_id(id, vv, r, cf, r->connection->log, NGX_ERROR);

    ngx_shmtx_lock(&shpool->mutex);
	node = get_node(&id, cf->shm_zone->data, shpool, r->connection->log);
	if (node == NULL) { //unable to allocate node
		ngx_shmtx_unlock(&shpool->mutex);
		return NGX_ERROR;
    }
	existing_request=node->request;
	ngx_shmtx_unlock(&shpool->mutex);
	
	ngx_http_discard_request_body(r); //don't care about the rest of this request
	
	if (existing_request!=NULL) { //oh shit, someone's already waiting for a message on this id.
		//TODO: add settings for this sort of thing.
		ngx_http_finalize_request(existing_request, NGX_HTTP_CONFLICT); //bump the old request. 
		ngx_shmtx_lock(&shpool->mutex);
		node->request=NULL; //finalize_request cleanup will do this anyway, but we want it done now.
		ngx_shmtx_unlock(&shpool->mutex);
	}
	
	ngx_shmtx_lock(&shpool->mutex);
	msg = ngx_http_push_dequeue_message(node);
	ngx_shmtx_unlock(&shpool->mutex);
	if (msg==NULL) { //message queue is empty
		//this means we must wait for a message.
		
		ngx_shmtx_lock(&shpool->mutex);
		node->request = r;
		ngx_shmtx_unlock(&shpool->mutex);
		r->read_event_handler = ngx_http_test_reading; //definitely test to see if the connection got closed or something.
		
		//attach a cleaner to remove the request from the node, if need be
		ngx_pool_cleanup_t              *cln = ngx_pool_cleanup_add(r->pool, sizeof(ngx_http_push_destination_cleanup_t));
		if (cln == NULL) { //make sure we can.
			return NGX_ERROR;
		}
		cln->handler = (ngx_pool_cleanup_pt) ngx_http_push_destination_cleanup;
		((ngx_http_push_destination_cleanup_t *) cln->data)->node = node;
		((ngx_http_push_destination_cleanup_t *) cln->data)->request = r;
		((ngx_http_push_destination_cleanup_t *) cln->data)->shpool = shpool;
		
		return NGX_DONE; //and wait.
	}
	else {
		//output the message		
		ngx_chain_t		*out; //output chain
		ngx_int_t		rc;
		ngx_shmtx_lock(&shpool->mutex);
		rc = ngx_http_push_set_destination_header(r, &msg->content_type); //content type is copied
		out = ngx_http_push_create_output_chain(r, msg->buf); 	//buffer is copied
		//we no longer need the message and can free its shm slab.
		ngx_slab_free_locked(shpool, msg->buf); //separate block, remember?
		ngx_slab_free_locked(shpool, msg); 
		ngx_shmtx_unlock(&shpool->mutex);
		if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
			return rc;
		}
		rc = ngx_http_send_header(r);
		if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
			return rc;
		}
		
		return ngx_http_push_set_destination_body(r, out);
	}
}

static void ngx_http_push_source_body_handler(ngx_http_request_t * r) {
    ngx_str_t                       id;
    ngx_http_push_loc_conf_t		*cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
	ngx_slab_pool_t                 *shpool =  (ngx_slab_pool_t *) cf->shm_zone->shm.addr;
	ngx_http_variable_value_t		*vv;
	ngx_buf_t						*buf, *buf_copy;
	ngx_http_push_node_t  			*node;
	ngx_http_request_t				*r_client;
    /* Is it a POST connection */
    if (r->method != NGX_HTTP_POST) {
        ngx_http_finalize_request(r, NGX_HTTP_NOT_ALLOWED);
		return;
    }
	
	//TODO: r->method _= NGX_HTTP_DELETE
	ngx_http_push_set_id(id, vv, r, cf, r->connection->log, );

	ngx_shmtx_lock(&shpool->mutex);
	node = get_node(&id, (ngx_rbtree_t *) cf->shm_zone->data, shpool, r->connection->log);
	r_client = node->request;
	ngx_shmtx_unlock(&shpool->mutex);
	
	if (node == NULL) {
		ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "push module: unable to allocate new node");
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
	}
	
	//save the freaking body
	buf = r->request_body->bufs->buf;
	if (buf==NULL) {
		return; //not yet i think?
	}
	ngx_http_push_msg_t		*msg;
	size_t 					 content_type_len = (r->headers_in.content_type==NULL ? 0 : r->headers_in.content_type->value.len);
	
	if (r_client==NULL) { //no clients are waiting for the message. create the message in shared mempry for storage
		msg = ngx_slab_alloc(shpool, sizeof(ngx_http_push_msg_t) + content_type_len);
		if (msg==NULL) {
			ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "push module: unable to allocate message in shared memory");
			ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
			return;
		}
		//create a buffer copy in shared mem
		ngx_shmtx_lock(&shpool->mutex);
		ngx_http_push_create_buf_copy(buf, buf_copy, shpool, ngx_slab_alloc_locked);
		ngx_shmtx_unlock(&shpool->mutex);
		if (buf_copy==NULL) {
			ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "push module: unable to allocate buffer in shared memory");
			ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
			return;
		}
		ngx_shmtx_lock(&shpool->mutex);
		msg->buf=buf_copy;
		ngx_queue_insert_tail(&node->message_queue->queue, &msg->queue);

		//store the content-type
		if(content_type_len>0) {
			msg->content_type.len=r->headers_in.content_type->value.len;
			msg->content_type.data=(u_char *) msg + sizeof(ngx_http_push_msg_t);
			ngx_memcpy(msg->content_type.data, r->headers_in.content_type->value.data, msg->content_type.len);
		}
		else {
			msg->content_type.len=0;
			msg->content_type.data=NULL;
		}		
		ngx_shmtx_unlock(&shpool->mutex);
		//okay, done storing. now respond to the source request
		r->headers_out.status=NGX_HTTP_OK;
		r->headers_out.status_line.len =sizeof("202 Accepted")- 1;
		r->headers_out.status_line.data=(u_char *) "202 Accepted";
	}
	else{
		ngx_shmtx_lock(&shpool->mutex);
		node->request = NULL;
		ngx_shmtx_unlock(&shpool->mutex);
				
		ngx_int_t		rc;
		rc = ngx_http_push_set_destination_header(r_client, (content_type_len>0 ? &r->headers_in.content_type->value : NULL));
		if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
			ngx_http_finalize_request(r_client, rc);
			ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
			return;
		}
		rc = ngx_http_send_header(r_client);
		if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
			ngx_http_finalize_request(r_client, rc);
			ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
			return;
		}
		
		ngx_http_finalize_request(r_client, ngx_http_push_set_destination_body(r_client, ngx_http_push_create_output_chain(r_client, buf)));
		
		r->headers_out.status=NGX_HTTP_CREATED;
	}
	
	r->headers_out.content_length_n = 0;
	r->header_only = 1;
	
	ngx_http_finalize_request(r, ngx_http_send_header(r));
	return;
}

static ngx_int_t ngx_http_push_source_handler(ngx_http_request_t * r)
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

static ngx_int_t ngx_http_push_set_destination_header(ngx_http_request_t *r, ngx_str_t *content_type) {
	//content-type is _copied_
	if (content_type!=NULL && content_type->data!=NULL) {
		r->headers_out.content_type.len=content_type->len;
		r->headers_out.content_type.data = ngx_palloc(r->pool, content_type->len);
		if(r->headers_out.content_type.data==NULL) {
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
		ngx_memcpy(r->headers_out.content_type.data, content_type->data, content_type->len);
	}
	r->headers_out.status=NGX_HTTP_OK;
	return NGX_OK;
}

static ngx_chain_t * ngx_http_push_create_output_chain(ngx_http_request_t *r, ngx_buf_t *buf)
{
	//buffer is _copied_
	ngx_chain_t		*out = ngx_pcalloc(r->pool, sizeof(ngx_chain_t));
	ngx_buf_t		*buf_copy;
	ngx_http_push_create_buf_copy(buf, buf_copy, r->pool, ngx_pcalloc);
	
	if (out==NULL || buf_copy==NULL) {
		return NULL;
	}
	
	buf_copy->last_buf = 1; 
	out->buf = buf_copy;
	out->next = NULL;
	return out;	
}

static ngx_int_t ngx_http_push_set_destination_body(ngx_http_request_t *r, ngx_chain_t *out)
{
	if (out==NULL) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	return ngx_http_output_filter(r, out);	
}


static void ngx_http_push_destination_cleanup(ngx_http_push_destination_cleanup_t *data) {
	ngx_shmtx_lock(&data->shpool->mutex);
	if(data->node->request == data->request) {
		data->node->request=NULL;
	}
	ngx_shmtx_unlock(&data->shpool->mutex);
}
