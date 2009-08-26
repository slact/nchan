/*
 *	Copyright 2009 Leo Ponomarev.
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#include <ngx_http_push_module.h>
#include <ngx_http_push_rbtree_util.c>
#include <ngx_http_push_module_setup.c>

static ngx_http_push_msg_t * ngx_http_push_dequeue_message(ngx_http_push_node_t * node){ //does NOT free associated memory.
	ngx_queue_t                    *sentinel = &node->message_queue->queue; 
	if(ngx_queue_empty(sentinel)) {
		return NULL;
	}
	ngx_queue_t                    *qmsg = ngx_queue_head(sentinel);
	ngx_queue_remove(qmsg);
	return ngx_queue_data(qmsg, ngx_http_push_msg_t, queue);
}

static ngx_int_t ngx_http_push_set_id(ngx_str_t *id, ngx_http_request_t *r, ngx_http_push_loc_conf_t *cf) {
	ngx_http_variable_value_t      *vv = ngx_http_get_indexed_variable(r, cf->index);
    if (vv == NULL || vv->not_found || vv->len == 0) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "http_push module: the \"$push_id\" variable is required but is not set");
        return NGX_ERROR;
    }
	if (id!=NULL){
		id->data=vv->data; //no need to copy anything? ok...
		id->len=vv->len;
		return NGX_OK;
	} else {
		return NGX_ERROR;
	}
}

//this is a macro because i don't want to mess with the alloc_function function pointer
#define ngx_http_push_create_buf_copy(buf, cbuf, pool, pool_alloc)            \
	(cbuf) = pool_alloc((pool), sizeof(ngx_buf_t) + (ngx_buf_in_memory((buf)) ? ngx_buf_size((buf)) : 0));\
	if ((cbuf)!=NULL) {                                                       \
		if(ngx_buf_in_memory((buf))) {                                        \
			(cbuf)->pos = ((u_char *) (cbuf)) + sizeof(ngx_buf_t);            \
			(cbuf)->last = (cbuf)->pos + ngx_buf_size((buf));                 \
			(cbuf)->start=(cbuf)->pos;                                        \
			(cbuf)->end = (cbuf)->start + ngx_buf_size((buf));                \
			ngx_memcpy((cbuf)->pos, (buf)->pos, ngx_buf_size((buf)));         \
			(cbuf)->memory=ngx_buf_in_memory_only((buf)) ? 1 : 0;             \
		}                                                                     \
		if ((buf)->in_file) {                                                 \
			(cbuf)->file_pos = (buf)->file_pos;                               \
			(cbuf)->file_last = (buf)->file_last;                             \
			(cbuf)->temp_file = (buf)->temp_file;                             \
			(cbuf)->file = (buf)->file;                                       \
			(cbuf)->in_file = 1;                                              \
		}                                                                     \
	}

static ngx_int_t ngx_http_push_destination_handler(ngx_http_request_t *r) {
    ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
	ngx_str_t                       id;
    ngx_http_push_node_t           *node;
	ngx_http_push_msg_t            *msg;
	ngx_http_request_t             *existing_request;
	if (r->method != NGX_HTTP_POST) {
		return NGX_HTTP_NOT_ALLOWED;
    }
	
	if(ngx_http_push_set_id(&id, r, cf) !=NGX_OK) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}

    ngx_shmtx_lock(&shpool->mutex);
	node = get_node(&id, ngx_http_push_shm_zone->data, shpool, r->connection->log);
	if (node == NULL) { //unable to allocate node
		ngx_shmtx_unlock(&shpool->mutex);
		return NGX_ERROR;
    }	
	ngx_shmtx_unlock(&shpool->mutex);
	
	ngx_http_discard_request_body(r); //don't care about the rest of this request
	
	ngx_shmtx_lock(&shpool->mutex);
	if (node->request!=NULL) { //oh shit, someone's already waiting for a message on this id.
		existing_request = node->request;
		node->request=NULL; //finalize_request cleanup will do this anyway, but we want it done now.
		ngx_shmtx_unlock(&shpool->mutex);
		ngx_http_finalize_request(existing_request, NGX_HTTP_CONFLICT); //bump the old request. 
		ngx_shmtx_lock(&shpool->mutex);
	}
	
	msg = ngx_http_push_dequeue_message(node); //expired messages are removed from queue during get_node()

	ngx_shmtx_unlock(&shpool->mutex);
	if (msg==NULL) { //message queue is empty
		//this means we must wait for a message.
		
		ngx_shmtx_lock(&shpool->mutex);
		node->request = r;
		ngx_shmtx_unlock(&shpool->mutex);
		r->read_event_handler = ngx_http_test_reading; //definitely test to see if the connection got closed or something.
		
		//attach a cleaner to remove the request from the node, if need be
		ngx_pool_cleanup_t         *cln = ngx_pool_cleanup_add(r->pool, sizeof(ngx_http_push_destination_cleanup_t));
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
		ngx_chain_t                *out; //output chain
		ngx_int_t                   rc;
		ngx_shmtx_lock(&shpool->mutex);
		ngx_file_t                 *file = NULL;
		if(msg->buf->in_file){
			ngx_file_t             *bfile = msg->buf->file;
			file = ngx_pcalloc(r->pool, sizeof(ngx_file_t) + bfile->name.len + 1);
			if(file==NULL){
				ngx_shmtx_unlock(&shpool->mutex);
				return NGX_HTTP_INTERNAL_SERVER_ERROR;
			}
			file->offset=bfile->offset;
			file->sys_offset=bfile->sys_offset;
			file->name.len=bfile->name.len;
			file->name.data=(u_char *) file + sizeof(ngx_file_t);
			ngx_memcpy(file->name.data, bfile->name.data, file->name.len); //the null at the end please
			ngx_slab_free_locked(shpool, bfile); 
			ngx_shmtx_unlock(&shpool->mutex);
			
			file->log=r->connection->log;
			//the following assumes file->name.data is already null-terminated
			file->fd=ngx_open_file(file->name.data, NGX_FILE_RDONLY, NGX_FILE_OPEN, NGX_FILE_OWNER_ACCESS); 
			if(file->fd!=NGX_INVALID_FILE){
				ngx_http_push_add_pool_cleaner_delete_file(r->pool, file);
			}
			else {
				ngx_slab_free(shpool, msg->buf);
				ngx_slab_free(shpool, msg); 
				return NGX_HTTP_INTERNAL_SERVER_ERROR;
			}
			
			ngx_shmtx_lock(&shpool->mutex);
		}
		rc = ngx_http_push_set_destination_header(r, &msg->content_type); //content type is copied
		out = ngx_http_push_create_output_chain(r, msg->buf); 	//buffer is copied
		out->buf->file=file;
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
    ngx_http_push_loc_conf_t        *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
	ngx_slab_pool_t                 *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
	ngx_buf_t                       *buf, *buf_copy;
	ngx_http_push_node_t            *node;
	ngx_http_request_t              *r_client;
    /* Is it a POST connection */
    if (r->method != NGX_HTTP_POST && r->method!=NGX_HTTP_PUT) {
        ngx_http_finalize_request(r, NGX_HTTP_NOT_ALLOWED);
		return;
    }
	
	//TODO: r->method _= NGX_HTTP_DELETE
	if(ngx_http_push_set_id(&id, r, cf) !=NGX_OK) {
		return;
	}

	ngx_shmtx_lock(&shpool->mutex);
	node = get_node(&id, (ngx_rbtree_t *) ngx_http_push_shm_zone->data, shpool, r->connection->log);
	r_client = node->request;
	ngx_shmtx_unlock(&shpool->mutex);
	
	if (node == NULL) {
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate new node");
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
	}
	
	//save the freaking body
	if(r->request_body->temp_file==NULL) {//everything in the first buffer
		buf=r->request_body->bufs->buf;
	}
	else if(r->request_body->bufs->next!=NULL) {
		buf=r->request_body->bufs->next->buf;
	}
	else {
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: request body buffer not found");
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return; 
	}
	if (buf==NULL) { //why no buffer?...
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: request body buffer is null");
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return; 
	}
	ngx_http_push_msg_t            *msg;
	size_t                          content_type_len; 
	content_type_len = (r->headers_in.content_type==NULL ? 0 : r->headers_in.content_type->value.len);
	
	if (r_client==NULL && r->method == NGX_HTTP_POST && cf->buffer_enabled!=0) {
	//no clients are waiting for the message, and buffers are not disabled. create the message in shared memory for storage
		msg = ngx_slab_alloc(shpool, sizeof(ngx_http_push_msg_t) + content_type_len);
		if (msg==NULL) {
			ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate message in shared memory");
			ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
			return;
		}
		
		//create a buffer copy in shared mem
		ngx_shmtx_lock(&shpool->mutex);
		ngx_http_push_create_buf_copy(buf, buf_copy, shpool, ngx_slab_alloc_locked);
		ngx_shmtx_unlock(&shpool->mutex);
		if (buf_copy==NULL) {
			ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate buffer in shared memory");
			ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
			return;
		}
		ngx_shmtx_lock(&shpool->mutex);
		if (buf_copy->in_file) {
			ngx_file_t             *file;
			file = ngx_slab_alloc_locked(shpool, sizeof(ngx_file_t) + buf->file->name.len + 1);
			//the +1 is for the null byte at the end --------------------------------------^^
			if(file==NULL){
				ngx_shmtx_unlock(&shpool->mutex);
				ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
				return;
			}
			file->fd=NGX_INVALID_FILE;
			file->log=NULL;
			file->offset=buf->file->offset;
			file->sys_offset=buf->file->sys_offset;
			file->name.len=buf->file->name.len;
			file->name.data=(u_char *) file + sizeof(ngx_file_t);
			ngx_memcpy(file->name.data, buf->file->name.data, file->name.len);
			buf_copy->file=file;
		}
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
		//set the expiration time
		time_t                      timeout = cf->buffer_timeout;
		msg->expires= timeout==0 ? 0 : (ngx_time() + timeout);
		ngx_shmtx_unlock(&shpool->mutex);
		//okay, done storing. now respond to the source request
		r->headers_out.status=NGX_HTTP_OK;
		r->headers_out.status_line.len =sizeof("202 Accepted")- 1;
		r->headers_out.status_line.data=(u_char *) "202 Accepted";
	}
	else if(r_client!=NULL) {
		ngx_shmtx_lock(&shpool->mutex);
		node->request = NULL;
		ngx_shmtx_unlock(&shpool->mutex);
				
		ngx_int_t                   rc;
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
		
		if(buf->in_file && buf->file->fd!=NGX_INVALID_FILE){
			//delete file when the push_source request finishes
			ngx_http_push_add_pool_cleaner_delete_file(r->pool, buf->file);
		}
		
		ngx_http_finalize_request(r_client, ngx_http_push_set_destination_body(r_client, ngx_http_push_create_output_chain(r_client, buf)));
		
		r->headers_out.status=NGX_HTTP_CREATED;
	}
	else {	//r_client==NULL && r->method == NGX_HTTP_PUT is all that remains
		r->headers_out.status=NGX_HTTP_OK;
	}
	
	r->headers_out.content_length_n = 0;
	r->header_only = 1;
	
	ngx_http_finalize_request(r, ngx_http_send_header(r));
	return;
}

static ngx_int_t ngx_http_push_source_handler(ngx_http_request_t * r) {
	ngx_int_t                       rc;
	
	/* Instruct ngx_http_read_client_request_body to store the request
       body entirely in a memory buffer or in a file */
    r->request_body_in_single_buf = 1;
    r->request_body_in_persistent_file = 1;
    r->request_body_in_clean_file = 0;
    r->request_body_file_log_level = 0;

	rc = ngx_http_read_client_request_body(r, ngx_http_push_source_body_handler);
	if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
		return rc;
	}
	return NGX_DONE;
}

static ngx_int_t ngx_http_push_set_destination_header(ngx_http_request_t *r, ngx_str_t *content_type) {
	//content-type is _copied_
	if (content_type!=NULL && content_type->data!=NULL && content_type->len > 0) {
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

static ngx_chain_t * ngx_http_push_create_output_chain(ngx_http_request_t *r, ngx_buf_t *buf) {
	//buffer is _copied_
	ngx_chain_t                    *out = ngx_pcalloc(r->pool, sizeof(ngx_chain_t));
	ngx_buf_t                      *buf_copy;
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

static ngx_int_t ngx_http_push_add_pool_cleaner_delete_file(ngx_pool_t *pool, ngx_file_t *file) {
	ngx_pool_cleanup_t             *cln = ngx_pool_cleanup_add(pool, sizeof(ngx_pool_cleanup_file_t));
	ngx_pool_cleanup_file_t        *clnf;
	if (cln == NULL) {
		return NGX_ERROR;
	}
	cln->handler = ngx_pool_delete_file;
	clnf = cln->data;
	clnf->fd = file->fd;
	clnf->name = file->name.data;
	clnf->log = file->log;
	return NGX_OK;
}