/*
 *	Copyright 2009 Leo Ponomarev.
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#include <ngx_http_push_module.h>
#include <ngx_http_push_rbtree_util.c>
#include <ngx_http_push_module_setup.c>

//shpool must be locked. No memory is freed.
static ngx_http_push_msg_t * ngx_http_push_get_last_message(ngx_http_push_node_t * node){
	ngx_queue_t                    *sentinel = &node->message_queue->queue; 
	if(ngx_queue_empty(sentinel)) {
		return NULL;
	}
	ngx_queue_t                    *qmsg = ngx_queue_last(sentinel);
	return ngx_queue_data(qmsg, ngx_http_push_msg_t, queue);
}

//shpool must be locked. No memory is freed.
static ngx_http_push_msg_t * ngx_http_push_get_message(ngx_http_push_node_t * node){
	ngx_queue_t                    *sentinel = &node->message_queue->queue; 
	if(ngx_queue_empty(sentinel)) {
		return NULL;
	}
	ngx_queue_t                    *qmsg = ngx_queue_head(sentinel);
	return ngx_queue_data(qmsg, ngx_http_push_msg_t, queue);
}

//shpool must be locked. No memory is freed.
static ngx_http_push_msg_t * ngx_http_push_dequeue_message(ngx_http_push_node_t * node) { 
	ngx_http_push_msg_t            *msg = ngx_http_push_get_message(node);
	ngx_queue_remove((&msg->queue));
	node->message_queue_size--;
	return msg;
}

//shpool must be locked. No memory is freed.
static ngx_http_push_listener_t * ngx_http_push_dequeue_listener(ngx_http_push_node_t * node) {
	ngx_queue_t                    *sentinel = &node->listener_queue->queue; 
	if(ngx_queue_empty(sentinel)) { return NULL; }
	ngx_queue_t                    *qlistener = ngx_queue_last(sentinel);
	ngx_http_push_listener_t       *listener = ngx_queue_data(qlistener, ngx_http_push_listener_t, queue);
	ngx_queue_remove(qlistener);
	node->message_queue_size--;
	return listener;
}

//shpool must be locked.
static ngx_http_push_listener_t * ngx_http_push_queue_listener_request(ngx_http_push_node_t * node, ngx_http_request_t *r, ngx_slab_t shpool) {
	ngx_http_push_listener_t       *listener = ngx_slab_alloc(shpool, sizeof(ngx_http_push_listener_t));
	if(listener==NULL) { //unable to allocate request queue element
		return NULL;
	}
	ngx_shmtx_lock(&shpool->mutex);
	ngx_queue_insert_tail(&node->listener_queue->queue, &listener->queue);
	listener->request = r;
	node->listener_queue_size++;
	return listener;
	
}

// remove a message from queue and free all associated memory. assumes shpool is already locked.
static void ngx_http_push_delete_message_locked(ngx_slab_pool_t *shpool, ngx_http_push_node_t *node, ngx_http_push_msg_t *msg) {
	ngx_queue_remove((msg->queue));
	if(msg->buf->file!=NULL) {
		ngx_delete_file(msg->buf->file->name); //should I care about deletion errors?
		ngx_close_file(msg->buf->file->fd); //again, carest thou aboutst thine errorests?
	}
	ngx_slab_free_locked(shpool, msg->buf); //separate block, remember?
	ngx_slab_free_locked(shpool, msg); 
}

// remove a message from queue and free all associated memory. first lock the shpool, though.
static ngx_inline void ngx_http_push_delete_message(ngx_slab_pool_t *shpool, ngx_http_push_node_t *node, ngx_http_push_msg_t *msg) {
	ngx_shmtx_lock(&shpool->mutex);
	ngx_http_push_delete_message_locked(shpool, node, msg);
	ngx_shmtx_unlock(&shpool->mutex);
}

//remove the oldest message from queue and free associated memory. assumes shpool is already locked.
static void ngx_http_push_delete_oldest_message_locked(ngx_slab_pool_t *shpool, ngx_http_push_node_t *node) {
	ngx_queue_t                    *sentinel = &node->message_queue->queue; 
	if(ngx_queue_empty(sentinel)) {
		return NULL;
	}
	return ngx_http_push_delete_message(shpool, node, ngx_queue_last(sentinel));
}

/** find message with entity tags matching those of the request r.
  * @param status NGX_OK for a found message, NGX_DONE for a future message or NGX_DECLINED for a message that no longer exists
  * @param r listener request
  */
static ngx_http_push_msg_t * ngx_http_push_find_message(ngx_http_push_node_t * node, ngx_http_request_t *r, ngx_int_t *status) {
	ngx_queue_t                    *sentinel = &node->message_queue->queue;
	ngx_queue_t                    *cur = sentinel->next;
	ngx_http_push_msg_t            *msg = NULL;
	ngx_int_t                       tag = 0;
	time_t                          time;
	time = (r->headers_in.if_modified_since == NULL) ? 0 :
		ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
	
	// do we want a future message?
	msg = ngx_queue_data(cur, ngx_http_push_msg_t, queue); 
	if(time >= msg->message_time || (time == msg->message_time && tag >= msg->message_tag)) {
		*status=NGX_DONE;
		return NULL;
	}
	
	for(msg = ngx_queue_data(cur, ngx_http_push_msg_t, queue); cur!=sentinel; cur = cur->next) {
		if (time < msg->message_time && msg->message_tag==tag) {
			*status = NGX_OK;
			return msg;
		}
	}
	*status = NGX_DECLINED; //message too old and was not found.
	return NULL;
}

static ngx_int_t ngx_http_push_set_id(ngx_str_t *id, ngx_http_request_t *r, ngx_http_push_loc_conf_t *cf) {
	ngx_http_variable_value_t      *vv = ngx_http_get_indexed_variable(r, cf->index);
    if (vv == NULL || vv->not_found || vv->len == 0) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "http_push module: the \"$push_id\" variable is required but is not set");
        return NGX_ERROR;
    }
	if (id!=NULL){
		//no need to copy anything? ok...
		id->data=vv->data; 
		id->len=vv->len;
		return NGX_OK;
	} else {
		return NGX_ERROR;
	}
}

static void ngx_http_push_copy_preallocated_buffer(ngx_buf_t *buf, ngx_buf_t *cbuf) {
	if (cbuf!=NULL) {
		if(ngx_buf_in_memory(buf)) {
			cbuf->pos = ((u_char *) cbuf) + sizeof(ngx_buf_t);
			cbuf->last = cbuf->pos + ngx_buf_size(buf);
			cbuf->start=cbuf->pos;
			cbuf->end = cbuf->start + ngx_buf_size(buf);
			ngx_memcpy(cbuf->pos, (buf)->pos, ngx_buf_size(buf));
			cbuf->memory=ngx_buf_in_memory_only(buf) ? 1 : 0;
		}
		if (buf->in_file) {
			cbuf->file_pos  = buf->file_pos;
			cbuf->file_last = buf->file_last;
			cbuf->temp_file = buf->temp_file;
			cbuf->in_file = 1;
			cbuf->file = ((ngx_file_t *) cbuf) + sizeof(cbuf) + (ngx_buf_in_memory(buf) ? ngx_buf_size(buf) : 0);
			if(cbuf->file!=NULL) {
				cbuf->file->fd=NGX_INVALID_FILE;
				cbuf->file->log=NULL;
				cbuf->file->offset=buf->file->offset;
				cbuf->file->sys_offset=buf->file->sys_offset;
				cbuf->file->name.len=buf->file->name.len;
				cbuf->file->name.data=(u_char *) (cbuf->file) + sizeof(ngx_file_t);
				ngx_memcpy(cbuf->file->name.data, buf->file->name.data, cbuf->file->name.len);
			}
		}
	}
}

//this is a macro because i don't want to mess with the alloc_function function pointer
#define ngx_http_push_create_buf_copy(buf, cbuf, pool, pool_alloc)            \
	(cbuf) = pool_alloc((pool), sizeof(ngx_buf_t) +                           \
		(ngx_buf_in_memory((buf)) ? ngx_buf_size((buf)) : 0) +                \
        ((buf)->in_file ? (sizeof(ngx_file_t) + (buf)->file->name.len + 1) : 0)); \
	if((cbuf)!=NULL) {                                                        \
		ngx_http_push_copy_preallocated_buffer((buf), (cbuf));                \
	}

static ngx_int_t ngx_http_push_listener_handler(ngx_http_request_t *r) {
	ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
	ngx_str_t                       id;
	ngx_http_push_node_t           *node;
	ngx_http_push_listener_t       *listener;
	ngx_http_push_msg_t            *msg;
	ngx_int_t                       status;
	if (r->method != NGX_HTTP_GET) {
		return NGX_HTTP_NOT_ALLOWED;
    }
	
	if(ngx_http_push_set_id(&id, r, cf) !=NGX_OK) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}

	ngx_shmtx_lock(&shpool->mutex);
	node = get_node(&id, ngx_http_push_shm_zone->data, shpool, r->connection->log);
	ngx_shmtx_unlock(&shpool->mutex);
	if (node==NULL) { //unable to allocate node
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
	
	ngx_http_discard_request_body(r); //don't care about the rest of this request
	
	ngx_shmtx_lock(&shpool->mutex);
	//expired messages are already removed from queue during get_node()
	msg = ngx_http_push_find_message(node, r, &status); 
	node->last_seen = ngx_time();
	
	//no matching message and it wasn't because an expired message was requested.
	if (status==NGX_DONE) {
		//this means we must wait for a message.
		if(ngx_http_push_queue_listener_request(node, r, shpool)==NULL) {
			//todo: emergency out-of-memory garbage collection, retry
			ngx_shmtx_unlock(&shpool->mutex);
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
		ngx_shmtx_unlock(&shpool->mutex);
		r->read_event_handler = ngx_http_test_reading; //definitely test to see if the connection got closed or something.
		return NGX_DONE; //and wait.
	}
	//listener wants an expired message
	else if (status==NGX_DECLINED) {
		//TODO: maybe respond with entity-identifiers for oldest available message?
		ngx_shmtx_lock(&shpool->mutex);
		return NGX_HTTP_NO_CONTENT; 
	}
	//found the message
	else if (status==NGX_OK) {
		ngx_chain_t                *out; //output chain
		ngx_int_t                   rc;
		rc = ngx_http_push_set_listener_header(r, msg); //all the headers are copied
		out = ngx_http_push_create_output_chain(r, msg->buf, shpool); 	//buffer is copied
		ngx_shmtx_unlock(&shpool->mutex);
		if (rc >= NGX_HTTP_SPECIAL_RESPONSE) { return rc; }
		rc = ngx_http_send_header(r);
		if (rc >= NGX_HTTP_SPECIAL_RESPONSE) { return rc; }
		return ngx_http_push_set_listener_body(r, out);
	}
}

static void ngx_http_push_sender_body_handler(ngx_http_request_t * r) { 
	ngx_str_t                       id;
	ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
	ngx_buf_t                      *buf, *buf_copy;
	ngx_http_push_node_t           *node;
	ngx_http_request_t             *r_listener = NULL;
	ngx_uint_t                      method = r->method;
	
	time_t                          last_seen = 0;
	ngx_uint_t                      queue_size = 0;
	
	if(ngx_http_push_set_id(&id, r, cf) !=NGX_OK) {
		return;
	}
	
	ngx_shmtx_lock(&shpool->mutex);
	if(method==NGX_HTTP_POST) {
		//find or create the node if it doesn't exist
		node = get_node(&id, (ngx_rbtree_t *) ngx_http_push_shm_zone->data, shpool, r->connection->log);
		if (node == NULL) {
			ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate new node");
			ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
			return;
		}
	}
	else{
		//just find the node. if it's not there, NULL.
		node = find_node(&id, (ngx_rbtree_t *) ngx_http_push_shm_zone->data, shpool, r->connection->log);
	}
	if (node!=NULL) {
		queue_size = node->message_queue_size;
		last_seen = node->last_seen;
	}
	ngx_shmtx_unlock(&shpool->mutex);
	
	if(method==NGX_HTTP_POST || method==NGX_HTTP_PUT) {
		//save the freaking body
		if(r->headers_in.content_length_n == -1 || r->headers_in.content_length_n == 0) {
			//empty message. blank buffer, please.
			buf = ngx_create_temp_buf(r->pool, 0);
		}
		else { //non-empty body
			if(r->request_body->temp_file==NULL) {//everything in the first buffer, please
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
		}
		if (buf==NULL) {
			ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate an empty buffer");
			ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
			return; 
		}
		ngx_http_push_msg_t            *msg=NULL, *previous_msg=NULL;
		size_t                          content_type_len; 
		content_type_len = (r->headers_in.content_type==NULL ? 0 : r->headers_in.content_type->value.len);
		
		if (r->method == NGX_HTTP_POST && cf->buffer_enabled!=0) {
		//buffers are not disabled. create the message in shared memory for storage
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
			
			msg->buf=buf_copy;
			previous_msg = ngx_http_push_get_latest_message(node);
			ngx_queue_insert_tail(&node->message_queue->queue, &msg->queue);
			
			//Stamp the new message with entity tags
			msg->message_time=ngx_time();
			msg->message_tag=(previous_msg!=NULL && msg->message_time == previous_msg->message_time) ? (previous_msg->message_tag + 1) : 0;
			node->message_queue_size++;
			
			//now see if the queue is too big
			if(node->message_queue_size > cf->max_message_queue_size) {
				ngx_http_push_delete_oldest_message_locked(shpool, node);
			}
			
			queue_size = node->message_queue_size;
			
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
			msg->expires=timeout==0 ? 0 : (ngx_time() + timeout);
			ngx_shmtx_unlock(&shpool->mutex);
			//okay, done storing. now respond to the sender request
			r->headers_out.status=NGX_HTTP_OK;
			r->headers_out.status_line.len =sizeof("202 Accepted")- 1;
			r->headers_out.status_line.data=(u_char *) "202 Accepted";
		}
		
		//go through all listeners and send them this message
		ngx_int_t                   rc=NGX_HTTP_OK;
		ngx_http_push_listener_t   *listener = NULL;
		if(msg!=NULL) {
			while ((listener=ngx_http_push_dequeue_listener(node))!=NULL) {
				ngx_http_request_t     *r_listener = listener->request;
				ngx_shmtx_lock(&shpool->mutex);
				rc = ngx_http_push_set_listener_header(r_listener, msg);
				ngx_shmtx_unlock(&shpool->mutex);
				rc = ngx_http_send_header(r_listener);
				
				ngx_shmtx_lock(&shpool->mutex);
				
				ngx_shmtx_unlock(&shpool->mutex);
				ngx_http_finalize_request(r_listener, ngx_http_push_set_listener_body(r_listener, ngx_http_push_create_output_chain(r_listener, buf)));
				
			}
		}
		//wrong!!
		if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
			ngx_http_finalize_request(r_listener, rc);
			ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
			return;
		}
		
		r->headers_out.status=NGX_HTTP_OK;
		r->headers_out.status_line.len =sizeof("201 Created")- 1;
		r->headers_out.status_line.data=(u_char *) "201 Created";
		//else {	//r_listener==NULL && r->method == NGX_HTTP_PUT is all that remains
		//	r->headers_out.status=NGX_HTTP_OK;
		//}
	}
	else if (method==NGX_HTTP_GET) {
		r->headers_out.status= node==NULL ? NGX_HTTP_NOT_FOUND : NGX_HTTP_OK;
	}
	else if (method==NGX_HTTP_DELETE) {
		if (node!=NULL) {
			ngx_http_push_msg_t        *msg;
			while((msg=ngx_http_push_dequeue_message(node))!=NULL) {
				//delete all the messages
				ngx_slab_free(shpool, msg->buf); //separate block, remember?
				ngx_slab_free(shpool, msg);
			};
			node->message_queue_size=0;
			ngx_http_push_listener_t   *listener = NULL;
			while((listener=ngx_http_push_dequeue_listener(node))!=NULL) {
				//delete all the requests
				ngx_shmtx_lock(&shpool->mutex);
				ngx_http_push_delete_node((ngx_rbtree_t *) ngx_http_push_shm_zone->data, (ngx_rbtree_node_t *) node, shpool);
				ngx_shmtx_unlock(&shpool->mutex);
				
				//respond to the listener request with a 410
				listener->request->headers_out.status=NGX_HTTP_NOT_FOUND; //play nice, NGINX.
				listener->request->headers_out.status_line.len =sizeof("410 Gone")- 1;
				listener->request->headers_out.status_line.data=(u_char *) "410 Gone";
				listener->request->headers_out.content_length_n = 0;
				listener->request->header_only = 1;
				ngx_http_finalize_request(listener->request, ngx_http_send_header(listener->request));
			}
			node->listener_queue_size=0;
			r->headers_out.status=NGX_HTTP_OK;
		}
		else {
			r->headers_out.status=NGX_HTTP_NOT_FOUND;
			r->header_only = 1;
		}
	}
	else {
		ngx_http_finalize_request(r, NGX_HTTP_NOT_ALLOWED);
		return;
	}
	if (r->header_only || node==NULL) {
		r->header_only = 1;
		r->headers_out.content_length_n = 0;
		ngx_http_finalize_request(r, ngx_http_send_header(r));	
	} 
	else {
		ngx_http_finalize_request(r, ngx_http_push_node_info(r, queue_size, last_seen));
	}
	return;
}

static ngx_int_t ngx_http_push_node_info(ngx_http_request_t *r, ngx_uint_t queue_size, time_t last_seen) {
	ngx_buf_t                      *b;
	ngx_uint_t                      len;
	len = sizeof("queued: \r\n") + NGX_INT_T_LEN + sizeof("requested: \r\n") + NGX_INT_T_LEN;
	
	b = ngx_create_temp_buf(r->pool, len);
	if (b == NULL) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	b->last = ngx_sprintf(b->last, "queued: %ui\r\n", queue_size);
	if(last_seen==0){
		b->last = ngx_cpymem(b->last, "requested: never\r\n", sizeof("requested: never\r\n") - 1);
	}
	else {
		b->last = ngx_sprintf(b->last, "requested: %ui\r\n", ngx_time() - last_seen);
	}
	
	r->headers_out.content_type.len = sizeof("text/plain") - 1;
    r->headers_out.content_type.data = (u_char *) "text/plain";
	if (ngx_http_send_header(r) > NGX_HTTP_SPECIAL_RESPONSE) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	
	return ngx_http_push_set_listener_body(r, ngx_http_push_create_output_chain(r, b));
}

static ngx_int_t ngx_http_push_sender_handler(ngx_http_request_t * r) {
	ngx_int_t                       rc;
	
	/* Instruct ngx_http_read_listener_request_body to store the request
	   body entirely in a memory buffer or in a file */
	r->request_body_in_single_buf = 1;
	r->request_body_in_persistent_file = 1;
	r->request_body_in_clean_file = 0;
	r->request_body_file_log_level = 0;

	rc = ngx_http_read_client_request_body(r, ngx_http_push_sender_body_handler);
	if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
		return rc;
	}
	return NGX_DONE;
}

static ngx_int_t ngx_http_push_set_listener_header(ngx_http_request_t *r, ngx_http_push_msg_t *msg) {
	//content-type is _copied_
	if (&msg->content_type!=NULL && msg->content_type.data!=NULL && msg->content_type.len > 0) {
		r->headers_out.content_type.len=msg->content_type.len;
		r->headers_out.content_type.data = ngx_palloc(r->pool, msg->content_type.len);
		if(r->headers_out.content_type.data==NULL) {
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
		ngx_memcpy(r->headers_out.content_type.data, msg->content_type.data, msg->content_type.len);
	}
	if(msg->message_time) {
		//if-modified-since header
		r->headers_out.last_modified_time=msg->message_time;
	}
	if(msg->message_tag) {
		//etag, if we need one
		r->headers_out.etag = ngx_list_push(&r->headers_out.headers);
		if (r->headers_out.etag == NULL) {
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
		r->headers_out.etag->hash = 1;
		r->headers_out.etag->key.len = sizeof("Etag") - 1;
		r->headers_out.etag->key.data = (u_char *) "Etag";
		r->headers_out.etag->value.len = NGX_INT_T_LEN;
		r->headers_out.etag->value.data = ngx_palloc(r->pool, NGX_INT_T_LEN);
		if(r->headers_out.etag->value.data==NULL) {
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
		r->headers_out.etag->value.len = ngx_sprintf(r->headers_out.etag->value.data, "%ui", msg->message_tag) - r->headers_out.etag->value.data;
	}
	r->headers_out.status=NGX_HTTP_OK;
	return NGX_OK;
}

/**
 * create a (finalized) output chain from a buffer, copying it into the relevant request's pool.
 * @param shpool -- optional shmem pool, if you want it to be unlocked while (and if) performing file i/o.
 */
static ngx_chain_t * ngx_http_push_create_output_chain(ngx_http_request_t *r, ngx_buf_t *buf, ngx_slab_pool_t *shpool) {
	//buffer is _copied_
	ngx_chain_t                    *out = ngx_pcalloc(r->pool, sizeof(ngx_chain_t));
	ngx_buf_t                      *buf_copy;
	ngx_http_push_create_buf_copy(buf, buf_copy, r->pool, ngx_pcalloc);
	if (out==NULL || buf_copy==NULL) {
		return NULL;
	}
	
	if(buf->in_file){
		//here we go with the file juggling
		ngx_file_t             *file = buf_copy->file;
		file->log=r->connection->log;
		if(shpool!=NULL) { ngx_shmtx_unlock(&shpool->mutex); } //unlock before performing i/o 
		//the following assumes file->name.data is already null-terminated
		file->fd=ngx_open_file(file->name.data, NGX_FILE_RDONLY, NGX_FILE_OPEN, NGX_FILE_OWNER_ACCESS);
		if(shpool!=NULL) { ngx_shmtx_lock(&shpool->mutex); } //back to lockdown
		if(file->fd==NGX_INVALID_FILE){
			/* i don't think we need to do any cleanup, since all allocations
			here were done in the request pool which gets cleaned all by itself. */
			return NULL;
		}
		if(shpool!=NULL) { ngx_shmtx_lock(&shpool->mutex); }
	}
	
	buf_copy->last_buf = 1; 
	out->buf = buf_copy;
	out->next = NULL;
	return out;	
}


static ngx_int_t ngx_http_push_set_listener_body(ngx_http_request_t *r, ngx_chain_t *out) {
	if (out==NULL) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	return ngx_http_output_filter(r, out);	
}
