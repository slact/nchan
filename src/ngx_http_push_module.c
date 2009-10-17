/*
 *	Copyright 2009 Leo Ponomarev.
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#include <ngx_http_push_module.h>
#include <ngx_http_push_rbtree_util.c>
#include <ngx_http_push_module_setup.c>

//shpool is assumed to be locked.
static ngx_http_push_msg_t * ngx_http_push_get_last_message_locked(ngx_http_push_node_t * node, ngx_slab_pool_t *shpool){
	ngx_queue_t                    *sentinel = &node->message_queue->queue; 
	if(ngx_queue_empty(sentinel)) {
		ngx_shmtx_unlock(&shpool->mutex);
		return NULL;
	}
	ngx_queue_t                    *qmsg = ngx_queue_last(sentinel);
	return ngx_queue_data(qmsg, ngx_http_push_msg_t, queue);
}

//shpool must be locked. No memory is freed.
static ngx_http_push_msg_t * ngx_http_push_get_oldest_message_locked(ngx_http_push_node_t * node){
	ngx_queue_t                    *sentinel = &node->message_queue->queue; 
	if(ngx_queue_empty(sentinel)) {
		return NULL;
	}
	ngx_queue_t                    *qmsg = ngx_queue_head(sentinel);
	return ngx_queue_data(qmsg, ngx_http_push_msg_t, queue);
}

static ngx_http_push_msg_t * ngx_http_push_dequeue_message_locked(ngx_http_push_node_t * node) {
	ngx_http_push_msg_t            *msg = ngx_http_push_get_oldest_message_locked(node);
	if(msg!=NULL) {
		ngx_queue_remove(&msg->queue);
		node->message_queue_size--;
	}
	return msg;
}

//shpool must be locked. No memory is freed.
static ngx_http_push_listener_t * ngx_http_push_dequeue_listener_locked(ngx_http_push_node_t * node) {
	ngx_queue_t                    *sentinel = &node->listener_queue->queue; 
	if(ngx_queue_empty(sentinel)) { return NULL; }
	ngx_queue_t                    *qlistener = ngx_queue_last(sentinel);
	ngx_http_push_listener_t       *listener = ngx_queue_data(qlistener, ngx_http_push_listener_t, queue);
	ngx_queue_remove(qlistener);
	node->listener_queue_size--;
	return listener;
}

static ngx_inline ngx_http_push_listener_t *ngx_http_push_queue_listener_request(ngx_http_push_node_t * node, ngx_http_request_t *r, ngx_slab_pool_t *shpool) {
	ngx_shmtx_lock(&shpool->mutex);
	ngx_http_push_listener_t       *listener = ngx_http_push_queue_listener_request_locked(node, r, shpool);
	ngx_shmtx_unlock(&shpool->mutex);
	return listener;
}

//shpool must be locked.
static ngx_inline ngx_http_push_listener_t *ngx_http_push_queue_listener_request_locked(ngx_http_push_node_t * node, ngx_http_request_t *r, ngx_slab_pool_t *shpool) {
	ngx_http_push_listener_t       *listener = ngx_slab_alloc_locked(shpool, sizeof(*listener));
	if(listener==NULL) { //unable to allocate request queue element
		return NULL;
	}
	ngx_queue_insert_tail(&node->listener_queue->queue, &listener->queue);
	listener->request = r;
	node->listener_queue_size++;
	return listener;
}

// remove a message from queue and free all associated memory. first lock the shpool, though.
static ngx_inline void ngx_http_push_delete_message(ngx_slab_pool_t *shpool, ngx_http_push_node_t *node, ngx_http_push_msg_t *msg) {
	ngx_shmtx_lock(&shpool->mutex);
	ngx_http_push_delete_message_locked(shpool, node, msg);
	ngx_shmtx_unlock(&shpool->mutex);
}

// remove a message from queue and free all associated memory. assumes shpool is already locked.
static ngx_inline void ngx_http_push_delete_message_locked(ngx_slab_pool_t *shpool, ngx_http_push_node_t *node, ngx_http_push_msg_t *msg) {
	if (msg==NULL) { return; }
	if(node!=NULL) {
		ngx_queue_remove(&msg->queue);
		node->message_queue_size--;
	}
	if(msg->buf->file!=NULL) {
		ngx_delete_file(&msg->buf->file->name); //should I care about deletion errors?
		ngx_close_file(msg->buf->file->fd); //again, carest thou aboutst thine errorests?
	}
	ngx_slab_free_locked(shpool, msg->buf); //separate block, remember?
	ngx_slab_free_locked(shpool, msg); 
}

/** find message with entity tags matching those of the request r.
  * @param status NGX_OK for a found message, NGX_DONE for a future message or NGX_DECLINED for a message that no longer exists
  * @param r listener request
  */
static ngx_http_push_msg_t * ngx_http_push_find_message_locked(ngx_http_push_node_t *node, ngx_http_request_t *r, ngx_int_t *status) {
	//TODO: consider using an RBTree for message storage.
	ngx_queue_t                    *sentinel = &node->message_queue->queue;
	ngx_queue_t                    *cur = sentinel->next;
	ngx_http_push_msg_t            *msg = NULL;
	ngx_int_t                       tag = -1;
	time_t                          time = (r->headers_in.if_modified_since == NULL) ? 0 : ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
	
	//channel's message buffer empty?
	if(node->message_queue_size==0) {
		*status=NGX_DONE; //wait.
		return NULL;
	}
	
	// do we want a future message?
	msg = ngx_queue_data(sentinel->prev, ngx_http_push_msg_t, queue); 
	if(time <= msg->message_time) { //that's an empty check (Sentinel's values are zero)
		if(time == msg->message_time) {
			if(tag<0) { tag = ngx_http_push_listener_get_etag_int(r); }
			if(tag >= msg->message_tag) {
				*status=NGX_DONE;
				return NULL;
			}
		}
	}
	else {
		*status=NGX_DONE;
		return NULL;
	}
	
	while(cur!=sentinel) {
		msg = ngx_queue_data(cur, ngx_http_push_msg_t, queue);
		if (time < msg->message_time) {
			*status = NGX_OK;
			return msg;
		}
		else if(time == msg->message_time) {
			if(tag<0) { tag = ngx_http_push_listener_get_etag_int(r); }
			while (tag >= msg->message_tag  && time == msg->message_time && cur->next!=sentinel) {
				cur=cur->next;
				msg = ngx_queue_data(cur, ngx_http_push_msg_t, queue);
			}
			if(time == msg->message_time && tag < msg->message_tag) {
				*status = NGX_OK;
				return msg;
			}
			continue;
		}
		cur=cur->next;
	}
	*status = NGX_DECLINED; //message too old and was not found.
	return NULL;
}

static ngx_int_t ngx_http_push_set_channel_id(ngx_str_t *id, ngx_http_request_t *r, ngx_http_push_loc_conf_t *cf) {
	ngx_http_variable_value_t      *vv = ngx_http_get_indexed_variable(r, cf->index);
    if (vv == NULL || vv->not_found || vv->len == 0) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "http_push module: the \"$push_channel_id\" variable is required but is not set");
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
		ngx_memcpy(cbuf, buf, sizeof(*buf)); //overkill?
		if(buf->temporary || buf->memory) { //we don't want to copy mmpapped memory, so no ngx_buf_in_momory(buf)
			cbuf->pos = (u_char *) (cbuf+1);
			cbuf->last = cbuf->pos + ngx_buf_size(buf);
			cbuf->start=cbuf->pos;
			cbuf->end = cbuf->start + ngx_buf_size(buf);
			ngx_memcpy(cbuf->pos, buf->pos, ngx_buf_size(buf));
			cbuf->memory=ngx_buf_in_memory_only(buf) ? 1 : 0;
		}
		if (buf->file!=NULL) {
			cbuf->file = (ngx_file_t *) (cbuf+1) + ((buf->temporary || buf->memory) ? ngx_buf_size(buf) : 0);
			cbuf->file->fd=NGX_INVALID_FILE;
			cbuf->file->log=NULL;
			cbuf->file->offset=buf->file->offset;
			cbuf->file->sys_offset=buf->file->sys_offset;
			cbuf->file->name.len=buf->file->name.len;
			cbuf->file->name.data=(u_char *) (cbuf->file+1);
			ngx_memcpy(cbuf->file->name.data, buf->file->name.data, buf->file->name.len);
		}
	}
}

//this is a macro because i don't want to mess with the alloc_function function pointer
#define ngx_http_push_create_buf_copy(buf, cbuf, pool, pool_alloc)            \
	(cbuf) = pool_alloc((pool), sizeof(*cbuf) +                           \
		(((buf)->temporary || (buf)->memory) ? ngx_buf_size(buf) : 0) +       \
        (((buf)->file!=NULL) ? (sizeof(*(buf)->file) + (buf)->file->name.len + 1) : 0)); \
	if((cbuf)!=NULL) {                                                        \
		ngx_http_push_copy_preallocated_buffer((buf), (cbuf));                \
	}

static ngx_str_t ngx_http_push_Allow_GET= ngx_string("GET");
static ngx_int_t ngx_http_push_listener_handler(ngx_http_request_t *r) {
	ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
	ngx_str_t                       id;
	ngx_http_push_node_t           *node;
	ngx_http_push_msg_t            *msg;
	ngx_int_t                       status;
	if (r->method != NGX_HTTP_GET) {
		ngx_http_push_add_response_header(r, &ngx_http_push_Allow, &ngx_http_push_Allow_GET);
		return NGX_HTTP_NOT_ALLOWED;
    }
	
	if(ngx_http_push_set_channel_id(&id, r, cf) !=NGX_OK) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}

	//get the node and check channel authorization while we're at it.
	ngx_shmtx_lock(&shpool->mutex);
	node = (cf->authorize_channel==1 ? find_node : get_node)(&id, ngx_http_push_shm_zone->data, shpool, r->connection->log);
	if (node==NULL) { //unable to allocate node
		ngx_shmtx_unlock(&shpool->mutex);
		return cf->authorize_channel==1 ? NGX_HTTP_FORBIDDEN : NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
	msg = ngx_http_push_find_message_locked(node, r, &status); 
	node->last_seen = ngx_time();
	ngx_shmtx_unlock(&shpool->mutex);
	
	ngx_http_discard_request_body(r); //don't care about the rest of this request
	if(ngx_http_push_handle_listener_concurrency_setting(cf->listener_concurrency, node, r, shpool) == NGX_DECLINED) { //this request was declined for some reason.
		//status codes and whatnot should have already been written. just get out of here quickly.
		return NGX_OK;
	}
	//no matching message and it wasn't because an expired message was requested.
	if (status==NGX_DONE) {
		if(cf->listener_poll_mechanism==NGX_HTTP_PUSH_LISTENER_LONGPOLL) {
			//long-polling listener. wait for a message.
			ngx_http_push_listener_t   *listener;
			ngx_shmtx_lock(&shpool->mutex);
			listener=ngx_http_push_queue_listener_request_locked(node, r, shpool);
			ngx_shmtx_unlock(&shpool->mutex);
			if(listener==NULL) { return NGX_HTTP_INTERNAL_SERVER_ERROR; } //todo: emergency out-of-memory garbage collection, retry
			//test to see if the connection was closed or something.
			r->read_event_handler = ngx_http_test_reading; 
			
			 //attach a cleaner to remove the request from the node, if need be (if the connection goes dead or something)
			ngx_pool_cleanup_t *cln = ngx_pool_cleanup_add(r->pool, sizeof(ngx_http_push_listener_cleanup_t));
			if (cln == NULL) { //make sure we can.
				return NGX_ERROR;
			}
			ngx_shmtx_lock(&shpool->mutex);
			listener->cleanup = ((ngx_http_push_listener_cleanup_t *) cln->data);
			ngx_shmtx_unlock(&shpool->mutex);
			cln->handler = (ngx_pool_cleanup_pt) ngx_http_push_listener_cleanup;
			((ngx_http_push_listener_cleanup_t *) cln->data)->node = node;
			((ngx_http_push_listener_cleanup_t *) cln->data)->listener = listener;
			((ngx_http_push_listener_cleanup_t *) cln->data)->shpool = shpool;
		
			return NGX_DONE; //and wait.
		}
		else if(cf->listener_poll_mechanism==NGX_HTTP_PUSH_LISTENER_INTERVALPOLL) {
			//interval-polling listener requests get a 204 with its entity tags preserved.
			if (r->headers_in.if_modified_since != NULL) {
				r->headers_out.last_modified_time=ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
			}
			ngx_str_t                  *etag;
			if ((etag=ngx_http_push_listener_get_etag(r)) != NULL) {
				r->headers_out.etag=ngx_http_push_add_response_header(r, &ngx_http_push_Etag, etag);
			}
			return NGX_HTTP_NOT_MODIFIED;
		}
		else {
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
	}
	//listener wants an expired message
	else if (status==NGX_DECLINED) {
		//TODO: maybe respond with entity-identifiers for oldest available message?
		return NGX_HTTP_NO_CONTENT; 
	}
	//found the message
	else { //status==NGX_OK
		ngx_chain_t                *out; //output chain
		ngx_int_t                   rc;
		ngx_shmtx_lock(&shpool->mutex);
		if((msg->received)!=(ngx_uint_t) NGX_MAX_UINT32_VALUE){ //overflow check?
			msg->received++;
		}
		ngx_shmtx_unlock(&shpool->mutex);
		rc = ngx_http_push_set_listener_header(r, msg, shpool); //all the headers are copied
		out = ngx_http_push_create_output_chain(r, msg->buf, shpool); 	//buffer is copied
		if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
			return rc; 
		}
		rc = ngx_http_send_header(r);
		if (rc >= NGX_HTTP_SPECIAL_RESPONSE) { 
			return rc; 
		}
		return ngx_http_push_set_listener_body(r, out);
	}
}

static ngx_str_t ngx_http_push_409_Conflict = ngx_string("409 Conflict");
static ngx_int_t ngx_http_push_handle_listener_concurrency_setting(ngx_int_t concurrency, ngx_http_push_node_t *node, ngx_http_request_t *r, ngx_slab_pool_t *shpool) {
	if(concurrency==NGX_HTTP_PUSH_LISTENER_BROADCAST) {
		return NGX_OK;
	}
	else if(node->listener_queue_size>0){
		if(concurrency == NGX_HTTP_PUSH_LISTENER_FIRSTIN) {
			ngx_http_push_reply_status_only(r, NGX_HTTP_NOT_FOUND, &ngx_http_push_409_Conflict);
			return NGX_DECLINED;
		}
		else{ //concurrency == NGX_HTTP_PUSH_LISTENER_LASTIN
			ngx_shmtx_lock(&shpool->mutex);
			ngx_http_push_listener_t *listener = ngx_http_push_dequeue_listener_locked(node);
			listener->cleanup->node=NULL; // so that the cleanup handler won't go dequeuing the request again
			ngx_http_request_t     *request_l = listener->request;
			ngx_shmtx_unlock(&shpool->mutex);
			ngx_http_push_reply_status_only(request_l, NGX_HTTP_NOT_FOUND, &ngx_http_push_409_Conflict);
			return NGX_OK;
		}
	}
	return NGX_OK;
}

#define NGX_HTTP_PUSH_SENDER_CHECK(val, fail, r, errormessage)                \
	if (val == fail) {                                                        \
		ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, errormessage);    \
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);         \
		return;                                                               \
	}
#define NGX_HTTP_PUSH_SENDER_CHECK_LOCKED(val, fail, r, errormessage, shpool) \
	if (val == fail) {                                                        \
		ngx_shmtx_unlock(&(shpool)->mutex);                                   \
		ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, errormessage);    \
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);         \
		return;                                                               \
	}

static ngx_str_t ngx_http_push_410_Gone = ngx_string("410 Gone");
static ngx_str_t ngx_http_push_Allow_GET_POST_PUT_DELETE= ngx_string("GET, POST, PUT, DELETE");
static void ngx_http_push_sender_body_handler(ngx_http_request_t * r) { 
	ngx_str_t                       id;
	ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
	ngx_buf_t                      *buf = NULL, *buf_copy;
	ngx_http_push_node_t           *node;
	ngx_uint_t                      method = r->method;
	
	time_t                          last_seen = 0;
	ngx_uint_t                      message_queue_size = 0, listener_queue_size = 0;
	
	NGX_HTTP_PUSH_SENDER_CHECK(ngx_http_push_set_channel_id(&id, r, cf), NGX_ERROR, r, "can't determine channel id")
	
	ngx_shmtx_lock(&shpool->mutex);
	//POST requests will need a channel node created if it doesn't yet exist.
	if(method==NGX_HTTP_POST || method==NGX_HTTP_PUT) {
		node = get_node(&id, (ngx_rbtree_t *) ngx_http_push_shm_zone->data, shpool, r->connection->log);
		NGX_HTTP_PUSH_SENDER_CHECK_LOCKED(node, NULL, r, "push module: unable to allocate new node", shpool);
		message_queue_size = node->message_queue_size;
		listener_queue_size = node->listener_queue_size;
		last_seen = node->last_seen;
	}
	//no other request method needs that.
	else{
		//just find the node. if it's not there, NULL.
		node = find_node(&id, (ngx_rbtree_t *) ngx_http_push_shm_zone->data, shpool, r->connection->log);
	}
	ngx_shmtx_unlock(&shpool->mutex);
	
	//POST requests are message submissions. queue the request body as a message.
	if(method==NGX_HTTP_POST) {
		//empty message. blank buffer, please.
		if(r->headers_in.content_length_n == -1 || r->headers_in.content_length_n == 0) { //should that read NGX_CONF_UNSET instead of -1?
			buf = ngx_create_temp_buf(r->pool, 0);
		}
		//non-empty body. your buffers, please.
		else {
			//note: this works only provided mostly because of r->request_body_in_single_buf = 1; which, i suppose, makes this module a little slower than it could be.
			//this block is a little hacky. might be a thorn for forward-compatibility.
			if(r->request_body->temp_file==NULL) { //everything in the first buffer, please
				//no file
				buf=r->request_body->bufs->buf;
			}
			else if(r->request_body->bufs->next!=NULL) {
				//there's probably a file
				buf=r->request_body->bufs->next->buf;
			}
		}
		NGX_HTTP_PUSH_SENDER_CHECK(buf, NULL, r, "push module: can't find or allocate buffer");
				
		//okay, now store the incoming message.
		size_t                          content_type_len = (r->headers_in.content_type==NULL ? 0 : r->headers_in.content_type->value.len);
		//create a buffer copy in shared mem
		ngx_shmtx_lock(&shpool->mutex);	
		ngx_http_push_msg_t            *msg = ngx_slab_alloc_locked(shpool, sizeof(*msg) + content_type_len);
		ngx_http_push_msg_t            *previous_msg=ngx_http_push_get_last_message_locked(node, shpool);
		NGX_HTTP_PUSH_SENDER_CHECK(msg, NULL, r, "push module: unable to allocate message in shared memory");
		ngx_http_push_create_buf_copy(buf, buf_copy, shpool, ngx_slab_alloc_locked);
		NGX_HTTP_PUSH_SENDER_CHECK_LOCKED(buf_copy, NULL, r, "push module: unable to allocate buffer in shared memory", shpool);
		msg->buf=buf_copy;
		if(cf->store_messages==1) {
			ngx_queue_insert_tail(&node->message_queue->queue, &msg->queue); //this line ought to appear after NGX_HTTP_PUSH_SENDER_CHECK, but this is easier.
			node->message_queue_size++;
			message_queue_size++;
		}
		//Stamp the new message with entity tags
		msg->message_time=ngx_time();
		msg->message_tag=(previous_msg!=NULL && msg->message_time == previous_msg->message_time) ? (previous_msg->message_tag + 1) : 0;		
		
		//store the content-type
		if(content_type_len>0) {
			msg->content_type.len=r->headers_in.content_type->value.len;
			msg->content_type.data=(u_char *) (msg+1);
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
		
		//go through all listeners and send them this message
		ngx_int_t                   rc=NGX_HTTP_OK, received=0;
		ngx_http_push_listener_t   *listener = NULL;
		if(msg!=NULL) {
			ngx_http_request_t     *r_listener;
			ngx_shmtx_lock(&shpool->mutex);
			while ((listener=ngx_http_push_dequeue_listener_locked(node))!=NULL) {
				r_listener = listener->request;
				listener->cleanup->node=NULL; // so that the cleanup handler won't go dequeuing the request again
				if((msg->received)!=(ngx_uint_t) NGX_MAX_UINT32_VALUE){ //overflow check?
					msg->received++;
				}
				ngx_shmtx_unlock(&shpool->mutex);
				if((rc = ngx_http_push_set_listener_header(r_listener, msg, shpool)) < NGX_HTTP_SPECIAL_RESPONSE) {
					//everything is going as planned
					rc = ngx_http_send_header(r_listener);
					ngx_http_finalize_request(r_listener, rc >= NGX_HTTP_SPECIAL_RESPONSE ? rc : ngx_http_push_set_listener_body(r_listener, ngx_http_push_create_output_chain(r_listener, buf, NULL)));				
					if(!received) {
						received=1;
					}
				}
				else { //something went wrong setting the header
					ngx_http_finalize_request(r_listener, rc);
				}
				ngx_shmtx_lock(&shpool->mutex);
			}
			ngx_shmtx_unlock(&shpool->mutex);
		}
		
		//status code for the sender response depends on whether the message was sent to any listeners
		r->headers_out.status=NGX_HTTP_OK;
		if(received) {
			r->headers_out.status_line.len =sizeof("201 Created")- 1;
			r->headers_out.status_line.data=(u_char *) "201 Created";
		}
		else {
			r->headers_out.status_line.len =sizeof("202 Accepted")- 1;
			r->headers_out.status_line.data=(u_char *) "202 Accepted";
		}
		
		//now see if the queue is too big -- we do this at the end because message queue size may be set to zero, and we don't want special-case code for that.
		if(node->message_queue_size > (ngx_uint_t) cf->max_message_queue_size) {
			//exceeeds max queue size. force-delete oldest message
			ngx_http_push_delete_message(shpool, node, ngx_http_push_get_oldest_message_locked(node));
		}
		if(node->message_queue_size > (ngx_uint_t) cf->min_message_queue_size) {
			//exceeeds min queue size. maybe delete the oldest message
			ngx_shmtx_lock(&shpool->mutex);
			ngx_http_push_msg_t    *msg = ngx_http_push_get_oldest_message_locked(node);
			NGX_HTTP_PUSH_SENDER_CHECK_LOCKED(msg, NULL, r, "push module: oldest message not found", shpool);
			if(msg->received >= (ngx_uint_t) cf->min_message_recipients) {
				//received more than min_message_recipients times
				ngx_http_push_delete_message_locked(shpool, node, msg);
			}
			ngx_shmtx_unlock(&shpool->mutex);
		}
	}
	else if (method==NGX_HTTP_GET || method==NGX_HTTP_PUT) {
		r->headers_out.status= node==NULL ? NGX_HTTP_NOT_FOUND : NGX_HTTP_OK;
	}
	else if (method==NGX_HTTP_DELETE) {
		if (node!=NULL) {
			ngx_http_push_msg_t        *msg;
			ngx_http_request_t         *r_listener;
			ngx_shmtx_lock(&shpool->mutex);
			while((msg=ngx_http_push_dequeue_message_locked(node))!=NULL) {
				//delete all the messages
				ngx_http_push_delete_message_locked(shpool, NULL, msg);
			};
			node->message_queue_size=0;
			ngx_http_push_listener_t   *listener = NULL;
			while((listener=ngx_http_push_dequeue_listener_locked(node))!=NULL) {
				//send a 410 Gone to everyone waiting for something
				r_listener=listener->request;
				listener->cleanup->node=NULL; //the node may be deleted by the time we get to the request pool cleanup.
				ngx_shmtx_unlock(&shpool->mutex);
				ngx_http_push_reply_status_only(r_listener, NGX_HTTP_NOT_FOUND, &ngx_http_push_410_Gone);
				ngx_shmtx_lock(&shpool->mutex);
			}
			ngx_http_push_delete_node_locked((ngx_rbtree_t *) ngx_http_push_shm_zone->data, (ngx_rbtree_node_t *) node, shpool);
			ngx_shmtx_unlock(&shpool->mutex);
			r->headers_out.status=NGX_HTTP_OK;
		}
		else {
			r->headers_out.status=NGX_HTTP_NOT_FOUND;
			r->header_only = 1;
		}
	}
	else {
		ngx_http_push_add_response_header(r, &ngx_http_push_Allow, &ngx_http_push_Allow_GET_POST_PUT_DELETE);
		ngx_http_finalize_request(r, NGX_HTTP_NOT_ALLOWED);
		return;
	}
	if (r->header_only || node==NULL) {
		r->header_only = 1;
		r->headers_out.content_length_n = 0;
		ngx_http_finalize_request(r, ngx_http_send_header(r));	
	} 
	else {
		ngx_http_finalize_request(r, ngx_http_push_node_info(r, message_queue_size, listener_queue_size, last_seen));
	}
	return;
}

//print information about a node ( channel )
static ngx_int_t ngx_http_push_node_info(ngx_http_request_t *r, ngx_uint_t message_queue_size, ngx_uint_t listener_queue_size, time_t last_seen) {
	ngx_buf_t                      *b;
	ngx_uint_t                      len;
	time_t                          time_elapsed = ngx_time() - last_seen;
	len = sizeof("queued messages: \r\n") + NGX_INT_T_LEN + sizeof("last requested:  seconds ago\r\n") + NGX_INT_T_LEN + sizeof("active listeners: \r\n") + NGX_INT_T_LEN;;
	
	b = ngx_create_temp_buf(r->pool, len);
	if (b == NULL) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	b->last = ngx_sprintf(b->last, "queued messages: %ui\r\n", message_queue_size);
	if(last_seen==0){
		b->last = ngx_cpymem(b->last, "last requested: never\r\n", sizeof("last requested: never\r\n") - 1);
	}
	else {		
		b->last = ngx_sprintf(b->last, time_elapsed==1 ? "last requested: %ui second ago\r\n" : "last requested: %ui seconds ago\r\n", time_elapsed);
	}
	b->last = ngx_sprintf(b->last, "active listeners: %ui", listener_queue_size);
	r->headers_out.content_type.len = sizeof("text/plain") - 1;
    r->headers_out.content_type.data = (u_char *) "text/plain";
	if (ngx_http_send_header(r) > NGX_HTTP_SPECIAL_RESPONSE) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	
	return ngx_http_push_set_listener_body(r, ngx_http_push_create_output_chain(r, b, NULL));
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

static ngx_str_t ngx_http_push_Vary_header_value = ngx_string("If-None-Match, If-Modified-Since");

//assumes that shpool is unlocked.
static ngx_int_t ngx_http_push_set_listener_header(ngx_http_request_t *r, ngx_http_push_msg_t *msg, ngx_slab_pool_t *shpool) {
	//content-type is _copied_
	ngx_shmtx_lock(&shpool->mutex);
	if (&msg->content_type!=NULL && msg->content_type.data!=NULL && msg->content_type.len > 0) {
		r->headers_out.content_type.len=msg->content_type.len;
		r->headers_out.content_type.data = ngx_palloc(r->pool, msg->content_type.len);
		if(r->headers_out.content_type.data==NULL) {
			ngx_shmtx_unlock(&shpool->mutex);
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
		ngx_str_t                  *etag=ngx_pcalloc(r->pool, sizeof(*etag) + NGX_INT_T_LEN);
		if (etag==NULL) { 
			ngx_shmtx_unlock(&shpool->mutex);
			return NGX_HTTP_INTERNAL_SERVER_ERROR; 
		}
		etag->data = (u_char *) (etag+1); 
		etag->len = ngx_sprintf(etag->data, "%ui", msg->message_tag) - etag->data;
		if ((r->headers_out.etag=ngx_http_push_add_response_header(r, &ngx_http_push_Etag, etag))==NULL) {
			ngx_shmtx_unlock(&shpool->mutex);
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
	}
	if(shpool!=NULL) { ngx_shmtx_unlock(&shpool->mutex); }
	//Vary header needed for proper caching.
	ngx_http_push_add_response_header(r, &ngx_http_push_Vary, &ngx_http_push_Vary_header_value);
	r->headers_out.status=NGX_HTTP_OK;
	return NGX_OK;
}

static ngx_table_elt_t * ngx_http_push_add_response_header(ngx_http_request_t *r, ngx_str_t *header_name, ngx_str_t *header_value) {
	ngx_table_elt_t                *h = ngx_list_push(&r->headers_out.headers);
	if (h == NULL) {
		return NULL;
	}
	h->hash = 1;
	h->key.len = header_name->len;
	h->key.data = header_name->data;
	h->value.len = header_value->len;
	h->value.data = header_value->data;
	return h;
}

static ngx_int_t ngx_http_push_listener_get_etag_int(ngx_http_request_t * r) {
	ngx_str_t                      *if_none_match = ngx_http_push_listener_get_etag(r);
	ngx_int_t                       tag;
	if(if_none_match==NULL || (if_none_match!=NULL && (tag = ngx_atoi(if_none_match->data, if_none_match->len))==NGX_ERROR)) {
		tag=0;
	}
	return ngx_abs(tag);
}
static ngx_str_t * ngx_http_push_listener_get_etag(ngx_http_request_t * r) {
    ngx_uint_t                       i;
    ngx_list_part_t                 *part = &r->headers_in.headers.part;
    ngx_table_elt_t                 *header= part->elts;

    for (i = 0; /* void */ ; i++) {
        if (i >= part->nelts) {
            if (part->next == NULL) {
                break;
            }
            part = part->next;
            header = part->elts;
            i = 0;
        }
        if (header[i].key.len == ngx_http_push_If_None_Match.len
            && ngx_strncasecmp(header[i].key.data, ngx_http_push_If_None_Match.data, header[i].key.len) == 0) {
            return &header[i].value;
        }
    }
	return NULL;
}

/**
 * create a (finalized) output chain from a buffer, copying it into the relevant request's pool.
 * @param shpool -- optional shmem pool, if you want it to be unlocked while (and if) performing file i/o.
 */
static ngx_chain_t * ngx_http_push_create_output_chain(ngx_http_request_t *r, ngx_buf_t *buf, ngx_slab_pool_t *shpool) {
	//buffer is _copied_
	ngx_chain_t                    *out = ngx_pcalloc(r->pool, sizeof(*out));
	ngx_buf_t                      *buf_copy;
	if(shpool!=NULL) { ngx_shmtx_lock(&shpool->mutex); }
	ngx_http_push_create_buf_copy(buf, buf_copy, r->pool, ngx_pcalloc);
	if (out==NULL || buf_copy==NULL) {
		return NULL;
	}
	
	if (buf->file!=NULL) {
		//here we go with the file juggling
		ngx_file_t             *file = buf_copy->file;
		file->log=r->connection->log;
		if(shpool!=NULL) { ngx_shmtx_unlock(&shpool->mutex); } //unlock before performing i/o 
		//the following assumes file->name.data is already null-terminated
		file->fd=ngx_open_file(file->name.data, NGX_FILE_RDONLY, NGX_FILE_OPEN, NGX_FILE_OWNER_ACCESS);
		if(file->fd==NGX_INVALID_FILE){
			/* i don't think we need to do any file cleanup, since all allocations
			here were done in the request pool which gets cleaned all by itself. */
			return NULL;
		}
	}
	else if(shpool!=NULL) { 
		ngx_shmtx_unlock(&shpool->mutex); 
	} //back to lockdown
	
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

static void ngx_http_push_listener_cleanup(ngx_http_push_listener_cleanup_t *data) {
	if(data->listener!=NULL) {
		if(data->node!=NULL) { 
			ngx_queue_remove(&data->listener->queue);
			data->node->listener_queue_size--;
		}
		ngx_slab_free(data->shpool, data->listener);
	}
}

static void ngx_http_push_reply_status_only(ngx_http_request_t *r, ngx_int_t code, ngx_str_t *statusline) {
	r->headers_out.status=code;
	r->headers_out.status_line.len =statusline->len;
	r->headers_out.status_line.data=statusline->data;
	r->headers_out.content_length_n = 0;
	r->header_only = 1;
	ngx_http_finalize_request(r, ngx_http_send_header(r));
}
