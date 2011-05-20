/*
 *	Copyright 2009 Leo Ponomarev.
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <nginx.h>

#include <ngx_http_push_module.h>
#include <ngx_http_push_rbtree_util.c>
#include <ngx_http_push_module_ipc.c>
#include <ngx_http_push_module_setup.c>

//emergency garbage collecting goodness;
ngx_http_push_channel_queue_t channel_gc_sentinel;

static void ngx_http_push_clean_timeouted_subscriber(ngx_event_t *ev)
{
	ngx_http_push_subscriber_t *subscriber = NULL;
	ngx_http_request_t *r = NULL;

	subscriber = ev->data;
	r = subscriber->request;
	r->discard_body=0; //hacky hacky!

	if (r->connection->destroyed) {
		return;
	}

	ngx_int_t rc = ngx_http_push_respond_status_only(r, NGX_HTTP_NOT_MODIFIED, NULL);
	ngx_http_finalize_request(r, rc);
	//the subscriber and channel counter will be freed by the pool cleanup callback
}

static void ngx_http_push_subscriber_del_timer(ngx_http_push_subscriber_t *sb) {
	if (sb->event.timer_set) {
		ngx_del_timer(&sb->event);
	}
}

static void ngx_http_push_subscriber_clear_ctx(ngx_http_push_subscriber_t *sb) {
	ngx_http_push_subscriber_del_timer(sb);
	sb->clndata->subscriber = NULL;
	sb->clndata->channel = NULL;
}

static ngx_int_t ngx_http_push_channel_collector(ngx_http_push_channel_t * channel, ngx_slab_pool_t * shpool) {
	if((ngx_http_push_clean_channel_locked(channel))!=NULL) { //we're up for deletion
		ngx_http_push_channel_queue_t *trashy;
		if((trashy = ngx_alloc(sizeof(*trashy), ngx_cycle->log))!=NULL) {
			//yeah, i'm allocating memory during garbage collection. sue me.
			trashy->channel=channel;
			ngx_queue_insert_tail(&channel_gc_sentinel.queue, &trashy->queue);
			return NGX_OK;
		}
		return NGX_ERROR;
	}
	return NGX_OK;
}

//garbage-collecting slab allocator
void * ngx_http_push_slab_alloc_locked(size_t size) {
	void  *p;
	if((p = ngx_slab_alloc_locked(ngx_http_push_shpool, size))==NULL) {
		ngx_http_push_channel_queue_t *ccur, *cnext;
		ngx_uint_t                  collected = 0;
		//failed. emergency garbage sweep, then.
		
		//collect channels
		ngx_queue_init(&channel_gc_sentinel.queue);
		ngx_http_push_walk_rbtree(ngx_http_push_channel_collector);
		for(ccur=(ngx_http_push_channel_queue_t *)ngx_queue_next(&channel_gc_sentinel.queue); ccur != &channel_gc_sentinel; ccur=cnext) {
			cnext = (ngx_http_push_channel_queue_t *)ngx_queue_next(&ccur->queue);
			ngx_http_push_delete_channel_locked(ccur->channel);
			ngx_free(ccur);
			collected++;
		}
		
		//todo: collect worker messages maybe
		
		ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "push module: out of shared memory. emergency garbage collection deleted %ui unused channels.", collected);
		
		return ngx_slab_alloc_locked(ngx_http_push_shpool, size);
	}
	return p;
}

void * ngx_http_push_slab_alloc(size_t size) {
	void  *p;
	ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
	p = ngx_http_push_slab_alloc_locked(size);
	ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
	return p;
}

//shpool is assumed to be locked.
static ngx_http_push_msg_t *ngx_http_push_get_latest_message_locked(ngx_http_push_channel_t * channel) {
	ngx_queue_t                    *sentinel = &channel->message_queue->queue; 
	if(ngx_queue_empty(sentinel)) {
		return NULL;
	}
	ngx_queue_t                    *qmsg = ngx_queue_last(sentinel);
	return ngx_queue_data(qmsg, ngx_http_push_msg_t, queue);
}

//shpool must be locked. No memory is freed. O(1)
static ngx_http_push_msg_t *ngx_http_push_get_oldest_message_locked(ngx_http_push_channel_t * channel) {
	ngx_queue_t                    *sentinel = &channel->message_queue->queue; 
	if(ngx_queue_empty(sentinel)) {
		return NULL;
	}
	ngx_queue_t                    *qmsg = ngx_queue_head(sentinel);
	return ngx_queue_data(qmsg, ngx_http_push_msg_t, queue);
}

static void ngx_http_push_reserve_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
	msg->refcount++;
	//we need a refcount because channel messages MAY be dequed before they are used up. It thus falls on the IPC stuff to free it.
}

static void ngx_http_push_release_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
	msg->refcount--;
	if(msg->queue.next==NULL && msg->refcount<=0) { 
		//message had been dequeued and nobody needs it anymore
		ngx_http_push_free_message_locked(msg, ngx_http_push_shpool);
	}
	if(channel->messages > msg->delete_oldest_received_min_messages && ngx_http_push_get_oldest_message_locked(channel) == msg) {
		ngx_http_push_delete_message_locked(channel, msg, ngx_http_push_shpool);
	}
}

// remove a message from queue and free all associated memory. assumes shpool is already locked.
static ngx_inline void ngx_http_push_general_delete_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t force, ngx_slab_pool_t *shpool) {
	if (msg==NULL) { 
		return; 
	}
	if(channel!=NULL) {
		ngx_queue_remove(&msg->queue);
		channel->messages--;
	}
	if(msg->refcount<=0 || force) {
		//nobody needs this message, or we were forced at integer-point to delete
		ngx_http_push_free_message_locked(msg, shpool);
	}
}

//free memory for a message. 
static ngx_inline void ngx_http_push_free_message_locked(ngx_http_push_msg_t *msg, ngx_slab_pool_t *shpool) {
	if(msg->buf->file!=NULL) {
		ngx_shmtx_unlock(&shpool->mutex);
		if(msg->buf->file->fd!=NGX_INVALID_FILE) {
			ngx_close_file(msg->buf->file->fd);
		}
		ngx_delete_file(msg->buf->file->name.data); //should I care about deletion errors? doubt it.
		ngx_shmtx_lock(&shpool->mutex);
	}
	ngx_slab_free_locked(shpool, msg->buf); //separate block, remember?
	ngx_slab_free_locked(shpool, msg);
}

/** find message with entity tags matching those of the request r.
  * @param r subscriber request
  */
static ngx_http_push_msg_t * ngx_http_push_find_message_locked(ngx_http_push_channel_t *channel, ngx_http_request_t *r, ngx_int_t *status) {
	//TODO: consider using an RBTree for message storage.
	ngx_queue_t                    *sentinel = &channel->message_queue->queue;
	ngx_queue_t                    *cur = ngx_queue_head(sentinel);
	ngx_http_push_msg_t            *msg;
	ngx_int_t                       tag = -1;
	time_t                          time = (r->headers_in.if_modified_since == NULL) ? 0 : ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
	
	//channel's message buffer empty?
	if(channel->messages==0) {
		*status=NGX_HTTP_PUSH_MESSAGE_EXPECTED; //wait.
		return NULL;
	}
	
	// do we want a future message?
	msg = ngx_queue_data(sentinel->prev, ngx_http_push_msg_t, queue); 
	if(time <= msg->id.message_time) { //that's an empty check (Sentinel's values are zero)
		if(time == msg->id.message_time) {
			if(tag<0) { tag = ngx_http_push_subscriber_get_etag_int(r); }
			if(tag >= msg->id.message_tag) {
				*status=NGX_HTTP_PUSH_MESSAGE_EXPECTED;
				return NULL;
			}
		}
	}
	else {
		*status=NGX_HTTP_PUSH_MESSAGE_EXPECTED;
		return NULL;
	}
	
	while(cur!=sentinel) {
		msg = ngx_queue_data(cur, ngx_http_push_msg_t, queue);
		if (time < msg->id.message_time) {
			*status = NGX_HTTP_PUSH_MESSAGE_FOUND;
			return msg;
		}
		else if(time == msg->id.message_time) {
			if(tag<0) { tag = ngx_http_push_subscriber_get_etag_int(r); }
			while (tag >= msg->id.message_tag  && time == msg->id.message_time && ngx_queue_next(cur)!=sentinel) {
				cur=ngx_queue_next(cur);
				msg = ngx_queue_data(cur, ngx_http_push_msg_t, queue);
			}
			if(time == msg->id.message_time && tag < msg->id.message_tag) {
				*status = NGX_HTTP_PUSH_MESSAGE_FOUND;
				return msg;
			}
			continue;
		}
		cur=ngx_queue_next(cur);
	}
	*status = NGX_HTTP_PUSH_MESSAGE_EXPIRED; //message too old and was not found.
	return NULL;
}

#define NGX_HTTP_PUSH_NO_CHANNEL_ID_MESSAGE "No channel id provided."
static ngx_str_t * ngx_http_push_get_channel_id(ngx_http_request_t *r, ngx_http_push_loc_conf_t *cf) {
	ngx_http_variable_value_t      *vv = ngx_http_get_indexed_variable(r, cf->index);
	ngx_str_t                      *group = &cf->channel_group;
	size_t                          group_len = group->len;
	size_t                          var_len;
	size_t                          len;
	ngx_str_t                      *id;
    if (vv == NULL || vv->not_found || vv->len == 0) {
        ngx_buf_t *buf = ngx_create_temp_buf(r->pool, sizeof(NGX_HTTP_PUSH_NO_CHANNEL_ID_MESSAGE));
		ngx_chain_t *chain;
		if(buf==NULL) {
			return NULL;
		}
		buf->pos=(u_char *)NGX_HTTP_PUSH_NO_CHANNEL_ID_MESSAGE;
		buf->last=buf->pos + sizeof(NGX_HTTP_PUSH_NO_CHANNEL_ID_MESSAGE)-1;
		chain = ngx_http_push_create_output_chain(buf, r->pool, r->connection->log);
		buf->last_buf=1;
		r->headers_out.content_length_n=ngx_buf_size(buf);
		r->headers_out.status=NGX_HTTP_NOT_FOUND;
		r->headers_out.content_type.len = sizeof("text/plain") - 1;
		r->headers_out.content_type.data = (u_char *) "text/plain";
		r->headers_out.content_type_len = r->headers_out.content_type.len;
		ngx_http_send_header(r);
		ngx_http_output_filter(r, chain);
		ngx_log_error(NGX_LOG_WARN, r->connection->log, 0,
            "push module: the $push_channel_id variable is required but is not set");
		return NULL;
    }
	//maximum length limiter for channel id
	var_len = vv->len <= cf->max_channel_id_length ? vv->len : cf->max_channel_id_length; 
	len = group_len + 1 + var_len;
	if((id = ngx_palloc(r->pool, sizeof(*id) + len))==NULL) {
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "push module: unable to allocate memory for $push_channel_id string");
		return NULL;
	}
	id->len=len;
	id->data=(u_char *)(id+1);
	ngx_memcpy(id->data, group->data, group_len);
	id->data[group_len]='/';
	ngx_memcpy(id->data + group_len + 1, vv->data, var_len);
	return id;
}

#define NGX_HTTP_PUSH_MAKE_ETAG(message_tag, etag, alloc_func, pool)                 \
    etag = alloc_func(pool, sizeof(*etag) + NGX_INT_T_LEN);                          \
    if(etag!=NULL) {                                                                 \
        etag->data = (u_char *)(etag+1);                                             \
        etag->len = ngx_sprintf(etag->data,"%ui", message_tag)- etag->data;          \
    }

#define NGX_HTTP_PUSH_MAKE_CONTENT_TYPE(content_type, content_type_len, msg, pool)  \
    if(((content_type) = ngx_palloc(pool, sizeof(*content_type)+content_type_len))!=NULL) { \
        (content_type)->len=content_type_len;                                        \
        (content_type)->data=(u_char *)((content_type)+1);                           \
        ngx_memcpy(content_type->data, (msg)->content_type.data, content_type_len);  \
    }

#define NGX_HTTP_PUSH_OPTIONS_OK_MESSAGE "Go ahead"

static ngx_int_t ngx_http_push_subscriber_handler(ngx_http_request_t *r) {
	ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *)ngx_http_push_shm_zone->shm.addr;
	ngx_str_t                      *id;
	ngx_http_push_channel_t        *channel;
	ngx_http_push_msg_t            *msg;
	ngx_int_t                       msg_search_outcome;
	
	ngx_str_t                      *content_type=NULL;
	ngx_str_t                      *etag;
	
    if (r->method == NGX_HTTP_OPTIONS) {
        ngx_buf_t *buf = ngx_create_temp_buf(r->pool, sizeof(NGX_HTTP_PUSH_OPTIONS_OK_MESSAGE));
		ngx_chain_t *chain;
		buf->pos=(u_char *)NGX_HTTP_PUSH_OPTIONS_OK_MESSAGE;
		buf->last=buf->pos + sizeof(NGX_HTTP_PUSH_OPTIONS_OK_MESSAGE)-1;
		chain = ngx_http_push_create_output_chain(buf, r->pool, r->connection->log);
		buf->last_buf=1;
        r->headers_out.content_length_n=ngx_buf_size(buf);
		r->headers_out.status=NGX_HTTP_OK;
		ngx_http_send_header(r);
		ngx_http_output_filter(r, chain);
        return NGX_OK;
    }
    
	if (r->method != NGX_HTTP_GET) {
		ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ALLOW, &NGX_HTTP_PUSH_ALLOW_GET); //valid HTTP for the win
		return NGX_HTTP_NOT_ALLOWED;
	}    
	
	if((id=ngx_http_push_get_channel_id(r, cf)) == NULL) {
		return r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR;
	}

	//get the channel and check channel authorization while we're at it.
	ngx_shmtx_lock(&shpool->mutex);
	if (cf->authorize_channel==1) {
		channel = ngx_http_push_find_channel(id, r->connection->log);
	}else{
		channel = ngx_http_push_get_channel(id, r->connection->log, cf->channel_timeout);
	}

	if (channel==NULL) {
		//unable to allocate channel OR channel not found
		ngx_shmtx_unlock(&shpool->mutex);
		if(cf->authorize_channel) {
			return NGX_HTTP_FORBIDDEN;
		}
		else {
			ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate shared memory for channel");
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
	}

    msg = ngx_http_push_find_message_locked(channel, r, &msg_search_outcome); 
    channel->last_seen = ngx_time();
    channel->expires = ngx_time() + cf->channel_timeout;
    ngx_shmtx_unlock(&shpool->mutex);
    
    if (cf->ignore_queue_on_no_cache && !ngx_http_push_allow_caching(r)) {
        msg_search_outcome = NGX_HTTP_PUSH_MESSAGE_EXPECTED; 
        msg = NULL;
    }
	
	switch(ngx_http_push_handle_subscriber_concurrency(r, channel, cf)) {
		case NGX_DECLINED: //this request was declined for some reason.
			//status codes and whatnot should have already been written. just get out of here quickly.
			return NGX_OK;
		case NGX_ERROR:
			ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: error handling subscriber concurrency setting");
			return NGX_ERROR;
	}

	switch(msg_search_outcome) {
		//for message-found:
		ngx_chain_t                *chain;
		time_t                      last_modified;
		size_t                      content_type_len;

		case NGX_HTTP_PUSH_MESSAGE_EXPECTED:
			// ♫ It's gonna be the future soon ♫
			switch(cf->subscriber_poll_mechanism) {
				//for NGX_HTTP_PUSH_MECHANISM_LONGPOLL
				ngx_http_push_pid_queue_t  *sentinel, *cur, *found;
				ngx_http_push_subscriber_t *subscriber;
				ngx_http_push_subscriber_t *subscriber_sentinel;
				
				case NGX_HTTP_PUSH_MECHANISM_LONGPOLL:
					//long-polling subscriber. wait for a message.
					
					//subscribers are queued up in a local pool. Queue sentinels are separate and also local, but not in the pool.
					ngx_shmtx_lock(&shpool->mutex);
					sentinel = &channel->workers_with_subscribers;
					cur = (ngx_http_push_pid_queue_t *)ngx_queue_head(&sentinel->queue);
					found = NULL;
					
					ngx_http_push_subscriber_cleanup_t *clndata;
					ngx_pool_cleanup_t             *cln;
					while(cur!=sentinel) {
						if(cur->pid==ngx_pid) {
							found = cur;
							break;
						}
						cur = (ngx_http_push_pid_queue_t *)ngx_queue_next(&cur->queue);
					}
					if(found == NULL) { //found nothing
						if((found=ngx_http_push_slab_alloc_locked(sizeof(*found)))==NULL) {
							ngx_shmtx_unlock(&shpool->mutex);
							ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate worker subscriber queue marker in shared memory");
							return NGX_HTTP_INTERNAL_SERVER_ERROR;
						}
						//initialize
						ngx_queue_insert_tail(&sentinel->queue, &found->queue);
						found->pid=ngx_pid;
						found->slot=ngx_process_slot;
						found->subscriber_sentinel=NULL;
					}
					ngx_shmtx_unlock(&shpool->mutex);
					
					if((subscriber = ngx_palloc(ngx_http_push_pool, sizeof(*subscriber)))==NULL) { //unable to allocate request queue element
						return NGX_ERROR;
					}
					
					 //attach a cleaner to remove the request from the channel.
					if ((cln=ngx_pool_cleanup_add(r->pool, sizeof(*clndata))) == NULL) { //make sure we can.
						return NGX_ERROR;
					}
					cln->handler = (ngx_pool_cleanup_pt) ngx_http_push_subscriber_cleanup;
					clndata = (ngx_http_push_subscriber_cleanup_t *) cln->data;
					clndata->channel=channel;
					clndata->subscriber=subscriber;
					
					subscriber->request = r;
					subscriber->clndata=clndata;
					
					ngx_shmtx_lock(&shpool->mutex);
					channel->subscribers++; // do this only when we know everything went okay.
					
					//figure out the subscriber sentinel
					subscriber_sentinel = ((ngx_http_push_pid_queue_t *)found)->subscriber_sentinel;
					if(subscriber_sentinel==NULL) {
						//it's perfectly nornal for the sentinel to be NULL.
						if((subscriber_sentinel=ngx_palloc(ngx_http_push_pool, sizeof(*subscriber_sentinel)))==NULL) {
							ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate channel subscriber sentinel");
							return NGX_HTTP_INTERNAL_SERVER_ERROR;
						}
						ngx_queue_init(&subscriber_sentinel->queue);
						((ngx_http_push_pid_queue_t *)found)->subscriber_sentinel=subscriber_sentinel;
					}
					ngx_shmtx_unlock(&shpool->mutex);
					
					ngx_queue_insert_tail(&subscriber_sentinel->queue, &subscriber->queue);
					
					ngx_memzero(&subscriber->event, sizeof(subscriber->event));
					if (cf->subscriber_timeout > 0) {		
						subscriber->event.handler = ngx_http_push_clean_timeouted_subscriber;	
						subscriber->event.data = subscriber;
						subscriber->event.log = r->connection->log;
						ngx_add_timer(&subscriber->event, cf->subscriber_timeout * 1000);
					}

					//r->read_event_handler = ngx_http_test_reading;
					//r->write_event_handler = ngx_http_request_empty_handler;
					r->discard_body = 1;
					//r->keepalive = 1; //stayin' alive!!
					return NGX_DONE;
					
				case NGX_HTTP_PUSH_MECHANISM_INTERVALPOLL:
				
					//interval-polling subscriber requests get a 304 with their entity tags preserved.
					if (r->headers_in.if_modified_since != NULL) {
						r->headers_out.last_modified_time=ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
					}
					if ((etag=ngx_http_push_subscriber_get_etag(r)) != NULL) {
						r->headers_out.etag=ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ETAG, etag);
					}
					return NGX_HTTP_NOT_MODIFIED;
					
				default:
					//if this ever happens, there's a bug somewhere else. probably config stuff.
					return NGX_HTTP_INTERNAL_SERVER_ERROR;
			}
		
		case NGX_HTTP_PUSH_MESSAGE_EXPIRED:
			//subscriber wants an expired message
			//TODO: maybe respond with entity-identifiers for oldest available message?
			return NGX_HTTP_NO_CONTENT; 
		
		case NGX_HTTP_PUSH_MESSAGE_FOUND:
			//found the message
			ngx_shmtx_lock(&shpool->mutex);
			ngx_http_push_reserve_message_locked(channel, msg);
			NGX_HTTP_PUSH_MAKE_ETAG(msg->id.message_tag, etag, ngx_palloc, r->pool);
			if(etag==NULL) {
				//oh, nevermind...
				ngx_shmtx_unlock(&shpool->mutex);
				ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate memory for Etag header");
				return NGX_ERROR;
			}
			
			content_type_len = msg->content_type.len;
			if(content_type_len>0) {
				NGX_HTTP_PUSH_MAKE_CONTENT_TYPE(content_type, content_type_len, msg, r->pool);
				if(content_type==NULL) {
					ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate memory for content-type header while responding to subscriber request");
					ngx_shmtx_unlock(&shpool->mutex);
					return NGX_ERROR;
				}
			}
			
			//preallocate output chain. yes, same one for every waiting subscriber
			if((chain = ngx_http_push_create_output_chain_locked(msg->buf, r->pool, r->connection->log, shpool))==NULL) {
				ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: unable to allocate buffer chain while responding to subscriber request");
				ngx_shmtx_unlock(&shpool->mutex);
				return NGX_ERROR;
			}
			
			last_modified = msg->id.message_time;
			
			//is the message still needed?
			ngx_http_push_release_message_locked(channel, msg);
			ngx_shmtx_unlock(&shpool->mutex);
			
			if(chain->buf->file!=NULL) {
				//close file when we're done with it
				ngx_pool_cleanup_t *cln;
				ngx_pool_cleanup_file_t *clnf;
			 
				if((cln = ngx_pool_cleanup_add(r->pool, sizeof(ngx_pool_cleanup_file_t)))==NULL) {
					return NGX_HTTP_INTERNAL_SERVER_ERROR;
				}
				cln->handler = ngx_pool_cleanup_file;
				clnf = cln->data;
				clnf->fd = chain->buf->file->fd;
				clnf->name = chain->buf->file->name.data;
				clnf->log = r->pool->log;
			}
			
			return ngx_http_push_prepare_response_to_subscriber_request(r, chain, content_type, etag, last_modified);
			
		default: //we shouldn't be here.
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
}

static ngx_int_t ngx_http_push_handle_subscriber_concurrency(ngx_http_request_t *r, ngx_http_push_channel_t *channel, ngx_http_push_loc_conf_t *loc_conf) {
	ngx_int_t                      max_subscribers = loc_conf->max_channel_subscribers;
	ngx_int_t                      current_subscribers;
	ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
	current_subscribers = channel->subscribers;
	ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
	if(current_subscribers==0) { 
		//empty channels are always okay.
		return NGX_OK;
	}	
	
	if(max_subscribers!=0 && current_subscribers >= max_subscribers) {
		//max_channel_subscribers setting
		ngx_http_push_respond_status_only(r, NGX_HTTP_FORBIDDEN, NULL);
		return NGX_DECLINED;
	}
	
	//nonzero number of subscribers present
	switch(loc_conf->subscriber_concurrency) {
		case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_BROADCAST:
			return NGX_OK;
		
		case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_LASTIN:
			ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
			//send "everyone" a 409 Conflict response.
			//in most reasonable cases, there'll be at most one subscriber on the
			//channel. However, since settings are bound to locations and not
			//specific channels, this assumption need not hold. Hence this broadcast.
			ngx_http_push_broadcast_status_locked(channel, NGX_HTTP_NOT_FOUND, &NGX_HTTP_PUSH_HTTP_STATUS_409, r->connection->log, ngx_http_push_shpool);
			ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
			
			return NGX_OK;
		
		case NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_FIRSTIN:
			ngx_http_push_respond_status_only(r, NGX_HTTP_NOT_FOUND, &NGX_HTTP_PUSH_HTTP_STATUS_409);
			return NGX_DECLINED;
		
		default:
			return NGX_ERROR;
	}
}

static ngx_int_t ngx_http_push_broadcast_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line, ngx_log_t *log, ngx_slab_pool_t *shpool) {
	//subscribers are queued up in a local pool. Queue heads, however, are located
	//in shared memory, identified by pid.
	ngx_http_push_pid_queue_t     *sentinel = &channel->workers_with_subscribers;
	ngx_http_push_pid_queue_t     *cur = sentinel;
	ngx_int_t                      received;
	received = channel->subscribers > 0 ? NGX_HTTP_PUSH_MESSAGE_RECEIVED : NGX_HTTP_PUSH_MESSAGE_QUEUED;

	if(msg!=NULL && received==NGX_HTTP_PUSH_MESSAGE_RECEIVED) {
		ngx_http_push_reserve_message_locked(channel, msg);
	}
	
	while((cur=(ngx_http_push_pid_queue_t *)ngx_queue_next(&cur->queue))!=sentinel) {
		pid_t           worker_pid  = cur->pid;
		ngx_int_t       worker_slot = cur->slot;
		ngx_http_push_subscriber_t *subscriber_sentinel= cur->subscriber_sentinel;
		
		ngx_shmtx_unlock(&shpool->mutex);
		if(worker_pid == ngx_pid) {
			//my subscribers
			ngx_http_push_respond_to_subscribers(channel, subscriber_sentinel, msg, status_code, status_line);
		}
		else {
			//some other worker's subscribers
			//interprocess communication breakdown
			if(ngx_http_push_send_worker_message(channel, subscriber_sentinel, worker_pid, worker_slot, msg, status_code, log) != NGX_ERROR) {
				ngx_http_push_alert_worker(worker_pid, worker_slot, log);
			}
			else {
				ngx_log_error(NGX_LOG_ERR, log, 0, "push module: error communicating with some other worker process");
			}
			
		}
		ngx_shmtx_lock(&shpool->mutex);
		/*
		each time all of a worker's subscribers are removed, so is the sentinel. 
		this is done to make garbage collection easier. Assuming we want to avoid
		placing the sentinel in shared memory (for now -- it's a little tricky
		to debug), the owner of the worker pool must be the one to free said sentinel.
		But channels may be deleted by different worker processes, and it seems unwieldy
		(for now) to do IPC just to delete one stinkin' sentinel. Hence a new sentinel
		is used every time the subscriber queue is emptied.
		*/
		cur->subscriber_sentinel = NULL; //think about it it terms of garbage collection. it'll make sense. sort of.
		
	}
	return received;
}

#define NGX_HTTP_BUF_ALLOC_SIZE(buf)                                          \
    (sizeof(*buf) +                                                           \
	 (((buf)->temporary || (buf)->memory) ? ngx_buf_size(buf) : 0) +          \
	 (((buf)->file!=NULL) ? (sizeof(*(buf)->file) + (buf)->file->name.len + 1) : 0))

#define NGX_HTTP_PUSH_PUBLISHER_CHECK(val, fail, r, errormessage)             \
    if (val == fail) {                                                        \
        ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, errormessage);    \
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);         \
        return;                                                               \
	}
#define NGX_HTTP_PUSH_PUBLISHER_CHECK_LOCKED(val, fail, r, errormessage, shpool) \
    if (val == fail) {                                                        \
        ngx_shmtx_unlock(&(shpool)->mutex);                                   \
        ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, errormessage);    \
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);         \
        return;                                                               \
    }
	
static void ngx_http_push_publisher_body_handler(ngx_http_request_t * r) { 
	ngx_str_t                      *id;
	ngx_http_push_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, ngx_http_push_module);
	ngx_slab_pool_t                *shpool = (ngx_slab_pool_t *) ngx_http_push_shm_zone->shm.addr;
	ngx_buf_t                      *buf = NULL, *buf_copy;
	ngx_http_push_channel_t        *channel;
	ngx_uint_t                      method = r->method;
	
	time_t                          last_seen = 0;
	ngx_uint_t                      subscribers = 0;
	ngx_uint_t                      messages = 0;
	
	if((id = ngx_http_push_get_channel_id(r, cf))==NULL) {
		ngx_http_finalize_request(r, r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
	}
	
	ngx_shmtx_lock(&shpool->mutex);
	//POST requests will need a channel created if it doesn't yet exist.
	if(method==NGX_HTTP_POST || method==NGX_HTTP_PUT) {
		if(method==NGX_HTTP_POST && (r->headers_in.content_length_n == -1 || r->headers_in.content_length_n == 0)) {
			NGX_HTTP_PUSH_PUBLISHER_CHECK_LOCKED(0, 0, r, "push module: trying to push an empty message", shpool);
		}
		channel = ngx_http_push_get_channel(id, r->connection->log, cf->channel_timeout);
		NGX_HTTP_PUSH_PUBLISHER_CHECK_LOCKED(channel, NULL, r, "push module: unable to allocate memory for new channel", shpool);
	}
	//no other request method needs that.
	else {
		//just find the channel. if it's not there, NULL.
		channel = ngx_http_push_find_channel(id, r->connection->log);
	}
	
	if(channel!=NULL) {
		subscribers = channel->subscribers;
		last_seen = channel->last_seen;
		messages  = channel->messages;
	}
	else {
		//404!
		ngx_shmtx_unlock(&shpool->mutex);
		r->headers_out.status=NGX_HTTP_NOT_FOUND;
		
		//just the headers, please. we don't care to describe the situation or
		//respond with an html page
		r->headers_out.content_length_n=0;
		r->header_only = 1;
		
		ngx_http_finalize_request(r, ngx_http_send_header(r));
		return;
	}
	ngx_shmtx_unlock(&shpool->mutex);

	switch(method) {
		ngx_http_push_msg_t        *msg, *previous_msg;
		size_t                      content_type_len;
		ngx_http_push_msg_t        *sentinel;
		
		case NGX_HTTP_POST:
			//first off, we'll want to extract the body buffer
		
			//note: this works mostly because of r->request_body_in_single_buf = 1; 
			//which, i suppose, makes this module a little slower than it could be.
			//this block is a little hacky. might be a thorn for forward-compatibility.
			if(r->headers_in.content_length_n == -1 || r->headers_in.content_length_n == 0) {
				buf = ngx_create_temp_buf(r->pool, 0);
				//this buffer will get copied to shared memory in a few lines, 
				//so it does't matter what pool we make it in.
			}
			else if(r->request_body->bufs->buf!=NULL) { //everything in the first buffer, please
				buf=r->request_body->bufs->buf;
			}
			else if(r->request_body->bufs->next!=NULL) {
				buf=r->request_body->bufs->next->buf;
			}
			else {
				ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, "push module: unexpected publisher message request body buffer location. please report this to the push module developers.");
				ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
				return;
			}
			
			NGX_HTTP_PUSH_PUBLISHER_CHECK(buf, NULL, r, "push module: can't find or allocate publisher request body buffer");
					
			content_type_len = (r->headers_in.content_type!=NULL ? r->headers_in.content_type->value.len : 0);
			
			ngx_shmtx_lock(&shpool->mutex);
			
			//create a buffer copy in shared mem
			msg = ngx_http_push_slab_alloc_locked(sizeof(*msg) + content_type_len);
			NGX_HTTP_PUSH_PUBLISHER_CHECK_LOCKED(msg, NULL, r, "push module: unable to allocate message in shared memory", shpool);
			previous_msg=ngx_http_push_get_latest_message_locked(channel); //need this for entity-tags generation
			
			buf_copy = ngx_http_push_slab_alloc_locked(NGX_HTTP_BUF_ALLOC_SIZE(buf));
			NGX_HTTP_PUSH_PUBLISHER_CHECK_LOCKED(buf_copy, NULL, r, "push module: unable to allocate buffer in shared memory", shpool) //magic nullcheck
			ngx_http_push_copy_preallocated_buffer(buf, buf_copy);
			
			msg->buf=buf_copy;
			
			if(cf->store_messages) {
				ngx_queue_insert_tail(&channel->message_queue->queue, &msg->queue);
				channel->messages++;
			}
			
			//Stamp the new message with entity tags
			msg->id.message_time=ngx_time(); //ESSENTIAL TODO: make sure this ends up producing GMT time
			msg->id.message_tag=(previous_msg!=NULL && msg->id.message_time == previous_msg->id.message_time) ? (previous_msg->id.message_tag + 1) : 0;		
			
			//store the content-type
			if(content_type_len>0) {
				msg->content_type.len=r->headers_in.content_type->value.len;
				msg->content_type.data=(u_char *) (msg+1); //we had reserved a contiguous chunk, myes?
				ngx_memcpy(msg->content_type.data, r->headers_in.content_type->value.data, msg->content_type.len);
			}
			else {
				msg->content_type.len=0;
				msg->content_type.data=NULL;
			}
			
			//set message expiration time
			time_t                  message_timeout = cf->buffer_timeout;
			msg->expires = (message_timeout==0 ? 0 : (ngx_time() + message_timeout));
			
			msg->delete_oldest_received_min_messages = cf->delete_oldest_received_message ? (ngx_uint_t) cf->min_messages : NGX_MAX_UINT32_VALUE;
			//NGX_MAX_UINT32_VALUE to disable, otherwise = min_message_buffer_size of the publisher location from whence the message came
			
			//FMI (For My Information): shm is still locked.
			switch(ngx_http_push_broadcast_message_locked(channel, msg, r->connection->log, shpool)) {
				
				case NGX_HTTP_PUSH_MESSAGE_QUEUED:
					//message was queued successfully, but there were no 
					//subscribers to receive it.
					r->headers_out.status = NGX_HTTP_ACCEPTED;
					r->headers_out.status_line.len =sizeof("202 Accepted")- 1;
					r->headers_out.status_line.data=(u_char *) "202 Accepted";
					break;
					
				case NGX_HTTP_PUSH_MESSAGE_RECEIVED:
					//message was queued successfully, and it was already sent
					//to at least one subscriber
					r->headers_out.status = NGX_HTTP_CREATED;
					r->headers_out.status_line.len =sizeof("201 Created")- 1;
					r->headers_out.status_line.data=(u_char *) "201 Created";
					
					//update the number of times the message was received.
					//in the interest of premature optimization, I assume all
					//current subscribers have received the message successfully.
					break;
					
				case NGX_ERROR:
					//WTF?
					ngx_shmtx_unlock(&shpool->mutex);
					ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: error broadcasting message to workers");
					ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
					return;
					
				default: 
					//for debugging, mostly. I don't expect this branch to be
					//hit during regular operation
					ngx_shmtx_unlock(&shpool->mutex);
					ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "push module: TOTALLY UNEXPECTED error broadcasting message to workers");
					ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
					return;
			}
			//shm is still locked I hope.
			
			if(buf->file!=NULL) {
				//future subscribers won't be able to use this file descriptor --
				//it will be closed once the publisher request is finalized. 
				//(That's about to happen a handful of lines below.)
				msg->buf->file->fd=NGX_INVALID_FILE;
			}
			
			//now see if the queue is too big
			if(channel->messages > (ngx_uint_t) cf->max_messages) {
				//exceeeds max queue size. force-delete oldest message
				ngx_http_push_force_delete_message_locked(channel, ngx_http_push_get_oldest_message_locked(channel), shpool);
			}
			if(channel->messages > (ngx_uint_t) cf->min_messages) {
				//exceeeds min queue size. maybe delete the oldest message
				ngx_http_push_msg_t    *oldest_msg = ngx_http_push_get_oldest_message_locked(channel);
				NGX_HTTP_PUSH_PUBLISHER_CHECK_LOCKED(oldest_msg, NULL, r, "push module: oldest message not found", shpool);
			}
			messages = channel->messages;
			
			ngx_shmtx_unlock(&shpool->mutex);
			ngx_http_finalize_request(r, ngx_http_push_channel_info(r, messages, subscribers, last_seen));
			return;
			
		case NGX_HTTP_PUT:
		case NGX_HTTP_GET:
			r->headers_out.status = NGX_HTTP_OK;
			ngx_http_finalize_request(r, ngx_http_push_channel_info(r, messages, subscribers, last_seen));
			return;
			
		case NGX_HTTP_DELETE:
			ngx_shmtx_lock(&shpool->mutex);
			sentinel = channel->message_queue; 
			msg = sentinel;
						
			while((msg=(ngx_http_push_msg_t *)ngx_queue_next(&msg->queue))!=sentinel) {
				//force-delete all the messages
				ngx_http_push_force_delete_message_locked(NULL, msg, shpool);
			}
			channel->messages=0;
			
			//410 gone
			NGX_HTTP_PUSH_PUBLISHER_CHECK_LOCKED(ngx_http_push_broadcast_status_locked(channel, NGX_HTTP_GONE, &NGX_HTTP_PUSH_HTTP_STATUS_410, r->connection->log, shpool), NGX_ERROR, r, "push module: unable to send current subscribers a 410 Gone response", shpool);
			ngx_http_push_delete_channel_locked(channel);
			ngx_shmtx_unlock(&shpool->mutex);
			//done.
			r->headers_out.status=NGX_HTTP_OK;
			ngx_http_finalize_request(r, ngx_http_push_channel_info(r, messages, subscribers, last_seen));
			return;
			
		default: 
			//some other weird request method
			ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ALLOW, &NGX_HTTP_PUSH_ALLOW_GET_POST_PUT_DELETE);
			ngx_http_finalize_request(r, NGX_HTTP_NOT_ALLOWED);
			return;
	}
}

static ngx_int_t ngx_http_push_respond_to_subscribers(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *sentinel, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line) {
	ngx_slab_pool_t                *shpool = ngx_http_push_shpool;
	ngx_http_push_subscriber_t     *cur, *next;
	ngx_int_t                       responded_subscribers=0;
	if(sentinel==NULL) {
		return NGX_OK;
	}
	
	cur=(ngx_http_push_subscriber_t *)ngx_queue_head(&sentinel->queue);
	if(msg!=NULL) {
		//copy everything we need first
		ngx_str_t                  *content_type=NULL;
		ngx_str_t                  *etag=NULL;
		time_t                      last_modified_time;
		ngx_chain_t                *chain;
		size_t                      content_type_len;
		ngx_http_request_t         *r;
		ngx_buf_t                  *buffer;
		u_char                     *pos;
		
		ngx_shmtx_lock(&shpool->mutex);
		
		//etag
		NGX_HTTP_PUSH_MAKE_ETAG(msg->id.message_tag, etag, ngx_pcalloc, ngx_http_push_pool);
		if(etag==NULL) {
			//oh, nevermind...
			ngx_shmtx_unlock(&shpool->mutex);
			return NGX_ERROR;
		}
		
		//content-type
		content_type_len = msg->content_type.len;
		if(content_type_len>0) {
			NGX_HTTP_PUSH_MAKE_CONTENT_TYPE(content_type, content_type_len, msg, ngx_http_push_pool);
			if(content_type==NULL) {
				ngx_shmtx_unlock(&shpool->mutex);
				ngx_pfree(ngx_http_push_pool, etag);
				ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to allocate memory for content-type header while responding to several subscriber request");
				return NGX_ERROR;
			}
		}
		
		//preallocate output chain. yes, same one for every waiting subscriber
		if((chain = ngx_http_push_create_output_chain_locked(msg->buf, ngx_http_push_pool, ngx_cycle->log, shpool))==NULL) {
			ngx_shmtx_unlock(&shpool->mutex);
			ngx_pfree(ngx_http_push_pool, etag);
			ngx_pfree(ngx_http_push_pool, content_type);
			ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "push module: unable to create output chain while responding to several subscriber request");
			return NGX_ERROR;
		}
		
		buffer = chain->buf;
		pos = buffer->pos;
		
		last_modified_time = msg->id.message_time;
		
		ngx_shmtx_unlock(&shpool->mutex);
		
		//now let's respond to some requests!
		while(cur!=sentinel) {
			next=(ngx_http_push_subscriber_t *)ngx_queue_next(&cur->queue);
			//in this block, nothing in shared memory should be dereferenced.
			r=cur->request;
			//cleanup oughtn't dequeue anything. or decrement the subscriber count, for that matter
			ngx_http_push_subscriber_clear_ctx(cur);
			
			r->discard_body=0; //hacky hacky!
			
			ngx_http_finalize_request(r, ngx_http_push_prepare_response_to_subscriber_request(r, chain, content_type, etag, last_modified_time)); //BAM!
			responded_subscribers++;
			
			//done with this subscriber. free the sucker.
			ngx_pfree(ngx_http_push_pool, cur);
			
			//rewind the buffer, please
			buffer->pos = pos;
			buffer->last_buf=1;
			
			cur=next;
		}
		
		//free everything relevant
		ngx_pfree(ngx_http_push_pool, etag);
		ngx_pfree(ngx_http_push_pool, content_type);
		if(buffer->file) {
			ngx_close_file(buffer->file->fd);
		}
		ngx_pfree(ngx_http_push_pool, buffer);
		ngx_pfree(ngx_http_push_pool, chain);
		
		if(responded_subscribers) {
			ngx_shmtx_lock(&shpool->mutex);
			//message deletion
			ngx_http_push_release_message_locked(channel, msg);
			ngx_shmtx_unlock(&shpool->mutex);
		}
	}
	else {
		//headers only probably
		ngx_http_request_t     *r;
		while(cur!=sentinel) {
			next=(ngx_http_push_subscriber_t *)ngx_queue_next(&cur->queue);
			r=cur->request;
			
			//cleanup oughtn't dequeue anything. or decrement the subscriber count, for that matter
			ngx_http_push_subscriber_clear_ctx(cur);
			ngx_http_finalize_request(r, ngx_http_push_respond_status_only(r, status_code, status_line));
			responded_subscribers++;
			ngx_pfree(ngx_http_push_pool, cur);
			cur=next;
		}
	}
	ngx_shmtx_lock(&shpool->mutex);
	channel->subscribers-=responded_subscribers;
	//is the message still needed?
	ngx_shmtx_unlock(&shpool->mutex);
	ngx_pfree(ngx_http_push_pool, sentinel);
	return NGX_OK;
}

static ngx_int_t ngx_http_push_publisher_handler(ngx_http_request_t * r) {
	ngx_int_t                       rc;
	
	/* Instruct ngx_http_read_subscriber_request_body to store the request
	   body entirely in a memory buffer or in a file */
	r->request_body_in_single_buf = 1;
	r->request_body_in_persistent_file = 1;
	r->request_body_in_clean_file = 0;
	r->request_body_file_log_level = 0;

	rc = ngx_http_read_client_request_body(r, ngx_http_push_publisher_body_handler);
	if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
		return rc;
	}
	return NGX_DONE;
}
		
static void ngx_http_push_match_channel_info_subtype(size_t off, u_char *cur, size_t rem, u_char **priority, const ngx_str_t **format, ngx_str_t *content_type) {
	static ngx_http_push_content_subtype_t subtypes[] = {
		{ "json"  , 4, &NGX_HTTP_PUSH_CHANNEL_INFO_JSON },
		{ "yaml"  , 4, &NGX_HTTP_PUSH_CHANNEL_INFO_YAML },
		{ "xml"   , 3, &NGX_HTTP_PUSH_CHANNEL_INFO_XML  },
		{ "x-json", 6, &NGX_HTTP_PUSH_CHANNEL_INFO_JSON },
		{ "x-yaml", 6, &NGX_HTTP_PUSH_CHANNEL_INFO_YAML }
	};
	u_char                         *start = cur + off;
	ngx_uint_t                      i;
	
	for(i=0; i<(sizeof(subtypes)/sizeof(ngx_http_push_content_subtype_t)); i++) {
		if(ngx_strncmp(start, subtypes[i].subtype, rem<subtypes[i].len ? rem : subtypes[i].len)==0) {
			if(*priority>start) {
				*format = subtypes[i].format;
				*priority = start;
				content_type->data=cur;
				content_type->len= off + 1 + subtypes[i].len;
			}
		}
	}
}

//print information about a channel
static ngx_int_t ngx_http_push_channel_info(ngx_http_request_t *r, ngx_uint_t messages, ngx_uint_t subscribers, time_t last_seen) {
	ngx_buf_t                      *b;
	ngx_uint_t                      len;
	ngx_str_t                       content_type = ngx_string("text/plain");
	const ngx_str_t                *format = &NGX_HTTP_PUSH_CHANNEL_INFO_PLAIN;
	time_t                          time_elapsed = ngx_time() - last_seen;
	
	if(r->headers_in.accept) {
		//lame content-negotiation (without regard for qvalues)
		u_char                    *accept = r->headers_in.accept->value.data;
		size_t                     len = r->headers_in.accept->value.len;
		size_t                     rem;
		u_char                    *cur = accept;
		u_char                    *priority=&accept[len-1];
		for(rem=len; (cur = ngx_strnstr(cur, "text/", rem))!=NULL; cur += sizeof("text/")-1) {
			rem=len - ((size_t)(cur-accept)+sizeof("text/")-1);
			if(ngx_strncmp(cur+sizeof("text/")-1, "plain", rem<5 ? rem : 5)==0) {
				if(priority) {
					format = &NGX_HTTP_PUSH_CHANNEL_INFO_PLAIN;
					priority = cur+sizeof("text/")-1;
					//content-type is already set by default
				}
			}
			ngx_http_push_match_channel_info_subtype(sizeof("text/")-1, cur, rem, &priority, &format, &content_type);
		}
		cur = accept;
		for(rem=len; (cur = ngx_strnstr(cur, "application/", rem))!=NULL; cur += sizeof("application/")-1) {
			rem=len - ((size_t)(cur-accept)+sizeof("application/")-1);
			ngx_http_push_match_channel_info_subtype(sizeof("application/")-1, cur, rem, &priority, &format, &content_type);
		}
	}

	r->headers_out.content_type.len = content_type.len;
	r->headers_out.content_type.data = content_type.data;
	r->headers_out.content_type_len = r->headers_out.content_type.len;
	
	len = format->len - 8 - 1 + 3*NGX_INT_T_LEN; //minus 8 sprintf
	
	if ((b = ngx_create_temp_buf(r->pool, len)) == NULL) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	b->last = ngx_sprintf(b->last, (char *)format->data, messages, last_seen==0 ? -1 : (ngx_int_t) time_elapsed ,subscribers);
	
	//lastly, set the content-length, because if the status code isn't 200, nginx may not do so automatically
	r->headers_out.content_length_n = ngx_buf_size(b);
	
	if (ngx_http_send_header(r) > NGX_HTTP_SPECIAL_RESPONSE) {
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	
	return ngx_http_output_filter(r, ngx_http_push_create_output_chain(b, r->pool, r->connection->log));
}

static ngx_table_elt_t * ngx_http_push_add_response_header(ngx_http_request_t *r, const ngx_str_t *header_name, const ngx_str_t *header_value) {
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

static ngx_int_t ngx_http_push_subscriber_get_etag_int(ngx_http_request_t * r) {
	ngx_str_t                      *if_none_match = ngx_http_push_subscriber_get_etag(r);
	ngx_int_t                       tag;
	if(if_none_match==NULL || (if_none_match!=NULL && (tag = ngx_atoi(if_none_match->data, if_none_match->len))==NGX_ERROR)) {
		tag=0;
	}
	return ngx_abs(tag);
}

static ngx_str_t * ngx_http_push_find_in_header_value(ngx_http_request_t * r, ngx_str_t header_name) {
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
        if (header[i].key.len == header_name.len
            && ngx_strncasecmp(header[i].key.data, header_name.data, header[i].key.len) == 0) {
            return &header[i].value;
        }
    }
	return NULL;
}

static ngx_int_t ngx_http_push_allow_caching(ngx_http_request_t * r) {
    ngx_str_t *tmp_header;
    ngx_str_t header_checks[2] = { NGX_HTTP_PUSH_HEADER_CACHE_CONTROL, NGX_HTTP_PUSH_HEADER_PRAGMA };
    ngx_int_t i = 0;

    for(; i < 2; i++) {
        tmp_header = ngx_http_push_find_in_header_value(r, header_checks[i]);

        if (tmp_header != NULL) {
            return !!ngx_strncasecmp(tmp_header->data, NGX_HTTP_PUSH_CACHE_CONTROL_VALUE.data, tmp_header->len);
        }
    }

    return 1;
}

static ngx_str_t * ngx_http_push_subscriber_get_etag(ngx_http_request_t * r) {
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
        if (header[i].key.len == NGX_HTTP_PUSH_HEADER_IF_NONE_MATCH.len
            && ngx_strncasecmp(header[i].key.data, NGX_HTTP_PUSH_HEADER_IF_NONE_MATCH.data, header[i].key.len) == 0) {
            return &header[i].value;
        }
    }
	return NULL;
}

//buffer is _copied_
//if shpool is provided, it is assumed that shm it is locked
static ngx_chain_t * ngx_http_push_create_output_chain_general(ngx_buf_t *buf, ngx_pool_t *pool, ngx_log_t *log, ngx_slab_pool_t *shpool) {
	ngx_chain_t                    *out;
	ngx_file_t                     *file;
	
	if((out = ngx_pcalloc(pool, sizeof(*out)))==NULL) {
		return NULL;
	}
	ngx_buf_t                      *buf_copy;

	if((buf_copy = ngx_pcalloc(pool, NGX_HTTP_BUF_ALLOC_SIZE(buf)))==NULL) {
		return NULL;
	}
	ngx_http_push_copy_preallocated_buffer(buf, buf_copy);

	if (buf->file!=NULL) {
		file = buf_copy->file;
		file->log=log;
		if(file->fd==NGX_INVALID_FILE) {
			if(shpool) {
				ngx_shmtx_unlock(&shpool->mutex);
				file->fd=ngx_open_file(file->name.data, NGX_FILE_RDONLY, NGX_FILE_OPEN, NGX_FILE_OWNER_ACCESS);
				ngx_shmtx_lock(&shpool->mutex);
			}
			else {
				file->fd=ngx_open_file(file->name.data, NGX_FILE_RDONLY, NGX_FILE_OPEN, NGX_FILE_OWNER_ACCESS);
			}
		}
		if(file->fd==NGX_INVALID_FILE) {
			return NULL;
		}
	}
	buf_copy->last_buf = 1;
	out->buf = buf_copy;
	out->next = NULL;
	return out;	
}

static void ngx_http_push_subscriber_cleanup(ngx_http_push_subscriber_cleanup_t *data) {
	if(data->subscriber!=NULL) { //still queued up
		ngx_http_push_subscriber_t* sb = data->subscriber;

		ngx_http_push_subscriber_del_timer(sb);
		ngx_queue_remove(&data->subscriber->queue);
		ngx_pfree(ngx_http_push_pool, data->subscriber); //was there an error? oh whatever.
	}
	if(data->channel!=NULL) { //we're expected to decrement the subscriber count
		ngx_shmtx_lock(&ngx_http_push_shpool->mutex);
		data->channel->subscribers--;
		ngx_shmtx_unlock(&ngx_http_push_shpool->mutex);
	}
}

static ngx_int_t ngx_http_push_respond_status_only(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *statusline) {
	r->headers_out.status=status_code;
	if(statusline!=NULL) {
		r->headers_out.status_line.len =statusline->len;
		r->headers_out.status_line.data=statusline->data;
	}
	r->headers_out.content_length_n = 0;
	r->header_only = 1;
	return ngx_http_send_header(r);
}

//allocates nothing
static ngx_int_t ngx_http_push_prepare_response_to_subscriber_request(ngx_http_request_t *r, ngx_chain_t *chain, ngx_str_t *content_type, ngx_str_t *etag, time_t last_modified) {
	ngx_int_t                      res;
	if (content_type!=NULL) {
		r->headers_out.content_type.len=content_type->len;
		r->headers_out.content_type.data = content_type->data;
		r->headers_out.content_type_len = r->headers_out.content_type.len;
	}
	if(last_modified) {
		//if-modified-since header
		r->headers_out.last_modified_time=last_modified;
	}
	if(etag!=NULL) {
		//etag, if we need one
		if ((r->headers_out.etag=ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_ETAG, etag))==NULL) {
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
	}
	//Vary header needed for proper HTTP caching.
	ngx_http_push_add_response_header(r, &NGX_HTTP_PUSH_HEADER_VARY, &NGX_HTTP_PUSH_VARY_HEADER_VALUE);
	
	r->headers_out.status=NGX_HTTP_OK;
	//we know the entity length, and we're using just one buffer. so no chunking please.
	r->headers_out.content_length_n=ngx_buf_size(chain->buf);
	if((res = ngx_http_send_header(r)) >= NGX_HTTP_SPECIAL_RESPONSE) {
		return res;
	}
	
	return ngx_http_output_filter(r, chain);
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
