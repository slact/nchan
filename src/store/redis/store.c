#include <ngx_http_push_module.h>
#include "store.h"

#include <store/memory/store.h>

redisAsyncContext *ngx_http_push_redis=NULL;

static ngx_int_t ngx_http_push_store_redis_init_postconfig(ngx_conf_t *cf) {
  return ngx_http_push_store_memory.init_postconfig(cf);
}

static ngx_int_t ngx_http_push_store_redis_init_module(ngx_cycle_t *cycle) {
  return ngx_http_push_store_memory.init_module(cycle);
}

static ngx_int_t ngx_http_push_store_redis_init_worker(ngx_cycle_t *cycle) {
  ngx_http_push_redis = redisAsyncConnect("127.0.0.1", 6379);
  return ngx_http_push_store_memory.init_worker(cycle);
}

void ngx_http_push_store_redis_create_main_conf(ngx_conf_t *cf, ngx_http_push_main_conf_t *mcf) {
  ngx_http_push_store_memory.create_main_conf(cf, mcf);
  return;
}


//shutdown
static void ngx_http_push_store_redis_exit_worker(ngx_cycle_t *cycle) {
  ngx_http_push_store_memory.exit_worker(cycle);
}
static void ngx_http_push_store_redis_exit_master(ngx_cycle_t *cycle) {
  ngx_http_push_store_memory.exit_master(cycle);
}

//channel stuff
static ngx_http_push_channel_t * ngx_http_push_store_redis_get_channel(ngx_str_t *id, time_t channel_timeout, ngx_log_t *log) {
  return ngx_http_push_store_memory.get_channel(id, channel_timeout, log);
}
static ngx_http_push_channel_t * ngx_http_push_store_redis_find_channel(ngx_str_t *id, time_t channel_timeout, ngx_log_t *log) {
  return ngx_http_push_store_memory.find_channel(id, channel_timeout, log);
}
static ngx_int_t ngx_http_push_store_redis_delete_channel(ngx_http_push_channel_t *channel, ngx_http_request_t *r) {
  return ngx_http_push_store_memory.delete_channel(channel, r);
}
static ngx_http_push_msg_t * ngx_http_push_store_redis_get_message(ngx_http_push_channel_t *channel, ngx_http_request_t *r, ngx_int_t *msg_search_outcome, ngx_http_push_loc_conf_t *cf, ngx_log_t *log) {
  return ngx_http_push_store_memory.get_message(channel, r, msg_search_outcome, cf, log);
}
static void ngx_http_push_store_redis_reserve_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
  return ngx_http_push_store_memory.reserve_message(channel, msg);
}
static void ngx_http_push_store_redis_release_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg) {
  return ngx_http_push_store_memory.release_message(channel, msg);
}


//pub/sub
static ngx_int_t ngx_http_push_store_redis_publish(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line, ngx_log_t *log) {
  return ngx_http_push_store_memory.publish(channel, msg, status_code, status_line, log);
}
static ngx_http_push_subscriber_t * ngx_http_push_store_redis_subscribe(ngx_http_push_channel_t *channel, ngx_http_request_t *r) {
  return ngx_http_push_store_memory.subscribe(channel, r);
}

//channel properties
static ngx_int_t ngx_http_push_store_redis_channel_subscribers(ngx_http_push_channel_t * channel) {
  return ngx_http_push_store_memory.channel_subscribers(channel);
}
static ngx_int_t ngx_http_push_store_redis_channel_worker_subscribers(ngx_http_push_subscriber_t * worker_sentinel) {
  return ngx_http_push_store_memory.channel_worker_subscribers(worker_sentinel);
}
static ngx_http_push_subscriber_t *ngx_http_push_store_redis_next_subscriber(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *sentinel, ngx_http_push_subscriber_t *cur, int release_previous) {
  return ngx_http_push_store_memory.next_subscriber(channel, sentinel, cur, release_previous);
}
static ngx_int_t ngx_http_push_store_redis_release_subscriber_sentinel(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *sentinel) {
  return ngx_http_push_store_memory.release_subscriber_sentinel(channel, sentinel);
}


//legacy shared-memory store helpers
static void ngx_http_push_store_redis_shm_lock(void) {
  ngx_http_push_store_memory.lock();
}
static void ngx_http_push_store_redis_shm_unlock(void) {
  ngx_http_push_store_memory.unlock();
}
static void * ngx_http_push_store_redis_alloc_locked(size_t size, char *label) {
  return ngx_http_push_store_memory.alloc_locked(size, label);
}
static void ngx_http_push_store_redis_free_locked(void *ptr) {
  ngx_http_push_store_memory.free_locked(ptr);
}

//message stuff
static ngx_http_push_msg_t * ngx_http_push_store_redis_create_message(ngx_http_push_channel_t *channel, ngx_http_request_t *r) {
  return ngx_http_push_store_memory.create_message(channel, r);
}
static ngx_int_t ngx_http_push_store_redis_delete_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t force) {
  return ngx_http_push_store_memory.delete_message(channel, msg, force);
}
static ngx_int_t ngx_http_push_store_redis_delete_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t force) {
  return ngx_http_push_store_memory.delete_message_locked(channel, msg, force);
}
static ngx_int_t ngx_http_push_store_redis_enqueue_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_http_push_loc_conf_t *cf) {
  return ngx_http_push_store_memory.enqueue_message(channel, msg, cf);
}
static ngx_str_t * ngx_http_push_store_redis_message_etag(ngx_http_push_msg_t *msg, ngx_pool_t *pool) {
  return ngx_http_push_store_memory.message_etag(msg, pool);
}
static ngx_str_t * ngx_http_push_store_redis_message_content_type(ngx_http_push_msg_t *msg, ngx_pool_t *pool) {
  return ngx_http_push_store_memory.message_content_type(msg, pool);
}


//interprocess communication
static ngx_int_t ngx_http_push_store_redis_send_worker_message(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *subscriber_sentinel, ngx_pid_t pid, ngx_int_t worker_slot, ngx_http_push_msg_t *msg, ngx_int_t status_code, ngx_log_t *log) {
  return ngx_http_push_store_memory.send_worker_message(channel, subscriber_sentinel, pid, worker_slot, msg, status_code, log);
}
static void ngx_http_push_store_redis_receive_worker_message(void){
  return ngx_http_push_store_memory.receive_worker_message();
}


ngx_http_push_store_t  ngx_http_push_store_redis = {
  //init
  ngx_http_push_store_redis_init_module,
  ngx_http_push_store_redis_init_worker,
  ngx_http_push_store_redis_init_postconfig,
  ngx_http_push_store_redis_create_main_conf,
  
  //shutdown
  ngx_http_push_store_redis_exit_worker,
  ngx_http_push_store_redis_exit_master,
  
  //channel stuff
  ngx_http_push_store_redis_get_channel,
  ngx_http_push_store_redis_find_channel,
  ngx_http_push_store_redis_delete_channel,
  ngx_http_push_store_redis_get_message,
  ngx_http_push_store_redis_reserve_message,
  ngx_http_push_store_redis_release_message,
  
  //pub/sub
  ngx_http_push_store_redis_publish,
  ngx_http_push_store_redis_subscribe,
  
  //channel properties
  ngx_http_push_store_redis_channel_subscribers,
  ngx_http_push_store_redis_channel_worker_subscribers,
  ngx_http_push_store_redis_next_subscriber,
  ngx_http_push_store_redis_release_subscriber_sentinel,
  
  //legacy shared-memory store helpers
  ngx_http_push_store_redis_shm_lock,
  ngx_http_push_store_redis_shm_unlock,
  ngx_http_push_store_redis_alloc_locked,
  ngx_http_push_store_redis_free_locked,
  
  //message stuff
  ngx_http_push_store_redis_create_message,
  ngx_http_push_store_redis_delete_message,
  ngx_http_push_store_redis_delete_message_locked,
  ngx_http_push_store_redis_enqueue_message,
  ngx_http_push_store_redis_message_etag,
  ngx_http_push_store_redis_message_content_type,
  
  //interprocess communication
  ngx_http_push_store_redis_send_worker_message,
  ngx_http_push_store_redis_receive_worker_message
};