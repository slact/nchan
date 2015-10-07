#include "store-private.h"
ngx_int_t memstore_ipc_send_subscribe(ngx_int_t owner, ngx_str_t *shm_chid, nhpm_channel_head_t *);
ngx_int_t memstore_ipc_send_memstore_subscriber_keepalive(ngx_int_t dst, ngx_str_t *chid, subscriber_t *sub, nhpm_channel_head_t *ch, callback_pt callback, void *privdata);
ngx_int_t memstore_ipc_send_unsubscribe(ngx_int_t dst, ngx_str_t *chid, void *subscriber, void* privdata);
ngx_int_t memstore_ipc_send_unsubscribed(ngx_int_t dst, ngx_str_t *chid, void* privdata);
ngx_int_t memstore_ipc_send_publish_message(ngx_int_t dst, ngx_str_t *chid, 
ngx_http_push_msg_t *shm_msg, ngx_int_t msg_timeout, ngx_int_t max_msgs, ngx_int_t min_msgs, callback_pt callback, void *privdata);
ngx_int_t memstore_ipc_send_publish_status(ngx_int_t dst, ngx_str_t *chid, ngx_int_t status,  void* privdata);
ngx_int_t memstore_ipc_send_get_message(ngx_int_t owner, ngx_str_t *shm_chid, ngx_http_push_msg_id_t *msgid, void * privdata);
ngx_int_t memstore_ipc_send_delete(ngx_int_t owner, ngx_str_t *shm_chid, callback_pt callback, void *privdata);
void memstore_ipc_alert_handler(ngx_int_t sender, ngx_uint_t code, void *data);
ngx_int_t memstore_ipc_send_get_channel_info(ngx_int_t dst, ngx_str_t *chid, callback_pt callback, void* privdata);
ngx_int_t memstore_ipc_send_does_channel_exist(ngx_int_t dst, ngx_str_t *chid, callback_pt callback, void* privdata);