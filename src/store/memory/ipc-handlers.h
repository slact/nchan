#include "store-private.h"

typedef void (*ipc_handler_pt)(ngx_int_t, void *);

ngx_int_t memstore_ipc_send_subscribe(ngx_int_t owner, ngx_str_t *shm_chid, memstore_channel_head_t *, nchan_loc_conf_t *);
ngx_int_t memstore_ipc_send_memstore_subscriber_keepalive(ngx_int_t dst, ngx_str_t *chid, subscriber_t *sub, memstore_channel_head_t *ch);
ngx_int_t memstore_ipc_send_unsubscribe(ngx_int_t dst, ngx_str_t *chid, void *subscriber, void* privdata);
ngx_int_t memstore_ipc_send_unsubscribed(ngx_int_t dst, ngx_str_t *chid, void* privdata);
ngx_int_t memstore_ipc_send_publish_message(ngx_int_t dst, ngx_str_t *chid, nchan_msg_t *shm_msg, nchan_loc_conf_t *cf, callback_pt callback, void *privdata);
ngx_int_t memstore_ipc_send_publish_status(ngx_int_t dst, ngx_str_t *chid, ngx_int_t status_code, const ngx_str_t *status_line, callback_pt callback, void *privdata);
ngx_int_t memstore_ipc_send_get_message(ngx_int_t owner, ngx_str_t *shm_chid, nchan_msg_id_t *msgid, void * privdata);
ngx_int_t memstore_ipc_send_delete(ngx_int_t owner, ngx_str_t *shm_chid, callback_pt callback, void *privdata);
void memstore_ipc_alert_handler(ngx_int_t sender, ngx_uint_t code, void *data);
ngx_int_t memstore_ipc_send_get_channel_info(ngx_int_t dst, ngx_str_t *chid, nchan_loc_conf_t *cf, callback_pt callback, void* privdata);
ngx_int_t memstore_ipc_send_channel_existence_check(ngx_int_t dst, ngx_str_t *chid, nchan_loc_conf_t *cf, callback_pt callback, void* privdata);
ngx_int_t memstore_ipc_broadcast_group(nchan_group_t *shared_group);
ngx_int_t memstore_ipc_send_get_group(ngx_int_t dst, ngx_str_t *group_id);
ngx_int_t memstore_ipc_broadcast_group_delete(nchan_group_t *shared_group);
ngx_int_t memstore_ipc_send_flood_test(ngx_int_t dst);
