#include <nchan_module.h>

ngx_int_t nchan_subscriber_subscribe(subscriber_t *sub, nchan_store_t *store, ngx_str_t *ch_id, callback_pt callback, void *privdata) {
  return store->subscribe(ch_id, sub, callback, privdata);
}

ngx_int_t nchan_subscriber_authorize_subscribe(subscriber_t *sub, nchan_store_t *store, ngx_str_t *ch_id, callback_pt callback, void *privdata) {
  return nchan_subscriber_subscribe(sub, store, ch_id, callback, privdata);
}