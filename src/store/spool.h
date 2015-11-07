#ifndef SPOOL_HEADER
#define SPOOL_HEADER

#include "rbtree_util.h"

typedef struct spooled_subscriber_s spooled_subscriber_t;
typedef struct subscriber_pool_s subscriber_pool_t;

typedef struct {
  spooled_subscriber_t  *ssub;
  subscriber_pool_t     *spool;
} spooled_subscriber_cleanup_t;
  
struct spooled_subscriber_s {
  ngx_uint_t                    id; //could be useful later
  subscriber_t                 *sub;
  spooled_subscriber_cleanup_t  dequeue_callback_data;
  spooled_subscriber_t         *next;
  spooled_subscriber_t         *prev;
}; //spooled_subscriber_t


struct subscriber_pool_s {
  nchan_msg_id_t              id;
  nchan_msg_t                *msg;
  nchan_msg_status_t          msg_status;
  spooled_subscriber_t       *first;
  ngx_pool_t                 *pool;
  ngx_uint_t                  sub_count;
  ngx_uint_t                  generation;
  ngx_uint_t                  responded_count;
  struct channel_spooler_s   *spooler;
}; // subscriber_pool_t

typedef struct channel_spooler_s channel_spooler_t; //holds many different spools

typedef struct {
  ngx_int_t            (*add)(channel_spooler_t *self, subscriber_t *sub);
  ngx_int_t            (*handle_channel_status_change)(channel_spooler_t *self);
  ngx_int_t            (*respond_message)(channel_spooler_t *self, nchan_msg_t *msg);
  ngx_int_t            (*respond_status)(channel_spooler_t *self, ngx_int_t status_code, const ngx_str_t *status_line);
  ngx_int_t            (*prepare_to_stop)(channel_spooler_t *self);
  ngx_int_t            (*set_add_handler)(channel_spooler_t *, void (*cb)(channel_spooler_t *, subscriber_t *, void *), void*);
  ngx_int_t            (*set_dequeue_handler)(channel_spooler_t *, void (*cb)(channel_spooler_t *, subscriber_t *, void*), void*);
  ngx_int_t            (*set_bulk_dequeue_handler)(channel_spooler_t *, void (*cb)(channel_spooler_t *, subscriber_type_t, ngx_int_t, void*), void*);
} channel_spooler_fn_t;

struct channel_spooler_s {
  rbtree_seed_t               spoolseed;
  nchan_msg_id_t              prev_msg_id;
  ngx_uint_t                  responded_count;
  ngx_str_t                  *chid;
  chanhead_pubsub_status_t   *channel_status;
  nchan_store_t              *store;
  channel_spooler_fn_t       *fn;  
  
  void                        (*add_handler)(channel_spooler_t *, subscriber_t *, void *);
  void                       *add_handler_privdata;
  
  void                        (*dequeue_handler)(channel_spooler_t *, subscriber_t *, void *);
  void                       *dequeue_handler_privdata;
  
  void                        (*bulk_dequeue_handler)(channel_spooler_t *, subscriber_type_t, ngx_int_t, void *); //called after dequeueing 1 or many subs
  void                       *bulk_dequeue_handler_privdata;
  
  unsigned                    running:1;
  unsigned                    want_to_stop:1;
};

channel_spooler_t *start_spooler(channel_spooler_t *spl, ngx_str_t *chid, chanhead_pubsub_status_t *channel_status, nchan_store_t *store);
ngx_int_t stop_spooler(channel_spooler_t *spl, uint8_t dequeue_subscribers);


#endif  /*SPOOL_HEADER*/