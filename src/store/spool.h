#ifndef SPOOL_HEADER
#define SPOOL_HEADER

#include <util/nchan_rbtree.h>

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
  
  //stack overflow prevention
  ngx_uint_t                  fetchmsg_prev_msec;
  ngx_int_t                   fetchmsg_current_count;
  ngx_event_t                 fetchmsg_ev;
  
  ngx_uint_t                  sub_count;
  ngx_uint_t                  non_internal_sub_count;
  //ngx_uint_t                  generation;
  //ngx_uint_t                  responded_count;
  uint8_t                     reserved;
  struct channel_spooler_s   *spooler;
}; // subscriber_pool_t

typedef struct channel_spooler_s channel_spooler_t; //holds many different spools
typedef struct channel_spooler_handlers_s channel_spooler_handlers_t; //spooler callbacks table

typedef struct {
  ngx_int_t            (*add)(channel_spooler_t *self, subscriber_t *sub);
  ngx_int_t            (*handle_channel_status_change)(channel_spooler_t *self);
  ngx_int_t            (*respond_message)(channel_spooler_t *self, nchan_msg_t *msg);
  ngx_int_t            (*respond_status)(channel_spooler_t *self, nchan_msg_id_t *id, ngx_int_t status_code, ngx_str_t *status_line);
  ngx_int_t            (*broadcast_status)(channel_spooler_t *self, ngx_int_t status_code, const ngx_str_t *status_line);
  ngx_int_t            (*broadcast_notice)(channel_spooler_t *self, ngx_int_t notice_code, void *data);
  ngx_int_t            (*prepare_to_stop)(channel_spooler_t *self);
} channel_spooler_fn_t;

typedef enum {NCHAN_SPOOL_FETCH, NCHAN_SPOOL_FETCH_IGNORE_MSG_NOTFOUND, NCHAN_SPOOL_PASSTHROUGH} spooler_fetching_strategy_t;


typedef struct fetchmsg_data_s fetchmsg_data_t;
struct fetchmsg_data_s {
  channel_spooler_t   *spooler;
  nchan_msg_id_t       msgid;
  fetchmsg_data_t     *next;
  fetchmsg_data_t     *prev;
};

typedef struct spooler_event_ll_s spooler_event_ll_t;
struct spooler_event_ll_s {
  spooler_event_ll_t   *prev;
  ngx_event_t           ev;
  void                (*callback)(void *);
  void                (*cancel)(void *);
  channel_spooler_t    *spooler;
  spooler_event_ll_t   *next;
};

struct channel_spooler_s {
  rbtree_seed_t               spoolseed;
  subscriber_pool_t           current_msg_spool;
  nchan_msg_id_t              prev_msg_id;
  ngx_uint_t                  responded_count;
  ngx_str_t                  *chid;
  chanhead_pubsub_status_t   *channel_status;
  uint8_t                    *channel_buffer_complete;
  nchan_store_t              *store;
  nchan_loc_conf_t           *cf;
  channel_spooler_fn_t       *fn;
  channel_spooler_handlers_t *handlers;
  void                       *handlers_privdata;
  fetchmsg_data_t            *fetchmsg_cb_data_list;
  spooler_event_ll_t         *spooler_dependent_events;
  spooler_fetching_strategy_t fetching_strategy;
  unsigned                    publish_events:1;
  unsigned                    running:1;
  unsigned                    want_to_stop:1;
  
};

struct channel_spooler_handlers_s {
  void                        (*add)(channel_spooler_t *, subscriber_t *, void *);
  void                        (*dequeue)(channel_spooler_t *, subscriber_t *, void *);
  void                        (*bulk_dequeue)(channel_spooler_t *, subscriber_type_t, ngx_int_t, void *); //called after dequeueing 1 or many subs
  void                        (*use)(channel_spooler_t *, void *);
  void                        (*get_message_start)(channel_spooler_t *, void *);
  void                        (*get_message_finish)(channel_spooler_t *, void *);
};

channel_spooler_t *start_spooler(channel_spooler_t *spl, ngx_str_t *chid, chanhead_pubsub_status_t *channel_status, uint8_t *channel_buffer_complete, nchan_store_t *store, nchan_loc_conf_t *cf, spooler_fetching_strategy_t fetching_strategy, channel_spooler_handlers_t *handlers, void *handlers_privdata);
ngx_int_t stop_spooler(channel_spooler_t *spl, uint8_t dequeue_subscribers);

ngx_int_t spooler_catch_up(channel_spooler_t *spl);

ngx_int_t spooler_print_contents(channel_spooler_t *spl);

ngx_event_t *spooler_add_timer(channel_spooler_t *spl, ngx_msec_t timeout, void (*cb)(void *), void (*cancel)(void *), void *pd);


#endif  /*SPOOL_HEADER*/
