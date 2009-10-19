#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

//with the declarations
typedef struct {
	ngx_int_t                       index;
	time_t                          buffer_timeout;
	ngx_int_t                       min_message_queue_size;
	ngx_int_t                       max_message_queue_size;
	ngx_int_t                       listener_concurrency;
	ngx_int_t                       listener_poll_mechanism;
	ngx_int_t                       authorize_channel;
	ngx_int_t                       store_messages;
	ngx_int_t                       min_message_recipients;
} ngx_http_push_loc_conf_t;

#define NGX_HTTP_PUSH_DEFAULT_SHM_SIZE 16777216 //16 megs
#define NGX_HTTP_PUSH_DEFAULT_BUFFER_TIMEOUT 3600
#define NGX_HTTP_PUSH_DEFAULT_MIN_MESSAGE_QUEUE_SIZE 5
#define NGX_HTTP_PUSH_DEFAULT_MAX_MESSAGE_QUEUE_SIZE 50
ngx_str_t NGX_HTTP_PUSH_CACHE_CONTROL_VALUE = ngx_string("no-cache");

#define NGX_HTTP_PUSH_LISTENER_LASTIN 0
#define NGX_HTTP_PUSH_LISTENER_FIRSTIN 1
#define NGX_HTTP_PUSH_LISTENER_BROADCAST 2

#define NGX_HTTP_PUSH_LISTENER_LONGPOLL 0
#define NGX_HTTP_PUSH_LISTENER_INTERVALPOLL 1

#define NGX_HTTP_PUSH_MIN_MESSAGE_RECIPIENTS 0
typedef struct {
	size_t                          shm_size;
} ngx_http_push_main_conf_t;

//message queue
typedef struct {
    ngx_queue_t                     queue;
	ngx_str_t                       content_type;
	ngx_str_t                       charset;
	ngx_buf_t                      *buf;
	time_t                          expires;
	ngx_uint_t                      received;
	time_t                          message_time; //tag message by time
	ngx_int_t                       message_tag; //used in conjunction with message_time if more than one message have the same time.
} ngx_http_push_msg_t;

typedef struct ngx_http_push_listener_s ngx_http_push_listener_t;
typedef struct ngx_http_push_channel_s ngx_http_push_channel_t;

//cleaning supplies
typedef struct {
	ngx_http_push_listener_t       *listener;
	ngx_http_push_channel_t        *channel;
} ngx_http_push_listener_cleanup_t;

//listener request queue
struct ngx_http_push_listener_s {
    ngx_queue_t                     queue;
	ngx_http_request_t             *request;
	ngx_int_t                       process_slot;
	ngx_http_push_listener_cleanup_t *cleanup;
};

//our typecast-friendly rbtree node (channel)
struct ngx_http_push_channel_s {
	ngx_rbtree_node_t               node;
	ngx_str_t                       id;
    ngx_http_push_msg_t            *message_queue;
	ngx_uint_t                      message_queue_size;
	ngx_http_push_listener_t       *listener_queue;
	ngx_uint_t                      listener_queue_size;
	time_t                          last_seen;
}; 

//sender stuff
static char *       ngx_http_push_sender(ngx_conf_t *cf, ngx_command_t *cmd, void *conf); //push_sender hook
static ngx_int_t    ngx_http_push_sender_handler(ngx_http_request_t * r);
static void         ngx_http_push_sender_body_handler(ngx_http_request_t * r);
static ngx_int_t    ngx_http_push_channel_info(ngx_http_request_t *r, ngx_uint_t message_queue_size, ngx_uint_t listener_queue_size, time_t last_seen);

//listener stuff
static char *       ngx_http_push_listener(ngx_conf_t *cf, ngx_command_t *cmd, void *conf); //push_listener hook
static ngx_int_t    ngx_http_push_listener_handler(ngx_http_request_t * r);
static ngx_str_t *  ngx_http_push_listener_get_etag(ngx_http_request_t * r);
static ngx_int_t    ngx_http_push_listener_get_etag_int(ngx_http_request_t * r);
static ngx_int_t    ngx_http_push_handle_listener_concurrency_setting(ngx_int_t concurrency, ngx_http_push_channel_t *channel, ngx_http_request_t *r, ngx_slab_pool_t *shpool);

//response generating stuff
static ngx_int_t    ngx_http_push_set_listener_header(ngx_http_request_t *r, ngx_http_push_msg_t *msg, ngx_slab_pool_t *shpool);
static ngx_chain_t *ngx_http_push_create_output_chain(ngx_http_request_t *r, ngx_buf_t *buf, ngx_slab_pool_t *shpool);
static void         ngx_http_push_copy_preallocated_buffer(ngx_buf_t *buf, ngx_buf_t *cbuf);
static ngx_int_t    ngx_http_push_set_listener_body(ngx_http_request_t *r, ngx_chain_t *out);

//misc stuff
ngx_shm_zone_t *    ngx_http_push_shm_zone = NULL;
static char *       ngx_http_push_setup_handler(ngx_conf_t *cf, void * conf, ngx_int_t (*handler)(ngx_http_request_t *));
static void *       ngx_http_push_create_main_conf(ngx_conf_t *cf);
static void *       ngx_http_push_create_loc_conf(ngx_conf_t *cf);
static char *       ngx_http_push_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child);
static ngx_int_t    ngx_http_push_set_up_shm(ngx_conf_t *cf, size_t shm_size);
static ngx_int_t    ngx_http_push_init_shm_zone(ngx_shm_zone_t * shm_zone, void * data);
static ngx_int_t    ngx_http_push_postconfig(ngx_conf_t *cf);
static char *       ngx_http_push_set_listener_concurrency(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void         ngx_http_push_reply_status_only(ngx_http_request_t *r, ngx_int_t code, ngx_str_t *statusline);
static ngx_table_elt_t * ngx_http_push_add_response_header(ngx_http_request_t *r, ngx_str_t *header_name, ngx_str_t *header_value);
static ngx_int_t    ngx_http_push_set_channel_id(ngx_str_t *id, ngx_http_request_t *r, ngx_http_push_loc_conf_t *cf);


static void ngx_http_push_listener_cleanup(ngx_http_push_listener_cleanup_t *data);
static ngx_http_push_listener_t * ngx_http_push_dequeue_listener_locked(ngx_http_push_channel_t * channel); //doesn't free associated memory
static ngx_inline ngx_http_push_listener_t *ngx_http_push_queue_listener_request_locked(ngx_http_push_channel_t * channel, ngx_http_request_t *r, ngx_slab_pool_t *shpool);

//message stuff
static ngx_http_push_msg_t * ngx_http_push_dequeue_message_locked(ngx_http_push_channel_t * channel); // doesn't free associated memory
static ngx_http_push_msg_t * ngx_http_push_find_message_locked(ngx_http_push_channel_t * channel, ngx_http_request_t *r, ngx_int_t *status);
static ngx_http_push_msg_t * ngx_http_push_get_latest_message_locked(ngx_http_push_channel_t * channel);
static ngx_http_push_msg_t * ngx_http_push_get_oldest_message_locked(ngx_http_push_channel_t * channel);
static ngx_inline void ngx_http_push_delete_message(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_slab_pool_t *shpool);
static ngx_inline void ngx_http_push_delete_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_slab_pool_t *shpool);

//missing in nginx < 0.7.?
#ifndef ngx_queue_insert_tail
#define ngx_queue_insert_tail(h, x)                                           \
    (x)->prev = (h)->prev;                                                    \
    (x)->prev->next = x;                                                      \
    (x)->next = h;                                                            \
    (h)->prev = x
#endif

//string constants
//headers
static ngx_str_t ngx_http_push_Etag = ngx_string("Etag");
static ngx_str_t ngx_http_push_If_None_Match = ngx_string("If-None-Match");
static ngx_str_t ngx_http_push_Vary = ngx_string("Vary");
static ngx_str_t ngx_http_push_Allow = ngx_string("Allow");
