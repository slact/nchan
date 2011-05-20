#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_channel.h>

#define NGX_HTTP_PUSH_DEFAULT_SHM_SIZE 33554432 //32 megs
#define NGX_HTTP_PUSH_DEFAULT_BUFFER_TIMEOUT 3600
#define NGX_HTTP_PUSH_DEFAULT_SUBSCRIBER_TIMEOUT 0  //default: never timeout
//(liucougar: this is a bit confusing, but it is what's the default behavior before this option is introducecd)
#define NGX_HTTP_PUSH_DEFAULT_CHANNEL_TIMEOUT 0 //default: timeout immediately

#define NGX_HTTP_PUSH_DEFAULT_MIN_MESSAGES 1
#define NGX_HTTP_PUSH_DEFAULT_MAX_MESSAGES 10

#define NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_LASTIN 0
#define NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_FIRSTIN 1
#define NGX_HTTP_PUSH_SUBSCRIBER_CONCURRENCY_BROADCAST 2

#define NGX_HTTP_PUSH_MECHANISM_LONGPOLL 0
#define NGX_HTTP_PUSH_MECHANISM_INTERVALPOLL 1

#define NGX_HTTP_PUSH_MIN_MESSAGE_RECIPIENTS 0

#define NGX_HTTP_PUSH_MAX_CHANNEL_ID_LENGTH 1024 //bytes

#ifndef NGX_HTTP_CONFLICT
#define NGX_HTTP_CONFLICT 409
#endif

#ifndef NGX_HTTP_GONE
#define NGX_HTTP_GONE 410
#endif

#ifndef NGX_HTTP_CREATED
#define NGX_HTTP_CREATED 201
#endif

#ifndef NGX_HTTP_ACCEPTED
#define NGX_HTTP_ACCEPTED 202
#endif


#define NGX_HTTP_PUSH_MESSAGE_RECEIVED 9000
#define NGX_HTTP_PUSH_MESSAGE_QUEUED   9001

#define NGX_HTTP_PUSH_MESSAGE_FOUND     1000 
#define NGX_HTTP_PUSH_MESSAGE_EXPECTED  1001
#define NGX_HTTP_PUSH_MESSAGE_EXPIRED   1002

//on with the declarations
typedef struct {
	size_t                          shm_size;
} ngx_http_push_main_conf_t;

typedef struct {
	ngx_int_t                       index;
	time_t                          buffer_timeout;
	ngx_int_t                       min_messages;
	ngx_int_t                       max_messages;
	ngx_int_t                       subscriber_concurrency;
	ngx_int_t                       subscriber_poll_mechanism;
	time_t                          subscriber_timeout;
	ngx_int_t                       authorize_channel;
	ngx_int_t                       store_messages;
	ngx_int_t                       delete_oldest_received_message;
	ngx_str_t                       channel_group;
	ngx_int_t                       max_channel_id_length;
	ngx_int_t                       max_channel_subscribers;
	ngx_int_t                       ignore_queue_on_no_cache;
	time_t                          channel_timeout;
} ngx_http_push_loc_conf_t;


typedef struct {
	time_t                          message_time; //tag message by time
	ngx_int_t                       message_tag;  //used in conjunction with message_time if more than one message have the same time.	
} ngx_http_push_msg_id_t;

//message queue
typedef struct {
    ngx_queue_t                     queue; //this MUST be first.
    ngx_http_push_msg_id_t          id;
	ngx_str_t                       content_type;
//	ngx_str_t                       charset;
	ngx_buf_t                      *buf;
	time_t                          expires;
	ngx_uint_t                      delete_oldest_received_min_messages; //NGX_MAX_UINT32_VALUE for 'never'
	ngx_int_t                       refcount;
} ngx_http_push_msg_t;

typedef struct ngx_http_push_subscriber_cleanup_s ngx_http_push_subscriber_cleanup_t;

//subscriber request queue
typedef struct {
    ngx_queue_t                     queue; //this MUST be first.
	ngx_http_request_t             *request;
	ngx_http_push_subscriber_cleanup_t *clndata; 
	ngx_event_t                     event;
} ngx_http_push_subscriber_t;

typedef struct {
	ngx_queue_t                     queue;
	pid_t                           pid;
	ngx_int_t                       slot;
	ngx_http_push_subscriber_t     *subscriber_sentinel;
} ngx_http_push_pid_queue_t; 

//our typecast-friendly rbtree node (channel)
typedef struct {
	ngx_rbtree_node_t               node; //this MUST be first.
	ngx_str_t                       id;
    ngx_http_push_msg_t            *message_queue;
	ngx_uint_t                      messages;
	ngx_http_push_pid_queue_t       workers_with_subscribers;
	ngx_uint_t                      subscribers;
	time_t                          last_seen;
	time_t                          expires;
} ngx_http_push_channel_t; 

//cleaning supplies
struct ngx_http_push_subscriber_cleanup_s {
	ngx_http_push_subscriber_t       *subscriber;
	ngx_http_push_channel_t        *channel;
};

//garbage collecting goodness
typedef struct {
	ngx_queue_t                     queue;
	ngx_http_push_channel_t        *channel;
} ngx_http_push_channel_queue_t;

//messages to worker processes
typedef struct {
	ngx_queue_t                     queue;
	ngx_http_push_msg_t            *msg; //->shared memory
	ngx_int_t                       status_code;
	ngx_pid_t                       pid; 
	ngx_http_push_channel_t        *channel; //->shared memory
	ngx_http_push_subscriber_t     *subscriber_sentinel; //->a worker's local pool
} ngx_http_push_worker_msg_t;

//shared memory
typedef struct {
	ngx_rbtree_t                    tree;
	ngx_uint_t                      channels; //# of channels being used
	ngx_http_push_worker_msg_t     *ipc; //interprocess stuff
} ngx_http_push_shm_data_t;

ngx_int_t           ngx_http_push_worker_processes;
ngx_pool_t         *ngx_http_push_pool;
ngx_slab_pool_t    *ngx_http_push_shpool;
ngx_shm_zone_t     *ngx_http_push_shm_zone = NULL;

//garbage-collecting shared memory slab allocation
void * ngx_http_push_slab_alloc(size_t size);
void * ngx_http_push_slab_alloc_locked(size_t size);

//channel messages
static ngx_http_push_msg_t *ngx_http_push_get_latest_message_locked(ngx_http_push_channel_t * channel);
static ngx_http_push_msg_t *ngx_http_push_get_oldest_message_locked(ngx_http_push_channel_t * channel);
static void ngx_http_push_reserve_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg);
static void ngx_http_push_release_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg);
static ngx_inline void ngx_http_push_general_delete_message_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t force, ngx_slab_pool_t *shpool);
#define ngx_http_push_delete_message_locked(channel, msg, shpool) ngx_http_push_general_delete_message_locked(channel, msg, 0, shpool)
#define ngx_http_push_force_delete_message_locked(channel, msg, shpool) ngx_http_push_general_delete_message_locked(channel, msg, 1, shpool)
static ngx_inline void ngx_http_push_free_message_locked(ngx_http_push_msg_t *msg, ngx_slab_pool_t *shpool);
static ngx_http_push_msg_t * ngx_http_push_find_message_locked(ngx_http_push_channel_t *channel, ngx_http_request_t *r, ngx_int_t *status);

//channel
static ngx_str_t * ngx_http_push_get_channel_id(ngx_http_request_t *r, ngx_http_push_loc_conf_t *cf);
static ngx_int_t ngx_http_push_channel_info(ngx_http_request_t *r, ngx_uint_t message_queue_size, ngx_uint_t subscriber_queue_size, time_t last_seen);

//subscriber
static ngx_int_t ngx_http_push_subscriber_handler(ngx_http_request_t *r);
static ngx_int_t ngx_http_push_handle_subscriber_concurrency(ngx_http_request_t *r, ngx_http_push_channel_t *channel, ngx_http_push_loc_conf_t *loc_conf);
static ngx_int_t ngx_http_push_broadcast_locked(ngx_http_push_channel_t *channel, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line, ngx_log_t *log, ngx_slab_pool_t *shpool);
#define ngx_http_push_broadcast_status_locked(channel, status_code, status_line, log, shpool) ngx_http_push_broadcast_locked(channel, NULL, status_code, status_line, log, shpool)
#define ngx_http_push_broadcast_message_locked(channel, msg, log, shpool) ngx_http_push_broadcast_locked(channel, msg, 0, NULL, log, shpool)

static ngx_int_t ngx_http_push_respond_to_subscribers(ngx_http_push_channel_t *channel, ngx_http_push_subscriber_t *sentinel, ngx_http_push_msg_t *msg, ngx_int_t status_code, const ngx_str_t *status_line);
static ngx_int_t ngx_http_push_allow_caching(ngx_http_request_t * r);
static ngx_int_t ngx_http_push_subscriber_get_etag_int(ngx_http_request_t * r);
static ngx_str_t * ngx_http_push_subscriber_get_etag(ngx_http_request_t * r);
static void ngx_http_push_subscriber_cleanup(ngx_http_push_subscriber_cleanup_t *data);
static ngx_int_t ngx_http_push_prepare_response_to_subscriber_request(ngx_http_request_t *r, ngx_chain_t *chain, ngx_str_t *content_type, ngx_str_t *etag, time_t last_modified);

//publisher
static ngx_int_t ngx_http_push_publisher_handler(ngx_http_request_t * r);
static void ngx_http_push_publisher_body_handler(ngx_http_request_t * r);

//utilities
//general request handling
static void ngx_http_push_copy_preallocated_buffer(ngx_buf_t *buf, ngx_buf_t *cbuf);
static ngx_table_elt_t * ngx_http_push_add_response_header(ngx_http_request_t *r, const ngx_str_t *header_name, const ngx_str_t *header_value);
static ngx_int_t ngx_http_push_respond_status_only(ngx_http_request_t *r, ngx_int_t status_code, const ngx_str_t *statusline);
static ngx_chain_t * ngx_http_push_create_output_chain_general(ngx_buf_t *buf, ngx_pool_t *pool, ngx_log_t *log, ngx_slab_pool_t *shpool);
#define ngx_http_push_create_output_chain(buf, pool, log) ngx_http_push_create_output_chain_general(buf, pool, log, NULL)
#define ngx_http_push_create_output_chain_locked(buf, pool, log, shpool) ngx_http_push_create_output_chain_general(buf, pool, log, shpool)

//string constants
//headers
const  ngx_str_t NGX_HTTP_PUSH_HEADER_ETAG = ngx_string("Etag");
const  ngx_str_t NGX_HTTP_PUSH_HEADER_IF_NONE_MATCH = ngx_string("If-None-Match");
const  ngx_str_t NGX_HTTP_PUSH_HEADER_VARY = ngx_string("Vary");
const  ngx_str_t NGX_HTTP_PUSH_HEADER_ALLOW = ngx_string("Allow");
const  ngx_str_t NGX_HTTP_PUSH_HEADER_CACHE_CONTROL = ngx_string("Cache-Control");
const  ngx_str_t NGX_HTTP_PUSH_HEADER_PRAGMA = ngx_string("Pragma");


//header values
const  ngx_str_t NGX_HTTP_PUSH_CACHE_CONTROL_VALUE = ngx_string("no-cache");

//status strings
const  ngx_str_t NGX_HTTP_PUSH_HTTP_STATUS_409 = ngx_string("409 Conflict");
const  ngx_str_t NGX_HTTP_PUSH_HTTP_STATUS_410 = ngx_string("410 Gone");

//other stuff
const  ngx_str_t NGX_HTTP_PUSH_ALLOW_GET_POST_PUT_DELETE= ngx_string("GET, POST, PUT, DELETE");
const  ngx_str_t NGX_HTTP_PUSH_ALLOW_GET= ngx_string("GET");
const  ngx_str_t NGX_HTTP_PUSH_VARY_HEADER_VALUE = ngx_string("If-None-Match, If-Modified-Since");


const ngx_str_t NGX_HTTP_PUSH_CHANNEL_INFO_PLAIN = ngx_string(
	"queued messages: %ui" CRLF
	"last requested: %d sec. ago (-1=never)" CRLF
	"active subscribers: %ui"
	"\0");
	
const ngx_str_t NGX_HTTP_PUSH_CHANNEL_INFO_JSON = ngx_string(
	"{\"messages\": %ui, "
	"\"requested\": %d, "
	"\"subscribers\": %ui }"
	"\0");

const ngx_str_t NGX_HTTP_PUSH_CHANNEL_INFO_XML = ngx_string(
	"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" CRLF
	"<channel>" CRLF
	"  <messages>%ui</messages>" CRLF
	"  <requested>%d</requested>" CRLF
	"  <subscribers>%ui</subscribers>" CRLF
	"</channel>"
	"\0");

const ngx_str_t NGX_HTTP_PUSH_CHANNEL_INFO_YAML = ngx_string(
	"---" CRLF
	"messages: %ui" CRLF
	"requested: %d" CRLF
	"subscribers %ui" CRLF
	"\0");

typedef struct {
	char                           *subtype;
	size_t                          len;
	const ngx_str_t                *format;
} ngx_http_push_content_subtype_t;
