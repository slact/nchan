#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>


//with the declarations
typedef struct {
	ngx_int_t                  	index;
	ngx_http_event_handler_pt	read_event_handler;
	ngx_shm_zone_t		 		*shm_zone;
} ngx_http_push_loc_conf_t;

//message queue
typedef struct {
    ngx_queue_t				queue;
	ngx_str_t				content_type;
	ngx_str_t				charset;
	unsigned				is_file:1;
	ngx_str_t				str;
} ngx_http_push_msg_t;

//our typecast-friendly rbtree node
typedef struct ngx_http_push_node_s ngx_http_push_node_t;
struct ngx_http_push_node_s {
	ngx_rbtree_node_t                node;
	ngx_str_t						 id;
    ngx_http_push_msg_t				*message_queue;
	ngx_http_request_t				*request;
};

//source stuff
static char *		ngx_http_push_source(ngx_conf_t *cf, ngx_command_t *cmd, void *conf); //push_source hook
static 	ngx_int_t 	ngx_http_push_source_handler(ngx_http_request_t * r);
static void 		ngx_http_push_source_body_handler(ngx_http_request_t * r);

//destination stuff
static char *		ngx_http_push_destination(ngx_conf_t *cf, ngx_command_t *cmd, void *conf); //push_destination hook
static ngx_int_t 	ngx_http_push_destination_handler(ngx_http_request_t * r);
static ngx_int_t 	ngx_http_push_send_message_to_destination_request(ngx_http_request_t *r, ngx_http_push_msg_t * msg);

//cleaning stuff
static void 		ngx_http_push_destination_request_cleanup(ngx_http_push_node_t * data); //request pool cleaner

//misc stuff
static void * 		ngx_http_push_create_loc_conf(ngx_conf_t *cf);
static ngx_int_t 	ngx_http_push_init_shm_zone(ngx_shm_zone_t * shm_zone, void * data);



static ngx_http_push_msg_t * ngx_http_push_dequeue_message(ngx_http_push_node_t * node); // doesn't free associated memory

//missing in nginx < 0.7.?
#ifndef ngx_queue_insert_tail
#define ngx_queue_insert_tail(h, x)                                           \
    (x)->prev = (h)->prev;                                                    \
    (x)->prev->next = x;                                                      \
    (x)->next = h;                                                            \
    (h)->prev = x
#endif

