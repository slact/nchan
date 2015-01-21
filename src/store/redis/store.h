#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "uthash.h"

extern ngx_http_push_store_t  ngx_http_push_store_redis;

typedef enum {LONGPOLL, EVENTSOURCE, WEBSOCKET} subscriber_type_t;

typedef struct nhpm_channel_head_s nhpm_channel_head_t;
typedef struct nhpm_channel_head_cleanup_s nhpm_channel_head_cleanup_t;
typedef struct nhpm_subscriber_cleanup_s nhpm_subscriber_cleanup_t;
typedef struct nhpm_subscriber_s nhpm_subscriber_t;

struct nhpm_subscriber_s {
  ngx_uint_t                  id;
  void                       *subscriber;
  subscriber_type_t           type;
  ngx_pool_t                 *pool;
  struct nhpm_subscriber_s   *prev;
  struct nhpm_subscriber_s   *next;
  ngx_http_cleanup_t         *cln;
};

struct nhpm_channel_head_s {
  ngx_str_t                    id; //channel id
  ngx_pool_t                  *pool;
  nhpm_subscriber_t           *sub;
  ngx_uint_t                   sub_count;
  nhpm_channel_head_cleanup_t *shared_cleanup;
  nhpm_llist_timed_t          *cleanlink;
  UT_hash_handle               hh;
};

struct nhpm_channel_head_cleanup_s {
  nhpm_channel_head_t        *head;
  ngx_str_t                   id; //channel id
  ngx_uint_t                  sub_count;
  ngx_pool_t                 *pool;
};

struct nhpm_subscriber_cleanup_s {
  nhpm_channel_head_cleanup_t  *shared;
  nhpm_subscriber_t            *sub;
};

#define CHANNEL_HASH_FIND(id_buf, p)    HASH_FIND( hh, subhash, (id_buf)->data, (id_buf)->len, p)
#define CHANNEL_HASH_ADD(chanhead)      HASH_ADD_KEYPTR( hh, subhash, (chanhead->id).data, (chanhead->id).len, chanhead)
#define CHANNEL_HASH_DEL(chanhead)      HASH_DEL( subhash, chanhead)

#undef uthash_malloc
#undef uthash_free
#define uthash_malloc(sz) ngx_alloc(sz, ngx_cycle->log)
#define uthash_free(ptr,sz) ngx_free(ptr)
