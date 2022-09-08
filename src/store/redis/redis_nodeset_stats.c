#include "redis_nodeset.h"
#include <assert.h>
#include "store-private.h"
#include <store/store_common.h>
#include "store.h"

#include <store/memory/store.h>
#include <store/memory/store-private.h>
#include <store/memory/ipc-handlers.h>

redis_node_command_stats_t *redis_nodeset_worker_command_stats_alloc(redis_nodeset_t *ns, size_t *node_stats_count) {
  int numnodes = 0;
  redis_node_t *node;
  for(node = nchan_list_first(&ns->nodes); node != NULL; node = nchan_list_next(node)) {
    numnodes++;
  }
  
  redis_node_command_stats_t *stats = ngx_alloc(sizeof(*stats) * numnodes, ngx_cycle->log);
  if(!stats) {
    return NULL;
  }
  
  int i=0;
  for(node = nchan_list_first(&ns->nodes); node != NULL; node = nchan_list_next(node)) {
    ngx_snprintf((u_char *)stats[i].name, sizeof(stats[i].name), "%s%Z", node_nickname_cstr(node));
    ngx_snprintf((u_char *)stats[i].id, sizeof(stats[i].name), "%V%Z", node->cluster.enabled ? &node->cluster.master_id : &node->run_id);
    memcpy(stats[i].timings, node->stats.timings, sizeof(node->stats.timings));
  }
  
  *node_stats_count = numnodes;
  return stats;
}

static void redis_node_merge_stats(redis_node_command_stats_t *dst, redis_node_command_stats_t *src) {
  unsigned i;
  for(i=0; i<=NCHAN_REDIS_CMD_OTHER; i++) {
    nchan_accumulator_merge(&dst->timings[i], &src->timings[i]);
  }
}

typedef struct {
  redis_nodeset_t               *ns;
  ngx_pool_t                    *pool;
  int                            waiting_for_reply_count;
  redis_nodeset_command_stats_t  stats;
  callback_pt                    cb;
  void                          *pd;
} nodeset_global_command_stats_state_t;


static ngx_int_t redis_stats_request_callback(ngx_int_t rc, void *d, void *pd);

ngx_int_t redis_nodeset_global_command_stats_palloc_async(ngx_str_t *nodeset_name, ngx_pool_t *pool, callback_pt cb, void *pd) {
  redis_nodeset_t  *ns = NULL;
  int i;
  for(i=0; i < redis_nodeset_count; i++) {
    if(nchan_strmatch(nodeset_name, 1, ns->name)) {
      ns = &redis_nodeset[i];
      break;
    }
  }
  
  nodeset_global_command_stats_state_t *state = ngx_palloc(pool, sizeof(*state));
  if(!state) {
    return NGX_ERROR;
  }
  
  if(ns == NULL) {
    char *nsname_cstr = ngx_palloc(pool, nodeset_name->len + 1);
    if(!nsname_cstr) {
      state->stats.name = "";
    }
    else {
      ngx_sprintf((u_char *)nsname_cstr, "%V%Z", nodeset_name);
      state->stats.name = nsname_cstr;
    }
    
    state->stats.error = "No Redis upstream configured with such name";
    state->stats.stats = NULL;
    
    //TODO: communicate error to callback
    
    return NGX_OK;
  }
  
  ipc_t *ipc = nchan_memstore_get_ipc();
  
  state->waiting_for_reply_count = ipc->workers;
  state->stats.stats = NULL;
  
  
  return memstore_ipc_broadcast_redis_stats_request(ns, redis_stats_request_callback, state);
}

static ngx_int_t redis_stats_request_callback(ngx_int_t statscount, void *d, void *pd) {
  size_t                                 stats_count = statscount;
  redis_node_command_stats_t            *stats = d;
  nodeset_global_command_stats_state_t  *state = pd;
  
  state->waiting_for_reply_count--;
  
  unsigned i, j;
  redis_node_command_stats_t *src, *dst;
  
  for(i=0; i < stats_count; i++) {
    src = &stats[i];
    dst = NULL;
    for(j=0; j < state->stats.count; j++) {
      if(strstr(stats[i].id, state->stats.stats[j].id) == 0) {
        dst = &state->stats.stats[j];
        break;
      }
    }
    
    if(!dst) {
      state->stats.stats = realloc(state->stats.stats, sizeof(*state->stats.stats) * (state->stats.count + 1));
      if(!state->stats.stats) {
        state->stats.error = "Unable to allcoate memory for redis server stats";
        state->stats.count = 0;
        break;
      }
      state->stats.count++;
      dst = &state->stats.stats[j];
      *dst = *src;
    }
    else {
      redis_node_merge_stats(dst, src);
    }
  }
  
  if(state->waiting_for_reply_count <= 0) {
    if(state->stats.stats && state->stats.count > 0) {
      redis_node_command_stats_t            *stats_in_pool = ngx_palloc(state->pool, sizeof(*stats_in_pool) * state->stats.count);
      if(!stats_in_pool) {
        state->stats.error = "Unable to allcoate memory for redis server stats";
        state->stats.count = 0;
        free(state->stats.stats);
        state->stats.stats = NULL;
      }
      memcpy(stats_in_pool, state->stats.stats, sizeof(*stats_in_pool) * state->stats.count);
      free(state->stats.stats);
      state->stats.stats = stats_in_pool;
    }
    
    state->cb(state->stats.error ? NGX_ERROR : NGX_OK, &state->stats, state->pd);
  }
  
  return NGX_OK;
}

typedef struct {
  ngx_chain_t  chain;
  ngx_buf_t    buf;
  char         data[1];
} bufchain_blob_t;

static int bufchain_add(ngx_pool_t *pool, ngx_chain_t **first, ngx_chain_t **last, char *data) {
  size_t datalen = strlen(data);
  
  bufchain_blob_t *bb = ngx_palloc(pool, sizeof(*bb) + datalen); // the +1 for the NUL byte is already in the struct as data[1];
  if(!bb) {
    return 0;
  }
  
  strcpy(bb->data, data);
  ngx_memzero(&bb->buf, sizeof(bb->buf));
  bb->buf.memory = 1;
  bb->buf.start = (u_char *)bb->data;
  bb->buf.pos =  (u_char *)bb->data;
  bb->buf.end =  (u_char *)bb->data + datalen;
  bb->buf.last = bb->buf.end;
  
  bb->chain.buf = &bb->buf;
  bb->chain.next = NULL;
  
  if(*first == NULL) {
    *first = &bb->chain;
  }
  if(*last) {
    (*last)->next = &bb->chain;
  }
  *last = &bb->chain;
  
  return 1;
}

static int compare_nodestats_by_name(const void *v1, const void *v2) {
  redis_node_command_stats_t *s1 = v1, *s2 = v2;
  return strcmp(s1->name, s2->name);
}

#define REDIS_NODE_STAT_FMT_ARGS(stat, cmdtag) \
  ((int )nchan_accumulator_value(&stat->timings[NCHAN_REDIS_CMD_ ## cmdtag])), \
  ((int )nchan_accumulator_weight(&stat->timings[NCHAN_REDIS_CMD_ ## cmdtag]))

ngx_chain_t *redis_nodeset_stats_response_body_chain_palloc(redis_nodeset_command_stats_t *nstats , ngx_pool_t *pool) {
  ngx_chain_t *first = NULL, *last = NULL;
  
  
  char cstrbuf[4096];
  cstrbuf[4095]='\0'; //just in case the sprintf output is 4095 chars long
  
  char *open_fmtstr = "{\n"
                      "  \"upstream\": \"%s\"\n"
                      "  \"nodes\": [\n";
  
  snprintf(cstrbuf, 4095, open_fmtstr, nstats->name);
  if(!bufchain_add(pool, &first, &last, cstrbuf)) {
    return NULL;
  } 
  
  qsort(nstats->stats, nstats->count, sizeof(*nstats->stats), compare_nodestats_by_name);
  
  char *nodestat_fmtstr = "    {\n"
                          "      \"address\"        : \"%s\"\n"
                          "      \"id\"             : \"%s\"\n"
                          "      \"command_totals\" : {\n"
                          "        \"connect\"    : {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"pubsub_subscribe\": {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"pubsub_unsubsribe\": {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_change_subscriber_count\": {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_delete\": {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_find\": {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_get_message\": {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_get_large_message\": {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_publish_message\": {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_request_subscriber_info\": {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_get_subscriber_info_id\": {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_subscribe\": {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_unsubscribe\": {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"other\"      : {\n"
                          "          \"msec\"     : %u\n"
                          "          \"times\"    : %u\n"
                          "        }\n"
                          "      }\n"
                          "    }%s\n";
  unsigned i;
  for(i=0; i < nstats->count; i++) {
    redis_node_command_stats_t *stat = &nstats->stats[i];
    snprintf(cstrbuf, 4095, nodestat_fmtstr, stat->name, stat->id,
             REDIS_NODE_STAT_FMT_ARGS(stat, CONNECT),
             REDIS_NODE_STAT_FMT_ARGS(stat, PUBSUB_SUBSCRIBE),
             REDIS_NODE_STAT_FMT_ARGS(stat, PUBSUB_UNSUBSCRIBE),
             REDIS_NODE_STAT_FMT_ARGS(stat, CHANNEL_CHANGE_SUBSCRIBER_COUNT),
             REDIS_NODE_STAT_FMT_ARGS(stat, CHANNEL_DELETE),
             REDIS_NODE_STAT_FMT_ARGS(stat, CHANNEL_FIND),
             REDIS_NODE_STAT_FMT_ARGS(stat, CHANNEL_GET_MESSAGE),
             REDIS_NODE_STAT_FMT_ARGS(stat, CHANNEL_GET_LARGE_MESSAGE),
             REDIS_NODE_STAT_FMT_ARGS(stat, CHANNEL_PUBLISH),
             REDIS_NODE_STAT_FMT_ARGS(stat, CHANNEL_REQUEST_SUBSCRIBER_INFO),
             REDIS_NODE_STAT_FMT_ARGS(stat, CHANNEL_GET_SUBSCRIBER_INFO_ID),
             REDIS_NODE_STAT_FMT_ARGS(stat, CHANNEL_SUBSCRIBE),
             REDIS_NODE_STAT_FMT_ARGS(stat, CHANNEL_UNSUBSCRIBE),
             i + 1 < nstats->count ? "," : ""
    );
    if(!bufchain_add(pool, &first, &last, cstrbuf)) {
      return NULL;
    } 
  }
  
  if(!bufchain_add(pool, &first, &last, "  ]\n}\n")) {
    return NULL;
  }
  
  return first;
}
