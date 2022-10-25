#include "redis_nodeset.h"
#include <assert.h>
#include "store-private.h"
#include <store/store_common.h>
#include "store.h"

#include <store/memory/store.h>
#include <store/memory/store-private.h>
#include <store/memory/ipc-handlers.h>

redis_node_command_stats_t *redis_nodeset_worker_command_stats_alloc(redis_nodeset_t *ns, size_t *node_stats_count) {
  if(!ns->node_stats.active) {
    *node_stats_count = 0;
    return NULL;
  }
  int numstats = nchan_list_count(&ns->node_stats.list);
  *node_stats_count = numstats;
  redis_node_command_stats_t *stats = ngx_alloc(sizeof(*stats) * numstats, ngx_cycle->log);
  if(!stats) {
    return NULL;
  }
    
  int i=0;
  redis_node_command_stats_t *cur;
  for(cur = nchan_list_first(&ns->node_stats.list); cur != NULL; cur = nchan_list_next(cur)) {
    assert(i<numstats);
    stats[i] = *cur;
    i++;
  }
  
  return stats;
}

static void redis_node_merge_stats(redis_node_command_stats_t *dst, redis_node_command_stats_t *src) {
  unsigned i;
  for(i=0; i<NCHAN_REDIS_CMD_ENUM_LAST; i++) {
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


static ngx_int_t redis_stats_request_callback(ngx_int_t statscount, void *d, void *pd);

static void stats_request_to_self(void *pd) {
  nodeset_global_command_stats_state_t  *state = pd;
  size_t                                 stats_count;
  redis_node_command_stats_t            *stats = redis_nodeset_worker_command_stats_alloc(state->ns, &stats_count);
  
  redis_stats_request_callback(stats_count, stats, pd);
  ngx_free(stats);
}

ngx_int_t redis_nodeset_global_command_stats_palloc_async(ngx_str_t *nodeset_name, ngx_pool_t *pool, callback_pt cb, void *pd) {
  redis_nodeset_t  *ns = NULL;
  int i;
  for(i=0; i < redis_nodeset_count; i++) {
    if(nchan_strmatch(nodeset_name, 1, redis_nodeset[i].name)) {
      ns = &redis_nodeset[i];
      break;
    }
  }
  
  if(ns == NULL) {
    return NGX_DECLINED;
  }
  
  nodeset_global_command_stats_state_t *state = ngx_palloc(pool, sizeof(*state));
  if(!state) {
    return NGX_ERROR;
  }
  
  ipc_t *ipc = nchan_memstore_get_ipc();
  
  state->waiting_for_reply_count = ipc->workers;
  state->stats.stats = NULL;
  state->stats.count = 0;
  state->stats.error = NULL;
  state->stats.name = ns->name;
  state->ns = ns;
  state->pool = pool;
  state->cb = cb;
  state->pd = pd;
  
  if(memstore_ipc_broadcast_redis_stats_request(ns, redis_stats_request_callback, state) != NGX_OK) {
    //ask other workers about it
    return NGX_ERROR;
  }
  
  nchan_add_oneshot_timer(stats_request_to_self, state, 0);
  return NGX_DONE;
}

static ngx_int_t redis_stats_request_callback(ngx_int_t statscount, void *d, void *pd) {
  size_t                                 stats_count = statscount;
  redis_node_command_stats_t            *stats = d;
  nodeset_global_command_stats_state_t  *state = pd;
  
  state->waiting_for_reply_count--;
  
  unsigned i, j;
  redis_node_command_stats_t *src, *dst;
  
  if(stats == NULL && stats_count > 0) {
    state->stats.error = "Unable to allocate memory for redis server stats";
    state->stats.count = 0;
    stats_count = 0;
  }
  
  for(i=0; i < stats_count; i++) {
    src = &stats[i];
    dst = NULL;
    for(j=0; j < state->stats.count; j++) {
      if(strlen(stats[i].id) == 0) {
        if(strcmp(stats[i].name, state->stats.stats[j].name) == 0) {
          //no id, but the names match
          dst = &state->stats.stats[j];
          break;
        }
      }
      else if(strcmp(stats[i].id, state->stats.stats[j].id) == 0) {
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
    //we're done here
    if(state->stats.stats && state->stats.count > 0) {
      //move stats data to request's pool
      redis_node_command_stats_t            *stats_in_pool = ngx_palloc(state->pool, sizeof(*stats_in_pool) * state->stats.count);
      if(!stats_in_pool) {
        state->stats.error = "Unable to allocate memory for redis server stats response";
        state->stats.count = 0;
        free(state->stats.stats);
        state->stats.stats = NULL;
      }
      else {
        memcpy(stats_in_pool, state->stats.stats, sizeof(*stats_in_pool) * state->stats.count);
        free(state->stats.stats);
        state->stats.stats = stats_in_pool;
      }
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
  const redis_node_command_stats_t *s1 = v1, *s2 = v2;
  int ret = strcmp(s1->name, s2->name);
  if(ret == 0) {
    ret = strcmp(s1->id, s2->id);
  }
  return ret;
}

#define REDIS_NODE_STAT_FMT_ARGS(stat, cmdtag) \
  ((int )nchan_accumulator_value(&stat->timings[NCHAN_REDIS_CMD_ ## cmdtag])), \
  ((int )nchan_accumulator_weight(&stat->timings[NCHAN_REDIS_CMD_ ## cmdtag]))

ngx_chain_t *redis_nodeset_stats_response_body_chain_palloc(redis_nodeset_command_stats_t *nstats , ngx_pool_t *pool) {
  ngx_chain_t *first = NULL, *last = NULL;
  
  
  char cstrbuf[4096];
  cstrbuf[4095]='\0'; //just in case the sprintf output is 4095 chars long
  
  char *open_fmtstr = "{\n"
                      "  \"upstream\": \"%s\",\n"
                      "  \"nodes\": [\n";
  
  snprintf(cstrbuf, 4095, open_fmtstr, nstats->name);
  if(!bufchain_add(pool, &first, &last, cstrbuf)) {
    return NULL;
  } 
  
  qsort(nstats->stats, nstats->count, sizeof(*nstats->stats), compare_nodestats_by_name);
  
  char *nodestat_fmtstr = "    {\n"
                          "      \"address\"        : \"%s\",\n"
                          "      \"id\"             : \"%s\",\n"
                          "      \"command_totals\" : {\n"
                          "        \"connect\"    : {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"pubsub_subscribe\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"pubsub_unsubsribe\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_change_subscriber_count\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_delete\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_find\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_get_message\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_get_large_message\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_publish_message\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_request_subscriber_info\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_get_subscriber_info_id\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_subscribe\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_unsubscribe\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"channel_keepalive\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"cluster_check\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"cluster_recover\": {\n"
                          "          \"msec\"     : %u,\n"
                          "          \"times\"    : %u\n"
                          "        },\n"
                          "        \"other\"      : {\n"
                          "          \"msec\"     : %u,\n"
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
             REDIS_NODE_STAT_FMT_ARGS(stat, CHANNEL_KEEPALIVE),
             REDIS_NODE_STAT_FMT_ARGS(stat, CLUSTER_CHECK),
             REDIS_NODE_STAT_FMT_ARGS(stat, CLUSTER_RECOVER),
             REDIS_NODE_STAT_FMT_ARGS(stat, OTHER),
             i + 1 < nstats->count ? "," : ""
    );
    if(!bufchain_add(pool, &first, &last, cstrbuf)) {
      return NULL;
    } 
  }
  
  if(!bufchain_add(pool, &first, &last, "  ]\n}\n")) {
    return NULL;
  }
  
  last->buf->last_in_chain = 1;
  last->buf->last_buf = 1;
  last->buf->flush = 1;
  return first;
}

static void nodeset_node_stats_cleanup_handler(ngx_event_t *ev) {
  redis_nodeset_t *ns = ev->data;
  
  if(!ev->timedout) {
    return;
  }
  int pending_removal = 0;
  redis_node_command_stats_t *stats, *next;
  for(stats = nchan_list_first(&ns->node_stats.list); stats != NULL; stats = next) {
    next = nchan_list_next(stats);
    if(stats->attached) {
      continue;
    }
    if(ngx_time() - stats->detached_time > ns->settings.node_stats.max_detached_time_sec) {
      nchan_list_remove(&ns->node_stats.list, stats);
    }
    else {
      pending_removal++;
    }
  }
  
  if(pending_removal > 0) {
    ngx_add_timer(ev, ns->settings.node_stats.max_detached_time_sec*1000);
  }
}

int redis_nodeset_stats_init(redis_nodeset_t *ns) {
  if(!ns->settings.node_stats.enabled) {
    //nothing to init, no errors either
    ns->node_stats.active = 0;
    return 1;
  }
  if(nchan_list_init(&ns->node_stats.list, sizeof(redis_node_command_stats_t), "node stats") != NGX_OK) {
    return 0;
  }
  if(nchan_init_timer(&ns->node_stats.cleanup_timer, nodeset_node_stats_cleanup_handler, ns) != NGX_OK) {
    return 0;
  }
  ns->node_stats.active = 1;
  return 1;
}

void redis_nodeset_stats_destroy(redis_nodeset_t *ns) {
  if(!ns->node_stats.active) {
    return;
  }
  if(ns->node_stats.cleanup_timer.timer_set) {
    ngx_del_timer(&ns->node_stats.cleanup_timer);
  }
  nchan_list_empty(&ns->node_stats.list);
}

void redis_node_stats_init(redis_node_t *node) {
  nchan_timequeue_init(&node->stats.timequeue.cmd, REDIS_NODE_CMD_TIMEQUEUE_LENGTH, NCHAN_REDIS_CMD_ANY);
  nchan_timequeue_init(&node->stats.timequeue.pubsub, REDIS_NODE_PUBSUB_TIMEQUEUE_LENGTH, NCHAN_REDIS_CMD_ANY);
  node->stats.data = NULL;
}

void redis_node_stats_destroy(redis_node_t *node) {
  nchan_timequeue_destroy(&node->stats.timequeue.cmd);
  nchan_timequeue_destroy(&node->stats.timequeue.pubsub);
}

redis_node_command_stats_t *redis_node_stats_attach(redis_node_t *node) {
  redis_nodeset_t *ns = node->nodeset;
  if(!ns->node_stats.active) {
    return NULL;
  }
  if(node->stats.data) {
    //already attached
    return node->stats.data;
  }
  
  const char    *name = node_nickname_cstr(node);
  ngx_str_t     *id = node->cluster.enabled ? &node->cluster.id : &node->run_id;
  
  redis_node_command_stats_t *stats;
  for(stats = nchan_list_first(&ns->node_stats.list); stats != NULL; stats = nchan_list_next(stats)) {
    if(stats->attached) {
      continue;
    }
    size_t stats_id_len = strlen(stats->id);
    
    if(strcmp(name, stats->name) == 0) {
      //match by name  
      if(id->len > 0 && stats_id_len > 0 && !nchan_strmatch(id, 1, stats->id)) {
          //ids present for both, and they don't match
        continue;
      }
      else {
        //either ids match, or at least one is missing. When don't have a definite id mismatch, 
        //we assume the stats are for the same node based on its name (address:port)
        if(stats_id_len == 0 && id->len > 0) {
          //stats has no id. assume it's a match by name (addr:port), and copy the id
          ngx_snprintf((u_char *)stats->id, sizeof(stats->id), "%V%Z", id);
          break;
        }
        
        break;
      }
    }
  }
  
  if(!stats) {
    //create new stats entry
    stats = nchan_list_append(&ns->node_stats.list);
    if(!stats) {
      node_log_error(node, "Failed to create stats data");
      return NULL;
    }
    ngx_snprintf((u_char *)stats->id, sizeof(stats->id), "%V%Z", id);
    ngx_snprintf((u_char *)stats->name, sizeof(stats->name), "%s%Z", name);
    //no buffer overflows pls
    stats->id[sizeof(stats->id)-1]='\0';
    stats->name[sizeof(stats->name)-1]='\0';
    
    stats->attached = 0;
    stats->detached_time = 0;
    
    int i;
    for(i=0; i<NCHAN_REDIS_CMD_ENUM_LAST; i++) {
      nchan_accumulator_init(&stats->timings[i], ACCUMULATOR_SUM, 0);
    }
  }
  
  assert(!stats->attached);
  stats->attached = 1;
  node->stats.data = stats;
  return stats;
}

void redis_node_stats_detach(redis_node_t *node) {
  redis_node_command_stats_t *stats = node->stats.data;
  redis_nodeset_t            *ns = node->nodeset;
  if (!ns->node_stats.active || stats == NULL) {
    //not attached
    return;
  }
  
  node->stats.data = NULL;
  
  stats->attached = 0;
  stats->detached_time = ngx_time();
  
  if(!ns->node_stats.cleanup_timer.timer_set) {
    ngx_add_timer(&ns->node_stats.cleanup_timer, ns->settings.node_stats.max_detached_time_sec*1000);
  }
}

redis_node_command_stats_t *redis_node_get_stats(redis_node_t *node) {
  if(node->stats.data) {
    return node->stats.data;
  }
  
  return redis_node_stats_attach(node);
}

void node_time_record(redis_node_t *node, redis_node_cmd_tag_t cmdtag, ngx_msec_t t) {
  if(!node->nodeset->node_stats.active) {
    return;
  }
  
  redis_node_command_stats_t *stats = redis_node_get_stats(node);
  if(stats == NULL) {
    node_log_error(node, "Unable to find stats data for node. cannot record command timing");
    return;
  }
  nchan_accumulator_update(&stats->timings[cmdtag], t);
}

static void node_time_start(redis_node_t *node, nchan_timequeue_t *tq, redis_node_cmd_tag_t cmdtag) {
  if(!node->nodeset->node_stats.active) {
    return;
  }
  
  assert(&node->stats.timequeue.cmd == tq || &node->stats.timequeue.pubsub == tq);
  nchan_timequeue_queue(tq, cmdtag);
}

void node_command_time_start(redis_node_t *node, redis_node_cmd_tag_t cmdtag) {
  node_time_start(node, &node->stats.timequeue.cmd, cmdtag);
}

void node_pubsub_time_start(redis_node_t *node, redis_node_cmd_tag_t cmdtag) {
  node_time_start(node, &node->stats.timequeue.pubsub, cmdtag);
}

static void node_time_finish(redis_node_t *node, nchan_timequeue_t *tq, redis_node_cmd_tag_t cmdtag, int strict_mode) {
  if(!node->nodeset->node_stats.active) {
    return;
  }
  
  assert(&node->stats.timequeue.cmd == tq || &node->stats.timequeue.pubsub == tq);
  nchan_timequeue_time_t    tqtime;
  
  if(!nchan_timequeue_dequeue(tq, strict_mode ? cmdtag : NCHAN_REDIS_CMD_ANY, &tqtime)) {
    if(strict_mode) {
      node_log_error(node, "timequeue dequeue error (expected_tag: %i, retrieved: %i)", cmdtag, tqtime.tag);
      return;
    }
    else if(tqtime.time_start == 0) {
      //fudge the time to make it seem like a o-time command
      tqtime.time_start = ngx_current_msec;
    }
  }
  ngx_msec_t t = ngx_current_msec - tqtime.time_start;
  
  assert(cmdtag >= 0 && cmdtag < NCHAN_REDIS_CMD_ENUM_LAST);
  node_time_record(node, cmdtag, t);
}

void node_command_time_finish(redis_node_t *node, redis_node_cmd_tag_t cmdtag) {
  node_time_finish(node, &node->stats.timequeue.cmd, cmdtag, 1);
}

void node_pubsub_time_finish(redis_node_t *node, redis_node_cmd_tag_t cmdtag) {
  node_time_finish(node, &node->stats.timequeue.pubsub, cmdtag, 1);
}

void node_command_time_finish_relaxed(redis_node_t *node, redis_node_cmd_tag_t cmdtag) {
  node_time_finish(node, &node->stats.timequeue.cmd, cmdtag, 0);
}
void node_pubsub_time_finish_relaxed(redis_node_t *node, redis_node_cmd_tag_t cmdtag) {
  node_time_finish(node, &node->stats.timequeue.pubsub, cmdtag, 0);
}
