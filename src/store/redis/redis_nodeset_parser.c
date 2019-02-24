#include <nchan_module.h>
#include <assert.h>

#include "redis_nginx_adapter.h"

#include <util/nchan_util.h>
#include <util/nchan_msg.h>
#include <store/store_common.h>
#include "redis_nodeset_parser.h"
#include "redis_nodeset.h"

#define nchan_scan_str(str_src, cur, chr, str)\
  (str)->data = (u_char *)memchr(cur, chr, (str_src)->len - (cur - (str_src)->data));\
  if(!(str)->data)                            \
    (str)->data = (str_src)->data + (str_src)->len;\
  if((str)->data) {                           \
    (str)->len = (str)->data - cur;           \
    (str)->data = cur;                        \
    cur+=(str)->len+1;                        \
  }                                           \
  else                                        \
    goto fail


static cluster_nodes_line_t   cluster_node_parsed_lines[MAX_CLUSTER_NODE_PARSED_LINES];
static redis_connect_params_t parsed_connect_params[MAX_NODE_SLAVES_PARSED];

static u_char *nodeset_parser_scan_cluster_nodes_slots_string(ngx_str_t *str, u_char *cur, redis_slot_range_t *r) {
  ngx_str_t       slot_min_str, slot_max_str, slot;
  ngx_int_t       slot_min,     slot_max;
  u_char         *dash;
  
  if(cur == NULL) {
    cur = str->data;
  }
  else if(cur >= str->data + str->len) {
    return NULL;
  }
  if(str->len == 0) {
    return NULL;
  }
  
  nchan_scan_str(str, cur, ' ', &slot);
  if(slot.data[0] == '[') {
    //transitional special slot. ignore it.
    return nodeset_parser_scan_cluster_nodes_slots_string(str, cur, r);
  }
  
  dash = (u_char *)memchr(slot.data, '-', slot.len);
  if(dash) {
    slot_min_str.data = slot.data;
    slot_min_str.len = dash - slot.data;
    
    slot_max_str.data = dash + 1;
    slot_max_str.len = slot.len - (slot_max_str.data - slot.data);
  }
  else {
    slot_min_str = slot;
    slot_max_str = slot;
  }
  
  slot_min = ngx_atoi(slot_min_str.data, slot_min_str.len);
  slot_max = ngx_atoi(slot_max_str.data, slot_max_str.len);
  
  //DBG("slots: %i - %i", slot_min, slot_max);
  
  r->min = slot_min;
  r->max = slot_max;
  
  return cur;
  
fail:
  return NULL;
}

int parse_cluster_node_slots(cluster_nodes_line_t *l, redis_slot_range_t *ranges) {
  //assumes the correct amount of ranges has already been allocated
int                    i = 0;
  redis_slot_range_t   range;
  u_char              *cur = NULL;
  while((cur = nodeset_parser_scan_cluster_nodes_slots_string(&l->slots, cur, &range)) != NULL) {
    if(i > l->slot_ranges_count) {
      return 0; //parsing more slot ranges than was expected
    }
    ranges[i]=range;
    i++;
  }
  if(i !=l->slot_ranges_count) {
    return 0; //parsed the wrong amount of slot ranges
  }
  return 1;
}

static char *nodeset_parser_scan_cluster_nodes_line(const char *line, cluster_nodes_line_t *l) {
  u_char     *cur = (u_char *)line;
  u_char     *max = cur;
  u_char     *tmp;
  ngx_str_t   rest_line;
  
  if(cur[0]=='\0')
    return NULL;
  
  nchan_scan_split_by_chr(&max, strlen((char *)max), &rest_line, '\n');
  l->line = rest_line;
  
  nchan_scan_until_chr_on_line(&rest_line, &l->id,           ' ');
  nchan_scan_until_chr_on_line(&rest_line, &l->address,      ' ');
  nchan_scan_until_chr_on_line(&rest_line, &l->flags,        ' ');
  
  nchan_scan_until_chr_on_line(&rest_line, &l->master_id,    ' ');
  nchan_scan_until_chr_on_line(&rest_line, &l->ping_sent,    ' ');
  nchan_scan_until_chr_on_line(&rest_line, &l->pong_recv,    ' ');
  nchan_scan_until_chr_on_line(&rest_line, &l->config_epoch, ' ');
  nchan_scan_until_chr_on_line(&rest_line, &l->link_state,   ' ');
  
  if(nchan_ngx_str_substr((&l->flags), "master")) {
    l->slots = rest_line;
    l->master = 1;
    
    //how many slot ranges?
    int                  i = 0;
    redis_slot_range_t   range;
    cur = NULL;
    while((cur = nodeset_parser_scan_cluster_nodes_slots_string(&l->slots, cur, &range)) != NULL) {
      i++;
    }
    l->slot_ranges_count = i;
  }
  else {
    l->slots.data = NULL;
    l->slots.len = 0;
    l->slot_ranges_count = 0;
    l->master = 0;
  }
  l->failed = nchan_ngx_str_substr((&l->flags), "fail");
  l->self = nchan_ngx_str_substr((&l->flags), "myself");
  l->noaddr = nchan_ngx_str_substr((&l->flags), "noaddr");
  l->handshake = nchan_ngx_str_substr((&l->flags), "handshake");
  
  l->connected = l->link_state.data[0]=='c' ? 1 : 0; //[c]onnected
  
  //redis >= 4.0 CLUSTER NODES format compatibility
  if((tmp = memrchr(l->address.data, '@', l->address.len)) != NULL) {
    l->address.len = tmp - l->address.data;
  }
  
  if((tmp = memrchr(l->address.data, ':', l->address.len)) != NULL) {
    //hostname and port from address
    ngx_str_t   port;
    l->hostname.data = l->address.data;
    l->hostname.len = tmp - l->address.data;
    port.len = l->address.len - l->hostname.len - 1;
    port.data = &tmp[1];
    l->port = ngx_atoi(port.data, port.len);
  }
  
  cur = max;
  if(&cur[-1] > (u_char *)line && cur[-1] == '\0')
    cur--;
  return (char *)cur;
}

redis_connect_params_t *parse_info_slaves(redis_node_t *node, const char *info, size_t *count) {
  char                   slavebuf[20]="slave0:";
  int                    i = 0, skipped = 0;
  redis_connect_params_t rcp;
  ngx_str_t              line;
  while(nchan_get_rest_of_line_in_cstr(info, slavebuf, &line)) {
    //ip=localhost,port=8537,state=online,offset=420,lag=1
    ngx_str_t hostname, port;
    nchan_scan_until_chr_on_line(&line, NULL,      '='); //ip=
    nchan_scan_until_chr_on_line(&line, &hostname, ','); //ip=([^,]*),
    nchan_scan_until_chr_on_line(&line, NULL,      '='); //port=
    nchan_scan_until_chr_on_line(&line, &port,     ','); //port=([^,]*),
    //don't care about the rest
    rcp.hostname = hostname;
    rcp.port = ngx_atoi(port.data, port.len);
    rcp.password = node->connect_params.password;
    rcp.peername.len = 0;
    rcp.db = node->connect_params.db;
    
    if(i < MAX_NODE_SLAVES_PARSED) {
      parsed_connect_params[i]=rcp;
    }
    else {
      node_log_error(node, "too many slaves, skipping slave %d", i+1);
    }
    i++;
    ngx_sprintf((u_char *)slavebuf, "slave%d:", i);
    
    
  }
  *count = i - skipped;
  return parsed_connect_params;
}

redis_connect_params_t *parse_info_master(redis_node_t *node, const char *info) {
  redis_connect_params_t    rcp;
  ngx_str_t                 port;
  if(!nchan_get_rest_of_line_in_cstr(info, "master_host:", &rcp.hostname)) {
    node_log_error(node, "failed to find master_host while discovering master");
    return 0;
  }
  
  if(!nchan_get_rest_of_line_in_cstr(info, "master_port:", &port)) {
    node_log_error(node, "failed to find master_port while discovering master");
    return 0;
  }
  rcp.port = ngx_atoi(port.data, port.len);
  if(rcp.port == NGX_ERROR) {
    node_log_error(node, "failed to parse master_port while discovering master");
    return 0;
  }
  rcp.db = node->connect_params.db;
  rcp.password = node->connect_params.password;

  rcp.peername.data = NULL;
  rcp.peername.len = 0;
  
  parsed_connect_params[0]=rcp;
  return &parsed_connect_params[0];
}

cluster_nodes_line_t *parse_cluster_nodes(redis_node_t *node, const char *clusternodes, size_t *count) {
  
  const char           *line = clusternodes;
  size_t                n = 0, discarded = 0;
  cluster_nodes_line_t  l;
  while((line = nodeset_parser_scan_cluster_nodes_line(line, &l)) != NULL) {
    if(n > MAX_CLUSTER_NODE_PARSED_LINES) {
      node_log_error(node, "too many cluster nodes, discarding line %d", n + discarded);
      discarded++;
    }
    else {
      cluster_node_parsed_lines[n]=l;
      n++;
    }
  }
  *count = n;
  return cluster_node_parsed_lines;
}
