#include <nchan_module.h>
#include <assert.h>

#include "redis_nginx_adapter.h"

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
  }
  else {
    l->slots.data = NULL;
    l->slots.len = 0;
    l->master = 0;
  }
  l->failed = nchan_ngx_str_substr((&l->flags), "fail");
  l->self = nchan_ngx_str_substr((&l->flags), "myself") ? 1 : 0;
  
  l->connected = l->link_state.data[0]=='c' ? 1 : 0; //[c]onnected
  
  //redis >= 4.0 CLUSTER NODES format compatibility
  if((tmp = memrchr(l->address.data, '@', l->address.len)) != NULL) {
    l->address.len = tmp - l->address.data;
  }
  
  cur = max;
  if(&cur[-1] > (u_char *)line && cur[-1] == '\0')
    cur--;
  return (char *)cur;
}

u_char *nodeset_parser_scan_cluster_nodes_slots_string(ngx_str_t *str, u_char *cur, redis_cluster_slot_range_t *r) {
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

int parse_info_discover_slaves(redis_node_t *node, const char *info) {
  char                   slavebuf[20]="slave0:";
  int                    i = 0;
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
    node_discover_slave(node, &rcp);
    //next slave
    i++;
    ngx_sprintf((u_char *)slavebuf, "slave%d:", i);    
  }
  return 1;
}

int parse_info_discover_master(redis_node_t *node, const char *info) {
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
  
  node_discover_master(node, &rcp);
  return 1;
}

int parse_cluster_nodes_discover_peers(redis_node_t *node, const char *clusternodes) {
  const char           *line = clusternodes;
  cluster_nodes_line_t  l;
  while((line = nodeset_parser_scan_cluster_nodes_line(line, &l)) != NULL) {
    if(l.failed) //ignore failed nodes
      continue;
    //if(l.self)
      
    
  }
  return 1;
}
