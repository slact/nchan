#ifndef REDIS_NODESET_PARSER_H
#define REDIS_NODESET_PARSER_H

#define MAX_CLUSTER_NODE_PARSED_LINES 512
#define MAX_NODE_SLAVES_PARSED 512

#include <nchan_module.h>
#include "store-private.h"

typedef struct {
  ngx_str_t      line;
  ngx_str_t      id;         //node id
  ngx_str_t      address;    //address as known by redis
  ngx_str_t      hostname;
  ngx_int_t      port;
  ngx_str_t      flags;
  
  ngx_str_t      master_id;  //if slave
  ngx_str_t      ping_sent;
  ngx_str_t      pong_recv;
  ngx_str_t      config_epoch;
  ngx_str_t      link_state; //connected or disconnected
  
  ngx_str_t      slots;
  ngx_int_t      slot_ranges_count;
  
  unsigned       connected:1;
  unsigned       master:1;
  unsigned       noaddr:1;
  unsigned       failed:1;
  unsigned       handshake:1;
  unsigned       self:1;
} cluster_nodes_line_t;

redis_connect_params_t *parse_info_slaves(redis_node_t *node, const char *info, size_t *count);
redis_connect_params_t *parse_info_master(redis_node_t *node, const char *info);
cluster_nodes_line_t *parse_cluster_nodes(redis_node_t *node, const char *clusternodes, size_t *count);
int parse_cluster_node_slots(cluster_nodes_line_t *l, redis_slot_range_t *ranges);
#endif /*REDIS_NODESET_PARSER_H */
