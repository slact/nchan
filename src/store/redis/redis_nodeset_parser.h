#ifndef REDIS_NODESET_PARSER_H
#define REDIS_NODESET_PARSER_H

#include <nchan_module.h>
#include "store-private.h"

typedef struct {
  ngx_str_t      line;
  ngx_str_t      id;         //node id
  ngx_str_t      address;    //address as known by redis
  ngx_str_t      flags;
  
  ngx_str_t      master_id;  //if slave
  ngx_str_t      ping_sent;
  ngx_str_t      pong_recv;
  ngx_str_t      config_epoch;
  ngx_str_t      link_state; //connected or disconnected
  
  ngx_str_t      slots;
  
  unsigned       connected:1;
  unsigned       master:1;
  unsigned       failed:1;
  unsigned       self:1;
} cluster_nodes_line_t;

int parse_info_discover_slaves(redis_node_t *node, const char *info);
int parse_info_discover_master(redis_node_t *node, const char *info);
int parse_cluster_nodes_discover_peers(redis_node_t *node, const char *clusternodes);

#endif /*REDIS_NODESET_PARSER_H */
