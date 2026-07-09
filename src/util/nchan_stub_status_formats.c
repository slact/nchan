#include "nchan_stub_status_formats.h"
#include <util/nchan_output.h>

// Common function to get stats data
static ngx_int_t get_stats_data(ngx_http_request_t *r,
    nchan_stats_worker_t *worker, nchan_stats_global_t *global,
    nchan_main_conf_t *mcf, float *shmem_used, float *shmem_max) {

  if(nchan_stats_get_all(worker, global) != NGX_OK) {
    nchan_log_request_error(r, "Failed to get stub status stats.");
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  *shmem_used = (float)((float)nchan_get_used_shmem() / 1024.0);
  *shmem_max = (float)((float)mcf->shm_size / 1024.0);

  return NGX_OK;
}

// Plain text format (original implementation)
ngx_int_t nchan_stub_status_plain(ngx_http_request_t *r,
    nchan_stats_worker_t *worker, nchan_stats_global_t *global,
    nchan_main_conf_t *mcf) {

  ngx_buf_t           *b;
  ngx_chain_t          out;
  float                shmem_used, shmem_max;

  char     *buf_fmt = "total published messages: %ui\n"
                       "stored messages: %ui\n"
                       "shared memory used: %fK\n"
                       "shared memory limit: %fK\n"
                       "channels: %ui\n"
                       "subscribers: %ui\n"
                       "redis pending commands: %ui\n"
                       "redis connected servers: %ui\n"
                       "redis unhealthy upstreams: %ui\n"
                       "total redis commands sent: %ui\n"
                       "total interprocess alerts received: %ui\n"
                       "interprocess alerts in transit: %ui\n"
                       "interprocess queued alerts: %ui\n"
                       "total interprocess send delay: %ui\n"
                       "total interprocess receive delay: %ui\n"
                       "nchan version: %s\n";

  if(get_stats_data(r, worker, global, mcf, &shmem_used, &shmem_max) != NGX_OK) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  if ((b = ngx_pcalloc(r->pool, sizeof(*b) + 800)) == NULL) {
    nchan_log_request_error(r, "Failed to allocate response buffer for nchan_stub_status.");
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  b->start = (u_char *)&b[1];
  b->pos = b->start;

  b->end = ngx_snprintf(b->start, 800, buf_fmt, global->total_published_messages, worker->messages, shmem_used, shmem_max, worker->channels, worker->subscribers, worker->redis_pending_commands, worker->redis_connected_servers, worker->redis_unhealthy_upstreams, global->total_redis_commands_sent, global->total_ipc_alerts_received, global->total_ipc_alerts_sent - global->total_ipc_alerts_received, worker->ipc_queue_size, global->total_ipc_send_delay, global->total_ipc_receive_delay, NCHAN_VERSION);
  b->last = b->end;

  b->memory = 1;
  b->last_buf = 1;

  r->headers_out.status = NGX_HTTP_OK;
  r->headers_out.content_type.len = sizeof("text/plain") - 1;
  r->headers_out.content_type.data = (u_char *) "text/plain";

  r->headers_out.content_length_n = b->end - b->start;
  ngx_http_send_header(r);

  out.buf = b;
  out.next = NULL;

  return ngx_http_output_filter(r, &out);
}

// JSON format
ngx_int_t nchan_stub_status_json(ngx_http_request_t *r,
    nchan_stats_worker_t *worker, nchan_stats_global_t *global,
    nchan_main_conf_t *mcf) {

  ngx_buf_t           *b;
  ngx_chain_t          out;
  float                shmem_used, shmem_max;

  char     *buf_fmt = "{\n"
                       "  \"total_published_messages\": %ui,\n"
                       "  \"stored_messages\": %ui,\n"
                       "  \"shared_memory\": {\n"
                       "    \"used_kb\": %f,\n"
                       "    \"limit_kb\": %f\n"
                       "  },\n"
                       "  \"channels\": %ui,\n"
                       "  \"subscribers\": %ui,\n"
                       "  \"redis\": {\n"
                       "    \"pending_commands\": %ui,\n"
                       "    \"connected_servers\": %ui,\n"
                       "    \"unhealthy_upstreams\": %ui,\n"
                       "    \"total_commands_sent\": %ui\n"
                       "  },\n"
                       "  \"interprocess\": {\n"
                       "    \"alerts\": {\n"
                       "      \"sent\": %ui,\n"
                       "      \"received\": %ui,\n"
                       "      \"in_transit\": %ui\n"
                       "    },\n"
                       "    \"queued_alerts\": %ui,\n"
                       "    \"send_delay\": %ui,\n"
                       "    \"receive_delay\": %ui\n"
                       "  },\n"
                       "  \"version\": \"%s\"\n"
                       "}";

  if(get_stats_data(r, worker, global, mcf, &shmem_used, &shmem_max) != NGX_OK) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  if ((b = ngx_pcalloc(r->pool, sizeof(*b) + 800)) == NULL) {
    nchan_log_request_error(r, "Failed to allocate response buffer for nchan_stub_status JSON.");
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  b->start = (u_char *)&b[1];
  b->pos = b->start;

  b->end = ngx_snprintf(b->start, 800, buf_fmt, global->total_published_messages, worker->messages, shmem_used, shmem_max, worker->channels, worker->subscribers, worker->redis_pending_commands, worker->redis_connected_servers, worker->redis_unhealthy_upstreams, global->total_redis_commands_sent, global->total_ipc_alerts_sent, global->total_ipc_alerts_received, global->total_ipc_alerts_sent - global->total_ipc_alerts_received, worker->ipc_queue_size, global->total_ipc_send_delay, global->total_ipc_receive_delay, NCHAN_VERSION);
  b->last = b->end;

  b->memory = 1;
  b->last_buf = 1;

  r->headers_out.status = NGX_HTTP_OK;
  r->headers_out.content_type.len = sizeof("application/json") - 1;
  r->headers_out.content_type.data = (u_char *) "application/json";

  r->headers_out.content_length_n = b->end - b->start;
  ngx_http_send_header(r);

  out.buf = b;
  out.next = NULL;

  return ngx_http_output_filter(r, &out);
}

// HTML table format
ngx_int_t nchan_stub_status_html(ngx_http_request_t *r,
    nchan_stats_worker_t *worker, nchan_stats_global_t *global,
    nchan_main_conf_t *mcf) {

  ngx_buf_t           *b;
  ngx_chain_t          out;
  float                shmem_used, shmem_max;

  char     *buf_fmt = "<!DOCTYPE html><html><head><title>Nchan Status</title>"
                       "<style>table{border-collapse:collapse}th,td{border:1px solid #888;padding:8px;text-align:left}th{background-color:#444;color:#fff}tr:nth-child(even){background-color:#f2f2f2}</style>"
                       "</head><body><h1>Nchan Status</h1><table>"
                       "<tr><th>Metric</th><th>Value</th></tr>"
                       "<tr><td>Total Published Messages</td><td>%ui</td></tr>"
                       "<tr><td>Stored Messages</td><td>%ui</td></tr>"
                       "<tr><td>Shared Memory Used</td><td>%fK</td></tr>"
                       "<tr><td>Shared Memory Limit</td><td>%fK</td></tr>"
                       "<tr><td>Channels</td><td>%ui</td></tr>"
                       "<tr><td>Subscribers</td><td>%ui</td></tr>"
                       "<tr><td>Redis Pending Commands</td><td>%ui</td></tr>"
                       "<tr><td>Redis Connected Servers</td><td>%ui</td></tr>"
                       "<tr><td>Redis Unhealthy Upstreams</td><td>%ui</td></tr>"
                       "<tr><td>Total Redis Commands Sent</td><td>%ui</td></tr>"
                       "<tr><td>Interprocess Alerts Sent</td><td>%ui</td></tr>"
                       "<tr><td>Interprocess Alerts Received</td><td>%ui</td></tr>"
                       "<tr><td>Interprocess Alerts In Transit</td><td>%ui</td></tr>"
                       "<tr><td>Interprocess Queued Alerts</td><td>%ui</td></tr>"
                       "<tr><td>Total Interprocess Send Delay</td><td>%ui</td></tr>"
                       "<tr><td>Total Interprocess Receive Delay</td><td>%ui</td></tr>"
                       "<tr><td>Nchan Version</td><td>%s</td></tr>"
                       "</table></body></html>";

  if(get_stats_data(r, worker, global, mcf, &shmem_used, &shmem_max) != NGX_OK) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  if ((b = ngx_pcalloc(r->pool, sizeof(*b) + 2048)) == NULL) {
    nchan_log_request_error(r, "Failed to allocate response buffer for nchan_stub_status HTML.");
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  b->start = (u_char *)&b[1];
  b->pos = b->start;

  b->end = ngx_snprintf(b->start, 2048, buf_fmt, global->total_published_messages, worker->messages, shmem_used, shmem_max, worker->channels, worker->subscribers, worker->redis_pending_commands, worker->redis_connected_servers, worker->redis_unhealthy_upstreams, global->total_redis_commands_sent, global->total_ipc_alerts_sent, global->total_ipc_alerts_received, global->total_ipc_alerts_sent - global->total_ipc_alerts_received, worker->ipc_queue_size, global->total_ipc_send_delay, global->total_ipc_receive_delay, NCHAN_VERSION);
  b->last = b->end;

  b->memory = 1;
  b->last_buf = 1;

  r->headers_out.status = NGX_HTTP_OK;
  r->headers_out.content_type.len = sizeof("text/html") - 1;
  r->headers_out.content_type.data = (u_char *) "text/html";

  r->headers_out.content_length_n = b->end - b->start;
  ngx_http_send_header(r);

  out.buf = b;
  out.next = NULL;

  return ngx_http_output_filter(r, &out);
}

// Prometheus format
ngx_int_t nchan_stub_status_prometheus(ngx_http_request_t *r,
    nchan_stats_worker_t *worker, nchan_stats_global_t *global,
    nchan_main_conf_t *mcf) {

  ngx_buf_t           *b;
  ngx_chain_t          out;
  float                shmem_used, shmem_max;

  char     *buf_fmt = "nchan_published_messages_total %ui\n"
                       "nchan_stored_messages %ui\n"
                       "nchan_shared_memory_used_kb %f\n"
                       "nchan_shared_memory_limit_kb %f\n"
                       "nchan_channels %ui\n"
                       "nchan_subscribers %ui\n"
                       "nchan_redis_pending_commands %ui\n"
                       "nchan_redis_connected_servers %ui\n"
                       "nchan_redis_unhealthy_upstreams %ui\n"
                       "nchan_redis_commands_sent_total %ui\n"
                       "nchan_ipc_alerts_sent_total %ui\n"
                       "nchan_ipc_alerts_received_total %ui\n"
                       "nchan_ipc_alerts_in_transit %ui\n"
                       "nchan_ipc_queued_alerts %ui\n"
                       "nchan_ipc_send_delay_total %ui\n"
                       "nchan_ipc_receive_delay_total %ui\n";

  if(get_stats_data(r, worker, global, mcf, &shmem_used, &shmem_max) != NGX_OK) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  if ((b = ngx_pcalloc(r->pool, sizeof(*b) + 800)) == NULL) {
    nchan_log_request_error(r, "Failed to allocate response buffer for nchan_stub_status Prometheus.");
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  b->start = (u_char *)&b[1];
  b->pos = b->start;

  b->end = ngx_snprintf(b->start, 800, buf_fmt, global->total_published_messages, worker->messages, shmem_used, shmem_max, worker->channels, worker->subscribers, worker->redis_pending_commands, worker->redis_connected_servers, worker->redis_unhealthy_upstreams, global->total_redis_commands_sent, global->total_ipc_alerts_sent, global->total_ipc_alerts_received, global->total_ipc_alerts_sent - global->total_ipc_alerts_received, worker->ipc_queue_size, global->total_ipc_send_delay, global->total_ipc_receive_delay);
  b->last = b->end;

  b->memory = 1;
  b->last_buf = 1;

  r->headers_out.status = NGX_HTTP_OK;
  r->headers_out.content_type.len = sizeof("text/plain") - 1;
  r->headers_out.content_type.data = (u_char *) "text/plain";

  r->headers_out.content_length_n = b->end - b->start;
  ngx_http_send_header(r);

  out.buf = b;
  out.next = NULL;

  return ngx_http_output_filter(r, &out);
}
