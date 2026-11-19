#ifndef NCHAN_STUB_STATUS_FORMATS_H
#define NCHAN_STUB_STATUS_FORMATS_H

#include <nchan_module.h>
#include <util/nchan_stats.h>

ngx_int_t nchan_stub_status_plain(ngx_http_request_t *r,
    nchan_stats_worker_t *worker, nchan_stats_global_t *global,
    nchan_main_conf_t *mcf);

ngx_int_t nchan_stub_status_json(ngx_http_request_t *r,
    nchan_stats_worker_t *worker, nchan_stats_global_t *global,
    nchan_main_conf_t *mcf);

ngx_int_t nchan_stub_status_html(ngx_http_request_t *r,
    nchan_stats_worker_t *worker, nchan_stats_global_t *global,
    nchan_main_conf_t *mcf);

ngx_int_t nchan_stub_status_prometheus(ngx_http_request_t *r,
    nchan_stats_worker_t *worker, nchan_stats_global_t *global,
    nchan_main_conf_t *mcf);

#endif //NCHAN_STUB_STATUS_FORMATS_H
