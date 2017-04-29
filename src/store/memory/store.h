#ifndef NCHAN_MEMSTORE_H
#define NCHAN_MEMSTORE_H

extern nchan_store_t  nchan_store_memory;

ngx_int_t memstore_channel_owner(ngx_str_t *id);

extern void  *nchan_store_memory_shmem;

nchan_loc_conf_shared_data_t *memstore_get_conf_shared_data(nchan_loc_conf_t *cf);
ngx_int_t memstore_reserve_conf_shared_data(nchan_loc_conf_t *cf);
#endif //NCHAN_MEMSTORE_H
