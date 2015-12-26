#include <nchan_module.h>
#include <assert.h>

void nchan_expand_msg_id_multi_tag(nchan_msg_id_t *id, uint8_t in_n, uint8_t out_n, int16_t fill) {
  int16_t v, n = id->tagcount;
  int16_t *tags = n <= NCHAN_MULTITAG_MAX ? id->tag.fixed : id->tag.allocd;
  uint8_t i;
  assert(n > in_n && n > out_n);
  v = tags[in_n];
  
  for(i=0; i < n; i++) {
    tags[i] = (i == out_n) ? v : fill;
  }
}

ngx_int_t nchan_copy_new_msg_id(nchan_msg_id_t *dst, nchan_msg_id_t *src) {
  ngx_memcpy(dst, src, sizeof(*src));
  if(src->tagcount > NCHAN_MULTITAG_MAX) {
    size_t sz = sizeof(*src->tag.allocd) * src->tagcount;
    if((dst->tag.allocd = ngx_alloc(sz, ngx_cycle->log)) == NULL) {
      return NGX_ERROR;
    }
    ngx_memcpy(dst->tag.allocd, src->tag.allocd, sz);
  }
  return NGX_OK; 
}
ngx_int_t nchan_copy_msg_id(nchan_msg_id_t *dst, nchan_msg_id_t *src, int16_t *largetags) {
  uint16_t dst_n = dst->tagcount, src_n = src->tagcount;
  dst->time = src->time;
  
  if(dst_n > NCHAN_MULTITAG_MAX && dst_n > src_n) {
    ngx_free(dst->tag.allocd);
    dst_n = NCHAN_MULTITAG_MAX;
  }
  
  dst->tagcount = src->tagcount;
  dst->tagactive = src->tagactive;
  
  if(src_n <= NCHAN_MULTITAG_MAX) {
    dst->tag = src->tag;
  }
  else {
    if(dst_n < src_n) {
      if(!largetags) {
        if((largetags = ngx_alloc(sizeof(*largetags) * src_n, ngx_cycle->log)) == NULL) {
          return NGX_ERROR;
        }
      }
      dst->tag.allocd = largetags;
    }
    
    ngx_memcpy(dst->tag.allocd, src->tag.allocd, sizeof(*src->tag.allocd) * src_n);
  }
  return NGX_OK;
}
ngx_int_t nchan_free_msg_id(nchan_msg_id_t *id) {
  if(id->tagcount > NCHAN_MULTITAG_MAX) {
    ngx_free(id->tag.allocd);
    id->tag.allocd = NULL;
  }
  return NGX_OK;
}