#include <nchan_module.h>
#include <util/nchan_output.h>
#include <assert.h>

//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, arg...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "BUFCHAINPOOL:" fmt, ##arg)
#define ERR(fmt, arg...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "BUFCHAINPOOL:" fmt, ##arg)
/*
static void validate_stuff(nchan_bufchain_pool_t *bcp) {
  nchan_bufchain_link_t      *cur;
  nchan_file_link_t          *fcur;
  int                         bcs=0, rbcs = 0, files=0, rfiles = 0;
  for(cur = bcp->bc_head; cur != NULL; cur = cur->next) {
    bcs++;
  }
  assert(bcs == bcp->bc_count);
  for(cur = bcp->bc_recycle_head; cur != NULL; cur = cur->next) {
    rbcs++;
  }
  assert(rbcs == bcp->bc_recycle_count);
  for(fcur = bcp->file_head; fcur != NULL; fcur = fcur->next) {
    files++;
  }
  assert(files == bcp->file_count);
  for(fcur = bcp->file_recycle_head; fcur != NULL; fcur = fcur->next) {
    rfiles++;
  }
  assert(rfiles == bcp->file_recycle_count);
}
*/



nchan_buf_and_chain_t *nchan_bufchain_pool_reserve(nchan_bufchain_pool_t *bcp, ngx_int_t count) {
  nchan_bufchain_link_t      *cur = NULL, *last = NULL, *first = NULL;
  nchan_bufchain_link_t      **rhead = &bcp->bc_recycle_head;
  //validate_stuff(bcp);
  if(count <= 0) {
    return NULL;
  }
  while(count > 0) {
    if(*rhead) {
      cur = *rhead;
      *rhead = cur->next;
      bcp->bc_recycle_count --;
    } else {
      cur = ngx_palloc(bcp->pool, sizeof(*cur));
      cur->bc.chain.buf = &cur->bc.buf;
    }
    if(!first) {
      first = cur;
    }
    if(last) {
      last->next = cur;
      last->bc.chain.next = &cur->bc.chain;
    }
    last = cur;
    count --;
    bcp->bc_count++;
  }
  last->next = bcp->bc_head;
  last->bc.chain.next=NULL;
  bcp->bc_head = first;
  DBG("%p bcs %i (rec. %i), files %i (rec. %i)", bcp, bcp->bc_count, bcp->bc_recycle_count, bcp->file_count, bcp->file_recycle_count);
  //validate_stuff(bcp);
  return &first->bc;
}

static ngx_buf_t *nchan_bufchain_append(nchan_bufchain_pool_t *bcp) {
  static ngx_buf_t            throwaway;
  ngx_chain_t                *chain;
  ngx_chain_t               **rhead = &bcp->bc.recycle_head;
  
  if(*rhead) {
    chain = *rhead;
    *rhead = chain->next;
    bcp->bc.recycle_count --;
  } 
  else {
    nchan_buf_and_chain_t *bufchain = ngx_palloc(bcp->pool, sizeof(*bufchain));
    if(bufchain == NULL) {
      nchan_log_error("unable to palloc bufchain");
      return &throwaway;
    }
    chain = &bufchain->chain;
    chain->buf = &bufchain->buf;
  }
  
  chain->buf->last_buf = 1;
  chain->buf->last_in_chain = 1;
  chain->next = NULL;
  
  if(!bcp->bc.head) {
    bcp->bc.head = chain;
  }
  
  if(bcp->bc.tail) {
    ngx_buf_t *tbuf = bcp->bc.tail->buf;
    tbuf->last_buf = 0;
    tbuf->last_in_chain = 0;
    bcp->bc.tail->next = chain;
  }
  bcp->bc.tail = chain;
  bcp->bc.count++;
  return chain->buf;
}

ngx_int_t nchan_bufchain_append_buf(nchan_bufchain_pool_t *bcp, ngx_buf_t *inbuf) {
  ngx_buf_t *buf = nchan_bufchain_append(bcp);
  *buf = *inbuf;
  buf->last_buf = 1;
  buf->last_in_chain = 1;
  bcp->bc.length += ngx_buf_size(inbuf);
  return NGX_OK;
}

ngx_int_t nchan_bufchain_append_str(nchan_bufchain_pool_t *bcp, ngx_str_t *str) {
  ngx_buf_t *buf = nchan_bufchain_append(bcp);
  ngx_memzero(buf, sizeof(*buf));
  buf->start = str->data;
  buf->pos = buf->start;
  buf->end = str->data + str->len;
  buf->last = buf->end;
  buf->memory = 1;
  buf->last_buf = 1;
  buf->last_in_chain = 1;
  bcp->bc.length += str->len;
  return NGX_OK;
}
ngx_int_t nchan_bufchain_append_cstr(nchan_bufchain_pool_t *bcp, char *cstr) {
  ngx_str_t str;
  str.data = (u_char *)cstr;
  str.len = strlen(cstr);
  return nchan_bufchain_append_str(bcp, &str);
}

size_t nchan_bufchain_length(nchan_bufchain_pool_t *bcp) {
  return bcp->bc.length;
}
ngx_chain_t *nchan_bufchain_first_chain(nchan_bufchain_pool_t *bcp) {
  return bcp->bc.head;
}


ngx_file_t *nchan_bufchain_pool_reserve_file(nchan_bufchain_pool_t *bcp) {
  nchan_file_link_t    *cur;
  //validate_stuff(bcp);
  if(bcp->file_recycle_head) {
    cur = bcp->file_recycle_head;
    bcp->file_recycle_head = cur->next;
    bcp->file_recycle_count --;
  }
  else {
    cur = ngx_palloc(bcp->pool, sizeof(*cur));
  }
  cur->next = bcp->file_head;
  bcp->file_head = cur;
  bcp->file_count++;
  DBG("%p bcs %i (rec. %i), files %i (rec. %i)", bcp, bcp->bc_count, bcp->bc_recycle_count, bcp->file_count, bcp->file_recycle_count);
  //validate_stuff(bcp);
  return &cur->file;
}

void nchan_bufchain_pool_refresh_files(nchan_bufchain_pool_t *bcp) {
  nchan_file_link_t    *cur;
  //validate_stuff(bcp);
  for(cur = bcp->file_head; cur != NULL; cur = cur->next) {
    cur->file.fd = nchan_fdcache_get(&cur->file.name);
  }
}

ngx_int_t nchan_bufchain_pool_init(nchan_bufchain_pool_t *bcp, ngx_pool_t *pool) {
  bcp->bc_count = 0;
  bcp->file_count = 0;
  
  bcp->bc_recycle_count = 0;
  bcp->file_recycle_count = 0;
  
  bcp->bc_head = NULL;
  bcp->bc_recycle_head = NULL;
  
  bcp->bc.head = NULL;
  bcp->bc.tail = NULL;
  bcp->bc.recycle_head = NULL;
  bcp->bc.count = 0;
  bcp->bc.recycle_count = 0;
  bcp->bc.length = 0;
  
  bcp->file_head = NULL;
  bcp->file_recycle_head = NULL;
  
  bcp->pool = pool;
  //validate_stuff(bcp);
  return NGX_OK;
}

void nchan_bufchain_pool_flush(nchan_bufchain_pool_t *bcp) {
  nchan_bufchain_link_t      *cur;
  nchan_file_link_t          *fcur, **fhead = &bcp->file_head, **rfhead = &bcp->file_recycle_head;
  //validate_stuff(bcp);
  
  if(bcp->bc.tail) {
    bcp->bc.tail->next = bcp->bc.recycle_head; //link just-used and recycled-in-waiting chains
  }
  bcp->bc.recycle_head = bcp->bc.head;
  bcp->bc.recycle_count += bcp->bc.count;
  bcp->bc.count = 0;
  bcp->bc.length = 0;
  bcp->bc.tail = NULL;
  bcp->bc.head = NULL;
  
  while(bcp->bc_head != NULL) {
    cur = bcp->bc_head;
    bcp->bc_head = cur->next;
    
    cur->next = bcp->bc_recycle_head;
    bcp->bc_recycle_head = cur;
    bcp->bc_count--;
    bcp->bc_recycle_count ++;
  }
  assert(bcp->bc_count == 0);
  //validate_stuff(bcp);
  while(*fhead) {
    fcur = *fhead;
    *fhead = fcur->next;
    
    fcur->next = *rfhead;
    *rfhead = fcur;
    bcp->file_count--;
    bcp->file_recycle_count ++;
  }
  assert(bcp->file_count == 0);
  //validate_stuff(bcp);
  DBG("%p bcs %i (rec. %i), files %i (rec. %i)", bcp, bcp->bc_count, bcp->bc_recycle_count, bcp->file_count, bcp->file_recycle_count);
}
