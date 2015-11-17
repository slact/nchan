/*
 *  Written by Leo Ponomarev 2009-2015
 */

#include <assert.h>
#include <nchan_module.h>

#include <subscribers/longpoll.h>
#include <subscribers/eventsource.h>
#include <subscribers/websocket.h>
#include <store/memory/store.h>
#include <store/redis/store.h>
#include <nchan_setup.c>
#include <store/memory/ipc.h>
#include <store/memory/shmem.h>
#include <store/memory/store-private.h> //for debugging
#include <nchan_output.h>
#include <nchan_websocket_publisher.h>

ngx_int_t           nchan_worker_processes;
ngx_pool_t         *nchan_pool;
ngx_module_t        nchan_module;


//#define DEBUG_LEVEL NGX_LOG_WARN
#define DEBUG_LEVEL NGX_LOG_DEBUG

#define DBG(fmt, args...) ngx_log_error(DEBUG_LEVEL, ngx_cycle->log, 0, "NCHAN(%i):" fmt, memstore_slot(), ##args)
#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "NCHAN(%i):" fmt, memstore_slot(), ##args)


static ngx_int_t nchan_http_publisher_handler(ngx_http_request_t * r);
static ngx_int_t channel_info_callback(ngx_int_t status, void *rptr, ngx_http_request_t *r);
static const ngx_str_t   TEXT_PLAIN = ngx_string("text/plain");

static ngx_int_t validate_id(ngx_http_request_t *r, ngx_str_t *id, nchan_loc_conf_t *cf) {
  if(id->len > cf->max_channel_id_length) {
    ngx_log_error(NGX_LOG_WARN, r->connection->log, 0, "nchan: channel id is too long: should be at most %i, is %i.", cf->max_channel_id_length, id->len);
    return NGX_ERROR;
  }
  return NGX_OK;
}

uint64_t nchan_update_msg_id_multi_tag(uint64_t multitag, uint8_t count, uint8_t n, uint64_t tag) {
  nchan_multi_msg_id_tag_t   mtag;
  mtag.n64 = multitag;
  if (count == 2) {
    mtag.n32[n] = tag;
  }
  else if (count <= 4) {
    mtag.n16[n] = tag;
  }
  else if (count <= 8) {
    mtag.n8[n] = tag;
  }
  else if(count == 1) {
    mtag.n64 = tag;
  }
  else {
    assert(0);
  }
  return mtag.n64;
}

uint64_t nchan_extract_msg_id_multi_tag(nchan_multi_msg_id_tag_t mtag, uint8_t count, uint8_t n) {
  if (count == 2) {
    return (mtag.n32[n] == (uint32_t) -1) ? (uint64_t ) -1 : mtag.n32[n];
  }
  else if (count <= 4) {
    return (mtag.n16[n] == (uint16_t) -1) ? (uint64_t ) -1 : mtag.n16[n];
  }
  else if (count <= 8) {
    return (mtag.n8[n] == (uint8_t) -1) ? (uint64_t ) -1 : mtag.n8[n];
  }
  else if(count == 1) {
    return mtag.n64;
  }
  else {
    assert(0);
  }
}

void nchan_decode_msg_id_multi_tag(uint64_t tag, uint8_t count, uint64_t tag_out[]) {
  nchan_multi_msg_id_tag_t   mtag;
  ngx_int_t                  i;
  mtag.n64 = tag;
  if (count == 2) {
    tag_out[0]= (mtag.n32[0] == (uint32_t) -1) ? (uint64_t ) -1 : mtag.n32[0];
    tag_out[1]= (mtag.n32[1] == (uint32_t) -1) ? (uint64_t ) -1 : mtag.n32[1];
  }
  else if (count <= 4) {
    for(i=0; i<count; i++) {
      tag_out[i]= (mtag.n16[i] == (uint16_t) -1) ? (uint64_t ) -1 : mtag.n16[i];
    }
  }
  else if (count <= 8) {
    for(i=0; i<count; i++) {
      tag_out[i]= (mtag.n8[i] == (uint8_t) -1) ? (uint64_t ) -1 : mtag.n8[i];
    }
  }
  else if(count == 1) {
    tag_out[0]=tag;
  }
  else {
    assert(0);
  }
}

uint64_t nchan_encode_msg_id_multi_tag(uint64_t tag, uint8_t n, uint8_t count, int8_t blankval) {
  nchan_multi_msg_id_tag_t   mtag;
  uint8_t                    i;
  assert(sizeof(mtag) == sizeof(tag));
  assert(n < count);
  if(count == 2) {
    assert(tag < (uint32_t )-1);
    for(i=0; i<count; i++) {
      mtag.n32[i] = (i == n) ? tag : (uint32_t ) blankval;
    }
    return mtag.n64;
  }
  else if(count <= 4) {
    assert(tag < (uint16_t )-1);
    for(i=0; i<count; i++) {
      mtag.n16[i] = (i == n) ? tag : (uint16_t ) blankval;
    }
    return mtag.n64;
  }
  else if (count <= 8) {
    assert(tag < (uint8_t )-1);
    for(i=0; i<count; i++) {
      mtag.n8[i] = (i == n) ? tag : (uint8_t ) blankval;
    }
    return mtag.n64;
  }
  else if(count == 1) {
    return tag;
  }
  else {
    assert(0);
  }
}

static ngx_int_t nchan_process_multi_channel_id(ngx_http_request_t *r, nchan_chid_loc_conf_t *idcf, nchan_loc_conf_t *cf, ngx_str_t **ret_id) {
  ngx_int_t                   i, n;
  ngx_str_t                   id[NCHAN_MEMSTORE_MULTI_MAX];
  ngx_str_t                  *id_out;
  ngx_str_t                  *group = &cf->channel_group;
  size_t                      sz = 0, grouplen = group->len;
  u_char                     *cur;
  
  n = idcf->n;  
  if(n>1) {
    sz += 3 + n; //space for null-separators and "m/<SEP>" prefix for multi-chid
  }
  
  for(i=0; i < n; i++) {
    ngx_http_complex_value(r, idcf->id[i], &id[i]);   
    if(validate_id(r, &id[i], cf) != NGX_OK) {
      *ret_id = NULL;
      return NGX_DECLINED;
    }
    sz += id[i].len;
    sz += 1 + grouplen; // "group/"
  }
  
  if((id_out = ngx_palloc(r->pool, sizeof(*id_out) + sz)) == NULL) {
    ngx_log_error(NGX_LOG_WARN, r->connection->log, 0, "nchan: can't allocate space for channel id");
    *ret_id = NULL;
    return NGX_ERROR;
  }
  id_out->len = sz;
  id_out->data = (u_char *)&id_out[1];
  cur = id_out->data;
  
  if(n > 1) {
    cur[0]='m';
    cur[1]='/';
    cur[2]=NCHAN_MULTI_SEP_CHR;
    cur+=3;
  }
  
  for(i = 0; i < n; i++) {
    ngx_memcpy(cur, group->data, grouplen);
    cur += grouplen;
    cur[0] = '/';
    cur++;
    ngx_memcpy(cur, id[i].data, id[i].len);
    cur += id[i].len;
    if(n>1) {
      cur[0] = NCHAN_MULTI_SEP_CHR;
      cur++;
    }
  }
  *ret_id = id_out;
  return NGX_OK;
}

static ngx_int_t nchan_process_legacy_channel_id(ngx_http_request_t *r, nchan_loc_conf_t *cf, ngx_str_t **ret_id) {
  static ngx_str_t            channel_id_var_name = ngx_string("push_channel_id");
  ngx_uint_t                  key = ngx_hash_key(channel_id_var_name.data, channel_id_var_name.len);
  ngx_http_variable_value_t  *vv = NULL;
  ngx_str_t                  *group = &cf->channel_group;
  ngx_str_t                   tmpid;
  ngx_str_t                  *id;
  size_t                      sz;
  u_char                     *cur;
  
  vv = ngx_http_get_variable(r, &channel_id_var_name, key);
  if (vv == NULL || vv->not_found || vv->len == 0) {
    //ngx_log_error(NGX_LOG_WARN, r->connection->log, 0, "nchan: the legacy $push_channel_id variable is not set");
    return NGX_ABORT;
  }
  else {
    tmpid.len = vv->len;
    tmpid.data = vv->data;
  }
  if(validate_id(r, &tmpid, cf) != NGX_OK) {
    *ret_id = NULL;
    return NGX_DECLINED;
  }
  
  sz = group->len + 1 + tmpid.len;
  if((id = ngx_palloc(r->pool, sizeof(*id) + sz)) == NULL) {
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "nchan: can't allocate space for legacy channel id");
    *ret_id = NULL;
    return NGX_ERROR;
  }
  id->len = sz;
  id->data = (u_char *)&id[1];
  cur = id->data;
  
  ngx_memcpy(cur, group->data, group->len);
  cur += group->len;
  cur[0]='/';
  cur++;
  ngx_memcpy(cur, tmpid.data, tmpid.len);
  
  *ret_id = id;
  return NGX_OK;
}

ngx_str_t *nchan_get_channel_id(ngx_http_request_t *r, pub_or_sub_t what, ngx_int_t fail_hard) {
  static const ngx_str_t          NO_CHANNEL_ID_MESSAGE = ngx_string("No channel id provided.");
  nchan_loc_conf_t               *cf = ngx_http_get_module_loc_conf(r, nchan_module);
  ngx_int_t                       rc;
  ngx_str_t                      *id;
  nchan_chid_loc_conf_t          *chid_conf;
  
  chid_conf = what == PUB ? &cf->pub_chid : &cf->sub_chid;
  if(chid_conf->n == 0) {
    chid_conf = &cf->pubsub_chid;
  }
  
  if(chid_conf->n > 0) {
    rc = nchan_process_multi_channel_id(r, chid_conf, cf, &id);
  }
  else {
    //fallback to legacy $push_channel_id
    rc = nchan_process_legacy_channel_id(r, cf, &id);
  }
  
  if(id == NULL && fail_hard) {
    assert(rc != NGX_OK);
    switch(rc) {
      case NGX_ERROR:
        nchan_respond_status(r, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, 0);
        break;
      
      case NGX_DECLINED:
        nchan_respond_status(r, NGX_HTTP_FORBIDDEN, NULL, 0);
        break;
      
      case NGX_ABORT:
        nchan_respond_string(r, NGX_HTTP_NOT_FOUND, &TEXT_PLAIN, &NO_CHANNEL_ID_MESSAGE, 0);
        break;
    }
    DBG("%s channel id NULL", what == PUB ? "pub" : "sub");
  }
  else {
    DBG("%s channel id %V", what == PUB ? "pub" : "sub", id);
  }
  
  return id;
}

ngx_str_t * nchan_get_header_value(ngx_http_request_t * r, ngx_str_t header_name) {
  ngx_uint_t                       i;
  ngx_list_part_t                 *part = &r->headers_in.headers.part;
  ngx_table_elt_t                 *header= part->elts;
  
  for (i = 0; /* void */ ; i++) {
    if (i >= part->nelts) {
      if (part->next == NULL) {
        break;
      }
      part = part->next;
      header = part->elts;
      i = 0;
    }
    if (header[i].key.len == header_name.len
      && ngx_strncasecmp(header[i].key.data, header_name.data, header[i].key.len) == 0) {
      return &header[i].value;
      }
  }
  return NULL;
}

static ngx_int_t nchan_detect_websocket_handshake(ngx_http_request_t *r) {
  ngx_str_t       *tmp;
  
  if(r->method != NGX_HTTP_GET) {
    return 0;
  }
  
  if((tmp = nchan_get_header_value(r, NCHAN_HEADER_CONNECTION))) {
    if(ngx_strncasecmp(tmp->data, NCHAN_UPGRADE.data, NCHAN_UPGRADE.len) != 0) return 0;
  }
  else return 0;
  
  if((tmp = nchan_get_header_value(r, NCHAN_HEADER_UPGRADE))) {
    if(ngx_strncasecmp(tmp->data, NCHAN_WEBSOCKET.data, NCHAN_WEBSOCKET.len) != 0) return 0;
  }
  else return 0;

  return 1;
}

static ngx_int_t nchan_detect_eventsource_request(ngx_http_request_t *r) {
  ngx_str_t       *accept_header;
  if(r->headers_in.accept == NULL) {
    return 0;
  }
  accept_header = &r->headers_in.accept->value;

  if(ngx_strnstr(accept_header->data, "text/event-stream", accept_header->len)) {
    return 1;
  }
  
  return 0;
}

ngx_str_t * nchan_subscriber_get_etag(ngx_http_request_t * r) {
  ngx_uint_t                       i;
  ngx_list_part_t                 *part = &r->headers_in.headers.part;
  ngx_table_elt_t                 *header= part->elts;
  
  for (i = 0; /* void */ ; i++) {
    if (i >= part->nelts) {
      if (part->next == NULL) {
        break;
      }
      part = part->next;
      header = part->elts;
      i = 0;
    }
    if (header[i].key.len == NCHAN_HEADER_IF_NONE_MATCH.len
      && ngx_strncasecmp(header[i].key.data, NCHAN_HEADER_IF_NONE_MATCH.data, header[i].key.len) == 0) {
      return &header[i].value;
      }
  }
  return NULL;
}

ngx_int_t nchan_subscriber_get_msg_id(ngx_http_request_t *r, nchan_msg_id_t *id) {
  static ngx_str_t                last_event_id_header = ngx_string("Last-Event-ID");
  ngx_str_t                      *last_event_id;
  ngx_str_t                      *if_none_match;
  char                           *strtoull_last;
  
  if((last_event_id = nchan_get_header_value(r, last_event_id_header)) != NULL) {
    u_char       *split, *last;
    ngx_int_t     time;
    //"<msg_time>:<msg_tag>"
    last = last_event_id->data + last_event_id->len;
    if((split = ngx_strlchr(last_event_id->data, last, ':')) != NULL) {
      time = ngx_atoi(last_event_id->data, split - last_event_id->data);
      split++;
      strtoull_last = (char *)last;
      if(time != NGX_ERROR) {
        id->time = time;
        id->tag = strtoull((const char *)split, &strtoull_last, 10);
        return NGX_OK;
      }
    }
  }
  
  if_none_match = nchan_subscriber_get_etag(r);
  id->time=(r->headers_in.if_modified_since == NULL) ? 0 : ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
  if(if_none_match==NULL) {
    id->tag=0;
  }
  else {
    strtoull_last = (char *)(if_none_match->data + if_none_match->len);
    id->tag = strtoull((const char *)if_none_match->data, &strtoull_last, 10);
  }
  return NGX_OK;
}


static void nchan_match_channel_info_subtype(size_t off, u_char *cur, size_t rem, u_char **priority, const ngx_str_t **format, ngx_str_t *content_type) {
  static nchan_content_subtype_t subtypes[] = {
    { "json"  , 4, &NCHAN_CHANNEL_INFO_JSON },
    { "yaml"  , 4, &NCHAN_CHANNEL_INFO_YAML },
    { "xml"   , 3, &NCHAN_CHANNEL_INFO_XML  },
    { "x-json", 6, &NCHAN_CHANNEL_INFO_JSON },
    { "x-yaml", 6, &NCHAN_CHANNEL_INFO_YAML }
  };
  u_char                         *start = cur + off;
  ngx_uint_t                      i;
  
  for(i=0; i<(sizeof(subtypes)/sizeof(nchan_content_subtype_t)); i++) {
    if(ngx_strncmp(start, subtypes[i].subtype, rem<subtypes[i].len ? rem : subtypes[i].len)==0) {
      if(*priority>start) {
        *format = subtypes[i].format;
        *priority = start;
        content_type->data=cur;
        content_type->len= off + 1 + subtypes[i].len;
      }
    }
  }
}

ngx_buf_t                       channel_info_buf;
u_char                          channel_info_buf_str[512]; //big enough
ngx_str_t                       channel_info_content_type;
ngx_buf_t *nchan_channel_info_buf(ngx_str_t *accept_header, ngx_uint_t messages, ngx_uint_t subscribers, time_t last_seen, ngx_str_t **generated_content_type) {
  ngx_buf_t                      *b = &channel_info_buf;
  ngx_uint_t                      len;
  const ngx_str_t                *format = &NCHAN_CHANNEL_INFO_PLAIN;
  time_t                          time_elapsed = ngx_time() - last_seen;
 
  ngx_memcpy(&channel_info_content_type, &TEXT_PLAIN, sizeof(TEXT_PLAIN));;
  
  b->start = channel_info_buf_str;
  b->pos = b->start;
  b->last_buf = 1;
  b->last_in_chain = 1;
  b->flush = 1;
  b->memory = 1;
  
  if(accept_header) {
    //lame content-negotiation (without regard for qvalues)
    u_char                    *accept = accept_header->data;
    size_t                     len = accept_header->len;
    size_t                     rem;
    u_char                    *cur = accept;
    u_char                    *priority=&accept[len-1];
    
    for(rem=len; (cur = ngx_strnstr(cur, "text/", rem))!=NULL; cur += sizeof("text/")-1) {
      rem=len - ((size_t)(cur-accept)+sizeof("text/")-1);
      if(ngx_strncmp(cur+sizeof("text/")-1, "plain", rem<5 ? rem : 5)==0) {
        if(priority) {
          format = &NCHAN_CHANNEL_INFO_PLAIN;
          priority = cur+sizeof("text/")-1;
          //content-type is already set by default
        }
      }
      nchan_match_channel_info_subtype(sizeof("text/")-1, cur, rem, &priority, &format, &channel_info_content_type);
    }
    cur = accept;
    for(rem=len; (cur = ngx_strnstr(cur, "application/", rem))!=NULL; cur += sizeof("application/")-1) {
      rem=len - ((size_t)(cur-accept)+sizeof("application/")-1);
      nchan_match_channel_info_subtype(sizeof("application/")-1, cur, rem, &priority, &format, &channel_info_content_type);
    }
  }
  
  if(generated_content_type) {
    *generated_content_type = &channel_info_content_type;
  }
  
  len = format->len - 8 - 1 + 3*NGX_INT_T_LEN; //minus 8 sprintf
  
  assert(len < 512);
  
  b->last = ngx_sprintf(b->start, (char *)format->data, messages, last_seen==0 ? -1 : (ngx_int_t) time_elapsed, subscribers);
  b->end = b->last;
  
  return b;
}

//print information about a channel
static ngx_int_t nchan_channel_info(ngx_http_request_t *r, ngx_uint_t messages, ngx_uint_t subscribers, time_t last_seen) {
  ngx_buf_t                      *b;
  ngx_str_t                      *content_type;
  ngx_str_t                      *accept_header = NULL;
  
  if(r->headers_in.accept) {
    accept_header = &r->headers_in.accept->value;
  }
  
  b = nchan_channel_info_buf(accept_header, messages, subscribers, last_seen, &content_type);
  
  //not sure why this is needed, but content-type directly from the request can't be reliably used in the response 
  //(it probably can, but i'm just doing it wrong)
  /*if(content_type != &TEXT_PLAIN) {
    ERR("WTF why must i do this %p %V", content_type, content_type);
    content_type_copy.len = content_type->len;
    content_type_copy.data = ngx_palloc(r->pool, content_type_copy.len);
    assert(content_type_copy.data);
    ngx_memcpy(content_type_copy.data, content_type->data, content_type_copy.len);
    content_type = &content_type_copy;
  }*/
  
  return nchan_respond_membuf(r, NGX_HTTP_OK, content_type, b, 0);
}

// this function adapted from push stream module. thanks Wandenberg Peixoto <wandenberg@gmail.com> and Rogério Carvalho Schneider <stockrt@gmail.com>
static ngx_buf_t * nchan_request_body_to_single_buffer(ngx_http_request_t *r) {
  ngx_buf_t *buf = NULL;
  ngx_chain_t *chain;
  ssize_t n;
  off_t len;

  chain = r->request_body->bufs;
  if (chain->next == NULL) {
    return chain->buf;
  }
  //ngx_log_error(NGX_LOG_WARN, ngx_cycle->log, 0, "nchan: multiple buffers in request, need memcpy :(");
  if (chain->buf->in_file) {
    if (ngx_buf_in_memory(chain->buf)) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "nchan: can't handle a buffer in a temp file and in memory ");
    }
    if (chain->next != NULL) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "nchan: error reading request body with multiple ");
    }
    return chain->buf;
  }
  buf = ngx_create_temp_buf(r->pool, r->headers_in.content_length_n + 1);
  if (buf != NULL) {
    ngx_memset(buf->start, '\0', r->headers_in.content_length_n + 1);
    while ((chain != NULL) && (chain->buf != NULL)) {
      len = ngx_buf_size(chain->buf);
      // if buffer is equal to content length all the content is in this buffer
      if (len >= r->headers_in.content_length_n) {
        buf->start = buf->pos;
        buf->last = buf->pos;
        len = r->headers_in.content_length_n;
      }
      if (chain->buf->in_file) {
        n = ngx_read_file(chain->buf->file, buf->start, len, 0);
        if (n == NGX_FILE_ERROR) {
          ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "nchan: cannot read file with request body");
          return NULL;
        }
        buf->last = buf->last + len;
        ngx_delete_file(chain->buf->file->name.data);
        chain->buf->file->fd = NGX_INVALID_FILE;
      } else {
        buf->last = ngx_copy(buf->start, chain->buf->pos, len);
      }

      chain = chain->next;
      buf->start = buf->last;
    }
    buf->last_buf = 1;
  }
  return buf;
}

static ngx_int_t nchan_response_channel_ptr_info(nchan_channel_t *channel, ngx_http_request_t *r, ngx_int_t status_code) {
  static const ngx_str_t CREATED_LINE = ngx_string("201 Created");
  static const ngx_str_t ACCEPTED_LINE = ngx_string("202 Accepted");
  
  time_t             last_seen = 0;
  ngx_uint_t         subscribers = 0;
  ngx_uint_t         messages = 0;
  if(channel!=NULL) {
    subscribers = channel->subscribers;
    last_seen = channel->last_seen;
    messages  = channel->messages;
    r->headers_out.status = status_code == (ngx_int_t) NULL ? NGX_HTTP_OK : status_code;
    if (status_code == NGX_HTTP_CREATED) {
      ngx_memcpy(&r->headers_out.status_line, &CREATED_LINE, sizeof(ngx_str_t));
    }
    else if (status_code == NGX_HTTP_ACCEPTED) {
      ngx_memcpy(&r->headers_out.status_line, &ACCEPTED_LINE, sizeof(ngx_str_t));
    }
    nchan_channel_info(r, messages, subscribers, last_seen);
  }
  else {
    //404!
    nchan_respond_status(r, NGX_HTTP_NOT_FOUND, NULL, 0);
  }
  return NGX_OK;
}

static ngx_int_t subscribe_longpoll_callback(ngx_int_t status, void *_, ngx_http_request_t *r) {
  return NGX_OK;
}
static ngx_int_t subscribe_eventsource_callback(ngx_int_t status, void *_, ngx_http_request_t *r) {
  return NGX_OK;
}
static ngx_int_t subscribe_websocket_callback(ngx_int_t status, void *_, ngx_http_request_t *r) {
  return NGX_OK;
}

static ngx_int_t subscribe_intervalpoll_callback(nchan_msg_status_t msg_search_outcome, nchan_msg_t *msg, ngx_http_request_t *r) {
  //inefficient, but close enough for now
  ngx_str_t               *etag;
  char                    *err;
  switch(msg_search_outcome) {
    case MSG_EXPECTED:
      //interval-polling subscriber requests get a 304 with their entity tags preserved.
      if (r->headers_in.if_modified_since != NULL) {
        r->headers_out.last_modified_time=ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
      }
      if ((etag=nchan_subscriber_get_etag(r)) != NULL) {
        nchan_add_response_header(r, &NCHAN_HEADER_ETAG, etag);
      }
      nchan_respond_status(r, NGX_HTTP_NOT_MODIFIED, NULL, 1);
      break;
      
    case MSG_FOUND:
      if(nchan_respond_msg(r, msg, 1, &err) != NGX_OK) {
        nchan_respond_cstring(r, NGX_HTTP_INTERNAL_SERVER_ERROR, &TEXT_PLAIN, err, 1);
      }
      break;
      
    case MSG_NOTFOUND:
    case MSG_EXPIRED:
      nchan_respond_status(r, NGX_HTTP_NOT_FOUND, NULL, 1);
      break;
      
    default:
      nchan_respond_status(r, NGX_HTTP_INTERNAL_SERVER_ERROR, NULL, 1);
      return NGX_ERROR;
  }
  return NGX_DONE;
}

static void memstore_sub_debug_start() {
#if FAKESHARD  
  #ifdef SUB_FAKE_WORKER
  memstore_fakeprocess_push(SUB_FAKE_WORKER);
  #else
  memstore_fakeprocess_push_random();
  #endif
#endif   
}
static void memstore_sub_debug_end() {
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}

static void memstore_pub_debug_start() {
#if FAKESHARD
  #ifdef PUB_FAKE_WORKER
  memstore_fakeprocess_push(PUB_FAKE_WORKER);
  #else
  memstore_fakeprocess_push_random();
  #endif
#endif
}
static void memstore_pub_debug_end() {
#if FAKESHARD
  memstore_fakeprocess_pop();
#endif
}

ngx_int_t nchan_pubsub_handler(ngx_http_request_t *r) {
  nchan_loc_conf_t       *cf = ngx_http_get_module_loc_conf(r, nchan_module);
  ngx_str_t              *channel_id;
  subscriber_t           *sub;
  nchan_msg_id_t          msg_id;
  ngx_int_t               rc = NGX_DONE;
  
  if((channel_id = nchan_get_channel_id(r, SUB, 1)) == NULL) {
    //just get the subscriber_channel_id for now. the publisher one is handled elsewhere
    return r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  if(nchan_detect_websocket_handshake(r)) {
    //want websocket?
    if(cf->sub.websocket) {
      //we prefer to subscribe
      memstore_sub_debug_start();
      nchan_subscriber_get_msg_id(r, &msg_id);
      if((sub = websocket_subscriber_create(r, &msg_id)) == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "unable to create websocket subscriber");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
      }
      cf->storage_engine->subscribe(channel_id, &msg_id, sub, (callback_pt )&subscribe_websocket_callback, (void *)r);
      
      memstore_sub_debug_end();
    }
    else if(cf->pub.websocket) {
      //no need to subscribe, but keep a connection open for publishing
      //not yet implemented
      nchan_create_websocket_publisher(r);
    }
    else goto forbidden;
    return NGX_DONE;
  }
  else {
    switch(r->method) {
      case NGX_HTTP_GET:
        if(cf->sub.eventsource && nchan_detect_eventsource_request(r)) {
          memstore_sub_debug_start();
          
          nchan_subscriber_get_msg_id(r, &msg_id);
          if((sub = eventsource_subscriber_create(r, &msg_id)) == NULL) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "unable to create longpoll subscriber");
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
          }
          cf->storage_engine->subscribe(channel_id, &msg_id, sub, (callback_pt )&subscribe_eventsource_callback, (void *)r);
          
          memstore_sub_debug_end();
        }
        else if(cf->sub.poll) {
          memstore_sub_debug_start();
          
          nchan_subscriber_get_msg_id(r, &msg_id);
          r->main->count++;
          cf->storage_engine->get_message(channel_id, &msg_id, (callback_pt )&subscribe_intervalpoll_callback, (void *)r);
          memstore_sub_debug_end();
        }
        else if(cf->sub.longpoll) {
          memstore_sub_debug_start();
          
          nchan_subscriber_get_msg_id(r, &msg_id);
          if((sub = longpoll_subscriber_create(r, &msg_id)) == NULL) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "unable to create longpoll subscriber");
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
          }
          cf->storage_engine->subscribe(channel_id, &msg_id, sub, (callback_pt )&subscribe_longpoll_callback, (void *)r);
          
          memstore_sub_debug_end();
        }
        else if(cf->pub.http) {
          nchan_http_publisher_handler(r);
        }
        else goto forbidden;
        break;
      
      case NGX_HTTP_POST:
      case NGX_HTTP_PUT:
        if(cf->pub.http) {
          nchan_http_publisher_handler(r);
        }
        else goto forbidden;
        break;
      
      case NGX_HTTP_DELETE:
        if(cf->pub.http) {
          nchan_http_publisher_handler(r);
        }
        else goto forbidden;
        break;
      
      case NGX_HTTP_OPTIONS:
        if(cf->pub.http) {
          nchan_OPTIONS_respond(r, &NCHAN_ANYSTRING, &NCHAN_ACCESS_CONTROL_ALLOWED_PUBLISHER_HEADERS, &NCHAN_ALLOW_GET_POST_PUT_DELETE_OPTIONS);
        }
        else if(cf->sub.poll || cf->sub.longpoll || cf->sub.eventsource || cf->sub.websocket) {
          nchan_OPTIONS_respond(r, &NCHAN_ANYSTRING, &NCHAN_ACCESS_CONTROL_ALLOWED_SUBSCRIBER_HEADERS, &NCHAN_ALLOW_GET_OPTIONS);
        }
        else goto forbidden;
        break;
    }
  }
  
  return rc;
  
forbidden:
  nchan_respond_status(r, NGX_HTTP_FORBIDDEN, NULL, 0);
  return NGX_OK;
}

static ngx_int_t channel_info_callback(ngx_int_t status, void *rptr, ngx_http_request_t *r) {
  ngx_http_finalize_request(r, nchan_response_channel_ptr_info( (nchan_channel_t *)rptr, r, 0));
  return NGX_OK;
}

static ngx_int_t publish_callback(ngx_int_t status, void *rptr, ngx_http_request_t *r) {
  nchan_channel_t       *ch = rptr;
    
  //DBG("publish_callback %V owner %i status %i", ch_id, memstore_channel_owner(ch_id), status);
  switch(status) {
    case NCHAN_MESSAGE_QUEUED:
      //message was queued successfully, but there were no subscribers to receive it.
      ngx_http_finalize_request(r, nchan_response_channel_ptr_info(ch, r, NGX_HTTP_ACCEPTED));
      return NGX_OK;
      
    case NCHAN_MESSAGE_RECEIVED:
      //message was queued successfully, and it was already sent to at least one subscriber
      ngx_http_finalize_request(r, nchan_response_channel_ptr_info(ch, r, NGX_HTTP_CREATED));
      return NGX_OK;
      
    case NGX_ERROR:
    case NGX_HTTP_INTERNAL_SERVER_ERROR:
      //WTF?
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "nchan: error publishing message");
      ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return NGX_ERROR;
      
    default:
      //for debugging, mostly. I don't expect this branch to behit during regular operation
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "nchan: TOTALLY UNEXPECTED error publishing message, status code %i", status);
      ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
      return NGX_ERROR;
  }
}

#define NGX_REQUEST_VAL_CHECK(val, fail, r, errormessage)                 \
if (val == fail) {                                                        \
  ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, errormessage);      \
  ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);           \
  return;                                                                 \
  }

static void nchan_publisher_body_handler(ngx_http_request_t * r) {
  ngx_str_t                      *channel_id;
  nchan_loc_conf_t               *cf = ngx_http_get_module_loc_conf(r, nchan_module);
  ngx_buf_t                      *buf;
  size_t                          content_type_len;
  nchan_msg_t                    *msg;
  struct timeval                  tv;
  
  if((channel_id = nchan_get_channel_id(r, PUB, 1))==NULL) {
    ngx_http_finalize_request(r, r->headers_out.status ? NGX_OK : NGX_HTTP_INTERNAL_SERVER_ERROR);
    return;
  }
  
  switch(r->method) {
    case NGX_HTTP_GET:
      cf->storage_engine->find_channel(channel_id, (callback_pt) &channel_info_callback, (void *)r);
      break;
    
    case NGX_HTTP_PUT:
    case NGX_HTTP_POST:
      memstore_pub_debug_start();
      
      msg = ngx_pcalloc(r->pool, sizeof(*msg));
      msg->shared = 0;
      NGX_REQUEST_VAL_CHECK(msg, NULL, r, "nchan: can't allocate msg in request pool");
      //buf = ngx_create_temp_buf(r->pool, 0);
      //NGX_REQUEST_VAL_CHECK(buf, NULL, r, "nchan: can't allocate buf in request pool");
      
      //content type
      content_type_len = (r->headers_in.content_type!=NULL ? r->headers_in.content_type->value.len : 0);
      if(content_type_len > 0) {
        msg->content_type.len = content_type_len;
        msg->content_type.data = r->headers_in.content_type->value.data;
      }
      
      if(r->headers_in.content_length_n == -1 || r->headers_in.content_length_n == 0) {
        buf = ngx_create_temp_buf(r->pool, 0);
      }
      else if(r->request_body->bufs!=NULL) {
        buf = nchan_request_body_to_single_buffer(r);
      }
      else {
        ngx_log_error(NGX_LOG_ERR, (r)->connection->log, 0, "nchan: unexpected publisher message request body buffer location. please report this to the nchan developers.");
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
      }
      
      ngx_gettimeofday(&tv);
      msg->id.time = tv.tv_sec;
      
      msg->buf = buf;
#if NCHAN_MSG_LEAK_DEBUG
      msg->lbl = r->uri;
#endif      
      cf->storage_engine->publish(channel_id, msg, cf, (callback_pt) &publish_callback, r);
      
      memstore_pub_debug_end();
      break;
      
    case NGX_HTTP_DELETE:
      cf->storage_engine->delete_channel(channel_id, (callback_pt) &channel_info_callback, (void *)r);
      break;
      
    default: 
      nchan_respond_status(r, NGX_HTTP_FORBIDDEN, NULL, 0);
  }
}


static ngx_int_t nchan_http_publisher_handler(ngx_http_request_t * r) {
  ngx_int_t                       rc;
  
  /* Instruct ngx_http_read_subscriber_request_body to store the request
     body entirely in a memory buffer or in a file */
  r->request_body_in_single_buf = 1;
  r->request_body_in_persistent_file = 1;
  r->request_body_in_clean_file = 0;
  r->request_body_file_log_level = 0;
  
  //don't buffer the request body --send it right on through
  //r->request_body_no_buffering = 1;

  rc = ngx_http_read_client_request_body(r, nchan_publisher_body_handler);
  if (rc >= NGX_HTTP_SPECIAL_RESPONSE) {
    return rc;
  }
  return NGX_DONE;
}


static ngx_int_t *verify_msg_id(nchan_msg_id_t *id1, nchan_msg_id_t *id2, uint8_t multi) {
  if(id1->time > 0 && id2->time > 0) {
    assert(id1->time == id2->time);
    if(multi == 0) {
      //TODO: do this better
      assert(id1->tag == id2->tag);
    }
  }
  return NGX_OK;
}

ngx_int_t *verify_subscriber_last_msg_id(subscriber_t *sub, nchan_msg_t *msg) {
  if(msg) {
    verify_msg_id(&sub->last_msg_id, &msg->prev_id, msg->multi);
    sub->last_msg_id = msg->id;
  }
  
  return NGX_OK;
}

#if NCHAN_SUBSCRIBER_LEAK_DEBUG

subscriber_t *subdebug_head = NULL;

void subscriber_debug_add(subscriber_t *sub) {
  if(subdebug_head == NULL) {
    sub->dbg_next = NULL;
    sub->dbg_prev = NULL;
  }
  else {
    sub->dbg_next = subdebug_head;
    sub->dbg_prev = NULL;
    assert(subdebug_head->dbg_prev == NULL);
    subdebug_head->dbg_prev = sub;
  }
  subdebug_head = sub;
}
void subscriber_debug_remove(subscriber_t *sub) {
  subscriber_t *prev, *next;
  prev = sub->dbg_prev;
  next = sub->dbg_next;
  if(subdebug_head == sub) {
    assert(sub->dbg_prev == NULL);
    if(next) {
      next->dbg_prev = NULL;
    }
    subdebug_head = next;
  }
  else {
    if(prev) {
      prev->dbg_next = next;
    }
    if(next) {
      next->dbg_prev = prev;
    }
  }
  
  sub->dbg_next = NULL;
  sub->dbg_prev = NULL;
}
void subscriber_debug_assert_isempty(void) {
  assert(subdebug_head == NULL);
}
#endif


#if NCHAN_MSG_LEAK_DEBUG

nchan_msg_t *msgdebug_head = NULL;

void msg_debug_add(nchan_msg_t *msg) {
  //ensure this message is present only once
  nchan_msg_t      *cur;
  for(cur = msgdebug_head; cur != NULL; cur = cur->dbg_next) {
    assert(cur != msg);
  }
  
  if(msgdebug_head == NULL) {
    msg->dbg_next = NULL;
    msg->dbg_prev = NULL;
  }
  else {
    msg->dbg_next = msgdebug_head;
    msg->dbg_prev = NULL;
    assert(msgdebug_head->dbg_prev == NULL);
    msgdebug_head->dbg_prev = msg;
  }
  msgdebug_head = msg;
}
void msg_debug_remove(nchan_msg_t *msg) {
  nchan_msg_t *prev, *next;
  prev = msg->dbg_prev;
  next = msg->dbg_next;
  if(msgdebug_head == msg) {
    assert(msg->dbg_prev == NULL);
    if(next) {
      next->dbg_prev = NULL;
    }
    msgdebug_head = next;
  }
  else {
    if(prev) {
      prev->dbg_next = next;
    }
    if(next) {
      next->dbg_prev = prev;
    }
  }
  
  msg->dbg_next = NULL;
  msg->dbg_prev = NULL;
}
void msg_debug_assert_isempty(void) {
  assert(msgdebug_head == NULL);
}
#endif
