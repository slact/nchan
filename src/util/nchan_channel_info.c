#include <nchan_module.h>

void nchan_match_channel_info_subtype(size_t off, u_char *cur, size_t rem, u_char **priority, const ngx_str_t **format, ngx_str_t *content_type) {
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
        content_type->len= off + subtypes[i].len;
      }
    }
  }
}

ngx_buf_t                       channel_info_buf;
u_char                          channel_info_buf_str[512]; //big enough
ngx_str_t                       channel_info_content_type;

ngx_buf_t *nchan_channel_info_buf(ngx_str_t *accept_header, ngx_uint_t messages, ngx_uint_t subscribers, time_t last_seen, nchan_msg_id_t *last_msgid, ngx_str_t **generated_content_type) {
  ngx_buf_t                      *b = &channel_info_buf;
  ngx_uint_t                      len;
  const ngx_str_t                *format = &NCHAN_CHANNEL_INFO_PLAIN;
  time_t                          time_elapsed = ngx_time() - last_seen;
  static nchan_msg_id_t           zero_msgid = NCHAN_ZERO_MSGID;
  if(!last_msgid) {
    last_msgid = &zero_msgid;
  }
 
  ngx_memcpy(&channel_info_content_type, &NCHAN_CONTENT_TYPE_TEXT_PLAIN, sizeof(NCHAN_CONTENT_TYPE_TEXT_PLAIN));
  
  b->start = channel_info_buf_str;
  b->pos = b->start;
  b->last_buf = 1;
  b->last_in_chain = 1;
  b->flush = 1;
  b->memory = 1;
  
  if(accept_header) {
    //lame content-negotiation (without regard for qvalues)
    u_char                    *accept = accept_header->data;
    len = accept_header->len;
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
  
  if(len > 512) {
    nchan_log_error("NCHAN: Channel info string too long: max: 512, is: %i", len);
  }
  
  b->last = ngx_snprintf(b->start, 512, (char *)format->data, messages, last_seen==0 ? -1 : (ngx_int_t) time_elapsed, subscribers, msgid_to_str(last_msgid));
  b->end = b->last;
  
  return b;
}

//print information about a channel
ngx_int_t nchan_channel_info(ngx_http_request_t *r, ngx_uint_t messages, ngx_uint_t subscribers, time_t last_seen, nchan_msg_id_t *msgid) {
  ngx_buf_t                      *b;
  ngx_str_t                      *content_type;
  ngx_str_t                      *accept_header = nchan_get_accept_header_value(r);

  b = nchan_channel_info_buf(accept_header, messages, subscribers, last_seen, msgid, &content_type);
  
  return nchan_respond_membuf(r, NGX_HTTP_OK, content_type, b, 0);
}

ngx_int_t nchan_response_channel_ptr_info(nchan_channel_t *channel, ngx_http_request_t *r, ngx_int_t status_code) {
  static const ngx_str_t CREATED_LINE = ngx_string("201 Created");
  static const ngx_str_t ACCEPTED_LINE = ngx_string("202 Accepted");
  
  time_t             last_seen = 0;
  ngx_uint_t         subscribers = 0;
  ngx_uint_t         messages = 0;
  nchan_msg_id_t    *msgid = NULL;
  if(channel!=NULL) {
    subscribers = channel->subscribers;
    last_seen = channel->last_seen;
    messages  = channel->messages;
    msgid = &channel->last_published_msg_id;
    r->headers_out.status = status_code == (ngx_int_t) NULL ? NGX_HTTP_OK : status_code;
    if (status_code == NGX_HTTP_CREATED) {
      ngx_memcpy(&r->headers_out.status_line, &CREATED_LINE, sizeof(ngx_str_t));
    }
    else if (status_code == NGX_HTTP_ACCEPTED) {
      ngx_memcpy(&r->headers_out.status_line, &ACCEPTED_LINE, sizeof(ngx_str_t));
    }
    return nchan_channel_info(r, messages, subscribers, last_seen, msgid);
  }
  else {
    //404!
    return nchan_respond_status(r, NGX_HTTP_NOT_FOUND, NULL, 0);
  }
}
