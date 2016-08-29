#include <nchan_module.h>
#include <assert.h>

#define ERR(fmt, args...) ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "NCHAN MSG_ID:" fmt, ##args)

void nchan_expand_msg_id_multi_tag(nchan_msg_id_t *id, uint8_t in_n, uint8_t out_n, int16_t fill) {
  int16_t v, n = id->tagcount;
  int16_t *tags = n <= NCHAN_FIXED_MULTITAG_MAX ? id->tag.fixed : id->tag.allocd;
  uint8_t i;
  assert(n > in_n && n > out_n);
  v = tags[in_n];
  
  for(i=0; i < n; i++) {
    tags[i] = (i == out_n) ? v : fill;
  }
}

void nchan_expand_tiny_msgid(nchan_msg_tiny_id_t *tinyid, nchan_msg_id_t *id) {
  id->time = tinyid->time;
  id->tag.fixed[0]=tinyid->tag;
  id->tagcount = 1;
  id->tagactive = 1;
}

void nchan_shrink_normal_msgid(nchan_msg_id_t *id, nchan_msg_tiny_id_t *tinyid) {
  assert(id->tagcount <= 1);
  tinyid->time = id->time;
  tinyid->tag = id->tag.fixed[0];
}

ngx_int_t nchan_copy_new_msg_id(nchan_msg_id_t *dst, nchan_msg_id_t *src) {
  ngx_memcpy(dst, src, sizeof(*src));
  if(src->tagcount > NCHAN_FIXED_MULTITAG_MAX) {
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
  
  if(dst_n > NCHAN_FIXED_MULTITAG_MAX && dst_n != src_n) {
    ngx_free(dst->tag.allocd);
    dst_n = NCHAN_FIXED_MULTITAG_MAX;
  }
  
  dst->tagcount = src->tagcount;
  dst->tagactive = src->tagactive;
  
  if(src_n <= NCHAN_FIXED_MULTITAG_MAX) {
    dst->tag = src->tag;
  }
  else {
    if(dst_n != src_n) {
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
  if(id->tagcount > NCHAN_FIXED_MULTITAG_MAX) {
    ngx_free(id->tag.allocd);
    id->tag.allocd = NULL;
  }
  return NGX_OK;
}

static ngx_int_t verify_msg_id(nchan_msg_id_t *id1, nchan_msg_id_t *id2, nchan_msg_id_t *msgid, char **err) {
  int16_t  *tags1 = id1->tagcount <= NCHAN_FIXED_MULTITAG_MAX ? id1->tag.fixed : id1->tag.allocd;
  int16_t  *tags2 = id2->tagcount <= NCHAN_FIXED_MULTITAG_MAX ? id2->tag.fixed : id2->tag.allocd;
  if(id1->time > 0 && id2->time > 0) {
    if(id1->time != id2->time) {
      //is this a missed message, or just a multi msg?
      
      if(id2->tagcount > 1) {
        int       i = -1, j, max = id2->tagcount;  
        int16_t  *msgidtags = msgid->tagcount <= NCHAN_FIXED_MULTITAG_MAX ? msgid->tag.fixed : msgid->tag.allocd;
        
        for(j=0; j < max; j++) {
          if(tags2[j] != -1) {
            if( i != -1) {
              *err = "more than one tag set to something besides -1. that means this isn't a single channel's forwarded multi msg";
              return NGX_ERROR;
            }
            else {
              i = j;
            }
          }
        }
        if(msgidtags[i] != 0) {
          *err = "only the first message in a given second is ok. anything else means a missed message.";
          return NGX_ERROR;
        }
        //ok, it's just the first-per-second message of a channel from a multi-channel
        //this is a rather convoluted description... but basically this is ok.
        return NGX_OK;
      }
      else {
        *err = "previous message id times don't match";
        return NGX_ERROR;
      }
    }
    
    if(id1->tagcount == 1) {
      if(tags1[0] != tags2[0]){
        *err = "previous message id tags don't match";
        return NGX_ERROR;
      }
    }
    else {
      int   i, max = id1->tagcount;
      for(i=0; i < max; i++) {
        if(tags2[i] != -1 && tags1[i] != tags2[i]) {
          *err = "previous message multitag mismatch";
          return NGX_ERROR;
        }
      }
    }
  }
  return NGX_OK;
}

void nchan_update_multi_msgid(nchan_msg_id_t *oldid, nchan_msg_id_t *newid, int16_t *largetags) {
  if(newid->tagcount == 1) {
    //nice and simple
    *oldid = *newid;
  }
  else {
    //DBG("======= updating multi_msgid ======");
    //DBG("======= old: %V", msgid_to_str(oldid));
    //DBG("======= new: %V", msgid_to_str(newid));
    uint16_t         newcount = newid->tagcount, oldcount = oldid->tagcount;
    if(newcount > NCHAN_FIXED_MULTITAG_MAX && oldcount < newcount) {
      int16_t       *oldtags, *old_largetags = NULL;
      int            i;
      size_t         sz = sizeof(*oldid->tag.allocd) * newcount;
      if(oldcount > NCHAN_FIXED_MULTITAG_MAX) {
        old_largetags = oldid->tag.allocd;
        oldtags = old_largetags;
      }
      else {
        oldtags = oldid->tag.fixed;
      }
      if(largetags == NULL) {
        largetags = ngx_alloc(sz, ngx_cycle->log);
      }
      oldid->tag.allocd = largetags;
      for(i=0; i < newcount; i++) {
        oldid->tag.allocd[i] = (i < oldcount) ? oldtags[i] : -1;
      }
      if(old_largetags) {
        ngx_free(old_largetags);
      }
      oldid->tagcount = newcount;
    }
    
    if(oldid->time != newid->time) {
      nchan_copy_msg_id(oldid, newid, NULL);
    }
    else {
      int i, max = newcount;
      int16_t  *oldtags = oldcount <= NCHAN_FIXED_MULTITAG_MAX ? oldid->tag.fixed : oldid->tag.allocd;
      int16_t  *newtags = oldcount <= NCHAN_FIXED_MULTITAG_MAX ? newid->tag.fixed : newid->tag.allocd;
      
      assert(max == oldcount);
      
      for(i=0; i< max; i++) {
        
        //DEBUG CHECK -- REMOVE BEFORE RELEASE
        if(newid->tagactive == i && newtags[i] != -1 && oldtags[i] != -1) {
          assert(newtags[i] > oldtags[i]);
        }
        
        
        if (newtags[i] != -1) {
          oldtags[i] = newtags[i];
        }
      }
      oldid->tagactive = newid->tagactive;
    }
    //DBG("=== updated: %V", msgid_to_str(oldid));
  }
}

ngx_int_t update_subscriber_last_msg_id(subscriber_t *sub, nchan_msg_t *msg) {
  if(msg) {
    char *err, *huh;
    if(verify_msg_id(&sub->last_msgid, &msg->prev_id, &msg->id, &err) == NGX_ERROR) {
      struct timeval    tv;
      time_t            time;
      int               ttl = msg->expires - msg->id.time;
      ngx_gettimeofday(&tv);
      time = tv.tv_sec;
      
      if(sub->last_msgid.time + ttl <= time) {
        huh = "The message probably expired.";
      }
      else {
        huh = "Try increasing the message buffer length.";
      }
      
      if(sub->type == INTERNAL) {
        nchan_log_warning("Missed message for internal %V subscriber: %s. %s", sub->name, err, huh);
      }
      else {
        nchan_log_request_warning(sub->request, "Missed message for %V subscriber: %s. %s", sub->name, err, huh);
      }
    }
    
    nchan_update_multi_msgid(&sub->last_msgid, &msg->id, NULL);
  }
  
  return NGX_OK;
}






static ngx_int_t nchan_parse_msg_tag(u_char *first, u_char *last, nchan_msg_id_t *mid, ngx_int_t expected_tag_count) {
  u_char           *cur = first;
  u_char            c;
  int16_t           i = 0;
  int8_t            sign = 1;
  int16_t           val = 0;
  static int16_t    tags[NCHAN_MULTITAG_MAX];
  
  while(cur <= last && i < NCHAN_MULTITAG_MAX) {
    if(cur == last) {
      tags[i]=(val == 0 && sign == -1) ? -1 : val * sign; //shorthand "-" for "-1";
      i++;
      break;
    }
    
    c = *cur;
    if(c == '-') {
      sign = -1;
    }
    else if (c >= '0' && c <= '9') {
      val = 10 * val + (c - '0');
    }
    else if (c == '[') {
      mid->tagactive = i;
    }
    else if (c == ',') {
      tags[i]=(val == 0 && sign == -1) ? -1 : val * sign; //shorthand "-" for "-1"
      sign=1;
      val=0;
      i++;
    }
    cur++;
  }
  if(expected_tag_count > i) {
    return NGX_ERROR;
  }
  mid->tagcount = i;
  
  if(i <= NCHAN_FIXED_MULTITAG_MAX) {
    ngx_memcpy(mid->tag.fixed, tags, sizeof(mid->tag.fixed));
  }
  else {
    mid->tag.allocd = tags;
  }
  return NGX_OK;
}

ngx_int_t nchan_extract_from_multi_msgid(nchan_msg_id_t *src, uint16_t n, nchan_msg_id_t *dst) {
  uint8_t count = src->tagcount;
  int16_t *tags;
  
  if(src->time == NCHAN_OLDEST_MSGID_TIME || src->time == NCHAN_NEWEST_MSGID_TIME) {
    dst->time = src->time;
    dst->tag.fixed[0] = 0;
    dst->tagcount = 1;
    dst->tagactive = 0;
    return NGX_OK;
  }
  else if(src->time == NCHAN_NTH_MSGID_TIME) {
    dst->time = src->time;
    dst->tag.fixed[0] = src->tag.fixed[0];
    dst->tagcount = 1;
    dst->tagactive = 0;
    return NGX_OK; 
  }
  
  if(n > count) {
    ERR("can't extract msgid %i from multi-msg of count %i", n, count);
    return NGX_ERROR;
  }
  tags = (count <= NCHAN_FIXED_MULTITAG_MAX) ? src->tag.fixed : src->tag.allocd;
  
  dst->time = src->time;
  if(tags[n] == -1) {
    dst->time --;
    dst->tag.fixed[0] = 32767; //eeeeeh this is bad. but it's good enough.
  }
  else {
    dst->tag.fixed[0] = tags[n];
  }
  dst->tagcount = 1;
  dst->tagactive = 0;
  
  return NGX_OK; 
}

static ngx_str_t *nchan_subscriber_get_etag(ngx_http_request_t * r) {
#if nginx_version >= 1008000
  return r->headers_in.if_none_match ? &r->headers_in.if_none_match->value : NULL;
#else
  ngx_uint_t                       i;
  ngx_list_part_t                 *part = &r->headers_in.headers.part;
  ngx_table_elt_t                 *header= part->elts;
  for (i = 0;  ; i++) {
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
#endif
}

ngx_int_t nchan_parse_compound_msgid(nchan_msg_id_t *id, ngx_str_t *str, ngx_int_t expected_tag_count) {
  u_char       *split, *last;
  ngx_int_t     time;
  uint8_t       len;
  //"<msg_time>:<msg_tag>"
  last = str->data + str->len;
  if((split = ngx_strlchr(str->data, last, ':')) != NULL) {
    len = 1;
  }
  else if( (str->len > 3 && (split = ngx_strnstr(str->data, "%3A", str->len)) != NULL)
        || (str->len > 3 && (split = ngx_strnstr(str->data, "%3a", str->len)) != NULL)) {
    len = 3;
  }
  else {
    len = 0; //placate dumb GCC warning
  }
  if(split) {
    time = ngx_atoi(str->data, split - str->data);
    split += len;
    if(time != NGX_ERROR) {
      id->time = time;
      return nchan_parse_msg_tag(split, last, id, expected_tag_count);
    }
    else {
      return NGX_ERROR;
    }
  }
  return NGX_DECLINED;
}



nchan_msg_id_t *nchan_subscriber_get_msg_id(ngx_http_request_t *r) {
  static nchan_msg_id_t           id = NCHAN_ZERO_MSGID;
  static nchan_msg_id_t           nth_msg_id = NCHAN_NTH_MSGID;
  static nchan_msg_id_t           oldest_msg_id = NCHAN_OLDEST_MSGID;
  static nchan_msg_id_t           newest_msg_id = NCHAN_NEWEST_MSGID;
  
  ngx_str_t                      *if_none_match;
  nchan_loc_conf_t               *cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  nchan_request_ctx_t            *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  int                             i;
  ngx_int_t                       rc;
  
  if_none_match = nchan_subscriber_get_etag(r);
  
  if(!cf->msg_in_etag_only && r->headers_in.if_modified_since != NULL) {
    id.time=ngx_http_parse_time(r->headers_in.if_modified_since->value.data, r->headers_in.if_modified_since->value.len);
    
    if(if_none_match==NULL) {
      id.tagcount=1;
      id.tagactive=0;
    }
    else {
      if(nchan_parse_msg_tag(if_none_match->data, if_none_match->data + if_none_match->len, &id, ctx->channel_id_count) == NGX_ERROR) {
        return NULL;
      }
    }
    return &id;
  }
  else if((cf->msg_in_etag_only || r->headers_in.if_modified_since == NULL) && if_none_match) {
    rc = nchan_parse_compound_msgid(&id, if_none_match, ctx->channel_id_count);
    if(rc == NGX_OK) {
      return &id;
    }
    else if(rc == NGX_ERROR) {
      return NULL;
    }
  }
  else {
    nchan_complex_value_arr_t   *alt_msgid_cv_arr = &cf->last_message_id;
    u_char                       buf[128];
    ngx_str_t                    str;
    int                          n = alt_msgid_cv_arr->n;
    ngx_int_t                    rc2;
    
    str.len = 0;
    str.data = buf;
    
    for(i=0; i < n; i++) {
      rc = ngx_http_complex_value_noalloc(r, alt_msgid_cv_arr->cv[i], &str, 128);
      if(str.len > 0 && rc == NGX_OK) {
        rc2 = nchan_parse_compound_msgid(&id, &str, ctx->channel_id_count);
        if(rc2 == NGX_OK) {
          return &id;
        }
        else if(rc2 == NGX_ERROR) {
          return NULL;
        }
      }
    }
  }
  
  //eh, we didn't find a valid alt_msgid value from variables. use the defaults
  switch(cf->subscriber_first_message) {
    case 1:
      id = oldest_msg_id;
      break;
    case 0: 
      id = newest_msg_id;
      break;
    default:
      id = nth_msg_id;
      id.tag.fixed[0] = cf->subscriber_first_message;
      break;
  }
  return &id;
}


int8_t nchan_compare_msgid_tags(nchan_msg_id_t *id1, nchan_msg_id_t *id2) {
  uint8_t active = id2->tagactive;
  int16_t *tags1, *tags2;
  int16_t t1, t2;
  
  tags1 = (id1->tagcount <= NCHAN_FIXED_MULTITAG_MAX) ? id1->tag.fixed : id1->tag.allocd;
  tags2 = (id2->tagcount <= NCHAN_FIXED_MULTITAG_MAX) ? id2->tag.fixed : id2->tag.allocd;
  
  //debugstuff that prevents this function from getting inlined
  assert(id1->time == id2->time);
  int i, nonnegs = 0;
  for (i=0; i < id2->tagcount; i++) {
    if(tags2[i] >= 0) nonnegs++;
  }
  assert(nonnegs == 1);
  
  if(id1->time == 0 && id2->time == 0) return 0; //always equal on zero-time
  
  t1 = (active < id1->tagcount) ? tags1[active] : -1;
  t2 = tags2[active];
  
  //ERR("Comparing msgids: id1: %V --", msgid_to_str(id1));
  //ERR("  --- id2: %V --", msgid_to_str(id2));
  
  if(t1 < t2){ 
    //ERR("id1 is smaller. -1");
    return -1;
  }
  if(t1 > t2){
    //ERR("id1 is larger. 1");
    return  1;
  }
  //ERR("id1 equals id2. 0");
  return 0;
}

int8_t nchan_compare_msgids(nchan_msg_id_t *id1, nchan_msg_id_t *id2) {
  assert(id1->tagcount == id2->tagcount);
  if(id1->time < id2->time) {
    return -1;
  }
  else if(id1->time > id2->time) {
    return 1;
  }
  else {
    assert(id1->tagcount == id2->tagcount);
    if(id1->tagcount == 1) {
      if(id1->tag.fixed[0] < id2->tag.fixed[0]) {
        return -1;
      }
      else if(id1->tag.fixed[0] > id2->tag.fixed[0]) {
        return 1;
      }
      else {
        return 0;
      }
    }
    else {
      return nchan_compare_msgid_tags(id1, id2);
    }
  }
}
