#include <nchan_module.h>
#include <assert.h>

static ngx_int_t validate_id(ngx_http_request_t *r, ngx_str_t *id, nchan_loc_conf_t *cf) {
  if(id->len > (unsigned )cf->max_channel_id_length) {
    nchan_log_request_warning(r, "channel id is too long: should be at most %i, is %i.", cf->max_channel_id_length, id->len);
    return NGX_ERROR;
  }
  return NGX_OK;
}

static ngx_int_t nchan_process_multi_channel_id(ngx_http_request_t *r, nchan_complex_value_arr_t *idcf, nchan_loc_conf_t *cf, ngx_str_t **ret_id) {
  ngx_int_t                   i, n = idcf->n, n_out = 0;
  ngx_str_t                   id[NCHAN_MULTITAG_MAX];
  ngx_str_t                  *id_out;
  ngx_str_t                  *group = &cf->channel_group;
  size_t                      sz = 0, grouplen = group->len;
  u_char                     *cur;
  
  //static ngx_str_t            empty_string = ngx_string("");
  
  nchan_request_ctx_t        *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  
  for(i=0; i < n && n_out < NCHAN_MULTITAG_MAX; i++) {
    ngx_http_complex_value(r, idcf->cv[i], &id[n_out]);
    if(validate_id(r, &id[n_out], cf) != NGX_OK) {
      *ret_id = NULL;
      return NGX_DECLINED;
    }
    
    if(cf->channel_id_split_delimiter.len > 0) {
      ngx_str_t  *delim = &cf->channel_id_split_delimiter;
      u_char     *cur_last, *last;
      cur = id[n_out].data;
      last = cur + id[n_out].len;
      
      u_char     *cur_first = cur;
      while ((cur_last = nchan_strsplit(&cur, delim, last)) != NULL) {
        id[n_out].data = cur_first;
        id[n_out].len = cur_last - cur_first;
        cur_first = cur;
        sz += id[n_out].len + 1 + grouplen; // "group/<channel-id>"
        if(n_out < NCHAN_MULTITAG_REQUEST_CTX_MAX) {
          ctx->channel_id[n_out] = id[n_out];
        }
        n_out++;
      }
      
    }
    else {
      sz += id[n_out].len + 1 + grouplen; // "group/<channel-id>"
      if(n_out < NCHAN_MULTITAG_REQUEST_CTX_MAX) {
        ctx->channel_id[n_out] = id[n_out];
      }
      n_out++;
    }
  }
  if(n_out>1) {
    sz += 3 + n_out; //space for null-separators and "m/<SEP>" prefix for multi-chid
  }
  if(ctx) {
    ctx->channel_id_count = n_out;
    //for(; i < NCHAN_MULTITAG_REQUEST_CTX_MAX; i++) {
    //  ctx->channel_id[i] = empty_string;
    //}
  }

  if((id_out = ngx_palloc(r->pool, sizeof(*id_out) + sz)) == NULL) {
    nchan_log_warning("can't allocate space for channel id");
    *ret_id = NULL;
    return NGX_ERROR;
  }
  id_out->len = sz;
  id_out->data = (u_char *)&id_out[1];
  cur = id_out->data;
  
  if(n_out > 1) {
    cur[0]='m';
    cur[1]='/';
    cur[2]=NCHAN_MULTI_SEP_CHR;
    cur+=3;
  }
  
  for(i = 0; i < n_out; i++) {
    ngx_memcpy(cur, group->data, grouplen);
    cur += grouplen;
    cur[0] = '/';
    cur++;
    ngx_memcpy(cur, id[i].data, id[i].len);
    cur += id[i].len;
    if(n_out>1) {
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
  nchan_request_ctx_t        *ctx = ngx_http_get_module_ctx(r, ngx_nchan_module);
  
  ctx->channel_id_count = 0;
  
  vv = ngx_http_get_variable(r, &channel_id_var_name, key);
  if (vv == NULL || vv->not_found || vv->len == 0) {
    //nchan_log_warning("the legacy $push_channel_id variable is not set");
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
    nchan_log_error("can't allocate space for legacy channel id");
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
  
  ctx->channel_id_count = 1;
  ctx->channel_id[0] = *id;
  
  *ret_id = id;
  return NGX_OK;
}

ngx_str_t *nchan_get_channel_id(ngx_http_request_t *r, pub_or_sub_t what, ngx_int_t fail_hard) {
  static const ngx_str_t          NO_CHANNEL_ID_MESSAGE = ngx_string("No channel id provided.");
  nchan_loc_conf_t               *cf = ngx_http_get_module_loc_conf(r, ngx_nchan_module);
  ngx_int_t                       rc;
  ngx_str_t                      *id = NULL;
  nchan_complex_value_arr_t          *chid_conf;
  
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
  
  if(cf->redis.enabled && id) {
    // make sure all closing curlybrace '}' are silently and unambiguously replaced by \31
    // that's because failing to do so will mess up cluster sharding {channel key strings}
    // it's not pretty, but it _is_ good enough.
    ngx_str_t id_cur = *id;
    char     *cur;
    if(memchr(id_cur.data, '\31', id_cur.len)) {
      nchan_log_request_warning(r, "character \\31 not allowed in channel id when using Redis.");
      id = NULL;
      rc = NGX_DECLINED;
      goto done;
    }
    
    while((cur = memchr(id_cur.data, '}', id_cur.len)) != NULL) {
      *cur='\31';
      id_cur.len -= (cur - (char *)id_cur.data + 1);
      id_cur.data = (u_char *)cur + 1;
    }
  }

done:
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
        nchan_respond_string(r, NGX_HTTP_NOT_FOUND, &NCHAN_CONTENT_TYPE_TEXT_PLAIN, &NO_CHANNEL_ID_MESSAGE, 0);
        break;
    }
    //DBG("%s channel id NULL", what == PUB ? "pub" : "sub");
  }
  else {
    //DBG("%s channel id %V", what == PUB ? "pub" : "sub", id);
  }
  
  return id;
}
