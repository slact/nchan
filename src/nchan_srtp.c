#include <nchan_module.h>
#include <nchan_srtp.h>

ngx_uint_t nchan_srtp_max_module;


static ngx_int_t ngx_tcp_cmp_conf_addrs(const void *one, const void *two) {
    nchan_srtp_conf_addr_t  *first, *second;

    first = (nchan_srtp_conf_addr_t *) one;
    second = (nchan_srtp_conf_addr_t *) two;

    if (first->wildcard) {
        /* a wildcard must be the last resort, shift it to the end */
        return 1;
    }

    if (first->bind && !second->bind) {
        /* shift explicit bind()ed addresses to the start */
        return -1;
    }

    if (!first->bind && second->bind) {
        /* shift explicit bind()ed addresses to the start */
        return 1;
    }

    /* do not sort by default */

    return 0;
}

static char *ngx_tcp_optimize_servers(ngx_conf_t *cf, nchan_srtp_core_main_conf_t *cmcf, ngx_array_t *ports) {
    ngx_uint_t                i, p, last, bind_wildcard;
    ngx_listening_t          *ls;
    nchan_srtp_port_t        *mport;
    nchan_srtp_conf_port_t   *port;
    nchan_srtp_conf_addr_t   *addr;

    port = ports->elts;
    for (p = 0; p < ports->nelts; p++) {

        ngx_sort(port[p].addrs.elts, (size_t) port[p].addrs.nelts,
                 sizeof(ngx_tcp_conf_addr_t), ngx_tcp_cmp_conf_addrs);

        addr = port[p].addrs.elts;
        last = port[p].addrs.nelts;

        /*
         * if there is the binding to the "*:port" then we need to bind()
         * to the "*:port" only and ignore the other bindings
         */

        if (addr[last - 1].wildcard) {
            addr[last - 1].bind = 1;
            bind_wildcard = 1;

        } else {
            bind_wildcard = 0;
        }

        i = 0;

        while (i < last) {

            if (bind_wildcard && !addr[i].bind) {
                i++;
                continue;
            }

            ls = ngx_create_listening(cf, addr[i].sockaddr, addr[i].socklen);
            if (ls == NULL) {
                return NGX_CONF_ERROR;
            }

            ls->addr_ntop = 1;
            ls->handler = ngx_tcp_init_connection;
            ls->pool_size = 256;

            /* TODO: error_log directive */
            ls->logp = &cf->cycle->new_log;
            ls->log.data = &ls->addr_text;
            ls->log.handler = ngx_accept_log_error;

#if (NGX_HAVE_INET6 && defined IPV6_V6ONLY)
            ls->ipv6only = addr[i].ipv6only;
#endif

            mport = ngx_palloc(cf->pool, sizeof(ngx_tcp_port_t));
            if (mport == NULL) {
                return NGX_CONF_ERROR;
            }

            ls->servers = mport;

            if (i == last - 1) {
                mport->naddrs = last;

            } else {
                mport->naddrs = 1;
                i = 0;
            }

            switch (ls->sockaddr->sa_family) {
#if (NGX_HAVE_INET6)
            case AF_INET6:
                if (ngx_tcp_add_addrs6(cf, mport, addr) != NGX_OK) {
                    return NGX_CONF_ERROR;
                }
                break;
#endif
            default: /* AF_INET */
                if (ngx_tcp_add_addrs(cf, mport, addr) != NGX_OK) {
                    return NGX_CONF_ERROR;
                }
                break;
            }

            addr++;
            last--;
        }
    }

    return NGX_CONF_OK;
}



static ngx_int_t ngx_tcp_add_ports(ngx_conf_t *cf, ngx_array_t *ports, nchan_srtp_listen_t *listen) {
  in_port_t                 p;
  ngx_uint_t                i;
  struct sockaddr          *sa;
  struct sockaddr_in       *sin;
  nchan_srtp_conf_port_t   *port;
  nchan_srtp_conf_addr_t   *addr;
#if (NGX_HAVE_INET6)
  struct sockaddr_in6   *sin6;
#endif

  sa = (struct sockaddr *) &listen->sockaddr;

  switch (sa->sa_family) {

#if (NGX_HAVE_INET6)
  case AF_INET6:
      sin6 = (struct sockaddr_in6 *) sa;
      p = sin6->sin6_port;
      break;
#endif

  default: /* AF_INET */
      sin = (struct sockaddr_in *) sa;
      p = sin->sin_port;
      break;
  }

  port = ports->elts;
  for (i = 0; i < ports->nelts; i++) {
      if (p == port[i].port && sa->sa_family == port[i].family) {

          /* a port is already in the port list */

          port = &port[i];
          goto found;
      }
  }

  /* add a port to the port list */

  port = ngx_array_push(ports);
  if (port == NULL) {
      return NGX_ERROR;
  }

  port->family = sa->sa_family;
  port->port = p;

  if (ngx_array_init(&port->addrs, cf->temp_pool, 2,
                      sizeof(nchan_srtp_conf_addr_t))
      != NGX_OK)
  {
      return NGX_ERROR;
  }

found:

  addr = ngx_array_push(&port->addrs);
  if (addr == NULL) {
      return NGX_ERROR;
  }

  ngx_memzero(addr, sizeof(nchan_srtp_conf_addr_t));

  addr->sockaddr = (struct sockaddr *) &listen->sockaddr;
  addr->socklen = listen->socklen;
  addr->ctx = listen->ctx;
  addr->bind = listen->bind;
  addr->wildcard = listen->wildcard;
  if (listen->default_port) {
      addr->default_ctx = listen->ctx;
  }
#if (NGX_TCP_SSL)
  addr->ssl = listen->ssl;
#endif
#if (NGX_HAVE_INET6 && defined IPV6_V6ONLY)
  addr->ipv6only = listen->ipv6only;
#endif

  return NGX_OK;
}


char *nchan_srtp_block(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  
  char                          *rv;
  ngx_uint_t                     i, m, mi, s;
  ngx_conf_t                     pcf;
  ngx_array_t                    ports;
  nchan_srtp_listen_t           *listen;
  nchan_srtp_module_t           *module;
  nchan_srtp_conf_ctx_t         *ctx;
  nchan_srtp_core_srv_conf_t   **cscfp;
  nchan_srtp_core_main_conf_t   *cmcf;


  /* the main tcp context */

  ctx = ngx_pcalloc(cf->pool, sizeof(*ctx));
  if (ctx == NULL) {
      return NGX_CONF_ERROR;
  }

  *(nchan_srtp_conf_ctx_t **) conf = ctx;

  /* count the number of the tcp modules and set up their indices */

  nchan_srtp_max_module = 0;
  for (m = 0; ngx_modules[m]; m++) {
      if (ngx_modules[m]->type != NCHAN_SRTP_MODULE) {
          continue;
      }

      ngx_modules[m]->ctx_index = nchan_srtp_max_module++;
  }


  /* the tcp main_conf context, it is the same in the all tcp contexts */

  ctx->main_conf = ngx_pcalloc(cf->pool,
                                sizeof(void *) * nchan_srtp_max_module);
  if (ctx->main_conf == NULL) {
      return NGX_CONF_ERROR;
  }


  /*
    * the tcp null srv_conf context, it is used to merge
    * the server{}s' srv_conf's
    */

  ctx->srv_conf = ngx_pcalloc(cf->pool, sizeof(void *) * nchan_srtp_max_module);
  if (ctx->srv_conf == NULL) {
      return NGX_CONF_ERROR;
  }


  /*
    * create the main_conf's, the null srv_conf's, and the null loc_conf's
    * of the all tcp modules
    */

  for (m = 0; ngx_modules[m]; m++) {
      if (ngx_modules[m]->type != NCHAN_SRTP_MODULE) {
          continue;
      }

      module = ngx_modules[m]->ctx;
      mi = ngx_modules[m]->ctx_index;

      if (module->create_main_conf) {
          ctx->main_conf[mi] = module->create_main_conf(cf);
          if (ctx->main_conf[mi] == NULL) {
              return NGX_CONF_ERROR;
          }
      }

      if (module->create_srv_conf) {
          ctx->srv_conf[mi] = module->create_srv_conf(cf);
          if (ctx->srv_conf[mi] == NULL) {
              return NGX_CONF_ERROR;
          }
      }
  }


  /* parse inside the tcp{} block */

  pcf = *cf;
  cf->ctx = ctx;

  cf->module_type = NCHAN_SRTP_MODULE;
  cf->cmd_type = NCHAN_SRTP_MAIN_CONF;
  rv = ngx_conf_parse(cf, NULL);

  if (rv != NGX_CONF_OK) {
      *cf = pcf;
      return rv;
  }


  /* init tcp{} main_conf's, merge the server{}s' srv_conf's */

  cmcf = ctx->main_conf[nchan_module.ctx_index];
  cscfp = cmcf->servers.elts;

  for (m = 0; ngx_modules[m]; m++) {
      if (ngx_modules[m]->type != NCHAN_SRTP_MODULE) {
          continue;
      }

      module = ngx_modules[m]->ctx;
      mi = ngx_modules[m]->ctx_index;

      /* init tcp{} main_conf's */

      cf->ctx = ctx;

      if (module->init_main_conf) {
          rv = module->init_main_conf(cf, ctx->main_conf[mi]);
          if (rv != NGX_CONF_OK) {
              *cf = pcf;
              return rv;
          }
      }

      for (s = 0; s < cmcf->servers.nelts; s++) {

          /* merge the server{}s' srv_conf's */

          cf->ctx = cscfp[s]->ctx;

          if (module->merge_srv_conf) {
              rv = module->merge_srv_conf(cf, ctx->srv_conf[mi],
                                          cscfp[s]->ctx->srv_conf[mi]);
              if (rv != NGX_CONF_OK) {
                  *cf = pcf;
                  return rv;
              }
          }
      }
  }

  *cf = pcf;

  if (ngx_array_init(&ports, cf->temp_pool, 4, sizeof(nchan_srtp_conf_port_t))
      != NGX_OK)
  {
      return NGX_CONF_ERROR;
  }

  listen = cmcf->listen.elts;

  for (i = 0; i < cmcf->listen.nelts; i++) {
      if (ngx_tcp_add_ports(cf, &ports, &listen[i]) != NGX_OK) {
          return NGX_CONF_ERROR;
      }

      /*
      if (ngx_tcp_add_virtual_servers(cf, cmcf, &listen[i]) != NGX_OK) {
          return NGX_CONF_ERROR;
      }
      */
  }

  return ngx_tcp_optimize_servers(cf, cmcf, &ports);
    
}