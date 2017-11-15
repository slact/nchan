//Copyright (c) 2014 Wandenberg Peixoto under the MIT Licence'
//additional code from Alexander Lyalin's redis_nginx_module (New BSD licence)
//edited by slact 2015-2016

#include <ngx_event.h>
#include <ngx_connection.h>
#include <signal.h>
#include "redis_nginx_adapter.h"

#include <nchan_module.h>
#include <store/redis/store.h>

#include <assert.h>

#define PING_DATABASE_COMMAND "PING"

#define EVENT_FLAGS ((ngx_event_flags & NGX_USE_CLEAR_EVENT) ?  NGX_CLEAR_EVENT : NGX_LEVEL_EVENT)

#define REDIS_MAX_BUFFERED_READ_SIZE 1024*18 //hardcoded into hiredis

//NGX_CLEAR_EVENT for kqueue, epoll
//NGX_LEVEL_EVENT for select, poll, /dev/poll

int redis_nginx_event_attach(redisAsyncContext *ac);
void redis_nginx_cleanup(void *privdata);
void redis_nginx_ping_callback(redisAsyncContext *ac, void *rep, void *privdata);


void redis_nginx_init(void) {
  signal(SIGPIPE, SIG_IGN);
}


redisAsyncContext *redis_nginx_open_context(ngx_str_t *host, int port, int database, ngx_str_t *password, void *privdata, redisAsyncContext **context) {
  redisAsyncContext *ac = NULL;
  u_char             hostchr[1024] = {0};
  if(host->len >= 1023) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: hostname is too long");
    return NULL;
  }
  ngx_memcpy(hostchr, host->data, host->len);
  
  if ((context == NULL) || (*context == NULL) || (*context)->err) {
    ac = redisAsyncConnect((const char *)hostchr, port);
    if (ac == NULL) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: could not allocate the redis context for %V:%d", host, port);
      return NULL;
    }
    
    if (ac->err) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: could not create the redis context for %V:%d - %s", host, port, ac->errstr);
      redisAsyncFree(ac);
      *context = NULL;
      return NULL;
    }
    
    if(redis_nginx_event_attach(ac) == REDIS_OK) {
      ac->data = privdata;
      *context = ac;
    }
  }
  else {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: redis context already open");
  }

  return ac;
}


redisContext *redis_nginx_open_sync_context(ngx_str_t *host, int port, int database, ngx_str_t *password, redisContext **context) {
  redisContext  *c = NULL;
  redisReply    *reply;
  
  u_char        hostchr[1024] = {0};
  if(host->len >= 1023) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: hostname is too long");
    return NULL;
  }
  ngx_memcpy(hostchr, host->data, host->len);
  
  if ((context == NULL) || (*context == NULL) || (*context)->err) {
    c = redisConnect((const char *)hostchr, port);
    if (c == NULL) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: could not allocate the redis sync context for %s:%d", host, port);
      return NULL;
    }
    
    if (c->err) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: could not create the redis sync context for %s:%d - %s", host, port, c->errstr);
      goto fail;
    }
    
    if (context != NULL) {
      *context = c;
    }
    if(password->len > 0) {
      reply = redisCommand(c, "AUTH %b", password->data, password->len);
      if ((reply == NULL) || (reply->type == REDIS_REPLY_ERROR)) {
        goto fail;
      }
    }
    if(database != -1) {
      reply = redisCommand(c, "SELECT %d", database);
      if ((reply == NULL) || (reply->type == REDIS_REPLY_ERROR)) {
        goto fail;
      }
    }
  }
  else {
    c = *context;
  }

  return c;
fail:
  if (context != NULL) {
    *context = NULL;
  }
  redisFree(c);
  return NULL;
}


void redis_nginx_force_close_context(redisAsyncContext **context) {
  if ((context != NULL) && (*context != NULL)) {
    redisAsyncContext *ac = *context;
    if (!ac->err) {
      redisAsyncDisconnect(ac);
    }
    *context = NULL;
  }
}

void redis_nginx_ping_callback(redisAsyncContext *ac, void *rep, void *privdata) {
  void *data = ac->data;
  void (*callback) (void *) = privdata;
  redisAsyncDisconnect(ac);
  if (callback != NULL) {
    callback(data);
  }
}

void redis_nginx_read_event(ngx_event_t *ev) {
  redisAsyncContext *ac = ((ngx_connection_t *)ev->data)->data;
  int                fd = ac->c.fd; //because redisAsyncHandleRead might free the redisAsyncContext
  int                bytes_left;
  redisAsyncHandleRead(ac);
  
  // we need to do this because hiredis, in its infinite wisdom, will read at 
  // most 16Kb of data, and there's no reliable way to tell if it read that 
  // whole amount in one gulp. Otherwise, we could just check if 16Kb have 
  //been read and try again. But no, apparently that's not an option.
  
  ioctl(fd, FIONREAD, &bytes_left);
  if (bytes_left > 0 && !ac->err) {
    //ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "again!");
    redis_nginx_read_event(ev);
  }
}

void redis_nginx_write_event(ngx_event_t *ev) {
  ngx_connection_t *connection = (ngx_connection_t *) ev->data;
  redisAsyncHandleWrite(connection->data);
}


int redis_nginx_fd_is_valid(int fd) {
  return (fd > 0) && ((fcntl(fd, F_GETFL) != -1) || (errno != EBADF));
}

void redis_nginx_add_read(void *privdata) {
  ngx_connection_t *connection = (ngx_connection_t *) privdata;
  ngx_int_t         flags = EVENT_FLAGS;
  if (!connection->read->active && redis_nginx_fd_is_valid(connection->fd)) {
    connection->read->handler = redis_nginx_read_event;
    connection->read->log = connection->log;
    if (ngx_add_event(connection->read, NGX_READ_EVENT, flags) == NGX_ERROR) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: could not add read event to redis");
    }
  }
}

void redis_nginx_del_read(void *privdata) {
  ngx_connection_t *connection = (ngx_connection_t *) privdata;
  ngx_int_t         flags = EVENT_FLAGS;
  if (connection->read->active && redis_nginx_fd_is_valid(connection->fd)) {
    if (ngx_del_event(connection->read, NGX_READ_EVENT, flags) == NGX_ERROR) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: could not delete read event to redis");
    }
  }
  else {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: didn't delete read event %p", connection->read);
  }
}

void redis_nginx_add_write(void *privdata) {
  ngx_connection_t *connection = (ngx_connection_t *) privdata;
  ngx_int_t         flags = EVENT_FLAGS;
  if (!connection->write->active && redis_nginx_fd_is_valid(connection->fd)) {
    connection->write->handler = redis_nginx_write_event;
    connection->write->log = connection->log;
    if (ngx_add_event(connection->write, NGX_WRITE_EVENT, flags) == NGX_ERROR) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: could not add write event to redis");
    }
  }
}

void redis_nginx_del_write(void *privdata) {
  ngx_connection_t *connection = (ngx_connection_t *) privdata;
  ngx_int_t         flags = EVENT_FLAGS;
  if (connection->write->active && redis_nginx_fd_is_valid(connection->fd)) {
    if (ngx_del_event(connection->write, NGX_WRITE_EVENT, flags) == NGX_ERROR) {
      ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: could not delete write event to redis");
    }
  }
}

void redis_nginx_cleanup(void *privdata) {
  if (privdata) {
    ngx_connection_t *connection = (ngx_connection_t *) privdata;
    redisAsyncContext *ac = (redisAsyncContext *) connection->data;
    // don't care why the connection is being cleaned up. if there's an error, close the fd. if an fd was being shared between
    // workers or connections -- tough luck
    
    if ((connection->fd != NGX_INVALID_FILE)) {
      if(connection->read->active) {
        redis_nginx_del_read(privdata);
      }
      if(connection->write->active) {
        redis_nginx_del_write(privdata);
      }
      ngx_close_connection(connection);
    } else {
      ngx_free_connection(connection);
    }
    
    ac->ev.data = NULL;
  }
}

int redis_nginx_event_attach(redisAsyncContext *ac) {
  ngx_connection_t *connection;
  redisContext *c = &(ac->c);
  
  /* Nothing should be attached when something is already attached */
  if (ac->ev.data != NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: context already attached");
    return REDIS_ERR;
  }
  
  connection = ngx_get_connection(c->fd, ngx_cycle->log);
  if (connection == NULL) {
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "redis_nginx_adapter: could not get a connection for fd #%d", c->fd);
    return REDIS_ERR;
  }
  
  /* Register functions to start/stop listening for events */
  ac->ev.addRead = redis_nginx_add_read;
  ac->ev.delRead = redis_nginx_del_read;
  ac->ev.addWrite = redis_nginx_add_write;
  ac->ev.delWrite = redis_nginx_del_write;
  ac->ev.cleanup = redis_nginx_cleanup;
  ac->ev.data = connection;
  connection->data = ac;
  
  return REDIS_OK;
}
