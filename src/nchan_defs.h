#ifndef NCHAN_DEFS_H
#define NCHAN_DEFS_H

#define NCHAN_DEFAULT_SHM_SIZE 33554432 //32 megs
#define NCHAN_DEFAULT_BUFFER_TIMEOUT 3600
#define NCHAN_DEFAULT_SUBSCRIBER_TIMEOUT 0  //default: never timeout
//(liucougar: this is a bit confusing, but it is what's the default behavior before this option is introducecd)
#define NCHAN_DEFAULT_CHANNEL_TIMEOUT 5 //default: timeout in 5 seconds

#define NCHAN_DEFAULT_MIN_MESSAGES 1
#define NCHAN_DEFAULT_MAX_MESSAGES 10

#define NCHAN_SUBSCRIBER_CONCURRENCY_LASTIN 0
#define NCHAN_SUBSCRIBER_CONCURRENCY_FIRSTIN 1
#define NCHAN_SUBSCRIBER_CONCURRENCY_BROADCAST 2

#define NCHAN_MECHANISM_LONGPOLL 0
#define NCHAN_MECHANISM_INTERVALPOLL 1

#define NCHAN_MIN_MESSAGE_RECIPIENTS 0

#define NCHAN_MAX_CHANNEL_ID_LENGTH 1024 //bytes

#ifndef NGX_HTTP_CONFLICT
#define NGX_HTTP_CONFLICT 409
#endif

#ifndef NGX_HTTP_GONE
#define NGX_HTTP_GONE 410
#endif

#ifndef NGX_HTTP_CREATED
#define NGX_HTTP_CREATED 201
#endif

#ifndef NGX_HTTP_ACCEPTED
#define NGX_HTTP_ACCEPTED 202
#endif


#define NCHAN_MESSAGE_RECEIVED 9000
#define NCHAN_MESSAGE_QUEUED   9001
#define NCHAN_MESSAGE_NOTFOUND  1404
#define NCHAN_MESSAGE_FOUND     1000
#define NCHAN_MESSAGE_EXPECTED  1001
#define NCHAN_MESSAGE_EXPIRED   1002

extern const  ngx_str_t NCHAN_HEADER_ETAG;
extern const  ngx_str_t NCHAN_HEADER_IF_NONE_MATCH;
extern const  ngx_str_t NCHAN_HEADER_VARY;
extern const  ngx_str_t NCHAN_HEADER_ALLOW;
extern const  ngx_str_t NCHAN_HEADER_CACHE_CONTROL;
extern const  ngx_str_t NCHAN_HEADER_PRAGMA;

extern const ngx_str_t NCHAN_HEADER_CONNECTION;
extern const ngx_str_t NCHAN_HEADER_UPGRADE;
extern const ngx_str_t NCHAN_HEADER_SEC_WEBSOCKET_KEY;
extern const ngx_str_t NCHAN_HEADER_SEC_WEBSOCKET_ACCEPT;
extern const ngx_str_t NCHAN_HEADER_SEC_WEBSOCKET_VERSION;

//header values
extern const  ngx_str_t NCHAN_CACHE_CONTROL_VALUE;

//status strings
extern const  ngx_str_t NCHAN_HTTP_STATUS_101;
extern const  ngx_str_t NCHAN_HTTP_STATUS_409;
extern const  ngx_str_t NCHAN_HTTP_STATUS_410;

//other stuff

extern const ngx_str_t NCHAN_UPGRADE;
extern const ngx_str_t NCHAN_WEBSOCKET;
extern const ngx_str_t NCHAN_ANYSTRING;
extern const ngx_str_t NCHAN_ACCESS_CONTROL_ALLOWED_PUBLISHER_HEADERS;
extern const ngx_str_t NCHAN_ACCESS_CONTROL_ALLOWED_SUBSCRIBER_HEADERS;
extern const ngx_str_t NCHAN_ALLOW_GET_POST_PUT_DELETE_OPTIONS;
extern const ngx_str_t NCHAN_ALLOW_GET_OPTIONS;
extern const ngx_str_t NCHAN_VARY_HEADER_VALUE;
extern const ngx_str_t NCHAN_CHANNEL_INFO_PLAIN;
extern const ngx_str_t NCHAN_CHANNEL_INFO_JSON;
extern const ngx_str_t NCHAN_CHANNEL_INFO_XML;
extern const ngx_str_t NCHAN_CHANNEL_INFO_YAML;

#endif /* NCHAN_DEFS_H */