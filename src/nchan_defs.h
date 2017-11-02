#ifndef NCHAN_DEFS_H
#define NCHAN_DEFS_H

#define NCHAN_DEFAULT_SHM_SIZE 33554432 //32 megs
#define NCHAN_DEFAULT_MESSAGE_TIMEOUT 3600
#define NCHAN_DEFAULT_REDIS_IDLE_CHANNEL_CACHE_TIMEOUT 30
#define NCHAN_DEFAULT_SUBSCRIBER_TIMEOUT 0  //default: never timeout
//(liucougar: this is a bit confusing, but it is what's the default behavior before this option is introducecd)
#define NCHAN_DEFAULT_WEBSOCKET_PING_INTERVAL 0
#define NCHAN_DEFAULT_SUBSCRIBER_DEFAULT_KEEP_ALIVE_INTERVAL 0

#define NCHAN_DEFAULT_CHANNEL_TIMEOUT 5 //default: timeout in 5 seconds

#define NCHAN_DEFAULT_MIN_MESSAGES 1
#define NCHAN_DEFAULT_MAX_MESSAGES 10

#define NCHAN_SUBSCRIBER_DEFAULT_FIRST_MESSAGE 1
#define NCHAN_SUBSCRIBER_FIRST_MESSAGE_UNSET 4096

#define NCHAN_MAX_CHANNEL_ID_LENGTH 1024 //bytes

#define NCHAN_META_CHANNEL_MAX_MESSAGES 100
#define NCHAN_META_CHANNEL_MESSAGE_TTL 10
#define NCHAN_META_CHANNEL_TIMEOUT 30

#ifndef NGX_HTTP_REQUEST_TIMEOUT
#define NGX_HTTP_REQUEST_TIMEOUT 408
#endif

#ifndef NGX_HTTP_CONFLICT
#define NGX_HTTP_CONFLICT 409
#endif

#ifndef NGX_HTTP_SWITCHING_PROTOCOLS
#define NGX_HTTP_SWITCHING_PROTOCOLS       101
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


#define NCHAN_MESSAGE_RECEIVED  9000
#define NCHAN_MESSAGE_QUEUED    9001

extern const ngx_str_t NCHAN_HEADER_ETAG;
extern const ngx_str_t NCHAN_HEADER_IF_NONE_MATCH;
extern const ngx_str_t NCHAN_HEADER_VARY;
extern const ngx_str_t NCHAN_HEADER_ALLOW;
extern const ngx_str_t NCHAN_HEADER_CACHE_CONTROL;
extern const ngx_str_t NCHAN_HEADER_PRAGMA;
extern const ngx_str_t NCHAN_HEADER_ORIGIN;

extern const ngx_str_t NCHAN_HEADER_ACCESS_CONTROL_ALLOW_HEADERS;
extern const ngx_str_t NCHAN_HEADER_ACCESS_CONTROL_ALLOW_METHODS;
extern const ngx_str_t NCHAN_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN;
extern const ngx_str_t NCHAN_HEADER_ACCESS_CONTROL_EXPOSE_HEADERS;

extern const ngx_str_t NCHAN_HEADER_EVENTSOURCE_EVENT;

extern const ngx_str_t NCHAN_HEADER_CONNECTION;
extern const ngx_str_t NCHAN_HEADER_UPGRADE;
extern const ngx_str_t NCHAN_HEADER_SEC_WEBSOCKET_KEY;
extern const ngx_str_t NCHAN_HEADER_SEC_WEBSOCKET_ACCEPT;
extern const ngx_str_t NCHAN_HEADER_SEC_WEBSOCKET_VERSION;
extern const ngx_str_t NCHAN_HEADERS_SEC_WEBSOCKET_PROTOCOL;
extern const ngx_str_t NCHAN_HEADER_SEC_WEBSOCKET_EXTENSIONS;

//header values
extern const  ngx_str_t NCHAN_CACHE_CONTROL_VALUE;

//status strings
extern const  ngx_str_t NCHAN_HTTP_STATUS_101;
extern const  ngx_str_t NCHAN_HTTP_STATUS_204;
extern const  ngx_str_t NCHAN_HTTP_STATUS_304;
extern const  ngx_str_t NCHAN_HTTP_STATUS_408;
extern const  ngx_str_t NCHAN_HTTP_STATUS_409;
extern const  ngx_str_t NCHAN_HTTP_STATUS_410;

//other stuff

extern const ngx_str_t NCHAN_UPGRADE;
extern const ngx_str_t NCHAN_SUBSCRIBER_TIMEOUT;
extern const ngx_str_t NCHAN_WEBSOCKET;
extern const ngx_str_t NCHAN_ANYSTRING;
extern const ngx_str_t NCHAN_ACCESS_CONTROL_ALLOWED_PUBLISHER_HEADERS;
extern const ngx_str_t NCHAN_ACCESS_CONTROL_ALLOWED_SUBSCRIBER_HEADERS;
extern const ngx_str_t NCHAN_ACCESS_CONTROL_ALLOWED_GROUP_HEADERS;
extern const ngx_str_t NCHAN_ALLOW_GET_POST_PUT_DELETE;
extern const ngx_str_t NCHAN_ALLOW_GET_POST_DELETE;
extern const ngx_str_t NCHAN_ALLOW_GET;
extern const ngx_str_t NCHAN_VARY_HEADER_VALUE;
extern const ngx_str_t NCHAN_MSG_RESPONSE_ALLOWED_HEADERS;
extern const char *NCHAN_MSG_RESPONSE_ALLOWED_CUSTOM_ETAG_HEADERS_STRF;
extern const ngx_str_t NCHAN_CONTENT_TYPE_TEXT_PLAIN;
extern const ngx_str_t NCHAN_CHANNEL_INFO_PLAIN;
extern const ngx_str_t NCHAN_CHANNEL_INFO_JSON;
extern const ngx_str_t NCHAN_CHANNEL_INFO_XML;
extern const ngx_str_t NCHAN_CHANNEL_INFO_YAML;

#define NCHAN_REDIS_DEFAULT_URL "redis://127.0.0.1:6379"

#endif /* NCHAN_DEFS_H */
