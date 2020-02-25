#include <ngx_http.h>
#include "nchan_defs.h"

//string constants
//headers
const  ngx_str_t NCHAN_HEADER_ETAG = ngx_string("Etag");
const  ngx_str_t NCHAN_HEADER_IF_NONE_MATCH = ngx_string("If-None-Match");
const  ngx_str_t NCHAN_HEADER_VARY = ngx_string("Vary");
const  ngx_str_t NCHAN_HEADER_ALLOW = ngx_string("Allow");
const  ngx_str_t NCHAN_HEADER_CACHE_CONTROL = ngx_string("Cache-Control");
const  ngx_str_t NCHAN_HEADER_PRAGMA = ngx_string("Pragma");
const  ngx_str_t NCHAN_HEADER_ORIGIN = ngx_string("Origin");

const  ngx_str_t NCHAN_HEADER_ACCESS_CONTROL_ALLOW_HEADERS = ngx_string("Access-Control-Allow-Headers");
const  ngx_str_t NCHAN_HEADER_ACCESS_CONTROL_ALLOW_METHODS = ngx_string("Access-Control-Allow-Methods");
const  ngx_str_t NCHAN_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN = ngx_string("Access-Control-Allow-Origin");
const  ngx_str_t NCHAN_HEADER_ACCESS_CONTROL_EXPOSE_HEADERS = ngx_string("Access-Control-Expose-Headers");
const  ngx_str_t NCHAN_HEADER_ACCESS_CONTROL_ALLOW_CREDENTIALS = ngx_string("Access-Control-Allow-Credentials");

const  ngx_str_t NCHAN_HEADER_EVENTSOURCE_EVENT = ngx_string("X-EventSource-Event");

//websocket headers

const ngx_str_t NCHAN_HEADER_CONNECTION = ngx_string("Connection");
const ngx_str_t NCHAN_HEADER_UPGRADE = ngx_string("Upgrade");
const ngx_str_t NCHAN_HEADER_SEC_WEBSOCKET_KEY = ngx_string("Sec-WebSocket-Key");
const ngx_str_t NCHAN_HEADER_SEC_WEBSOCKET_ACCEPT = ngx_string("Sec-WebSocket-Accept");
const ngx_str_t NCHAN_HEADER_SEC_WEBSOCKET_VERSION = ngx_string("Sec-WebSocket-Version");
const ngx_str_t NCHAN_HEADERS_SEC_WEBSOCKET_PROTOCOL = ngx_string("Sec-WebSocket-Protocol");
const ngx_str_t NCHAN_HEADER_SEC_WEBSOCKET_EXTENSIONS = ngx_string("Sec-WebSocket-Extensions");

//and some wobsockety values
const ngx_str_t NCHAN_UPGRADE = ngx_string("Upgrade");
const ngx_str_t NCHAN_WEBSOCKET = ngx_string("websocket");


//header values
const  ngx_str_t NCHAN_CACHE_CONTROL_VALUE = ngx_string("no-cache");

//status strings
const  ngx_str_t NCHAN_HTTP_STATUS_101 = ngx_string("101 Switching Protocols");
const  ngx_str_t NCHAN_HTTP_STATUS_204 = ngx_string("204 No Content");
const  ngx_str_t NCHAN_HTTP_STATUS_304 = ngx_string("304 Not Modified");
const  ngx_str_t NCHAN_HTTP_STATUS_400 = ngx_string("400 Bad Request");
const  ngx_str_t NCHAN_HTTP_STATUS_408 = ngx_string("408 Request Timeout");
const  ngx_str_t NCHAN_HTTP_STATUS_409 = ngx_string("409 Conflict");
const  ngx_str_t NCHAN_HTTP_STATUS_410 = ngx_string("410 Gone");

//other stuff
const  ngx_str_t NCHAN_SUBSCRIBER_TIMEOUT= ngx_string("Subscriber Timeout");
const  ngx_str_t NCHAN_ANYSTRING= ngx_string("*");
const  ngx_str_t NCHAN_ACCESS_CONTROL_ALLOWED_GROUP_HEADERS = ngx_string("Accept, Content-Type");
const  ngx_str_t NCHAN_ACCESS_CONTROL_ALLOWED_PUBLISHER_HEADERS = ngx_string("Content-Type");
const  ngx_str_t NCHAN_ACCESS_CONTROL_ALLOWED_SUBSCRIBER_HEADERS = ngx_string("If-None-Match, If-Modified-Since, Content-Type, Cache-Control, X-EventSource-Event");
const  ngx_str_t NCHAN_ACCESS_CONTROL_ALLOWED_PUBSUB_HEADERS = ngx_string("If-None-Match, If-Modified-Since, Content-Type, Cache-Control, X-EventSource-Event");
const  ngx_str_t NCHAN_ALLOW_GET_POST_PUT_DELETE= ngx_string("GET, POST, PUT, DELETE");
const  ngx_str_t NCHAN_ALLOW_GET= ngx_string("GET");
const  ngx_str_t NCHAN_ALLOW_GET_POST_DELETE= ngx_string("GET, POST, DELETE");


const  ngx_str_t NCHAN_VARY_HEADER_VALUE = ngx_string("If-None-Match, If-Modified-Since");
const  ngx_str_t NCHAN_MSG_RESPONSE_ALLOWED_HEADERS = ngx_string("Last-Modified, Etag, Content-Type");
const  char *NCHAN_MSG_RESPONSE_ALLOWED_CUSTOM_ETAG_HEADERS_STRF = "Last-Modified, %V, Content-Type";

const ngx_str_t  NCHAN_CONTENT_TYPE_TEXT_PLAIN = ngx_string("text/plain");

const ngx_str_t NCHAN_CHANNEL_INFO_PLAIN = ngx_string(
  "queued messages: %ui" CRLF
  "last requested: %d sec. ago" CRLF
  "active subscribers: %ui" CRLF
  "last message id: %V" 
  "\0");

const ngx_str_t NCHAN_CHANNEL_INFO_JSON = ngx_string(
  "{\"messages\": %ui, "
  "\"requested\": %d, "
  "\"subscribers\": %ui, "
  "\"last_message_id\": \"%V\" }"
  "\0");

const ngx_str_t NCHAN_CHANNEL_INFO_XML = ngx_string(
  "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" CRLF
  "<channel>" CRLF
  "  <messages>%ui</messages>" CRLF
  "  <requested>%d</requested>" CRLF
  "  <subscribers>%ui</subscribers>" CRLF
  "  <last_message_id>%V</last_message_id>" CRLF
  "</channel>"
  "\0");

const ngx_str_t NCHAN_CHANNEL_INFO_YAML = ngx_string(
  "---" CRLF
  "messages: %ui" CRLF
  "requested: %d" CRLF
  "subscribers: %ui" CRLF
  "last_message_id: \"%V\"" CRLF
  CRLF
  "\0");
