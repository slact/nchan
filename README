Nginx HTTP push module - Turn nginx into a long-polling push server that 
relays messages. 

If you want a long-polling server but don't want to wait on idle connections 
via upstream proxies, use this module to have nginx accept and hold 
long-polling client connections. Send responses to those clients by sending 
an HTTP request to a different location.

--------------------------- Example Config -----------------------------------
http {
  #maximum amount of memory the push module is allowed to use 
  #for buffering and stuff
  push_buffer_size	12M; #default is 3M

  #sender
  server {
  listen       localhost:8089; 
    location / {
      default_type  text/plain;
      set $push_id $arg_id; #/?id=239aff3 or somesuch
      push_sender;
      push_message_timeout 2h; #buffered messages expire after 2 hours
    }
  }

  #receiver
  server {
    listen       8088;
    location / {
      default_type  text/plain;
      set $push_id $arg_id; #/?id=239aff3 or somesuch
      push_listener;
    }
  }
}

---------------- Configuration directives & variables ------------------------
directives:
push_sender
  default: none
  context: server, location
  
  Defines a server or location as the sender. Requests from a sender will be 
  treated as messages to send to listeners.See protocol documentation 
  for more info. 

push_listener
  default: none
  context: server, location
  
  Defines a server or location as a listener. Requests from a listener will 
  not be responded to until a message for the listener (identified by 
  $push_id) becomes available. See protocol documentation for more info. 


push_queue_messages [ on | off ]
  default: on
  context: http, server, location
  
  Whether or not message queuing is enabled. If set to off, messages will be 
  delivered only if a push_listener connection is already present for the id. 
  Applicable only if a push_sender is present in this or a child context. 

push_message_timeout [ time ]
  default: 1h
  context: http, server, location
  How long a message may be queued before it is considered expired. If you do 
  not want messages to expire, set this to 0. Applicable only if a push_sender 
  is present in this or a child context. 

push_buffer_size [ size ]
  default: 3M
  context: http
  The size of the memory chunk this module will use for all message queuing 
  and buffering. 

Variables:
$push_id
  The id associated with a push_listener or push_sender. Must be present next
  to said directives.
  Example:
    set $push_id $arg_id #$push_id is now the url parameter "id"

---------------------------- Operation ---------------------------------------
Assuming the example config given above:
Clients will connect to http://example.com:8088/?id=... and have the 
response delayed until a message is POSTed to http://localhost:8089/?id=...
Messages can be sent to clients that have not yet connected, i.e. they are 
queued.

Upon sending a request to a push_sender location, the server will respond with 
a 201 Created if the message has been sent. If it must be queued up (i.e. the 
push_listener with this id is presently connected), a 202 Accepted will be sent.
 
If you indend to have the push_sender be a server-side application, 
it's a damn good idea to make sure the push_server location is not visible
publically, as it is intended for use only by your application.

----------------------- "Protocol" spec --------------------------------------
see http://wiki.github.com/slact/nginx_http_push_module/queuing-long-poll-relay-protocol

---------------------------- todo --------------------------------------------
- Add a directive apply to push_listeners regarding what to do when 
  multiple simultaneous requests with the same $push_id are received.
  Options will be "unique", "broadcast", "fifo" and "filo".
- Add other mechanisms of server pushing. The list should include
  "long-poll" (default), "interval-poll".
- Add a push_accomodate_strangers setting (presently defaulting to on). 
  When set to off, requests with a previously-unseen $push_id 
  will be rejected. 
- When POSTing to push_server, if Content-Type is "message/http", the 
  response sent to $push_id should be created from the body of the request.

