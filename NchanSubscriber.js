
// https://github.com/yanatan16/nanoajax
!function(t,e){function n(t){return t&&e.XDomainRequest&&!/MSIE 1/.test(navigator.userAgent)?new XDomainRequest:e.XMLHttpRequest?new XMLHttpRequest:void 0}function o(t,e,n){t[e]=t[e]||n}var r=["responseType","withCredentials","timeout","onprogress"];t.ajax=function(t,a){function s(t,e){return function(){c||(a(void 0===f.status?t:f.status,0===f.status?"Error":f.response||f.responseText||e,f),c=!0)}}var u=t.headers||{},i=t.body,d=t.method||(i?"POST":"GET"),c=!1,f=n(t.cors);f.open(d,t.url,!0);var l=f.onload=s(200);f.onreadystatechange=function(){4===f.readyState&&l()},f.onerror=s(null,"Error"),f.ontimeout=s(null,"Timeout"),f.onabort=s(null,"Abort"),i&&(o(u,"X-Requested-With","XMLHttpRequest"),e.FormData&&i instanceof e.FormData||o(u,"Content-Type","application/x-www-form-urlencoded"));for(var p,m=0,v=r.length;v>m;m++)p=r[m],void 0!==t[p]&&(f[p]=t[p]);for(var p in u)f.setRequestHeader(p,u[p]);return f.send(i),f},e.nanoajax=t}({},function(){return this}());

// https://github.com/component/emitter
function Emitter(t){return t?mixin(t):void 0}function mixin(t){for(var e in Emitter.prototype)t[e]=Emitter.prototype[e];return t}Emitter.prototype.on=Emitter.prototype.addEventListener=function(t,e){return this._callbacks=this._callbacks||{},(this._callbacks["$"+t]=this._callbacks["$"+t]||[]).push(e),this},Emitter.prototype.once=function(t,e){function i(){this.off(t,i),e.apply(this,arguments)}return i.fn=e,this.on(t,i),this},Emitter.prototype.off=Emitter.prototype.removeListener=Emitter.prototype.removeAllListeners=Emitter.prototype.removeEventListener=function(t,e){if(this._callbacks=this._callbacks||{},0==arguments.length)return this._callbacks={},this;var i=this._callbacks["$"+t];if(!i)return this;if(1==arguments.length)return delete this._callbacks["$"+t],this;for(var r,s=0;s<i.length;s++)if(r=i[s],r===e||r.fn===e){i.splice(s,1);break}return this},Emitter.prototype.emit=function(t){this._callbacks=this._callbacks||{};var e=[].slice.call(arguments,1),i=this._callbacks["$"+t];if(i){i=i.slice(0);for(var r=0,s=i.length;s>r;++r)i[r].apply(this,e)}return this},Emitter.prototype.listeners=function(t){return this._callbacks=this._callbacks||{},this._callbacks["$"+t]||[]},Emitter.prototype.hasListeners=function(t){return!!this.listeners(t).length};


"use strict";
function NchanSubscriber(url, opt) {
  this.url = url;
  
  //which transport should i use?
  if(typeof opt === "string") {
    opt = {subscriber: opt}; 
  }
  if(opt.transport && !opt.subscriber) {
    opt.subscriber = opt.transport;
  }
  if(typeof opt.subscriber === "string") {
    opt.subscriber = [ opt.subscriber ];
  }
  var tryInitializeTransport = function(name) {
    if(!this.SubscriberClass[name]) {
      throw "unknown subscriber type " + name;
    }
    try {
      this.transport = new this.SubscriberClass[name](this.emit.bind(this)); //try it
      return this.transport;
    } catch(err) { /*meh...*/ }
  }.bind(this);
  
  var i;
  if(opt.subscriber) {
    for(i in opt.subscriber) {
      if(tryInitializeTransport(opt.subscriber[i])) {
        break;
      }
    }
  }
  else {
    for(i in this.SubscriberClass) {
      if(tryInitializeTransport(i)) {
        break;
      }
    }
  }
  if(! this.transport) {
    throw "can't use any transport type";
  }
  
  this.msgId = opt.id || opt.msgId;
  this.reconnect = typeof opt.reconnect == "undefined" ? true : opt.reconnect;
  this.reconnectTimeout = opt.reconnectTimeout || 10;
  
  var restartTimeoutIndex;
  var stopHandler = function() {
    if(!restartTimeoutIndex && this.running && this.reconnect && !this.transport.reconnecting) {
      restartTimeoutIndex = setTimeout(function() {
        restartTimeoutIndex = null;
        this.start();
      }.bind(this), this.reconnectTimeout);
    }
  }.bind(this);
  
  this.on("message", function msg(msg, meta) {
    this.lastMessageId=meta.id;
    console.log(msg, meta);
  });
  this.on("error", function fail(code, text) {
    stopHandler(code, text);
    console.log("failure", code, text);
  });
  this.on("connect", function() {
    this.connected = true;
  });
  this.on("__disconnect", function fail(code, text) {
    this.connected = false;
    this.emit("disconnect", code, text);
    stopHandler(code, text);
    console.log("__disconnect", code, text);
  }.bind(this));
  
  /*
  //explicitly stop just before leaving the page
  ['unload', 'beforeunload'].each(function(ev) {
    window.addEvent(ev, this.stop.bind(this));
  }.bind(this));
  */
}

Emitter(NchanSubscriber.prototype);

NchanSubscriber.prototype.start = function() {
  this.transport.listen(this.url, this.lastMessageId);
  this.running = true;
  return this;
};

NchanSubscriber.prototype.stop = function() {
  this.running = false;
  this.transport.cancel();
  return this;
};

NchanSubscriber.prototype.saveState = function() {
  throw "not implemented";
};

(function() {
  function addLastMsgIdToQueryString(url, msgid) {
    if(msgid) {
      var m = url.match(/(\?.*)$/);
      url += (m ? "&" : "?") + "last_event_id=" + encodeURIComponent(msgid);
    }
    return url;
  }

  NchanSubscriber.prototype.SubscriberClass = {
    'longpoll': (function () {
      function Longpoll(emit) {
        this.headers = {};
        this.longPollStartTime = null;
        this.maxLongPollTime = 5*60*1000; //5 minutes
        this.emit = emit;
      }
      
      Longpoll.prototype.listen = function(url, msgid) {
        if(this.req) {
          throw "already listening";
        }
        if(url) { this.url=url; }
        var setHeader = function(incoming, name) {
          if(incoming) { this.headers[name]= incoming; }
        }.bind(this);
        
        if(headers) {
          this.headers = {"Etag": msgid};
        }
        
        this.reqStartTime = new Date().getTime();
        
        var  requestCallback;
        requestCallback = function (code, response_text, req) {
          setHeader(req.getResponseHeader('Last-Modified'), 'If-Modified-Since');
          setHeader(req.getResponseHeader('Etag'), 'If-None-Match');
          
          if(code >= 200 && code <= 210) {
            //legit reply
            var content_type = req.getResponseHeader('Content-Type');
            if (!this.parseMultipartMixedMessage(content_type, response_text, req)) {
              this.emit("message", response_text || "", {'content-type': content_type, 'id': this.msgIdFromResponseHeaders(req)});
            }
            
            this.reqStartTime = new Date().getTime();
            this.req = nanoajax.ajax({url: this.url, headers: this.headers}, requestCallback);
          }
          else if((code == 0 && response_text == "Error" && req.readyState == 4) || (code === null && response_text != "Abort")) {
            console.log("abort!!!");
            this.emit("__disconnect", code || 0, response_text);
            delete this.req;
          }
          else if(code !== null) {
            //HTTP error
            this.emit("error", code, response_text);
            delete this.req;
          }
          else {
            //don't care about abortions 
            delete this.req;
            this.emit("__disconnect");
            console.log("abort!");
          }
        }.bind(this);
        
        this.reqStartTime = new Date().getTime();
        this.req = nanoajax.ajax({url: this.url, headers: this.headers}, requestCallback);
        this.emit("connect");
        
        return this;
      };
      
      Longpoll.prototype.parseMultipartMixedMessage = function(content_type, text, req) {
        var m = content_type && content_type.match(/^multipart\/mixed;\s+boundary=(.*)$/);
        if(!m) { 
          return false;
        }
        var boundary = m[1];
        
        var msgs = text.split("--" + boundary);
        if(msgs[0] != "" || !msgs[msgs.length-1].match(/--\r?\n/)) { throw "weird multipart/mixed split"; }
        
        msgs = msgs.slice(1, -1);
        for(var i in msgs) {
          m = msgs[i].match(/^(.*)\r?\n\r?\n([\s\S]*)\r?\n$/m);
          var hdrs = m[1].split("\n");
          
          var meta = {};
          for(var j in hdrs) {
            var hdr = hdrs[j].match(/^([^:]+):\s+(.*)/);
            if(hdr && hdr[1] == "Content-Type") {
              meta["content-type"] = hdr[2];
            }
          }
          
          if(i == msgs.length - 1) {
            meta["id"] = this.msgIdFromResponseHeaders(req);
          }
          this.emit('message', m[2], meta);
        }
        return true;
      };
      
      Longpoll.prototype.msgIdFromResponseHeaders = function(req) {
        var lastModified, etag;
        lastModified = req.getResponseHeader('Last-Modified');
        etag = req.getResponseHeader('Etag');
        if(lastModified) {
          return "" + Date.parse(lastModified)/1000 + ":" + (etag || "0");
        }
        else if(etag) {
          return etag;
        }
        else {
          return null;
        }
      };
      
      Longpoll.prototype.cancel = function() {
        if(this.req) {
          this.req.abort();
          delete this.req;
        }
        return this; 
      };
      
      return Longpoll;
    })(),
    
    'eventsource': (function() {
      function ESWrapper(emit) {
        EventSource;
        this.emit = emit;
      }
      
      ESWrapper.prototype.listen= function(url, msgid) {
        url = addLastMsgIdToQueryString(url, msgid);
        if(this.listener) {
          throw "there's a ES listener running already";
        }
        this.listener = new EventSource(url);
        var l = this.listener;
        l.onmessage = function(evt){
          console.log("message", evt);
          this.emit('message', evt.data, {id: evt.lastEventId});
        }.bind(this);
        
        l.onopen = function(evt) {
          this.reconnecting = false;
          console.log("connect", evt);
          this.emit('connect', evt);
        }.bind(this);
        
        l.onerror = function(evt) {
          //EventSource will try to reconnect by itself
          console.log("onerror", this.listener.readyState, evt);
          if(this.listener.readyState == EventSource.CONNECTING && !this.reconnecting) {
            if(!this.reconnecting) {
              this.reconnecting = true;
              this.emit('__disconnect', evt);
            }
          }
          else {
            this.emit('__disconnect', evt);
            console.log('other __disconnect', evt);
          }
        }.bind(this);
      };
      
      ESWrapper.prototype.cancel= function() {
        if(this.listener) {
          this.listener.close();
          delete this.listener;
        }
      };
      
      return ESWrapper;
    })(),
    
    'websocket': (function() {
      function WSWrapper(emit) {
        WebSocket;
        this.emit = emit;
      }
      
      WSWrapper.prototype.websocketizeURL = function(url) {
        var m = url.match(/^((\w+:)?\/\/([^\/]+))?(\/)?(.*)/);
        var protocol = m[2];
        var host = m[3];
        var absolute = m[4];
        var path = m[5];
        
        var loc;
        if(typeof window == "object") {
          loc = window.location;
        }
        
        if(!protocol && loc) {
          protocol = loc.protocol;
        }
        if(protocol == "https:") {
          protocol = "wss:";
        }
        else if(protocol == "http:") {
          protocol = "ws:";
        }
        else {
          protocol = "wss:"; //default setting: secure
        }
        
        if(!host && loc) {
          host = loc.host;
        }
        
        if(!absolute) {
          path = loc ? loc.pathname.match(/(.*\/)[^/]*/)[1] + path : "/" + path;
        }
        else {
          path = "/" + path;
        }
        
        return protocol + "//" + host + path;
      };
      
      WSWrapper.prototype.listen = function(url, msgid) {
        url = this.websocketizeURL(url);
        url = addLastMsgIdToQueryString(url, msgid);
        console.log(url);
        if(this.listener) {
          throw "websocket already listening";
        }
        this.listener = new WebSocket(url, 'ws+meta.nchan');
        var l = this.listener;
        l.onmessage = function(evt) {
          var m = evt.data.match(/^id: (.*)\n(content-type: (.*)\n)?\n/m);
          this.emit('message', evt.data.substr(m[0].length), {'id': m[1], 'content-type': m[3]});
        }.bind(this);
        
        l.onopen = function(evt) {
          this.emit('connect', evt);
          console.log("connect", evt);
        }.bind(this);
        
        l.onerror = function(evt) {
          console.log("error", evt);
          this.emit('error', evt, l);
          delete this.listener;
        }.bind(this);
        
        l.onclose = function(evt) {
          this.emit('__disconnect', evt);
          delete this.listener;
        }.bind(this);
      };
      
      WSWrapper.prototype.cancel = function() {
        if(this.listener) {
          this.listener.close();
          delete this.listener;
        }
      };
      
      return WSWrapper;
    })()
  };
})();

