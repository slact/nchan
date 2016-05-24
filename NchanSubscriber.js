/*
 * NchanSubscriber
 * usage: var sub = new NchanSubscriber(url, opt);
 * 
 * opt = {
 *   subscriber: 'longpoll', 'eventsource', or 'websocket',
 *     //or an array of the above indicating subscriber type preference
 *   reconnect: undefined or 'session' or 'persist'
 *     //if the HTML5 sessionStore or localStore should be used to resume
 *     //connections interrupted by a page load
 *   shared: true or undefined
 *     //share connection to same subscriber url between browser windows and tabs 
 *     //using localStorage.
 * }
 * 
 * sub.on("message", function(message, message_metadata) {
 *   // message is a string
 *   // message_metadata may contain 'id' and 'content-type'
 * });
 * 
 * sub.on('connect', function(evt) {
 *   //fired when first connected. 
 * }
 * 
 * sub.on('disconnect', function(evt) {
 *   // when disconnected.
 * }
 * 
 * sub.on('error', function(code, message) {
 *   //error callback. not sure about the parameters yet
 * }
 * 
 * sub.reconnect; // should subscriber try to reconnect? true by default.
 * sub.reconnectTimeout; //how long to wait to reconnect? does not apply to EventSource
 * sub.lastMessageId; //last message id. useful for resuming a connection without loss or repetition.
 * 
 * sub.start(); // begin (or resume) subscribing
 * sub.stop(); // stop subscriber. do not reconnect.
 */

//Thanks Darren Whitlen ( @prawnsalad ) for your feedback

(function(global, undefined) {

// https://github.com/yanatan16/nanoajax
!function(t,e){function n(t){return t&&e.XDomainRequest&&!/MSIE 1/.test(navigator.userAgent)?new XDomainRequest:e.XMLHttpRequest?new XMLHttpRequest:void 0}function o(t,e,n){t[e]=t[e]||n}var r=["responseType","withCredentials","timeout","onprogress"];t.ajax=function(t,a){function s(t,e){return function(){c||(a(void 0===f.status?t:f.status,0===f.status?"Error":f.response||f.responseText||e,f),c=!0)}}var u=t.headers||{},i=t.body,d=t.method||(i?"POST":"GET"),c=!1,f=n(t.cors);f.open(d,t.url,!0);var l=f.onload=s(200);f.onreadystatechange=function(){4===f.readyState&&l()},f.onerror=s(null,"Error"),f.ontimeout=s(null,"Timeout"),f.onabort=s(null,"Abort"),i&&(o(u,"X-Requested-With","XMLHttpRequest"),e.FormData&&i instanceof e.FormData||o(u,"Content-Type","application/x-www-form-urlencoded"));for(var p,m=0,v=r.length;v>m;m++)p=r[m],void 0!==t[p]&&(f[p]=t[p]);for(var p in u)f.setRequestHeader(p,u[p]);return f.send(i),f},e.nanoajax=t}({},function(){return this}());

// https://github.com/component/emitter
function Emitter(t){return t?mixin(t):void 0}function mixin(t){for(var e in Emitter.prototype)t[e]=Emitter.prototype[e];return t}Emitter.prototype.on=Emitter.prototype.addEventListener=function(t,e){return this._callbacks=this._callbacks||{},(this._callbacks["$"+t]=this._callbacks["$"+t]||[]).push(e),this},Emitter.prototype.once=function(t,e){function i(){this.off(t,i),e.apply(this,arguments)}return i.fn=e,this.on(t,i),this},Emitter.prototype.off=Emitter.prototype.removeListener=Emitter.prototype.removeAllListeners=Emitter.prototype.removeEventListener=function(t,e){if(this._callbacks=this._callbacks||{},0==arguments.length)return this._callbacks={},this;var i=this._callbacks["$"+t];if(!i)return this;if(1==arguments.length)return delete this._callbacks["$"+t],this;for(var r,s=0;s<i.length;s++)if(r=i[s],r===e||r.fn===e){i.splice(s,1);break}return this},Emitter.prototype.emit=function(t){this._callbacks=this._callbacks||{};var e=[].slice.call(arguments,1),i=this._callbacks["$"+t];if(i){i=i.slice(0);for(var r=0,s=i.length;s>r;++r)i[r].apply(this,e)}return this},Emitter.prototype.listeners=function(t){return this._callbacks=this._callbacks||{},this._callbacks["$"+t]||[]},Emitter.prototype.hasListeners=function(t){return!!this.listeners(t).length};

var ughbind = (Function.prototype.bind
  ? function ughbind(fn, thisObj) {
    return fn.bind(thisObj);
  }
  : function ughbind(fn, thisObj) {
    return function() {
      fn.apply(thisObj, arguments);
    };
  }
);

"use strict"; 
function NchanSubscriber(url, opt) {
  if(this === window) {
    throw "use 'new NchanSubscriber(...)' to initialize";
  }
  
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
  
  if(opt.shared) {
    this.shared = true;
    
    var pre = "NchanSubscriber:" + url + ":shared:";
    var keys = {
      status: pre + "status",
      slaves: pre + "slaves",
      masterLottery: pre + "lottery",
      masterLotteryTime: pre + "lotteryTime",
      
      msg: pre + "msg",
      msgId: pre + "msg:id",
      msgContentType: pre + "msg:content-type",
      
      error: pre + "error"
    };
    this.storageKeys = this.storageKeys;
    
    var storage = global.localStorage;
    this.sharedStorage = storage;
    this.sharedKeys = keys;
    
    var status = this.sharedStorage.getItem(this.sharedKeys.status);
    global.addEventListener("storage", ughbind(function(ev) {
      if(ev.key == this.sharedKeys.status) {
        if(ev.newValue == "disconnected" && this.sharedRole == "slave") {
          //play the promotion lotery
          this.maybePromoteToMaster();
        }
      }
    }, this));
    if(status == "disconnected") {
      this.maybePromoteToMaster();
    }
    else {
      this.sharedRole = status ? "slave" : "master";
    }
  }
  
  this.initializeTransport(opt.subscriber);
  
  this.lastMessageId = opt.id || opt.msgId;
  this.reconnect = typeof opt.reconnect == "undefined" ? true : opt.reconnect;
  this.reconnectTimeout = opt.reconnectTimeout || 10;
  
  
  var saveConnectionState;
  if(!opt.reconnect) {
    saveConnectionState = function() {};
  }
  else {
    var index = "NchanSubscriber:" + url + ":lastMessageId";
    var storage;
    if(opt.reconnect == "persist") {
      storage = global.localStorage;
      if(!storage)
        throw "can't use reconnect: 'persist' option: localStorage not available";
    }
    else if(opt.reconnect == "session") {
      storage = global.sessionStorage;
      if(!storage)
        throw "can't use reconnect: 'session' option: sessionStorage not available";
    }
    else {
      throw "invalid 'reconnect' option value " + opt.reconnect;
    }
    saveConnectionState = ughbind(function(msgid) {
      if(this.sharedRole != "slave") {
        storage.setItem(index, msgid);
      }
    }, this);
    this.lastMessageId = storage.getItem(index);
  }
  
  var notifySharedSubscribers = function(){};
  if(opt.shared) {
    var onUnloadEvent = ughbind(function(ev) {
      console.log("unloadEvent");
      if(this.running) {
        this.stop();
      }
      if(this.sharedRole == "master") {
        storage.setItem(this.sharedKeys.status, "disconnected");
      }
    }, this);
    
    global.addEventListener('beforeunload', onUnloadEvent, false);
    // swap `beforeunload` to `unload` after DOM is loaded
    global.addEventListener('DOMContentLoaded', function() {
      global.removeEventListener('beforeunload', onUnloadEvent, false);
      global.addEventListener('unload', onUnloadEvent, false);
    }, false);
    
    notifySharedSubscribers = ughbind(function(name, data) {
      if(this.sharedRole != "master") {
        return;
      }
      
      console.log("notifySharedSubscribers", name, data);
      
      if(name == "message") {
        storage.setItem(this.sharedKeys.msgId, data[1] && data[1].id || "");
        storage.setItem(this.sharedKeys.msgContentType, data[1] && data[1]["content-type"] || "");
        storage.setItem(this.sharedKeys.msg, data[0]);
      }
      else if(name == "error") {
        //TODO 
      }
      else if(name == "connecting") {
        storage.setItem(this.sharedKeys.status, "connecting");
      }
      else if(name == "connect") {
        storage.setItem(this.sharedKeys.status, "connected");
      }
      else if(name == "reconnect") {
        console.log("setItem reconnect");
        storage.setItem(this.sharedKeys.status, "reconnecting");
      }
      else if(name == "disconnect") {
        console.log("setItem disconnect");
        storage.setItem(this.sharedKeys.status, "disconnected");
      }
    }, this);
  }
  
  
  var restartTimeoutIndex;
  var stopHandler = ughbind(function() {
    if(!restartTimeoutIndex && this.running && this.reconnect && !this.transport.reconnecting) {
      notifySharedSubscribers("reconnect");
      restartTimeoutIndex = setTimeout(ughbind(function() {
        restartTimeoutIndex = null;
        this.start();
      }, this), this.reconnectTimeout);
    }
    else {
      notifySharedSubscribers("disconnect");
    }
  }, this);
  
  this.on("message", function msg(msg, meta) {
    this.lastMessageId=meta.id;
    if(meta.id) {
      saveConnectionState(meta.id);
    }
    notifySharedSubscribers("message", [msg, meta]);
    //console.log(msg, meta);
  });
  this.on("error", function fail(code, text) {
    stopHandler(code, text);
    notifySharedSubscribers("error", [code, text]);
    //console.log("failure", code, text);
  });
  this.on("connect", function() {
    this.connected = true;
    notifySharedSubscribers("connect");
  });
  this.on("__disconnect", function fail(code, text) {
    this.connected = false;
    this.emit("disconnect", code, text);
    stopHandler(code, text);
    //console.log("__disconnect", code, text);
  });
  
  /*
  //explicitly stop just before leaving the page
  ['unload', 'beforeunload'].each(function(ev) {
    window.addEvent(ev, this.stop.bind(this));
  }.bind(this));
  */
}

Emitter(NchanSubscriber.prototype);

NchanSubscriber.prototype.initializeTransport = function(possibleTransports) {
  if(possibleTransports) {
    this.desiredTransport = possibleTransports;
  }
  if(this.sharedRole == "slave") {
    this.transport = new this.SubscriberClass["__slave"](ughbind(this.emit, this)); //try it
  }
  else {
    var tryInitializeTransport = ughbind(function(name) {
      if(!this.SubscriberClass[name]) {
        throw "unknown subscriber type " + name;
      }
      try {
        this.transport = new this.SubscriberClass[name](ughbind(this.emit, this)); //try it
        return this.transport;
      } catch(err) { /*meh...*/ }
    }, this);
    
    var i;
    if(this.desiredTransport) {
      for(i in this.desiredTransport) {
        if(tryInitializeTransport(this.desiredTransport[i])) {
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
  }
  if(! this.transport) {
    throw "can't use any transport type";
  }
};

NchanSubscriber.prototype.maybePromoteToMaster = function() {
  var processRoll;
  
  var lotteryRoundDuration = 500;
  var currentContenders = 0;
  
  //roll the dice
  var roll = Math.random();
  this.sharedStorage.setItem(this.sharedKeys.masterLottery, roll);
  this.sharedStorage.setItem(this.sharedKeys.masterLotteryTime, new Date().getTime() / 1000);
  var bestRoll = roll;
  
  var checkRollInterval;
  var checkRoll = ughbind(function() {
    var latestSharedRoll = parseFloat(this.sharedStorage.getItem(this.sharedKeys.masterLottery));
    if(latestSharedRoll && (!bestRoll || latestSharedRoll > bestRoll)) {
      bestRoll = latestSharedRoll;
    }
    console.log("check roll", roll, bestRoll, "lastestSharedRoll", latestSharedRoll);
    processRoll();
  }, this);
  
  var rollCallback = ughbind(function(ev) {
    if(ev.key != this.sharedKeys.masterLottery)
      return;
    console.log("rollCallback ev", ev, bestRoll);
    
    if(ev.newValue) {
      currentContenders += 1;
      var newVal = parseFloat(ev.newValue);
      if(!bestRoll || newVal >= bestRoll) {
        bestRoll = newVal;
        console.log("got better bestRoll: " + ev.newValue);
      }
    }
  }, this);
  global.addEventListener("storage", rollCallback);
  
  var finish = ughbind(function() {
    global.removeEventListener("storage", rollCallback);
    if(checkRollInterval) {
      clearInterval(checkRollInterval);
    }
    if(this.sharedRole == "master") {
      this.sharedStorage.removeItem(this.sharedKeys.masterLottery);
      this.sharedStorage.removeItem(this.sharedKeys.masterLotteryTime);
      if(this.running) {
        this.stop();
      }
      this.initializeTransport();
      this.start();
    }
  }, this);
  
  processRoll = ughbind(function() {
    console.log("process roll (" + roll + ", best " + bestRoll + ")");
    if(roll < bestRoll) {
      //loser
      console.log("loser");
      this.sharedRole = "slave";
      finish();
    }
    else if(roll >= bestRoll) {
      var lotteryTime = parseFloat(this.sharedStorage.getItem(this.sharedKeys.masterLotteryTime));
      var now = new Date().getTime() / 1000;
      console.log(lotteryTime, now - lotteryRoundDuration/1000);
      if(currentContenders == 0) {
        console.log("winner, no more contenders!");
        this.sharedRole = "master";
        finish();
      }
      else {
        console.log("winning, but have contenders", currentContenders);
        currentContenders = 0;
      }
    }
  }, this);
  
  checkRollInterval = setInterval(checkRoll, lotteryRoundDuration);
};

NchanSubscriber.prototype.start = function() {
  if(!this.shared || this.sharedRole == "master") {
    if(this.shared) {
      this.sharedStorage.setItem(this.sharedKeys.status, "connecting");
    }
    this.transport.listen(this.url, this.lastMessageId);
  }
  else if(this.sharedRole == "slave") {
    this.transport.listen(this.url, this.sharedKeys);
  }
  this.running = true;
  return this;
};

NchanSubscriber.prototype.stop = function() {
  this.running = false;
  if(!this.slave) {
    this.transport.cancel();
  }
  return this;
};

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
      var setHeader = ughbind(function(incoming, name) {
        if(incoming) { this.headers[name]= incoming; }
      }, this);
      
      if(msgid) {
        this.headers = {"Etag": msgid};
      }
      
      this.reqStartTime = new Date().getTime();
      
      var  requestCallback;
      requestCallback = ughbind(function (code, response_text, req) {
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
          //console.log("abort!!!");
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
          //console.log("abort!");
        }
      }, this);
      
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
      l.onmessage = ughbind(function(evt){
        //console.log("message", evt);
        this.emit('message', evt.data, {id: evt.lastEventId});
      }, this);
      
      l.onopen = ughbind(function(evt) {
        this.reconnecting = false;
        //console.log("connect", evt);
        this.emit('connect', evt);
      }, this);
      
      l.onerror = ughbind(function(evt) {
        //EventSource will try to reconnect by itself
        //console.log("onerror", this.listener.readyState, evt);
        if(this.listener.readyState == EventSource.CONNECTING && !this.reconnecting) {
          if(!this.reconnecting) {
            this.reconnecting = true;
            this.emit('__disconnect', evt);
          }
        }
        else {
          this.emit('__disconnect', evt);
          //console.log('other __disconnect', evt);
        }
      }, this);
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
      //console.log(url);
      if(this.listener) {
        throw "websocket already listening";
      }
      this.listener = new WebSocket(url, 'ws+meta.nchan');
      var l = this.listener;
      l.onmessage = ughbind(function(evt) {
        var m = evt.data.match(/^id: (.*)\n(content-type: (.*)\n)?\n/m);
        this.emit('message', evt.data.substr(m[0].length), {'id': m[1], 'content-type': m[3]});
      }, this);
      
      l.onopen = ughbind(function(evt) {
        this.emit('connect', evt);
        //console.log("connect", evt);
      }, this);
      
      l.onerror = ughbind(function(evt) {
        //console.log("error", evt);
        this.emit('error', evt, l);
        delete this.listener;
      }, this);
      
      l.onclose = ughbind(function(evt) {
        this.emit('__disconnect', evt);
        delete this.listener;
      }, this);
    };
    
    WSWrapper.prototype.cancel = function() {
      if(this.listener) {
        this.listener.close();
        delete this.listener;
      }
    };
    
    return WSWrapper;
  })(),
  
  '__slave': (function() {
    function LocalStoreSlaveTransport(emit) {
      this.emit = emit;
    }
    
    LocalStoreSlaveTransport.prototype.listen = function(url, keys) {
      var pre = "NchanSubscriber:" + url + ":shared:";
      this.keys = keys;
      
      var storage = global.localStorage;
      
      this.statusChangeChecker = ughbind(function(ev) {
        if(ev.key == this.keys.msg) {
          var msgId = storage.getItem(this.keys.msgId);
          var contentType = storage.getItem(this.keys.msgContentType);
          this.emit('message', storage.getItem(this.keys.msg), {'id': msgId == "" ? undefined : msgId, 'content-type': contentType == "" ? undefined : contentType});
        }
      }, this);
      global.addEventListener("storage", this.statusChangeChecker);
    };
      
    LocalStoreSlaveTransport.prototype.cancel = function() {
      global.removeEventListener("storage", this.statusChangeChecker);
    };
    
    return LocalStoreSlaveTransport;
  })()

};

global.NchanSubscriber = NchanSubscriber;
})(window);
