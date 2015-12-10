#!/usr/bin/ruby
require 'typhoeus'
require 'json'
require 'pry'
require 'celluloid/current'
require 'date'
Typhoeus::Config.memoize = false
require 'celluloid/io'
require 'websocket'
require 'uri'

PUBLISH_TIMEOUT=3 #seconds

class Message
  attr_accessor :content_type, :message, :times_seen, :etag, :last_modified
  def initialize(msg, last_modified=nil, etag=nil)
    @times_seen=1
    @message, @last_modified, @etag = msg, last_modified, etag
  end
  def serverside_id
    timestamp=nil
    if last_modified
      timestamp = DateTime.httpdate(last_modified).to_time.utc.to_i
    end
    "#{timestamp}:#{etag}"
  end
  def id=(val)
    @id=val
  end
  def id
    @id||=serverside_id
  end
  def to_s
    @message
  end
end

class MessageStore
  include Enumerable
  attr_accessor :msgs, :quit_message, :name

  def matches? (other_msg_store)
    my_messages = messages
    if MessageStore === other_msg_store
      other_messages = other_msg_store.messages
      other_name = other_msg_store.name
    else
      other_messages = other_msg_store
      other_name = "?"
    end
    unless my_messages.count == other_messages.count 
      err =  "Message count doesn't match:\r\n"
      err << "#{self.name}: #{my_messages.count}\r\n"
      err << "#{self.to_s}\r\n"
      
      err << "#{other_name}: #{other_messages.count}\r\n"
      err << "#{other_msg_store.to_s}"
      return false, err
    end
    other_messages.each_with_index do |msg, i|
      return false, "Message #{i} doesn't match. (#{self.name} |#{my_messages[i].length}|, #{other_name} |#{msg.length}|) " if my_messages[i] != msg
    end
    true
  end

  def initialize(opt={})
    @array||=opt[:noid]
    clear
  end

  def messages
    self.to_a.map{|m|m.to_s}
  end

  #remove n oldest messages
  def remove_old(n=1)
    n.times {@msgs.shift}
    @msgs.count
  end
  
  def clear
    @msgs= @array ? [] : {}
  end
  
  def to_a
    @array ? @msgs : @msgs.values
  end
  def to_s
    buf=""
    each do |msg|
      m = msg.to_s
      m = m.length > 20 ? "#{m[0...20]}..." : m
      buf<< "<#{msg.id}> \"#{m}\" (count: #{msg.times_seen})\r\n"
    end
    buf
  end

  def each
    if @array
      @msgs.each {|msg| yield msg }
    else
      @msgs.each {|key, msg| yield msg }
    end
  end
  def <<(msg)
    if @array
      @msgs << msg
    else
      if (cur_msg=@msgs[msg.id])
        puts "Different messages with same id: #{msg.id}, \"#{msg.to_s}\" then \"#{cur_msg.to_s}\"" unless cur_msg.message == msg.message
        cur_msg.times_seen+=1
        cur_msg.times_seen
      else
        @msgs[msg.id]=msg
        1
      end
    end
  end
end

class Subscriber
  class WebSocketClient
    #a little sugar for handshake errors
    
    #class WebSocket::EventMachine::Client
    #  attr_accessor :handshake # we want this for erroring
    #  attr_accessor :last_event_time
    #end
    
    class WebSocket::Handshake::Client
      attr_accessor :data
      def response_code(what=:code)
        resp=@data.match(/^HTTP\/1.1 (?<code>\d+) (?<line>[^\\\r\\\n]+)/)
        resp[what]
      end
      def response_line
        response_code :line
      end
    end
    
    class WebSocketErrorResponse
      attr_accessor :code, :msg, :connected
      def initialize(code, msg, connected)
        self.code = code
        self.msg = msg
        self.connected = connected
      end
    end
    
    class WebSocketBundle
      attr_accessor :ws, :sock, :last_message_time
      def initialize(handshake, sock)
        @buf=""
        self.ws = WebSocket::Frame::Incoming::Client.new(version: handshake.version)
        self.sock = sock
      end
      
      def read
        @buf.clear
        ws << sock.readpartial(4096, @buf)
      end
      
      def next
        ws.next
      end
    end
    
    include Celluloid::IO
    
    attr_accessor :last_modified, :etag, :timeout
    attr_accessor :active_connections
    def initialize(subscr, opt={})
      @last_modified, @etag, @timeout = opt[:last_modified], opt[:etag], opt[:timeout].to_i || 10
      @connect_timeout = opt[:connect_timeout]
      @subscriber=subscr
      @url=subscr.url
      @url = @url.gsub(/^http(s)?:/, "ws\\1:")
      
      @concurrency=(opt[:concurrency] || opt[:clients] || 1).to_i
      @retry_delay=opt[:retry_delay]
      @ws = {}
      @connected=0
      current_actor.active_connections = 0
      @nomsg = opt[:nomsg]
    end
    
    def try_halt
      @disconnected ||= 0
      @disconnected += 1
      if @disconnected == @concurrency
        halt
      end
    end
    
    def halt
      @halting = true
    end
    
    def run(was_success = nil)
      uri = URI.parse(@url)
      port = uri.port || (uri.scheme == "ws" ? 80 : 443)
      @ready=Celluloid::Condition.new
      @connected = @concurrency
      @concurrency.times do
        if uri.scheme == "ws"
          sock = Celluloid::IO::TCPSocket.new(uri.host, port)
        elsif uri.scheme == "wss"
          sock = Celluloid::IO::SSLSocket.new(uri.host, port)
        else
          raise ArgumentError, "invalid websocket scheme #{uri.scheme} in #{@url}"
        end
          
        handshake = WebSocket::Handshake::Client.new(url: @url)
        sock << handshake.to_s
        
        #handshake response
        loop do
          handshake << sock.readline
          if handshake.finished?
            unless handshake.valid?
              @subscriber.on_failure WebSocketErrorResponse.new(handshake.response_code, handshake.response_line, false)
            end
            break
          end
        end
        
        if handshake.valid?
          bundle = WebSocketBundle.new(handshake, sock)
          @ws[bundle]=true
          async.listen bundle
        end
      end
    end
    
    def listen(bundle)
      loop do
        begin
          bundle.read
          while msg = bundle.next do
            if on_message(msg.data, msg.type, bundle) == false
              close bundle
            end
          end
        rescue EOFError
          puts "*** disconnected"
          socket.close
        end
      end
    end
    
    def on_open(*args)
      current_actor.active_connections += 1
      puts "open #{current_actor.active_connections}"
    end
    
    def on_error(err, err2)
      puts "Received error #{err}"
      if !@connected[ws]
        @subscriber.on_failure WebSocketErrorResponse.new(ws.handshake.response_code, ws.handshake.response_line, @connected[ws])
        try_halt
      end
    end
    
    def on_message(data, type, bundle)
      #puts "Received message: #{data} type: #{type}"
      msg= @nomsg ? data : Message.new(data)
      bundle.last_message_time=Time.now.to_f
      @subscriber.on_message(msg, bundle)
    end
    
    
    def close(bundle)
      @ws.delete bundle
      @connected -= 1
      if @connected == 0
        binding.pry unless @ws.count == 0
        puts "the future is now!"
        #binding.pry
        @ready.signal true
      end
    end
    
    def poke
      @connected > 0 && @ready.wait
    end
  end
  
  class LongPollClient
    include Celluloid
    attr_accessor :last_modified, :etag, :hydra, :timeout
    def initialize(subscr, opt={})
      @last_modified, @etag, @timeout = opt[:last_modified], opt[:etag], opt[:timeout].to_i || 10
      @connect_timeout = opt[:connect_timeout]
      @subscriber=subscr
      @url=subscr.url
      @concurrency=opt[:concurrency] || opt[:clients] || 1
      @hydra= Typhoeus::Hydra.new( max_concurrency: @concurrency, pipelining: opt[:pipelining])
      @gzip=opt[:gzip]
      @retry_delay=opt[:retry_delay]
      @nomsg=opt[:nomsg]
    end
    
    def response_success(response, req)
      #puts "received OK response at #{req.url}"
      #parse it
      unless @nomsg
        msg=Message.new response.body, response.headers["Last-Modified"], response.headers["Etag"]
        msg.content_type=response.headers["Content-Type"]
        req.options[:headers]["If-None-Match"]=msg.etag
        req.options[:headers]["If-Modified-Since"]=msg.last_modified
      else
        msg=response.body
        req.options[:headers]["If-None-Match"] = response.headers["Etag"]
        req.options[:headers]["If-Modified-Since"] = response.headers["Last-Modified"]
      end
      
      unless @subscriber.on_message(msg, req) == false
        @subscriber.waiting+=1
        Celluloid.sleep @retry_delay if @retry_delay
        @hydra.queue new_request(old_request: req)
      else
        @subscriber.finished+=1
      end
    end
    
    def queue_and_run(req)
      queue_size = @hydra.queued_requests.count
      @hydra.queue new_request(old_request: req)
      if queue_size == 0
        #puts "rerun"
        #binding.pry
        @hydra.run
      end
    end
    
    def response_failure(response, req)
      #puts "received bad or no response at #{req.url}"
      unless @subscriber.on_failure(response) == false
        @subscriber.waiting+=1
        Celluloid.sleep @retry_delay if @retry_delay
        @hydra.queue  new_request(old_request: req)
      else
        @subscriber.finished+=1
      end
    end
    
    def new_request(opt = {})
      headers = {}
      headers["User-Agent"] = opt[:useragent] if opt[:useragent]
      if opt[:old_request]
        #req = Typhoeus::Request.new(opt[:old_request].url, opt[:old_request].options)
        
        #reuse request
        req = opt[:old_request]
      else
        req = Typhoeus::Request.new(@url, timeout: @timeout, connecttimeout: @connect_timeout, accept_encoding: (@gzip ? "gzip" : nil), headers: headers )
          req.on_complete do |response|
          @subscriber.waiting-=1
          if response.success?
            response_success response, req
          else
            response_failure response, req
          end
        end
      end
      
      req
    end

    def run(was_success=nil)
      #puts "running #{self.class.name} hydra with #{@hydra.queued_requests.count} requests."
      (@concurrency - @hydra.queued_requests.count).times do |n|
        @subscriber.waiting+=1
        @hydra.queue new_request(useragent: "pubsub.rb #{self.class.name} ##{n}")
      end
      
      @hydra.run
    end
    
    def poke
      #while @subscriber.finished < @concurrency
      #  Celluloid.sleep 0.1
      #end
    end
  end
  
  class EventSourceClient < LongPollClient
    def initialize(subscr, opt={})
      @ready = Celluloid::Future.new
      @buf={}
      opt.merge(pipelining: 0)
      super
    end
    
    def buf(req)
      @buf[req]
    end
    
    def reset_bufs(req)
      @buf[req]||={}
      @buf[req][:data] = []
      @buf[req][:event_id] = []
      @buf[req][:comments] = []
      @buf[req][:retry_timeout] = nil
      @buf[req][:event] = nil
    end
    
    def wait_until_ready
      while !@ready do
        sleep 0.3
      end
      self
    end
    
    def new_request(opt = {})
      req = super
      
      req.options[:headers]["Accept"] = "text/event-stream"
      
      req.on_headers do |headers|
        if @subscriber.waiting == @concurrency
          @ready = true
        end
      end
      
      req.on_body do |body|
        parse_body body, req
      end
      
      reset_bufs req
      
      req
    end
    
    def parse_event(req)
      bufs = buf req
      
      if bufs[:data].length > 0
        unless @nomsg
          msg=Message.new bufs[:data].join("\n")
          msg.id= bufs[:id] if bufs[:id]
        else
          msg = bufs[:data].join("\n")
        end
        
        if @subscriber.on_message(msg, req) == false
          @subscriber.finished+=1
          return :abort
        else
          req.original_options[:last_msg_time]=Time.now.to_f
        end
      end
      
      reset_bufs req
    end
    
    def parse_body(body, req)
      #puts body
      lines = body.lines
      
      ret = nil
      
      lines.each do |line|
        case line
        when /^: ?(.*)/
          buf(req)[:comments] << $1
        when /^data(: (.*))?/
          buf(req)[:data] << $2 or ""
        when /^id(: (.*))?/
          buf(req)[:id] = $2 or ""
        when /^event(: (.*))?/
          buf(req)[:event] = $2 or ""
        when /^retry: (.*)/
          buf(req)[:retry_timeout] = $1
        when /^$/
          ret = parse_event req
        end
      end
      
      ret
    end
  end
  
  class IntervalPollClient < LongPollClient
    def initialize(subscr, opt={})
      @last_modified=nil
      @etag=nil
      super
    end
    
    def store_msg_id(response, req)
      @last_modified=response.headers["Last-Modified"] if response.headers["Last-Modified"]
      @etag=response.headers["Etag"] if response.headers["Etag"]
      req.options[:headers]["If-Modified-Since"]=@last_modified
      req.options[:headers]["If-None-Match"]=@etag
    end
    
    def response_success(response, req)
      store_msg_id(response, req)
      super response, req
    end
    def response_failure(response, req)
      if @subscriber.on_failure(response) != false
        @subscriber.waiting+=1
        Celluloid.sleep @retry_delay if @retry_delay
        @hydra.queue req
      else
        @subscriber.finished+=1
      end
    end
    
    def run
      super
    end
    
    def poke
      while @subscriber.finished < @concurrency do
        Celluloid.sleep 0.3
      end
    end
  end
  
  attr_accessor :url, :client, :messages, :max_round_trips, :quit_message, :errors, :concurrency, :waiting, :finished, :client_class
  def initialize(url, concurrency=1, opt={})
    @care_about_message_ids=opt[:use_message_id].nil? ? true : opt[:use_message_id]
    @url=url
    @timeout=opt[:timeout] || 30
    @connect_timeout=opt[:connect_timeout] || 5
    @quit_message=opt[:quit_message]
    @gzip=opt[:gzip]
    @retry_delay=opt[:retry_delay]
    #puts "Starting subscriber on #{url}"
    case opt[:client]
    when :longpoll, :long, nil
      @client_class=LongPollClient
    when :interval, :intervalpoll
      @client_class=IntervalPollClient
    when :ws, :websocket
      @care_about_message_ids = false
      @client_class=WebSocketClient
    when :es, :eventsource, :sse
      @client_class=EventSourceClient
    else
      raise "unknown client type #{opt[:client]}"
    end
    @nomsg=opt[:nomsg]
    @nostore=opt[:nostore]
    if !@nostore && @nomsg
      @nomsg = nil
      puts "nomsg reverted to false because nostore is false"
    end
    @concurrency=concurrency
    @client_class ||= opt[:client_class] || LongPollClient
    reset
    new_client @client_class
  end
  def new_client(client_class=LongPollClient)
    @client=client_class.new(self, concurrency: @concurrency, timeout: @timeout, connect_timeout: @connect_timeout, gzip: @gzip, retry_delay: @retry_delay, nomsg: @nomsg)
  end
  def reset
    @errors=[]
    unless @nostore
      @messages=MessageStore.new :noid => !@care_about_message_ids
      @messages.name="sub"
    end
    @waiting=0
    @finished=0
    new_client(@client_class) if terminated?
    self
  end
  def abort
    @client.terminate
  end
  def errors?
    not no_errors?
  end
  def no_errors?
    @errors.empty?
  end
  def match_errors(regex)
    @errors.each do |err|
      return false unless err =~ regex
    end
    true
  end
  
 
  def run
    begin
      client.current_actor
    rescue Celluloid::DeadActorError
      return false
    end
    @client.async.run
    self
  end
  def terminate
    begin
      @client.terminate
    rescue Celluloid::DeadActorError
      return false
    end
    true
  end
  def terminated?
    begin
      client.current_actor unless client == nil
    rescue Celluloid::DeadActorError
      return true
    end
    false
  end
  def wait
    @client.poke
  end

  def on_message(msg=nil, req=nil, &block)
    #puts "received message #{msg.to_s[0..15]}"
    if block_given?
      @on_message=block
    else
      @messages << msg if @messages
      if @quit_message == msg.to_s
        return false 
      end
      @on_message.call(msg, req) if @on_message
    end
  end
  
  def on_failure(response=nil, &block)
    if block_given?
      @on_failure=block
    else
      case client
      when LongPollClient, IntervalPollClient
        #puts "failed with #{response.to_s}. handler is #{@on_failure.to_s}"
        if response.timed_out?
          # aw hell no
          @errors << "Client response timeout."
        elsif response.code == 0
          # Could not get an http response, something's wrong.
          @errors << response.return_message
        else
          # Received a non-successful http response.
          if response.code != 200 #eventsource can be triggered to quite with a 200, which is not an error
            @errors << "HTTP request failed: #{response.return_message} (code #{response.code})"
          end
        end
      when WebSocketClient
        error = response.connected ? "Websocket connection failed: " : "Websocket handshake failed: "
        error << "#{response.msg} (code #{response.code})"
        @errors << error
      else
        binding.pry
      end
      @on_failure.call(response) if @on_failure.respond_to? :call
    end
  end
end

class Publisher
  #include Celluloid
  attr_accessor :messages, :response, :response_code, :response_body, :nofail, :accept
  def initialize(url, opt={})
    @url= url
    unless opt[:nostore]
      @messages = MessageStore.new :noid => true
      @messages.name = "pub"
    end
    @timeout = opt[:timeout]
  end
  
  def on_complete(&block)
    raise ArgumentError, "block must be given" unless block
    @on_complete = block
  end
  
  def submit(body, method=:POST, content_type= :'text/plain', &block)
    self.response=nil
    self.response_code=nil
    self.response_body=nil

    if Enumerable===body
      i=0
      body.each{|b| i+=1; submit(b, method, content_type, &block)}
      return i
    end
    post = Typhoeus::Request.new(
      @url,
      headers: {:'Content-Type' => content_type, :'Accept' => accept},
      method: method,
      body: body,
      timeout: @timeout || PUBLISH_TIMEOUT,
      connecttimeout: @timeout || PUBLISH_TIMEOUT
    )
    if @messages
      msg=Message.new body
      msg.content_type=content_type
    end
    if @on_complete
      post.on_complete @on_complete
    else
      post.on_complete do |response|
        self.response=response
        self.response_code=response.response_code
        self.response_body=response.response_body
        if response.success?
          #puts "published message #{msg.to_s[0..15]}"
          @messages << msg if @messages
        elsif response.timed_out?
          # aw hell no
          #puts "publisher err: timeout"

          url=URI.parse(response.request.url)
          url = "#{url.path}#{url.query ? "?#{url.query}" : nil}"
          raise "Publisher #{response.request.options[:method]} to #{url} timed out."
        elsif response.code == 0
          # Could not get an http response, something's wrong.
          #puts "publisher err: #{response.return_message}"
          errmsg="No HTTP response: #{response.return_message}"
          unless self.nofail then
            raise errmsg
          end
        else
          # Received a non-successful http response.
          #puts "publisher err: #{response.code.to_s}"
          errmsg="HTTP request failed: #{response.code.to_s}"
          unless self.nofail then
            raise errmsg
          end
        end
        block.call(self) if block
      end
    end
    #puts "publishing to #{@url}"
    begin
      post.run
    rescue Exception => e
      last=nil, i=0
      e.backtrace.select! do |bt|
        if bt.match(/(gems\/(typhoeus|ethon)|pubsub\.rb)/)
          last=i
          false
        else
          i+=1
          true
        end 
      end
      e.backtrace.insert last, "..."
      raise e
    end
  end
  
  def get(accept_header=nil)
    self.accept=accept_header
    submit nil, :GET
    self.accept=nil
  end
  def delete
    submit nil, :DELETE
  end
  def post(body, content_type=nil, &block)
    submit body, :POST, content_type, &block
  end
  def put(body, content_type=nil, &block)
    submit body, :PUT, content_type, &block
  end

  
end
