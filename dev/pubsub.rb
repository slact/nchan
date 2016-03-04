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
require "http/parser"
require "http/2"

SUBSCRIBER_TYPES=[ :longpoll, :intervalpoll, :eventsource, :websocket ]
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
  def unique_id
    if id.include? ","
      time, etag = id.split ":"
      etag = etag.split(",").map{|x| x[0] == "[" ? x : "?"}.join "," #]
      [time, etag].join ":"
    else
      id
    end
  end
  def to_s
    @message
  end
  
  def self.each_multipart_message(content_type, body)
    content_type = content_type.last if Array === content_type 
    matches=/^multipart\/mixed; boundary=(?<boundary>.*)/.match content_type
    
    if matches
      splat = body.split(/^--#{Regexp.escape matches[:boundary]}-?-?\r?\n?/)
      splat.shift
      
      splat.each do |v|
        mm=(/(Content-Type:\s(?<content_type>.*?)\r\n)?\r\n(?<body>.*)\r\n/m).match v
        yield mm[:content_type], mm[:body], true
      end
      
    else
      yield content_type, body
    end
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

  def [](i)
    @msgs[i]
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
      if (cur_msg=@msgs[msg.unique_id])
        puts "Different messages with same id: #{msg.id}, \"#{msg.to_s}\" then \"#{cur_msg.to_s}\"" unless cur_msg.message == msg.message
        cur_msg.times_seen+=1
        cur_msg.times_seen
      else
        @msgs[msg.unique_id]=msg
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
      @cooked=Celluloid::Condition.new
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
        @cooked.signal true
      end
    end
    
    def poke
      @connected > 0 && @cooked.wait
    end
  end
  
  class FastLongPollClient
    include Celluloid::IO
    
    class HTTPBundle
      attr_accessor :parser, :sock, :last_message_time, :done, :time_requested, :request_time
      def initialize(uri, sock, user_agent)
        @rcvbuf=""
        @sndbuf=""
        @parser = Http::Parser.new
        @sock = sock
        @done = false
        @send_noid_str= <<-END.gsub(/^ {10}/, '')
          GET #{uri.path} HTTP/1.1
          Host: #{uri.host}#{uri.default_port == uri.port ? "" : ":#{uri.port}"}
          Accept: */*
          User-Agent: #{user_agent || "HTTPBundle"}
          
        END
        
        @send_withid_fmt= <<-END.gsub(/^ {10}/, '')
          GET #{uri.path} HTTP/1.1
          Host: #{uri.host}#{uri.default_port == uri.port ? "" : ":#{uri.port}"}
          Accept: */*
          User-Agent: #{user_agent || "HTTPBundle"}
          If-Modified-Since: %s
          If-None-Match: %s
          
        END
      end
      
      def send_GET(msg_time=nil, msg_tag=nil)
        @sndbuf.clear
        if msg_time
          @sndbuf << sprintf(@send_withid_fmt, msg_time, msg_tag)
        else
          @sndbuf << @send_noid_str
        end
        @time_requested=Time.now.to_f
        @sock << @sndbuf
      end
      
      def read
        @rcvbuf.clear
        sock.readpartial(4096, @rcvbuf)
        #puts "\"#{@buf}\""
        @parser << @rcvbuf
        return false if @done || sock.closed?
      end
    end
    
    attr_accessor :last_modified, :etag, :timeout
    def initialize(subscr, opt={})
      @last_modified, @etag, @timeout = opt[:last_modified], opt[:etag], opt[:timeout].to_i || 10
      @connect_timeout = opt[:connect_timeout]
      @subscriber=subscr
      @url=subscr.url
      @concurrency=opt[:concurrency] || opt[:clients] || 1
      @gzip=opt[:gzip]
      @retry_delay=opt[:retry_delay]
      @nomsg=opt[:nomsg]
      @http={}
      @body_buf=""
    end
    
    def run(was_success = nil)
      uri = URI.parse(@url)
      port = uri.port || (uri.scheme == "http" ? 80 : 443)
      @cooked=Celluloid::Condition.new
      @connected = @concurrency
      @concurrency.times do |i|
        if uri.scheme == "http"
          sock = Celluloid::IO::TCPSocket.new(uri.host, port)
        elsif uri.scheme == "https"
          sock = Celluloid::IO::SSLSocket.new(uri.host, port)
        else
          raise ArgumentError, "invalid HTTP scheme #{uri.scheme} in #{@url}"
        end
        bundle = new_bundle(uri, sock, "pubsub.rb #{self.class.name} #{@use_http2 ? "(http/2)" : ""} ##{i}")
        @http[bundle]=true
        bundle.send_GET
        async.listen bundle
      end
    end
    
    def new_bundle(uri, sock, useragent)
      b=HTTPBundle.new(uri, sock, useragent)
      prsr = b.parser
      prsr.on_headers_complete = proc do
        @body_buf.clear
      end
      prsr.on_message_complete = proc do
        # Headers and body is all parsed
        @last_modified = prsr.headers["Last-Modified"]
        @etag = prsr.headers["Etag"]
        b.request_time = Time.now.to_f - b.time_requested
        if prsr.status_code != 200
          binding.pry
          unless @subscriber.on_failure(prsr) == false
            Celluloid.sleep @retry_delay if @retry_delay
          else
            @subscriber.finished+=1
            close b
          end
        end
        
        unless @nomsg
          msg=Message.new @body_buf, @last_modified, @etag
          msg.content_type=prsr.headers["Content-Type"]
        else
          msg=@body_buf
        end
        
        unless @subscriber.on_message(msg, b) == false
          @subscriber.waiting+=1
          Celluloid.sleep @retry_delay if @retry_delay
          b.send_GET @last_modified, @etag
        else
          @subscriber.finished+=1
          close b
        end
        
      end
      
      prsr.on_body = proc do |chunk|
        @body_buf << chunk
      end
      b
    end
    
    def listen(bundle)
      loop do
        begin
          return if bundle.read == false
        rescue EOFError
          close bundle
        end
      end
    end
    
    def close(bundle)
      bundle.done=true
      bundle.sock.close
      @http.delete bundle
      @connected -= 1
      if @connected == 0
        @cooked.signal true
      end
    end
    
    def poke
      @connected > 0 && @cooked.wait
    end
  end
  
  class FastHTTP2LongPollClient < FastLongPollClient
    class HTTP2Bundle
      attr_accessor :stream, :sock, :last_message_time, :done, :time_requested, :request_time
      GET_METHOD="GET"
      def initialize(uri, sock, user_agent)
        @done = false
        @sock = sock
        @head = {
          ':method' => GET_METHOD,
          ':path' => uri.path,
          ':authority' => [uri.host, uri.port].join(':'),
          'user-agent' => "#{user_agent || "HTTP2Bundle"}",
          'accept' => '*/*'
        }
        
        @client = HTTP2::Client.new
        @client.on(:frame) do |bytes|
          puts "Sending bytes: #{bytes.unpack("H*").first}"
          @sock.print bytes
          @sock.flush
        end
        
        @client.on(:frame_sent) do |frame|
          puts "Sent frame: #{frame.inspect}"
        end
        @client.on(:frame_received) do |frame|
          puts "Received frame: #{frame.inspect}"
        end
        
      end
      
      def send_GET(msg_time=nil, msg_tag=nil)
        @time_requested=Time.now.to_f
        if msg_time
          @head['if-modified-since'] = msg_time.to_s
        else
          @head.delete @head['if-modified-since']
        end
        
        if msg_tag
          @head['if-none-match'] = msg_tag.to_s
        else
          @head.delete @head['if-none-match']
        end
        
        @stream = @client.new_stream
        @stream.on(:close) do
          puts 'stream closed'
          on_response @response_headers, @response_data
        end
        @stream.on(:headers) do |h|
          puts "response headers: #{h}"
          @response_headers = h
        end
        @stream.on(:data) do |d|
          puts "response data chunk: <<#{d}>>"
          @response_data = d
        end
        
        @stream.on(:altsvc) do |f|
          log.info "received ALTSVC #{f}"
        end
        
        @stream.on(:half_close) do
          puts 'closing client-end of the stream'
        end
        
        @stream.on(:altsvc) do |f|
          puts "received ALTSVC #{f}"
        end
        
        
        
        @stream.headers(@head, end_stream: true)
      end
      
      def read
        @sock.readpartial(4096, @val)
        puts "received #{@val.nil? ? "0" : @val.count} bytes"
        begin
          @client << @val
        rescue => e
          puts "Exception: #{e}, #{e.message} - closing socket."
          @sock.close
        end
        return false if @done || @sock.closed?
      end
      
      def on_response(h=nil, d=nil, &block)
        if block_given?
          @on_response = block
        elsif @on_response
          @on_response.call h, d
        end
      end
      
    end
    
    def initialize(*arg)
      @use_http2 = true
      super
    end
    
    def new_bundle(uri, sock, useragent)
      b=HTTP2Bundle.new(uri, sock, useragent)
      
      b.on_response do |headers, body|
        # Headers and body is all parsed
        binding.pry
        @last_modified = headers["Last-Modified"]
        @etag = headers["Etag"]
        
        b.request_time = Time.now.to_f - b.time_requested
        
        if prsr.status_code != 200
          binding.pry
          unless @subscriber.on_failure(prsr) == false
            Celluloid.sleep @retry_delay if @retry_delay
          else
            @subscriber.finished+=1
            close b
          end
        end
        
        unless @nomsg
          msg=Message.new @body_buf, @last_modified, @etag
          msg.content_type=prsr.headers["Content-Type"]
        else
          msg=@body_buf
        end
        
        unless @subscriber.on_message(msg, b) == false
          @subscriber.waiting+=1
          Celluloid.sleep @retry_delay if @retry_delay
          b.send_GET @last_modified, @etag
        else
          @subscriber.finished+=1
          close b
        end
        
      end
      
      b
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
      @extra_headers=opt[:extra_headers]
    end
    
    def response_success(response, req)
      #puts "received OK response at #{req.url}"
      #parse it
      req.options[:headers]["If-None-Match"] = response.headers["Etag"]
      req.options[:headers]["If-Modified-Since"] = response.headers["Last-Modified"]
      
      on_message_ret = nil
      
      Message.each_multipart_message(response.headers["Content-Type"], response.body) do |content_type, body, multi|
        unless @nomsg
          msg=Message.new body
          msg.content_type=content_type
          unless multi
            msg.last_modified= response.headers["Last-Modified"]
            msg.etag= response.headers["Etag"]
          end
        else
          msg=body
        end
        
        on_message_ret = @subscriber.on_message(msg, req)
      end
      
      unless on_message_ret == false
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
      if @extra_headers
        headers.merge! @extra_headers
      end
      
      if opt[:old_request]
        #req = Typhoeus::Request.new(opt[:old_request].url, opt[:old_request].options)
        
        #reuse request
        req = opt[:old_request]
      else
        req = Typhoeus::Request.new(@url, timeout: @timeout, connecttimeout: @connect_timeout, accept_encoding: (@gzip ? "gzip" : nil), headers: headers )

        #req.on_body do |chunk|
        #  puts chunk
        #end
        
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
  
  class EventSourceClient < FastLongPollClient
    
    include Celluloid::IO
    
    class EventSourceBundle < FastLongPollClient::HTTPBundle
      attr_accessor :buf, :on_headers
      def initialize(uri, sock, user_agent)
        super
        @send_noid_str= <<-END.gsub(/^ {10}/, '')
          GET #{uri.path} HTTP/1.1
          Host: #{uri.host}#{uri.default_port == uri.port ? "" : ":#{uri.port}"}
          Accept: text/event-stream
          User-Agent: #{user_agent || "HTTPBundle"}
          
        END
        
        @send_withid_fmt= <<-END.gsub(/^ {10}/, '')
          GET #{uri.path} HTTP/1.1
          Host: #{uri.host}#{uri.default_port == uri.port ? "" : ":#{uri.port}"}
          Accept: text/event-stream
          User-Agent: #{user_agent || "HTTPBundle"}
          Last-Event-ID: %s
          
        END
        @buf={data: "", event_id: "", comments: ""}
        #binding.pry
        
        @parser.on_headers_complete= proc do 
          @on_headers.call parser
          @gotheaders = true
        end
      end
      
      def on_headers(&block)
        @on_headers = block
      end
    
      def read
        @rcvbuf.clear
        @rcvbuf << sock.readline
        #puts @rcvbuf
        unless @gotheaders
          @parser << @rcvbuf
        else 
          parse_line @rcvbuf
        end
        return false if @done || sock.closed?
      end
      
      def buf_reset
        @buf[:data].clear
        @buf[:event_id].clear
        @buf[:comments].clear
        @buf[:retry_timeout] = nil
        @buf[:event] = nil
      end
      
      def parse_event
        if @buf[:comments].length > 0
          @on_event.call :comment, @buf[:comments].chomp!
        else
          @on_event.call @buf[:event] || :message, @buf[:data].chomp!, @buf[:id]
        end
        buf_reset
      end
      
      def on_event(&block)
        @on_event=block
      end
      
      def parse_line(line)
        ret = nil
        case line
        when /^: ?(.*)/
          @buf[:comments] << "#{$1}\n"
        when /^data(: (.*))?/
          @buf[:data] << "#{$2}\n" or "\n"
        when /^id(: (.*))?/
          @buf[:id] = $2 or ""
        when /^event(: (.*))?/
          @buf[:event] = $2 or ""
        when /^retry: (.*)/
          @buf[:retry_timeout] = $1
        when /^$/
          ret = parse_event
        end
        ret
      end
      
    end
    
     def new_bundle(uri, sock, useragent)
      b=EventSourceBundle.new(uri, sock, useragent)
      b.on_headers do |parser|
        if parser.status_code != 200
          @subscriber.on_failure(parser)
          close b
        end
      end
      b.on_event do |evt, data, evt_id|
        if evt == :message
          unless @nomsg
            msg=Message.new data
            msg.id=evt_id
          else
            msg=data
          end
          
          if @subscriber.on_message(msg, b) == false
            close b
          end
          
        end
      end
      b
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
    @extra_headers = opt[:extra_headers]
    #puts "Starting subscriber on #{url}"
    case opt[:client]
    when :longpoll, :long, nil
      @client_class=LongPollClient
    when :fastlongpoll, nil
      @client_class=FastLongPollClient
    when :fastlongpoll_http2, nil
      @client_class=FastHTTP2LongPollClient
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
    @client=client_class.new(self, concurrency: @concurrency, timeout: @timeout, connect_timeout: @connect_timeout, gzip: @gzip, retry_delay: @retry_delay, nomsg: @nomsg, extra_headers: @extra_headers)
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
    #puts "received message #{msg && msg.to_s[0..15]}"
    if block_given?
      @on_message=block
    else
      @messages << msg if @messages
      if @quit_message == msg.to_s
        @on_message.call(msg, req) if @on_message
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
      when FastLongPollClient, EventSourceClient
        @errors << "HTTP request failed: (code #{response.status_code})"
      else
        binding.pry
      end
      @on_failure.call(response) if @on_failure.respond_to? :call
    end
  end
end

class Publisher
  #include Celluloid
  attr_accessor :messages, :response, :response_code, :response_body, :nofail, :accept, :url, :extra_headers
  def initialize(url, opt={})
    @url= url
    unless opt[:nostore]
      @messages = MessageStore.new :noid => true
      @messages.name = "pub"
    end
    @timeout = opt[:timeout]
    @accept = opt[:accept]
  end
  
  def with_url(alt_url)
    prev_url=@url
    @url=alt_url
    if block_given?
      yield
      @url=prev_url
    else
      self
    end
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
    headers = {:'Content-Type' => content_type, :'Accept' => accept}
    headers.merge! @extra_headers if @extra_headers
    post = Typhoeus::Request.new(
      @url,
      headers: headers,
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
          
          pub_url=URI.parse(response.request.url)
          pub_url = "#{pub_url.path}#{pub_url.query ? "?#{pub_url.query}" : nil}"
          raise "Publisher #{response.request.options[:method]} to #{pub_url} timed out."
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
