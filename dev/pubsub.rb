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
  attr_accessor :msgs, :name

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
        #puts "Different messages with same id: #{msg.id}, \"#{msg.to_s}\" then \"#{cur_msg.to_s}\"" unless cur_msg.message == msg.message
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
  
  class Client
    class ErrorResponse
      attr_accessor :code, :msg, :connected, :caller
      def initialize(code, msg, connected=false, what=nil, failword=nil)
        self.code = code
        self.msg = msg
        self.connected = connected
        
        @what = what || ["handshake", "connection"]
        @failword = failword || " failed"
      end
      
      def to_s
        "#{(caller.class.name.split('::').last || self.class.name.split('::')[-2])} #{connected ? @what.last : @what.first}#{@failword}: #{msg} (code #{code})"
      end  
    
    end
    
    def self.inherited(subclass)
      @@inherited||=[]
      @@inherited << subclass
    end
    
    def self.lookup(name)
      @@inherited.each do |klass|
        return klass if klass.aliases.include? name
      end
      nil
    end
    def self.aliases
      []
    end
      
    def self.unique_aliases
      uniqs=[]
      @@inherited.each do |klass|
        uniqs << klass.aliases.first if klass.aliases.length > 0
      end
      uniqs
    end
    
    def error(code, msg, connected=nil)
      err=ErrorResponse.new code, msg, connected=nil, @error_what, @error_failword
      err.caller=self
      err
    end
    
    class ParserBundle
      attr_accessor :body_buf, :connected, :verbose, :parser, :subparser
      def buffer_body!
        @body_buf||=""
      end
      def connected?
        @connected
      end
      def on_headers(code=nil, h=nil, &block)
        @body_buf.clear if @body_buf
        if block_given?
          @on_headers = block
        else
          @on_headers.call(code, h) if @on_headers
        end
      end
      
      def on_chunk(ch=nil, &block)
        if block_given?
          @on_chunk = block
        else
          @body_buf << ch if @body_buf
          @on_chunk.call(ch) if @on_chunk
        end
      end
      
      def on_response(code=nil, headers=nil, &block)
        if block_given?
          @on_response = block
        else
          @on_response.call(code, headers, @body_buf) if @on_response
        end
      end
      
      def on_error(msg=nil, e=nil, &block)
        if block_given?
          @on_error = block
        else
          @on_error.call(msg, e) if @on_error
        end
      end
    end
    
    def handle_bundle_error(bundle, msg, err)
      if err && !(EOFError === err)
        msg="<#{msg}>\n#{err.backtrace.join "\n"}"
      end
      @subscriber.on_failure error(0, msg, bundle.connected?)
      @subscriber.finished+=1
      close bundle
    end
    
  end
  
  class LongPollClient < Client
    include Celluloid
    
    def self.aliases
      [:longpoll, :http]
    end
    
    def error(*a)
      @error_what ||= ["HTTP Request"]
      super
    end
    def error_from_response(response)
      if response.timed_out?
        msg = "Client response timeout."
        code = 0
      else
        msg = response.return_message
        code = response.code
      end
      error code, msg
    end
    
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
    
    def response_failure(response, req)
      #puts "received bad or no response at #{req.url}"
      unless @subscriber.on_failure(error_from_response(response), response) == false
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

  class IntervalPollClient < LongPollClient
    
    def self.aliases
      [:intervalpoll, :http, :interval, :poll]
    end
    
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
      if @subscriber.on_failure(error_from_response(response), response) != false
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

  class WebSocketClient < Client
    include Celluloid::IO
    
    def self.aliases
      [:websocket, :ws]
    end
    
    #a little sugar for handshake errors
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
    
    
    attr_accessor :last_modified, :etag, :timeout
    def initialize(subscr, opt={})
      @last_modified, @etag, @timeout = opt[:last_modified], opt[:etag], opt[:timeout].to_i || 10
      @connect_timeout = opt[:connect_timeout]
      @subscriber=subscr
      @url=subscr.url
      @url = @url.gsub(/^h(ttp|2)(s)?:/, "ws\\2:")
      
      @concurrency=(opt[:concurrency] || opt[:clients] || 1).to_i
      @retry_delay=opt[:retry_delay]
      @ws = {}
      @connected=0
      @nomsg = opt[:nomsg]
      if @timeout
        @timer = after(@timeout) do
          @subscriber.on_failure error(0, "Timeout", true)
          @ws.each do |b, v|
            close b
          end
        end
      end
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
        begin
          if uri.scheme == "ws"
            sock = Celluloid::IO::TCPSocket.new(uri.host, port)
          elsif uri.scheme == "wss"
            sock = Celluloid::IO::SSLSocket.new(uri.host, port)
          else
            raise ArgumentError, "invalid websocket scheme #{uri.scheme} in #{@url}"
          end
        rescue SystemCallError => e
          @subscriber.on_failure(error(0, e.to_s, 0))
          close nil
          return
        end
          
        @handshake = WebSocket::Handshake::Client.new(url: @url)
        sock << @handshake.to_s
        
        #handshake response
        loop do
          @handshake << sock.readline
          if @handshake.finished?
            unless @handshake.valid?
              @subscriber.on_failure error(@handshake.response_code, @handshake.response_line, false)
            end
            break
          end
        end
        
        if @handshake.valid?
          bundle = WebSocketBundle.new(@handshake, sock)
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
            @timer.reset if @timer
            if on_message(msg.data, msg.type, bundle) == false
              close bundle
              return 
            end
          end
        rescue EOFError
          bundle.sock.close
          close bundle
          return
        end
      end
    end
    
    def on_error(err, err2)
      puts "Received error #{err}"
      if !@connected[ws]
        @subscriber.on_failure error(ws.handshake.response_code, ws.handshake.response_line, @connected[ws]), ws
        try_halt
      end
    end
    
    def on_message(data, type, bundle)
      #puts "Received message: #{data} type: #{type}"
      if type==:close
        close_frame = WebSocket::Frame::Outgoing::Client.new(version: @handshake.version, type: :close)
        bundle.sock << close_frame.to_s
      elsif type==:ping
        ping_frame = WebSocket::Frame::Outgoing::Client.new(version: @handshake.version, data: data, type: :pong)
        bundle.sock << ping_frame.to_s
      elsif type==:text
        msg= @nomsg ? data : Message.new(data)
        bundle.last_message_time=Time.now.to_f
        @subscriber.on_message(msg, bundle)
      else
        raise "unexpected websocket frame #{type} data #{data}"
      end
    end
    
    def close(bundle)
      if bundle
        @ws.delete bundle
        bundle.sock.close unless bundle.sock.closed?
      end
      @connected -= 1
      if @connected <= 0
        binding.pry unless @ws.count == 0
        #binding.pry
        @cooked.signal true
      end
    end
    
    def poke
      @connected > 0 && @cooked.wait
    end
  end
  
  class FastLongPollClient < Client
    include Celluloid::IO
    
    def self.aliases
      [:fastlongpoll]
    end
    
    def error(*args)
      @error_what||= ["#{@http2 ? "HTTP/2" : "HTTP"} Request"]
      super
    end
    
    class HTTPBundle < ParserBundle
      attr_accessor :parser, :sock, :last_message_time, :done, :time_requested, :request_time
      def initialize(uri, sock, user_agent, accept="*/*", extra_headers={})
        @accept = accept
        @rcvbuf=""
        @sndbuf=""
        @parser = Http::Parser.new
        @sock = sock
        @done = false
        extra_headers = extra_headers.map{|k,v| "#{k}: #{v}\n"}.join ""
        @send_noid_str= <<-END.gsub(/^ {10}/, '')
          GET #{uri.path} HTTP/1.1
          Host: #{uri.host}#{uri.default_port == uri.port ? "" : ":#{uri.port}"}
          #{extra_headers}Accept: #{@accept}
          User-Agent: #{user_agent || "HTTPBundle"}
          
        END
        
        @send_withid_fmt= <<-END.gsub(/^ {10}/, '')
          GET #{uri.path} HTTP/1.1
          Host: #{uri.host}#{uri.default_port == uri.port ? "" : ":#{uri.port}"}
          #{extra_headers}Accept: #{@accept}
          User-Agent: #{user_agent || "HTTPBundle"}
          If-Modified-Since: %s
          If-None-Match: %s
          
        END
        
        @parser.on_headers_complete = proc do |h|
          if verbose 
            puts h.map {|k,v| "< #{k}: #{v}"}.join "\r\n"
          end
          on_headers @parser.status_code, h
        end
        
        @parser.on_body = proc do |chunk|
          on_chunk chunk
        end
        
        @parser.on_message_complete = proc do
          on_response @parser.status_code, @parser.headers
        end
        
      end
      
      def send_GET(msg_time=nil, msg_tag=nil)
        @sndbuf.clear 
        data = msg_time ? sprintf(@send_withid_fmt, msg_time, msg_tag) : @send_noid_str
        @sndbuf << data
        if verbose
          puts "", data.gsub(/^.*$/, "> \\0")
        end
      
        @time_requested=Time.now.to_f
        @sock << @sndbuf
      end
      
      def read
        @rcvbuf.clear
        begin
          sock.readpartial(4096, @rcvbuf)
          @parser << @rcvbuf
        rescue HTTP::Parser::Error => e
          on_error "Invalid HTTP Respose - #{e}", e
        rescue EOFError => e
          on_error "Server closed connection...", e
        rescue => e 
          on_error "#{e.class}: #{e}", e
        end
        return false if @done || sock.closed?
      end
    end
    
    class HTTP2Bundle < ParserBundle
      attr_accessor :stream, :sock, :last_message_time, :done, :time_requested, :request_time
      GET_METHOD="GET"
      def initialize(uri, sock, user_agent, accept="*/*", extra_headers=nil)
        @done = false
        @sock = sock
        @rcvbuf=""
        @head = {
          ':scheme' => uri.scheme,
          ':method' => GET_METHOD,
          ':path' => uri.path,
          ':authority' => [uri.host, uri.port].join(':'),
          'user-agent' => "#{user_agent || "HTTP2Bundle"}",
          'accept' => accept
        }
        if extra_headers
          extra_headers.each do |h, v|
            binding.pry
          end
        end
        
        @client = HTTP2::Client.new
        @client.on(:frame) do |bytes|
          #puts "Sending bytes: #{bytes.unpack("H*").first}"
          @sock.print bytes
          @sock.flush
        end
        
        @client.on(:frame_sent) do |frame|
          #puts "Sent frame: #{frame.inspect}" if verbose
        end
        @client.on(:frame_received) do |frame|
          #puts "Received frame: #{frame.inspect}" if verbose
        end
        @resp_headers={}
        @resp_code=nil
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
        @resp_headers.clear
        @resp_code=0
        @stream.on(:close) do |k,v|
          on_response @resp_code, @resp_headers
        end
        @stream.on(:headers) do |h|
          h.each do |v|
            puts "< #{v.join ': '}" if verbose
            case v.first
            when ":status"
              @resp_code = v.last.to_i
            when /^:/
              @resp_headers[v.first] = v.last
            else
              @resp_headers[v.first.gsub(/(?<=^|\W)\w/) { |v| v.upcase }]=v.last
            end
          end
          on_headers @resp_code, @resp_headers
        end
        @stream.on(:data) do |d|
          #puts "got data chunk #{d}"
          on_chunk d
        end
        
        @stream.on(:altsvc) do |f|
          puts "received ALTSVC #{f}"
        end
        
        @stream.on(:half_close) do
          puts "", @head.map {|k,v| "> #{k}: #{v}"}.join("\r\n") if verbose
        end
        
        @stream.headers(@head, end_stream: true)
      end
      
      def read
        return false if @done || @sock.closed?
        begin
          @rcv = @sock.readpartial 1024
          @client << @rcv
        rescue EOFError => e
          if @rcv && @rcv[0..5]=="HTTP/1"
            on_error @rcv.match(/^HTTP\/1.*/)[0].chomp, e
          else
            on_error "Server closed connection...", e
          end
          @sock.close
        rescue => e
          on_error "#{e.class}: #{e.to_s}", e
          @sock.close
        end
        return false if @done || @sock.closed?
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
      @bundles={}
      @body_buf=""
      @verbose=opt[:verbose]
      @http2=opt[:http2] || opt[:h2]
      if @timeout
        @timer = after(@timeout) do 
          @subscriber.on_failure error(0, "Timeout")
          @bundles.each do |b, v|
            close b
          end
        end
      end
    end
    
    def run(was_success = nil)
      uri = URI.parse(@url)
      port = uri.port || (uri.scheme == "http" ? 80 : 443)
      @cooked=Celluloid::Condition.new
      @connected = @concurrency
      @concurrency.times do |i|
        begin
          if uri.scheme == "http" || uri.scheme == "h2"
            sock = Celluloid::IO::TCPSocket.new(uri.host, port)
          elsif uri.scheme == "https" || uri.scheme == "h2s"
            sock = Celluloid::IO::SSLSocket.new(uri.host, port)
          else
            raise ArgumentError, "invalid HTTP scheme #{uri.scheme} in #{@url}"
          end
        rescue SystemCallError => e
          @subscriber.on_failure error(0, e.to_s)
          close nil
          return
        end
        bundle = new_bundle(uri, sock, "pubsub.rb #{self.class.name} #{@use_http2 ? "(http/2)" : ""} ##{i}")
        @bundles[bundle]=true
        bundle.send_GET
        async.listen bundle
      end
    end
    
    def request_code_ok(code, bundle=nil, etc=nil)
      if code != 200
        if code == 304
          @subscriber.on_failure error(code, ""), etc
          @subscriber.finished+=1
          close bundle
        elsif @subscriber.on_failure(error(code, ""), etc) == false
          @subscriber.finished+=1
          close bundle
        else
          Celluloid.sleep @retry_delay if @retry_delay
          bundle.send_GET @last_modified, @etag
        end
        false
      else
        @timer.reset if @timer
        true
      end
    end
    
    def new_bundle(uri, sock, useragent, accept="*/*", extra_headers={})
      b=(@http2 ? HTTP2Bundle : HTTPBundle).new(uri, sock, useragent, accept, extra_headers)
      b.on_error do |msg, err|
        handle_bundle_error b, msg, err
      end
      b.verbose=@verbose
      setup_bundle b
      b
    end
    
    def setup_bundle(b)
      b.buffer_body!
      b.on_response do |code, headers, body|
        # Headers and body is all parsed
        @last_modified = headers["Last-Modified"]
        @etag = headers["Etag"]
        b.request_time = Time.now.to_f - b.time_requested
        if request_code_ok(code, b)
          unless @nomsg
            msg=Message.new body, @last_modified, @etag
            msg.content_type=headers["Content-Type"]
          else
            msg=body
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
      end
      
      b.on_error do |msg, err|
        handle_bundle_error b, msg, err
      end
    end
    
    def listen(bundle)
      loop do
        begin
          return false if bundle.read == false
        rescue EOFError
          binding.pry
          @subscriber.on_failure(error(0, "Server Closed Connection"), bundle.connected)
          close bundle
          return false
        end
      end
    end
    
    def close(bundle)
      if bundle
        bundle.done=true
        bundle.sock.close unless bundle.sock.closed?
        @bundles.delete bundle
      end
      @connected -= 1
      if @connected <= 0
        @cooked.signal true
      end
    end
    
    def poke
      @connected > 0 && @cooked.wait
    end
  end
  
  class EventSourceClient < FastLongPollClient
    include Celluloid::IO
    
    def self.aliases
      [:eventsource, :sse]
    end
    
    def error(c,m,cn=nil)
      @error_what ||= [ "#{@http2 ? 'HTTP/2' : 'HTTP'} Request failed", "connection closed" ]
      @error_failword ||= ""
      super
    end
    
    class EventSourceParser
      attr_accessor :buf, :on_headers, :connected
      def initialize
        @buf={data: "", id: "", comments: ""}
        buf_reset
      end
      
      def buf_reset
        @buf[:data].clear
        @buf[:id].clear
        @buf[:comments].clear
        @buf[:retry_timeout] = nil
        @buf[:event] = nil
      end
      
      def buf_empty?
        @buf[:comments].length == 0 && @buf[:data].length == 0
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
      
      def parse_event
        
        if @buf[:comments].length > 0
          @on_event.call :comment, @buf[:comments].chomp!
        elsif @buf[:data].length > 0 || @buf[:id].length > 0 || !@buf[:event].nil?
          @on_event.call @buf[:event] || :message, @buf[:data].chomp!, @buf[:id]
        end
        buf_reset
      end
      
      def on_event(&block)
        @on_event=block
      end
      
    end
    
    def new_bundle(uri, sock, useragent=nil)
      super uri, sock, useragent, "text/event-stream"
    end
    
    def setup_bundle(b)
      b.on_headers do |code, headers|
        if code != 200
          @subscriber.on_failure error(code, "", false)
          close b
        else
          b.connected = true
        end
      end
      b.buffer_body!
      b.subparser=EventSourceParser.new
      b.on_chunk do |chunk|
        while b.body_buf.slice! /^.*\n/ do
          b.subparser.parse_line $~[0]
        end
      end
      b.on_error do |msg, err|
        if EOFError === err && !b.subparser.buf_empty?
          b.subparser.parse_line "\n"
        end
        handle_bundle_error b, msg, err
      end
      
      b.on_response do |code, headers, body|
        if !b.subparser.buf_empty?
          b.subparser.parse_line "\n"
        else
          @subscriber.on_failure error(0, "Response completed unexpectedly", bundle.connected?)
        end
        @subscriber.finished+=1
        close b
      end
      
      b.subparser.on_event do |evt, data, evt_id|
        case evt 
        when :message
          @timer.reset if @timer
          unless @nomsg
            msg=Message.new data
            msg.id=evt_id
          else
            msg=data
          end
          
          if @subscriber.on_message(msg, b) == false
            close b
          end
        when :comment
          if data.match(/^(?<code>\d+): (?<message>.*)/)
            @subscriber.on_failure error($~[:code].to_i, $~[:message], b.connected)
          end
        end
      end
      b
    end
    
  end
  
  class MultipartMixedClient < FastLongPollClient
    include Celluloid::IO
    
    def self.aliases 
      [:multipart, :multipartmixed, :mixed]
    end
    
    class MultipartMixedParser
      attr_accessor :bound, :finished, :buf
      def initialize(multipart_header)
        matches=/^multipart\/mixed; boundary=(?<boundary>.*)/.match multipart_header
        raise "malformed Content-Type multipart/mixed header" unless matches[:boundary]
        @bound = matches[:boundary]
        @buf = ""
        @preambled = false
        @headered = nil
        @headers = {}
        @ninished = nil
      end
      
      def on_part(&block)
        @on_part = block
      end
      def on_finish(&block)
        @on_finish = block
      end
      
      def <<(chunk)
        @buf << chunk
        repeat = true
        while repeat do
          if !@preambled && @buf.slice!(/^--#{Regexp.escape @bound}/)
            @finished = nil
            @preambled = true
            @headered = nil
          end
          
          if @preambled && @buf.slice!(/^(\r\n(.*?))?\r\n\r\n/m)
            @headered = true
            
            ($~[2]).each_line do |l|
              if l.match(/(?<name>[^:]+):\s(?<val>[^\r\n]*)/)
                @headers[$~[:name]]=$~[:val]
              end
            end
            @headered = true
          else
            repeat = false
          end
          
          if @headered && @buf.slice!(/^(.*?)\r\n--#{Regexp.escape @bound}/m)
            @on_part.call @headers, $~[1]
            @headered = nil
            @headers.clear
          else
            repeat = false
          end
          
          if (@preambled && !@headered && @buf.slice!(/^--\r\n/)) ||
            (!@preambled && @buf.slice!(/^--#{Regexp.escape @bound}--\r\n/))
            @on_finish.call
            repeat = false
          end
        end
      end
      
    end
    
    def new_bundle(uri, sock, useragent=nil)
      super uri, sock, useragent, "multipart/mixed"
    end
    
    def setup_bundle b
      super
      b.on_headers do |code, headers|
        if code != 200
          @subscriber.on_failure error(code, "", false)
          close b
        else
          b.connected = true
          
          b.subparser = MultipartMixedParser.new headers["Content-Type"]
          b.subparser.on_part do |headers, message|
            unless @nomsg
              msg=Message.new message, headers["Last-Modified"], headers["Etag"]
              msg.content_type=headers["Content-Type"]
            else
              msg=message
            end
            
            if @subscriber.on_message(msg, b) == false
              @subscriber.finished+=1
              close b
            end
          end
          
          b.subparser.on_finish do
            b.subparser.finished = true
          end
        end
      end
      
      b.on_chunk do |chunk|
        b.subparser << chunk if b.subparser
        if HTTPBundle === b && b.subparser.finished
          @subscriber.on_failure(error(410, "Server Closed Connection", true))
          @subscriber.finished+=1
          close b
        end
      end
      
      b.on_response do |code, headers, body|
        if b.subparser.finished
          @subscriber.on_failure(error(410, "Server Closed Connection", true))
        else
          @subscriber.on_failure error(0, "Response completed unexpectedly", b.connected?)        
        end
        @subscriber.finished+=1
        close b
      end
    end
  end
  
  class HTTPChunkedClient < EventSourceClient
    include Celluloid::IO
    
    def self.aliases
      [:chunked]
    end
    
    class HTTPChunkedBundle < FastLongPollClient::HTTPBundle
      attr_accessor :ok
      def initialize(uri, sock, user_agent)
        super uri, sock, user_agent, "*/*", {"TE": "Chunked"}
      end
    end
    
    def new_bundle(uri, sock, useragent)
      b=HTTPChunkedBundle.new(uri, sock, useragent)
      prsr = b.parser
      prsr.on_headers_complete = proc do |headers|
        if prsr.status_code != 200
          @subscriber.on_failure(error(prsr.status_code, "", false))
          @subscriber.finished+=1
          close b
        elsif headers["Transfer-Encoding"] != "chunked"
          @subscriber.on_failure(error(0, "Transfer-Encoding should be 'chunked', was '#{headers["Transfer-Encoding"]}'", false))
          @subscriber.finished+=1
          close b
        else
          b.ok=true
        end
      end
      
      prsr.on_body = proc do |chunk|
        next unless b.ok
        @timer.reset if @timer
        unless @nomsg
            msg=Message.new chunk, nil, nil
          else
            msg=@body_buf
          end
          
          if @subscriber.on_message(msg, b) == false
            @subscriber.finished+=1
            close b
          end
      end
      
      prsr.on_message_complete = proc do
        @subscriber.on_failure(HTTPChunkedErrorResponse.new(410, "Server Closed Connection", true))
      end
      
      b
    end
    
  end

  attr_accessor :url, :client, :messages, :max_round_trips, :quit_message, :errors, :concurrency, :waiting, :finished, :client_class
  def initialize(url, concurrency=1, opt={})
    @care_about_message_ids=opt[:use_message_id].nil? ? true : opt[:use_message_id]
    @url=url
    @quit_message = opt[:quit_message]
    opt[:timeout] ||= 30
    opt[:connect_timeout] ||= 5
    #puts "Starting subscriber on #{url}"
    @Client_Class = Client.lookup(opt[:client] || :longpoll)
    if @Client_Class.nil?
      raise "unknown client type #{opt[:client]}"
    end
    
    if !opt[:nostore] && opt[:nomsg]
      opt[:nomsg] = nil
      puts "nomsg reverted to false because nostore is false"
    end
    opt[:concurrency]=concurrency
    
    @opt=opt
    reset
    new_client
  end
  def new_client
    @client=@Client_Class.new self, @opt
  end
  def reset
    @errors=[]
    unless @nostore
      @messages=MessageStore.new :noid => !@care_about_message_ids
      @messages.name="sub"
    end
    @waiting=0
    @finished=0
    new_client if terminated?
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
  
  def make_error(client, what, code, msg, failword=" failed")
    "#{client.class.name.split('::').last} #{what}#{failword}: #{msg} (code #{code})"
  end
  
  def on_failure(err=nil, etc=nil, &block)
    if block_given?
      @on_failure=block
    else
      @errors << err.to_s
      @on_failure.call(@errors.last, etc) if @on_failure.respond_to? :call
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
