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
    attr_accessor :concurrency
    class ErrorResponse
      attr_accessor :code, :msg, :connected, :caller, :bundle
      def initialize(code, msg, bundle=nil, what=nil, failword=nil)
        self.code = code
        self.msg = msg
        if Subscriber::Client::ParserBundle === bundle
          self.bundle=bundle
          self.connected = bundle.connected?
        else
          self.connected = bundle
        end
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
    
    def provides_msgid?
      true
    end
    
    def error(code, msg, bundle=nil)
      err=ErrorResponse.new code, msg, bundle, @error_what, @error_failword
      err.caller=self
      err
    end
    
    class ParserBundle
      attr_accessor :uri, :sock, :body_buf, :connected, :verbose, :parser, :subparser, :headers, :code, :last_modified, :etag
      def initialize(uri, *arg)
        @uri=uri
        open_socket
      end
      def open_socket
        case uri.scheme
        when /^(ws|http|h2c)$/
          @sock = Celluloid::IO::TCPSocket.new(uri.host, uri.port)
        when /^(wss|https|h2)$/
          @sock = Celluloid::IO::SSLSocket.new(Celluloid::IO::TCPSocket.new(uri.host, uri.port))
        else
          raise ArgumentError, "unexpected uri scheme #{uri.scheme}"
        end
        self
      end
      
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
      @subscriber.on_failure error(0, msg, bundle)
      @subscriber.finished+=1
      close bundle
    end
    
    def poke(what=nil)
      if what == :ready
        (@notready.nil? || @notready > 0) && @cooked_ready.wait
      else
        @connected > 0 && @cooked.wait
      end
    end
    
    def initialize(*arg)
      @notready = 9000
      @cooked_ready=Celluloid::Condition.new
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
    
    def provides_msgid?
      false
    end
    
    attr_accessor :last_modified, :etag, :timeout
    def initialize(subscr, opt={})
      super
      @last_modified, @etag, @timeout = opt[:last_modified], opt[:etag], opt[:timeout].to_i || 10
      @connect_timeout = opt[:connect_timeout]
      @subscriber=subscr
      @url=subscr.url
      @url = @url.gsub(/^h(ttp|2)(s)?:/, "ws\\2:")
      
      @concurrency=(opt[:concurrency] || opt[:clients] || 1).to_i
      @retry_delay=opt[:retry_delay]
      @ws = {}
      @connected=0
      @notready=@concurrency
      @nomsg = opt[:nomsg]
      @http2 = opt[:http2]
      if @timeout
        @timer = after(@timeout) do
          @subscriber.on_failure error(0, "Timeout", b)
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
      uri.port ||= (uri.scheme == "ws" ? 80 : 443)
      @cooked=Celluloid::Condition.new
      @connected = @concurrency
      if @http2
        @subscriber.on_failure error(0, "Refusing to try websocket over HTTP/2")
        @connected = 0
        @notready = 0
        @cooked_ready.signal false
        @cooked.signal true
        return
      end
      raise ArgumentError, "invalid websocket scheme #{uri.scheme} in #{@url}" unless uri.scheme.match /^wss?$/
      @concurrency.times do
        begin
          sock = ParserBundle.new(uri).open_socket.sock
        rescue SystemCallError => e
          @subscriber.on_failure error(0, e.to_s)
          close nil
          return
        end
          
        @handshake = WebSocket::Handshake::Client.new(url: @url)
        sock << @handshake.to_s
        
        #handshake response
        loop do
          @handshake << sock.readline
          break if @handshake.finished?
        end
        
        if @handshake.valid?
          bundle = WebSocketBundle.new(@handshake, sock)
          @ws[bundle]=true
          async.listen bundle
          @notready-=1
          @cooked_ready.signal true if @notready == 0
        else
          @subscriber.on_failure error(@handshake.response_code, @handshake.response_line)
          close nil
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
    
    def on_error
      if !@connected[ws]
        @subscriber.on_failure error(ws.handshake.response_code, ws.handshake.response_line)
        try_halt
      end
    end
    
    def on_message(data, type, bundle)
      #puts "Received message: #{data} type: #{type}"
      if type==:close
        close_frame = WebSocket::Frame::Outgoing::Client.new(version: @handshake.version, type: :close)
        bundle.sock << close_frame.to_s
        #server should close the connection, just reply
        data.match(/(\d+)\s(.*)/)
        @subscriber.on_failure error(($~[1] || 0).to_i, $~[2] || "", true)
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
    
  end
  
  class LongPollClient < Client
    include Celluloid::IO
    
    def self.aliases
      [:longpoll]
    end
    
    def error(*args)
      @error_what||= ["#{@http2 ? "HTTP/2" : "HTTP"} Request"]
      super
    end
    
    class HTTPBundle < ParserBundle
      attr_accessor :parser, :sock, :last_message_time, :done, :time_requested, :request_time, :stop_after_headers
      
      def initialize(uri, user_agent, accept="*/*", extra_headers={})
        super
        @accept = accept
        @rcvbuf=""
        @sndbuf=""
        @parser = Http::Parser.new
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
            puts "< HTTP/1.1 #{@parser.status_code} [...]\r\n#{h.map {|k,v| "< #{k}: #{v}"}.join "\r\n"}"
          end
          @headers=h
          @last_modified = h['Last-Modified']
          @etag = h['Etag']
          @chunky = h['Transfer-Encoding']=='chunked'
          @gzipped = h['Content-Encoding']=='gzip'
          @code=@parser.status_code
          on_headers @parser.status_code, h
          if @stop_after_headers
            @bypass_parser = true
            :stop
          end
        end
        
        @parser.on_body = proc do |chunk|
          handle_chunk chunk
        end
        
        @parser.on_message_complete = proc do
          @chunky = nil
          @gzipped = nil
          on_response @parser.status_code, @parser.headers
        end
        
      end
      

      def handle_chunk(chunk)
        chunk = Zlib::GzipReader.new(StringIO.new(chunk)).read if @gzipped 
        on_chunk chunk
      end
      private :handle_chunk
      
      def reconnect?
        true
      end
      
      def send_GET(msg_time=nil, msg_tag=nil)
        @last_modified = msg_time.to_s if msg_time
        @etag = msg_tag.to_s if msg_tag
        @sndbuf.clear 
        data = @last_modified ? sprintf(@send_withid_fmt, @last_modified, @etag) : @send_noid_str
        @sndbuf << data
        
        if @headers && @headers["Connection"]=="close" && [200, 201, 202, 304, 408].member?(@parser.status_code) && reconnect?
          sock.close
          open_socket
          @parser.reset!
        end
        
        @time_requested=Time.now.to_f
        if verbose
          puts "", data.gsub(/^.*$/, "> \\0")
        end
        sock << @sndbuf
      end
      
      def read
        @rcvbuf.clear
        begin
          sock.readpartial(1024*10000, @rcvbuf)
          unless @bypass_parser
            @parser << @rcvbuf
          else
            handle_chunk @rcvbuf
          end
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
      def initialize(uri, user_agent, accept="*/*", extra_headers=nil)
        super
        @done = false
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
          extra_headers.each{ |h, v| @head[h.to_s.downcase]=v }
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
      
      def reconnect?
        false
      end
      
      def send_GET(msg_time=nil, msg_tag=nil)
        @last_modified = msg_time.to_s if msg_time
        @etag = msg_tag.to_s if msg_tag
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
          @headers = @resp_headers
          @code = @resp_code
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
    
    attr_accessor :timeout
    def initialize(subscr, opt={})
      super
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
      @extra_headers = opt[:extra_headers]
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
      uri.port||= uri.scheme.match(/^(ws|http)$/) ? 80 : 443
      @cooked=Celluloid::Condition.new
      @connected = @concurrency
      @notready = @concurrency
      @concurrency.times do |i|
        begin
          bundle = new_bundle(uri, "pubsub.rb #{self.class.name} #{@use_http2 ? "(http/2)" : ""} ##{i}")
        rescue SystemCallError => e
          @subscriber.on_failure error(0, e.to_s)
          close nil
          return
        end
        
        @bundles[bundle]=true
        bundle.send_GET @last_modified, @etag
        async.listen bundle
      end
    end
    
    def request_code_ok(code, bundle)
      if code != 200
        if code == 304 || code == 408
          @subscriber.on_failure error(code, "", bundle)
          @subscriber.finished+=1
          close bundle
        elsif @subscriber.on_failure(error(code, "", bundle)) == false
          @subscriber.finished+=1
          close bundle
        else
          binding.pry
          Celluloid.sleep @retry_delay if @retry_delay
          bundle.send_GET
        end
        false
      else
        @timer.reset if @timer
        true
      end
    end
    
    def new_bundle(uri, useragent, accept="*/*", extra_headers={})
      if @extra_headers
        extra_headers = extra_headers.merge @extra_headers
      end
      if @gzip
        extra_headers = extra_headers.merge({ "Accept-Encoding" => "gzip, deflate"})
      end
      b=(@http2 ? HTTP2Bundle : HTTPBundle).new(uri, useragent, accept, extra_headers)
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
        @subscriber.waiting-=1
        # Headers and body is all parsed
        b.last_modified = headers["Last-Modified"]
        b.etag = headers["Etag"]
        b.request_time = Time.now.to_f - b.time_requested
        if request_code_ok(code, b)
          on_message_ret=nil
          Message.each_multipart_message(headers["Content-Type"], body) do |content_type, msg_body, multi|
            unless @nomsg
              msg=Message.new msg_body.dup
              msg.content_type=content_type
              unless multi
                msg.last_modified= headers["Last-Modified"]
                msg.etag= headers["Etag"]
              end
            else
              msg=msg_body.dup
            end
            
            on_message_ret= @subscriber.on_message(msg, b)
          end
          
          unless on_message_ret == false
            @subscriber.waiting+=1
            b.send_GET
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
          @subscriber.on_failure error(0, "Server Closed Connection"), bundle
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
    
  end
  
  class IntervalPollClient < LongPollClient
    def self.aliases
      [:intervalpoll, :http, :interval, :poll]
    end
    
    def request_code_ok(code, bundle)
      if code == 304
        if @subscriber.on_failure(error(code, "", bundle), true) == false
          @subscriber.finished+=1
          close bundle
        else
          Celluloid.sleep(@retry_delay || 1)
          bundle.send_GET
          false
        end
      else
        super
      end
    end
  end
  
  class EventSourceClient < LongPollClient
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
    
    def new_bundle(uri, useragent=nil)
      super uri, useragent, "text/event-stream"
    end
    
    def setup_bundle(b)
      b.on_headers do |code, headers|
        if code != 200
          @subscriber.on_failure error(code, "")
          close b
          b.on_response do |code, headers, body|
            @subscriber.finished+=1
            close b
          end
        else
          @notready-=1
          @cooked_ready.signal true if @notready == 0
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
          @subscriber.on_failure error(0, "Response completed unexpectedly", b)
        end
        @subscriber.finished+=1
        close b
      end
      
      b.subparser.on_event do |evt, data, evt_id|
        case evt 
        when :message
          @timer.reset if @timer
          unless @nomsg
            msg=Message.new data.dup
            msg.id=evt_id
          else
            msg=data
          end
          
          if @subscriber.on_message(msg, b) == false
            @subscriber.finished+=1
            close b
          end
        when :comment
          if data.match(/^(?<code>\d+): (?<message>.*)/)
            @subscriber.on_failure error($~[:code].to_i, $~[:message], b)
            @subscriber.finished+=1
            close b
          end
        end
      end
      b
    end
    
  end
  
  class MultiparMixedClient < LongPollClient
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
        #puts @buf
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
          else
            repeat = false
          end
          
          if @headered && @buf.slice!(/^(.*?)\r\n--#{Regexp.escape @bound}/m)
            @on_part.call @headers, $~[1]
            @headered = nil
            @headers.clear
            repeat = true
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
    
    def new_bundle(uri, useragent=nil)
      super uri, useragent, "multipart/mixed"
    end
    
    def setup_bundle b
      super
      b.on_headers do |code, headers|
        if code != 200
          @subscriber.on_failure error(code, "", b)
          close b
        else
          b.connected = true
          @notready -= 1
          @cooked_ready.signal true if @notready == 0
          b.subparser = MultipartMixedParser.new headers["Content-Type"]
          b.subparser.on_part do |headers, message|
            unless @nomsg
              @timer.reset if @timer
              msg=Message.new message.dup, headers["Last-Modified"], headers["Etag"]
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
          @subscriber.on_failure error(410, "Server Closed Connection", b)
          @subscriber.finished+=1
          close b
        end
      end
      
      b.on_response do |code, headers, body|
        if b.subparser.finished
          @subscriber.on_failure error(410, "Server Closed Connection", b)
        else
          @subscriber.on_failure error(0, "Response completed unexpectedly", b)
        end
        @subscriber.finished+=1
        close b
      end
    end
  end
  
  class HTTPChunkedClient < LongPollClient
    include Celluloid::IO
    
    def provides_msgid?
      false
    end
    
    def run(*args)
      if @http2
        @subscriber.on_failure error(0, "Chunked transfer is not allowed in HTTP/2")
        @connected = 0
        return
      end
      super
    end
    
    def self.aliases
      [:chunked]
    end
    
    def new_bundle(uri, user_agent)
      super uri, user_agent, "*/*", {"TE": "Chunked"}
    end
    
    def setup_bundle(b)
      super
      b.body_buf = nil
      b.on_headers do |code, headers|
        if code != 200
          @subscriber.on_failure(error(code, "", b))
          close b
        elsif headers["Transfer-Encoding"] != "chunked"
          @subscriber.on_failure error(0, "Transfer-Encoding should be 'chunked', was '#{headers["Transfer-Encoding"]}'.", b)
          close b
        else
          @notready -= 1
          @cooked_ready.signal true if @notready == 0
          b.connected= true
        end
      end
      
      b.stop_after_headers = true
      @inchunk = false
      @chunksize = 0
      @repeat = true
      @chunkbuf = ""
      b.on_chunk do |chunk|
        #puts "yeah"
        @chunkbuf << chunk
        @repeat = true
        while @repeat
          @repeat = false
          if !@inchunk && @chunkbuf.slice!(/^([a-fA-F0-9]+)\r\n/m)
            @chunksize = $~[1].to_i(16)
            @inchunk = true
          end
          
          if @inchunk
            if @chunkbuf.length >= @chunksize + 2
              msgbody = @chunkbuf.slice!(0...@chunksize)
              @chunkbuf.slice!(/^\r\n/m)
              @timer.reset if @timer
              unless @nomsg
                msg=Message.new msgbody, nil, nil
              else
                msg=msgbody
              end
              if @subscriber.on_message(msg, b) == false
                @subscriber.finished+=1
                close b
              end
              @repeat = true if @chunkbuf.length > 0
              @inchunk = false
              @chunksize = 0
            end
          end
        end
      end
      
      b.on_response do |code, headers, body|
        @subscriber.on_failure error(410, "Server Closed Connection", b)
        close b
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
    @concurrency = opt[:concurrency]
    @opt=opt
    new_client
    reset
  end
  def new_client
    @client=@Client_Class.new self, @opt
  end
  def reset
    @errors=[]
    unless @nostore
      @messages=MessageStore.new :noid => !(client.provides_msgid? && @care_about_message_ids)
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
  def wait(until_what=nil)
    @client.poke until_what
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
  
  def on_failure(err=nil, nostore=false, &block)
    if block_given?
      @on_failure=block
    else
      @errors << err.to_s unless nostore
      @on_failure.call(err.to_s, err.bundle) if @on_failure.respond_to? :call
    end
  end
end

class Publisher
  #include Celluloid
  attr_accessor :messages, :response, :response_code, :response_body, :nofail, :accept, :url, :extra_headers, :verbose
  def initialize(url, opt={})
    @url= url
    unless opt[:nostore]
      @messages = MessageStore.new :noid => true
      @messages.name = "pub"
    end
    @timeout = opt[:timeout]
    @accept = opt[:accept]
    @verbose = opt[:verbose]
    @on_response = opt[:on_response]
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
  
  def on_response(&block)
    @on_response = block if block_given?
    @on_response
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
      connecttimeout: @timeout || PUBLISH_TIMEOUT,
      verbose: @verbose
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
        block.call(self.response_code, self.response_body) if block
        on_response.call(self.response_code, self.response_body) if on_response
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
