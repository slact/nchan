#!/usr/bin/ruby
require 'typhoeus'
require 'json'
require 'pry'
require 'celluloid'
Typhoeus::Config.memoize = false

class Message
  attr_accessor :content_type, :message, :times_seen, :etag, :last_modified
  def initialize(msg, last_modified=nil, etag=nil)
    @times_seen=1
    @message, @last_modified, @etag = msg, last_modified, etag
  end
  def id
    @id||="#{last_modified}:#{etag}"
  end
  def to_s
    @message
  end
end

class MessageStore
  include Enumerable
  attr_accessor :msgs, :quit_message

  def matches? (msg_store)
    my_messages = messages
    if MessageStore === msg_store
      other_messages = msg_store.messages
    else
      other_messages = msg_store
    end
    return false, "Message count doesn't match. ( #{my_messages.count}, #{other_messages.count})" unless my_messages.count == other_messages.count
    other_messages.each_with_index do |msg, i|
      return false, "Message #{i} doesn't match. (|#{my_messages[i].length}|, |#{msg.length}|) " if my_messages[i] != msg
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
  def pp
    each do |msg|
      puts "\"#{msg.to_s}\" (seen #{msg.times_seen} times.)"
    end
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
        puts "Received different messages with same message id #{msg.id}: '#{cur_msg.message}' and '#{msg.message}'" unless cur_msg.message == msg.message
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
  class LongPollClient
    include Celluloid
    attr_accessor :last_modified, :etag, :hydra, :timeout
    def initialize(subscr, opt={})
      @last_modified, @etag, @timeout = opt[:last_modified], opt[:etag], opt[:timeout] || 10
      @connect_timeout = opt[:connect_timeout]
      @subscriber=subscr
      @url=subscr.url
      @concurrency=opt[:concurrency] || opt[:clients] || 1
      @hydra= Typhoeus::Hydra.new( max_concurrency: @concurrency)
      @gzip=opt[:gzip]
      @retry_delay=opt[:retry_delay]
    end

    def response_success(response, req)
      #puts "received OK response at #{req.url}"
      #parse it
      msg=Message.new response.body, response.headers["Last-Modified"], response.headers["Etag"]
      msg.content_type=response.headers["Content-Type"]
      req.options[:headers]["If-None-Match"]=msg.etag
      req.options[:headers]["If-Modified-Since"]=msg.last_modified
      unless @subscriber.on_message(msg) == false
        @subscriber.waiting+=1
        Celluloid.sleep @retry_delay if @retry_delay
        @hydra.queue req
      else
        @subscriber.finished+=1
      end
    end
    
    def response_failure(response, req)
      #puts "received bad or no response at #{req.url}"
      unless @subscriber.on_failure(response) == false
        @subscriber.waiting+=1
        Celluloid.sleep @retry_delay if @retry_delay
        @hydra.queue req
      else
        @subscriber.finished+=1
      end
    end
    
    def new_request
      req=Typhoeus::Request.new(@url, timeout: @timeout, connecttimeout: @connect_timeout, accept_encoding: (@gzip ? "gzip" : nil) )
      req.on_complete do |response|
        @subscriber.waiting-=1
        if response.success?
          response_success response, req
        else
          response_failure response, req
        end
      end
      req
    end

    def run(was_success=nil)
      #puts "running #{self.class.name} hydra with #{@hydra.queued_requests.count} requests."
      (@concurrency - @hydra.queued_requests.count).times do
        @subscriber.waiting+=1
        @hydra.queue new_request
      end
      @hydra.run
    end
    
    def poke
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
      if response.code == 304 || @subscriber.on_failure(response) != false
        @subscriber.waiting+=1
        Celluloid.sleep @retry_delay if @retry_delay
        @hydra.queue req
      else
        @subscriber.finished+=1
      end
    end
    
    def poke
      while @subscriber.finished < @concurrency do
        sleep 1
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
    reset
    #puts "Starting subscriber on #{url}"
    case opt[:client]
    when :longpoll, :long, nil
      @client_class=LongPollClient
    when :interval, :intervalpoll
      @client_class=IntervalPollClient
    when :ws, :websocket
      raise "websocket client not yet implemented"
    when :es, :eventsource
      raise "EventSource client not yet implemented"
    else
      raise "unknown client type #{opt[:client]}"
    end
    @concurrency=concurrency
    @client_class ||= opt[:client_class] || LongPollClient
    new_client @client_class
  end
  def new_client(client_class=LongPollClient)
    @client=client_class.new(self, concurrency: @concurrency, timeout: @timeout, connect_timeout: @connect_timeout, gzip: @gzip, retry_delay: @retry_delay)
  end
  def reset
    @errors=[]
    @messages=MessageStore.new :noid => !@care_about_message_ids
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
      return false
    end
    true
  end
  def wait
    @client.poke
  end

  def on_message(msg=nil, &block)
    #puts "received message #{msg.to_s[0..15]}"
    if block_given?
      @on_message=block
    else
      @messages << msg
      return false if @quit_message == msg.to_s
      @on_message.call(msg) if @on_message.respond_to? :call
    end
  end
  def on_failure(response=nil, &block)
    if block_given?
      @on_failure=block
    else
      #puts "failed with #{response.to_s}. handler is #{@on_failure.to_s}"
      if response.timed_out?
        # aw hell no
        @errors << "Client response timeout."
      elsif response.code == 0
        # Could not get an http response, something's wrong.
        @errors << response.return_message
      else
        # Received a non-successful http response.
        @errors << "HTTP request failed: #{response.return_message} (code #{response.code})"
      end
      @on_failure.call(response) if @on_failure.respond_to? :call
    end
  end
end

class Publisher
  include Celluloid
  attr_accessor :messages, :response, :response_code, :response_body, :nofail, :accept
  def initialize(url)
    @url= url
    @messages = MessageStore.new :noid => true
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
    )
    msg=Message.new body
    msg.content_type=content_type
    post.on_complete do |response|
      self.response=response
      self.response_code=response.response_code
      self.response_body=response.response_body
      if response.success?
        #puts "published message #{msg.to_s[0..15]}"
        @messages << msg
      elsif response.timed_out?
        # aw hell no
        #puts "publisher err: timeout"
        raise "Response timed out."
      elsif response.code == 0
        # Could not get an http response, something's wrong.
        #puts "publisher err: #{response.return_message}"
        raise "No HTTP response: #{response.return_message}" unless self.nofail
      else
        # Received a non-successful http response.
        #puts "publisher err: #{response.code.to_s}"
        raise "HTTP request failed: #{response.code.to_s}" unless self.nofail
      end
      block.call(self) if block
    end
    #puts "publishing to #{@url}"
    post.run
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
