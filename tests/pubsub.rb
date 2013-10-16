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
    other_messages = msg_store.messages
    return false unless my_messages.count == other_messages.count
    other_messages.each_with_index do |msg, i|
      return false if my_messages[i] != msg
    end
    true
  end

  def initialize(opt={})
    @array=opt[:noid]
    @array ? @msgs=[] : @msgs={}
  end

  def messages
    self.to_a.map{|m|m.to_s}
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
  attr_accessor :url, :client, :messages, :max_round_trips, :quit_message
  def initialize(url, num_clients=1, opt={})
    @url=url
    @timeout=opt[:timeout] || 60
    @quit_message=opt[:quit_message]
    @messages = MessageStore.new
    #puts "Starting client to #{url}"
    @client=LongPollClient.new(self, :num_clients => num_clients, :timeout => @timeout)
  end
  def abort
    #destroy the client
  end

  class LongPollClient
    include Celluloid
    attr_accessor :last_modified, :etag, :hydra, :timeout
    def initialize(subscr, opt={})
      @last_modified, @etag, @timeout = opt[:last_modified], opt[:etag], opt[:timeout] || 10
      @subscriber=subscr
      @url=subscr.url
      @num_clients=opt[:num_clients] || opt[:clients] || 1
      @hydra= Typhoeus::Hydra.new( max_concurrency: @num_clients)
      
      (opt[:num_clients] || opt[:clients] || 1).times do
        req=Typhoeus::Request.new(@url, timeout: @timeout)
        req.on_complete do |response|
          #puts "recieved response at #{req.url}"
          if response.success?
            #parse it
            msg=Message.new response.body, response.headers["Last-Modified"], response.headers["Etag"]
            msg.content_type=response.headers["Content-Type"]
            req.options[:headers]["If-None-Match"]=msg.etag
            req.options[:headers]["If-Modified-Since"]=msg.last_modified
            unless @subscriber.on_message(msg) == false
              @hydra.queue req 
            end
          else
            unless @subscriber.on_failure(response) == false
              @hydra.queue req
            end
          end
        end
        @hydra.queue req
      end
    end
    
    def run(was_success=nil)
      #puts "running #{self.class.name} hydra with #{@hydra.queued_requests.count} requests."
      @hydra.run
    end
    
    def poke
    end
  end
  
  def run
    @client.async.run
  end
  def wait
    @client.poke
  end
  def on_message(msg=nil, &block)
    if block_given?
      @on_message=block
    else
      @messages << msg
      return false if @quit_message == msg.to_s
      @on_message.call(msg) if @on_message.respond_to? :call
    end
  end
  def on_failure(response=nil, request=nil, &block)
    if block_given?
      @on_failure=block
    else
      @on_failure.call(response, request) if @on_failure.respond_to? :call
    end
  end
end

class Publisher
  include Celluloid
  attr_accessor :messages
  def initialize(url)
    @url= url
    @messages = MessageStore.new :noid => true
  end
  
  def post(body, content_type='text/plain')
    post = Typhoeus::Request.new(
      @url,
      headers: {:'Content-Type' => content_type},
      method: "POST",
      body: body,
    )
    msg=Message.new body
    msg.content_type=content_type

    post.on_complete do |response|
      if response.success?
        @messages << msg
      else
        raise "Problem submitting request"
      end
    end
    post.run
  end
end
