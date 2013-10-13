#!/usr/bin/ruby
require 'typhoeus'
require 'json'
require 'pry'
require 'celluloid'
Typhoeus::Config.memoize = false

class Subscriber
  include Celluloid
  attr_accessor :clients, :hydra
  def initialize(url, num_clients=1, timeout=60)
    @url=url
    @timeout=timeout

    @etag = ""
    @last_modified = ""

    @hydra = Typhoeus::Hydra.new(:max_concurrency => num_clients + 1)
    @clients = []
    puts "Starting #{num_clients} subscribers to #{url}"
    num_clients.times do
      client = Client.new(url, self)
    end
  end
  
  def log_response(resp)
    puts "received \"#{resp.body}\""
  end
  def log_failure(failure)
    puts "failed!"
  end
  
  class Client
    attr_accessor :subscriber, :last_modified, :etag
    def initialize(url, subscr)
      @subscriber = subscr
      @last_modified, @etag = last_modified, etag
      @request = Typhoeus::Request.new(
        url,
        timeout: 60000,
      )
      @request.on_complete do |response|
        puts "recieved response at #{@request.url}"
        if response.success?
          @request.options[:headers]["If-None-Match"]=response.headers["Etag"]
          @request.options[:headers]["If-Modified-Since"]=response.headers["Last-Modified"]
          @subscriber.log_response response
          keep_running true
        else
          @subscriber.log_failure
          keep_running false
        end
      end
      keep_running
    end
    def keep_running(was_success=nil)
      @subscriber.hydra.queue @request
      puts "queued request for #{@request.url}"
    end
  end
  
  def run
    @hydra.run
  end
end

class Publisher
  include Celluloid
  def initialize(url)
    @url= url
  end
  def post(body, content_type='text/plain')
    post = Typhoeus::Request.new(
      @url,
      headers: {:'Content-Type' => content_type},
      method: "POST",
      body: body,
    )
    #puts "Posting message to #{@url}"
    post.on_complete do |response|
      if response.success?
        # hell yeah
        #puts response.response_headers
        #puts response.body
      else
        puts "problem submitting request"
      end
    end
    post.run
  end
end

pub = Publisher.new("http://localhost:8082/broadcast/pub/foo")
sub = Subscriber.new("http://localhost:8082/broadcast/sub/foo", 1)

sub.async.run
pub.post "hello there"
pub.post "i am here"


def simple_test
  sub = Subscriber.new "http://localhost:8082/broadcast/sub/simple", 60
  sub.async.run
  pub = Publisher.new("http://localhost:8082/broadcast/pub/foo")
  5000.times do
    pub.post "hi there"
  end
end

simple_test
sleep
