#!/usr/bin/ruby
require 'securerandom'
require_relative 'pubsub.rb'
require 'json'
require "reel"

PUB_URL="http://127.0.0.1:8083/action/pub/"
SUB_URL="http://127.0.0.1:8082/action/sub/"

AUTH_PORT=8053

TIMEOUT=1000
DEFAULT_CLIENT=:longpoll

#Typhoeus::Config.verbose = true
def short_id
  SecureRandom.hex.to_i(16).to_s(36)[0..5]
end

def short_msg
  SecureRandom.hex.to_i(16).to_s(36)
end

Celluloid.boot

class AuthServerRunner
  include Celluloid
  class AuthServer < Reel::Server::HTTP
    def initialize(host = "127.0.0.1", port = AUTH_PORT)
      puts "init"
      super(host, port, &method(:on_connection))
    end
    
    def on_connection(connection)
      connection.each_request do |request|
        handle_request(request)
      end
    end
    
    def handle_request(request)
      request.respond :ok, ""
    end
  end
  
  def run
    AuthServer.run
  end
end

class SubTest
  attr_accessor :id, :sub
  def initialize(id, out=nil)
    @id=id
    @out=out
    @sub=Subscriber.new "#{SUB_URL}#{id}", 1, timeout: TIMEOUT, quit_message: 'FIN', client: DEFAULT_CLIENT, nostore: true
    @sub.on_message do |msg|
      if @expected != msg.message 
        raise "unexpected message for id #{id}. expected #{@expected}, got #{msg.message}"
      elsif @out
        puts "received correct message for #{id}"
      end
    end
  end
  
  def expect_msg(msg)
    @expected=msg
  end
  
  def run
    @sub.run
  end
end

class TestRig
  def initialize
    @ids=[]
    @subs={}
    @pub = Publisher.new '', accept: 'text/json', nostore: true
  end
  
  def [](id)
    @subs[id]
  end
  
  def sub(id=nil)
    id = short_id if id.nil?
    raise "#{id} already running" if @subs[id]
    @ids << id
    @subs[id] = SubTest.new(id, @out)
    @subs[id].run
  end
  
  def pub(id)
    raise "#{id} not subscribed" unless @subs[id]
    msg = short_msg
    @subs[id].expect_msg(msg)
    @pub.with_url("#{PUB_URL}#{id}").post(msg)
    
    info=JSON.parse @pub.response_body
    
    if info["subscribers"] != 1
      raise "subscribers != 1 for id #{id}"
    end
  end
  
  def unsub(id=nil)
    @subs[id].terminate
    @subs.delete id
    @ids.delete id
  end
  
  def unsub_all
    @subs.each do |sub|
      sub.terminate
    end
    @subs.empty!
    @ids.clear
  end
  
  def last_id
    @last_id ||= random_id
  end
  
  def random_id
    @ids.sample
  end
  
  def post_again
    id=last_id
    out "post again to #{id}"
    pub(id)
  end

  def post_testingly
    id = random_id
    out "post to #{id}"
    pub(id)
    @last_id=id
  end
  
  def out(str)
    puts str if @out
  end
  
  def output!
    @out=true
  end
end
  

auth = AuthServerRunner.new
auth.async.run
sleep 5

rig = TestRig.new

class Runner 
  def initialize(rig, count)
    @rig = rig
    @rig.output!
    count.times do
      @rig.sub
    end
  end
  
  def run
    while true do
      if rand(2)==1
        @rig.post_testingly
      else
        @rig.post_again
      end
      
      
      t = rand(1..7)
      puts "wait #{t} sec"
      sleep t
    end
  end
end

runner = Runner.new(rig, 20)

runner.run
