#!/usr/bin/ruby
require 'digest/sha1'
require "redis"

require 'minitest'
require 'minitest/reporters'
Minitest::Reporters.use! Minitest::Reporters::DefaultReporter.new(color: true)
require "minitest/autorun"
require 'securerandom'
require "pry"



class PubSubTest < Minitest::Test
  def self.test_order
    :alpha
  end
  
  def setup
    @@redis||=Redis.new(:host => "127.0.0.1", :port => 8537, :db => 1)
    @@scripts||={}
    @@hashes||={}
    @redis, @scripts, @hashes = @@redis, @@scripts, @@hashes #persist!
  end
  
  def test_00_connection
    assert_equal "PONG", @redis.ping
  end
  
  def test_01_syntax
    Dir[ "#{File.dirname(__FILE__)}/*.lua" ].each do |f|
      @scripts[File.basename(f, ".lua") .to_sym]=IO.read f
    end
    
    @scripts.each do |name, script|
      begin
        h=@redis.script :load, script
        @hashes[name]=h
      rescue Redis::CommandError => e
        e.message.gsub!(/:\s+(user_script):(\d+):/, ": \n#{name}.lua:\\2:")
        def e.backtrace; nil; end
        raise e
      end
    end
  end
  
  def test_02_hash
    @scripts.each do |name, script|
      assert_equal @hashes[name], Digest::SHA1.hexdigest(script), "Hashes for script #{name} don't match"
    end
  end
  
  def test_publish
    pubhash = @hashes[:publish]
    assert pubhash, "Publish script missing"
    
    id="ok/foobar"
    msg="hi how are you"
    ttl=20 #seconds
    
    msg_tag, channel = @redis.evalsha pubhash, ["pushmodule:message_time_offset"], [id, Time.now.utc.to_i, msg, "text/plain", ttl]
    
    #binding.pry
  end
end
