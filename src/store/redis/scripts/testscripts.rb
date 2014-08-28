#!/usr/bin/ruby
require 'digest/sha1'
require "redis"
require 'open3'
require 'minitest'
require 'minitest/reporters'
Minitest::Reporters.use! Minitest::Reporters::DefaultReporter.new(color: true)
require "minitest/autorun"
require 'securerandom'
require "pry"


class PubSubTest < Minitest::Test
  @@redis=nil
  @@scripts= {}
  @@files=   {}
  @@scripts= {}
  @@hashes=  {}
  
  def self.test_order
    :alpha
  end
  
  def self.luac
    if @@scripts
      @@scripts.each do |name, script|
        Open3.popen2e('luac', "-p", @@files[name]) do |stdin, stdouterr, process|
          raise stdouterr.read unless process.value.success?
        end
      end
    else
      raise "scripts not loaded yet"
    end
  end
  
  def self.loadscripts
    @@scripts.each do |name, script|
      begin
        h=@@redis.script :load, script
        @@hashes[name]=h
      rescue Redis::CommandError => e
        e.message.gsub!(/:\s+(user_script):(\d+):/, ": \n#{name}.lua:\\2:")
        def e.backtrace; []; end
        raise e
      end
    end
  end
  
  def setup
    unless @@redis
      @@redis=Redis.new(:host => "127.0.0.1", :port => 8537, :db => 1)

      Dir[ "#{File.dirname(__FILE__)}/*.lua" ].each do |f|
        scriptname=File.basename(f, ".lua").to_sym
        @@scripts[scriptname]=IO.read f
        @@files[scriptname]=f
      end
      self.class.luac
      self.class.loadscripts
    end
  end

  def redis; @@redis; end
  def hashes; @@hashes; end

  def publish(channel_id, msg)
    time=msg[:time] || Time.now.utc.to_i
    msg_tag, channel=redis.evalsha hashes[:publish], [], [channel_id, time, msg[:data], msg[:'content_type'] || msg[:'content-type'], msg[:ttl]]
    return "#{time}:#{msg_tag}", channel
  end
  
  def getmsg(ch_id, msg_id=nil)
    msg_time, msg_tag = msg_id.split(":") if msg_id
    status, msg_time, msg_tag, msg = redis.evalsha hashes[:get_message], [], [ch_id, msg_time, msg_tag]
    if status == 404
      return false, false
    elsif status == 418
      return true, false
    elsif status == 200
      return "#{msg_time}:#{msg_tag}", msg
    end
  end
  
  def randid
    SecureRandom.hex
  end
  
  def test_connection
    assert_equal "PONG", redis.ping
  end
  
  #now for the real tests
  def test_publish
    msg_id, channel = publish randid, data: "hello what is this", ttl: 10, content_type: "X-fruit/banana"
  end
  
  def test_nomessage
    id = randid
    publish id, data:"whatever this is", ttl: 100
    msgid, channel = getmsg id, "1200:0"
    refute (msgid || channel), "message should be absent"
  end
  
  def test_simple
    id=randid
    msg2_data = "what is this i don't even"
    msgid, channel   = publish id, data: "whatever",ttl: 100, content_type: "X-fruit/banana"
    msgid2, channel2 = publish id, data: msg2_data, ttl: 100, content_type: "X-fruit/banana"
    refute_equal msgid, msgid2, "two different messages should have different message ids"
    msgid_got, msg_got = getmsg id, msgid
    assert_equal msg2_data, msg_got, "retrieved message data doesn't match"
    assert_equal msgid2, msgid_got, "retrieved message id doesn't match"
  end
  
  def test_publish_invalid_content_type
    e = assert_raises Redis::CommandError do
      publish randid, data: "this message has an invalid content-type", content_type: "foo:bar", ttl: 10
    end
    assert_match /cannot contain ":"/, e.message
  end
end
