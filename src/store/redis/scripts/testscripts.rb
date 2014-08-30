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

  class Msg
    attr_accessor :data, :chid, :time, :tag, :id, :content_type, :channel_info, :ttl
    def initialize(channel_id, arg)
      @chid= channel_id
      id(arg[:id]) if arg[:id]
      @time= (arg[:time] || Time.now.utc).to_i
      @id=arg[:id]
      @data= arg[:data]
      @ttl=arg[:ttl]
      @content_type=arg[:content_type]
    end
    def ==(msg2)
      return @chid==msg2.chid && @id==msg2.id && @data==msg2.data
    end
    
    def id(*arg)
      if arg.length == 0
        raise "id not ready" unless @time.nil? || @tag.nil?
        @id||= "#{time}:#{tag}"
        return @id
      elsif arg.length == 2
        @time= arg.first.to_i
        @tag=  arg.last.to_i
        @id= "#{time}:#{tag}"
      elsif arg.length == 1
        a = arg.first
        case a
        when Hash
          if a[:id]
            @time, @tag = a[:id].split ":"
          else
            @time = a[:time].to_i
            @tag  = a[:tag].to_i
          end
        when String
          @time, @tag = a.split ":"
          @time=@time.to_i
          if @time==0 || @time.nil? || @tag.nil?
            @time, @tag, @id = nil, nil, nil
          end
        end
        @id= "#{time}:#{tag}"
      end
      @id
    end
    def time=(t)
      id(time: t)
    end
    def tag=(t)
      id(tag: t)
    end
  end

  def publish(*arg)
    if arg.length==1
      msg = arg.first
      msg=Msg.new(msg[:channel_id] || msg[:chid], msg) if Hash === msg
    elsif arg.length==2
      msg=Msg.new(arg.first, arg.last)
    end

    msg.time= Time.now.utc.to_i unless msg.time
    msg_tag, channel_info=redis.evalsha hashes[:publish], [], [msg.chid, msg.time, msg.data, msg.content_type, msg.ttl]
    msg.channel_info=channel_info
    return msg
  end

  def getmsg(*arg)
    ch_id, msg_id, msg=nil, nil, nil
    if Msg === arg.first
      msg=arg.first
      msg_id, msg_time, msg_tag = msg.id, msg.time, msg.tag
      ch_id=msg.chid
    elsif arg.count==2 #channel_id, msg_id
      ch_id = arg.first
      msg_id = arg.last
      msg_time, msg_tag = msg_id.split(":")
      raise "invalid message id" if msg_time.nil? or msg_tag.nil?
    elsif arg.count == 1
      raise "Not enough arguments to getmsg"
    end
    msg_tag=0 unless msg_tag
    status, msg_time, msg_tag, msg, msg_content_type = redis.evalsha hashes[:get_message], [], [ch_id, msg_time, msg_tag]
    if status == 404
      return nil
    elsif status == 418 #not ready
      return false
    elsif status == 200
      return Msg.new ch_id, time: msg_time, tag: msg_tag, data: msg, content_type: msg_content_type
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
    msg_id, channel = publish chid: randid, data: "hello what is this", ttl: 10, content_type: "X-fruit/banana"
  end
  
  def test_nomessage
    id = randid
    publish chid: id, data:"whatever this is", ttl: 100
    msg = getmsg randid, "1200:0"
    refute msg, "message should be absent"
  end
  
  def test_simple
    id=randid
    msg2_data = "what is this i don't even"
    msg1 = publish chid: id, data: "whatever",ttl: 100, content_type: "X-fruit/banana"
    msg2 = publish chid: id, data: msg2_data, ttl: 100, content_type: "X-fruit/banana"
    refute_equal msg1, msg2, "two different messages should have different message ids"
    msg = getmsg msg1
    assert_equal msg2, msg, "retrieved message doesn't match"
  end
  
  def test_publish_invalid_content_type
    e = assert_raises Redis::CommandError do
      publish randid, data: "this message has an invalid content-type", content_type: "foo:bar", ttl: 10
    end
    assert_match /cannot contain ":"/, e.message
  end
end
