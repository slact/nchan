#!/usr/bin/ruby
require 'digest/sha1'
require "redis"
require 'open3'
require 'minitest'
require 'minitest/reporters'
Minitest::Reporters.use! [Minitest::Reporters::SpecReporter.new(:color => true)]
require "minitest/autorun"
require 'securerandom'
require "pry"

REDIS_HOST="127.0.0.1"
REDIS_PORT=8537
REDIS_DB=1
NAMESPACE="nchan:"

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
      @@redis=Redis.new(:host => REDIS_HOST, :port => REDIS_PORT, :db => REDIS_DB)

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
    attr_accessor :data, :chid, :time, :tag, :id, :content_type, :channel_info, :ttl, :max_buf_size, :eventsource_event
    def _empty_is_nil(v)
      v == "" ? nil : v
    end
    def initialize(channel_id, arg={})
      @chid= _empty_is_nil channel_id
      id(arg[:id]) if arg[:id]
      @time= _empty_is_nil arg[:time]
      @tag= _empty_is_nil arg[:tag]
      @data= _empty_is_nil arg[:data]
      if @data && @time.nil?
        @time=Time.now.utc.to_i
      end
      @ttl=_empty_is_nil arg[:ttl]
      @eventsource_event = _empty_is_nil arg[:eventsource_event]
      @max_buf_size = _empty_is_nil arg[:max_buf_size] || 100
      @content_type=_empty_is_nil arg[:content_type]
    end
    def ==(m)
      self.to_s==m.to_s
    end
    
    def to_s
      "[#{@chid}]#{id}<#{@content_type}> #{@data}"
    end
    
    def id(msgid=nil)
      if msgid
        @id=msgid
        @time, @tag =@id.split(":")
      else
        @id||= "#{time}:#{tag}" unless time.nil? || tag.nil?
      end
      @id
    end
    def time=(t)
      @time=t
      id
    end
    def tag=(t)
      @tag=t
      id
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
    msg_tag, channel_info=redis.evalsha hashes[:publish], [], [NAMESPACE, msg.chid, msg.time, msg.data, msg.content_type, msg.eventsource_event, msg.ttl, msg.max_buf_size, 'publish', 1]
    msg.tag=msg_tag
    msg.channel_info=channel_info
    return msg
  end

  def delete(ch_id)
    redis.evalsha hashes[:delete], [], [NAMESPACE, ch_id]
  end

  def getmsg(msg, opt={})
    if String===opt
      ch_id=msg
      msg_id=opt
      msg_time, msg_tag = msg_id.split(":")
    elsif Msg === msg
      msg_id, msg_time, msg_tag = msg.id, msg.time, msg.tag
      ch_id=msg.chid
    else
      msg_time, msg_tag = msg.split(":")
      msg_id=msg
    end
    traverse_order=((Hash === opt) && opt[:getfirst]) ? 'FIFO' : 'FILO'
    msg_tag=0 if msg_time && msg_tag.nil?
    #binding.pry
    status, msg_ttl, msg_time, msg_tag, prev_msg_time, prev_msg_tag, msg_data, msg_content_type, msg_eventsource_event, subscriber_count = redis.evalsha hashes[:get_message], [], [NAMESPACE, ch_id, msg_time || 0, msg_tag || 0, traverse_order, 15 ]
    if status == 404
      return nil
    elsif status == 418 #not ready
      return false
    elsif status == 200
      return Msg.new ch_id, time: msg_time, tag: msg_tag, data: msg_data, content_type: msg_content_type, eventsource_event: msg_eventsource_event, ttl: msg_ttl
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
    m=[]
    m << Msg.new(id, data: "whatever",ttl: 100, content_type: "X-fruit/banana")
    m << Msg.new(id, data: msg2_data, ttl: 100, content_type: "X-fruit/banana")
    m.each do |msg|
      publish msg
    end
    refute_equal m[0], m[1], "two different messages should have different message ids"
    msg = getmsg m[0]

    assert_equal m[1], msg, "retrieved message doesn't match"
  end
  
  def test_publish_invalid_content_type
    e = assert_raises Redis::CommandError do
      publish randid, data: "this message has an invalid content-type", content_type: "foo:bar", ttl: 10
    end
    assert_match /cannot contain ":"/, e.message
  end
  
  def test_walk_buffer
    id=randid
    sent=[]
    %w( foobar1 whatthe-somethng-of isnot-even-meaning -has-you-evenr-as-sofar-as-to -even-move-zig).each do |w|
      sent << Msg.new(id, data: w, ttl: 100)
      sleep rand(0..1.1)
    end
    sent.each do |m|
      publish m
    end
    
    cur=Msg.new(id)
    while sent.length>0
      #binding.pry
      cur=getmsg cur
      n=sent.shift
      assert_equal cur, n
      cur=n
    end
    assert_equal false, getmsg(cur), "Last message should return 'false'"
  end
  
  def test_timeout
    id=randid
    sent=[]
    sent << publish(Msg.new(id, data: "fee", ttl: 3))
    sleep 2
    sent << publish(Msg.new(id, data: "foo", ttl: 3))
    sent << publish(Msg.new(id, data: "fie", ttl: 3))
    sleep 1.5
    cur=getmsg id, ""
    assert_equal cur, sent[1]
    sleep 1.6
    cur=getmsg id, ""
    assert_equal false, cur
    #nice!
  end
  
  def test_largemsg
    id=randid
    sent= publish(Msg.new id, data: "0" * 100000 * 1024, ttl: 20)
    cur = getmsg id, ""
    assert_equal cur, sent
  end
  
  def test_delete
    id=randid
    sent=[]
    sent << publish(Msg.new(id, data: "fee", ttl: 30))
    sent << publish(Msg.new(id, data: "foo", ttl: 30))
    sent << publish(Msg.new(id, data: "foo2", ttl: 30))
    
    cur=getmsg id, ""
    assert_equal sent[0], cur
    
    cur=getmsg cur
    assert_equal sent[1], cur
    
    cur=getmsg cur
    assert_equal sent[2], cur
    
    delete id
    
    cur=getmsg Msg.new(id), sub: 'foobar'
    assert_equal false, cur
  end
  
end
