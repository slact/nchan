#!/usr/bin/ruby
require 'minitest'
require 'minitest/reporters'
require "minitest/autorun"
Minitest::Reporters.use! [Minitest::Reporters::SpecReporter.new(:color => true)]
require 'securerandom'
require_relative 'pubsub.rb'
require_relative 'authserver.rb'

SERVER=ENV["PUSHMODULE_SERVER"] || "127.0.0.1"
PORT=ENV["PUSHMODULE_PORT"] || "8082"
DEFAULT_CLIENT=:longpoll
OMIT_LONGMSG=ENV["OMIT_LONGMSG"]
#Typhoeus::Config.verbose = true
def short_id
  SecureRandom.hex.to_i(16).to_s(36)[0..5]
end

def url(part="")
  part=part[1..-1] if part[0]=="/"
  "http://#{SERVER}:#{PORT}/#{part}"
end
puts "Server at #{url}"
def pubsub(concurrent_clients=1, opt={})
  test_name = caller_locations(1,1)[0].label
  urlpart=opt[:urlpart] || 'broadcast'
  timeout = opt[:timeout]
  sub_url=opt[:sub] || "sub/broadcast/"
  pub_url=opt[:pub] || "pub/"
  chan_id = opt[:channel] || SecureRandom.hex
  sub = Subscriber.new url("#{sub_url}#{chan_id}?test=#{test_name}"), concurrent_clients, timeout: timeout, use_message_id: opt[:use_message_id], quit_message: 'FIN', gzip: opt[:gzip], retry_delay: opt[:retry_delay], client: opt[:client] || DEFAULT_CLIENT, extra_headers: opt[:extra_headers], verbose: opt[:verbose]
  pub = Publisher.new url("#{pub_url}#{chan_id}?test=#{test_name}"), timeout: timeout
  return pub, sub
end
def verify(pub, sub, check_errors=true)
  assert sub.errors.empty?, "There were subscriber errors: \r\n#{sub.errors.join "\r\n"}" if check_errors
  ret, err = sub.messages.matches?(pub.messages)
  assert ret, err || "Messages don't match"
  i=0
  sub.messages.each do |msg|
    assert_equal sub.concurrency, msg.times_seen, "Concurrent subscribers didn't all receive message #{i}."
    i+=1
  end
end

class PubSubTest <  Minitest::Test
  def setup
    Celluloid.boot
  end
  
  def test_interval_poll
    pub, sub=pubsub 1, sub: "/sub/intervalpoll/", client: :intervalpoll, quit_message: 'FIN', retry_delay: 0.5
    ws_sub=Subscriber.new(sub.url, 1, client: :websocket, quit_message: 'FIN')

    got_304s=0
    sub.on_failure do |msg|
      got_304s += 1
      assert_match /code 304/, msg #handshake will be treated as intervalpoll client?...
    end
    
    ws_sub.run
    sub.run

    sleep 0.4
    assert ws_sub.match_errors(/code 403/), "expected 403 for all non-intervalpoll subscribers, got #{sub.errors.pretty_inspect}"
    ws_sub.terminate
    
    pub.post ["hello this", "is a thing"]
    sleep 0.3
    pub.post ["oh now what", "is this even a thing?"]
    sleep 0.1
    
    sleep 1.5
    
    pub.post "yoo"
    pub.post "FIN"
    
    sub.wait

    verify pub, sub
    sub.terminate
    assert got_304s > 0, "Expected at least one 304 response"
  end
  
  def test_channel_info
    require 'json'
    require 'nokogiri'
    require 'yaml'
    
    subs=20
    
    chan=SecureRandom.hex
    pub, sub = pubsub(subs, channel: chan, client: :eventsource)
    pub.nofail=true
    pub.get
    assert_equal 404, pub.response_code
    
    pub.post ["hello", "what is this i don't even"]
    assert_equal 202, pub.response_code
    pub.get
    
    assert_equal 200, pub.response_code
    assert_match /last requested: -?\d+ sec/, pub.response_body
    
    pub.get "text/json"
    
    info_json=JSON.parse pub.response_body
    assert_equal 2, info_json["messages"]
    #assert_equal 0, info_json["requested"]
    assert_equal 0, info_json["subscribers"]

    
    sub.run
    sub.wait :ready
    sleep 0.15
    pub.get "text/json"

    info_json=JSON.parse pub.response_body
    assert_equal 2, info_json["messages"]
    #assert_equal 0, info_json["requested"]
    assert_equal subs, info_json["subscribers"], "text/json subscriber count"

    pub.get "text/xml"
    ix = Nokogiri::XML pub.response_body
    assert_equal 2, ix.at_xpath('//messages').content.to_i
    #assert_equal 0, ix.at_xpath('//requested').content.to_i
    assert_equal subs, ix.at_xpath('//subscribers').content.to_i
    
    pub.get "text/yaml"
    yaml_resp1=pub.response_body
    pub.get "application/yaml"
    yaml_resp2=pub.response_body
    pub.get "application/x-yaml"
    yaml_resp3=pub.response_body
    yam=YAML.load pub.response_body
    assert_equal 2, yam["messages"]
    #assert_equal 0, yam["requested"]
    assert_equal subs, yam["subscribers"]
    
    assert_equal yaml_resp1, yaml_resp2
    assert_equal yaml_resp2, yaml_resp3
    
    
    pub.accept="text/json"

    pub.post "FIN"
    #stats right before FIN was issued
    info_json=JSON.parse pub.response_body
    assert_equal 3, info_json["messages"]
    #assert_equal 0, info_json["requested"]
    assert_equal subs, info_json["subscribers"]
    
    sub.wait
    
    pub.get "text/json"
    info_json=JSON.parse pub.response_body
    assert_equal 3, info_json["messages"], "number of messages received by channel is wrong"
    #assert_equal 0, info_json["requested"]
    assert_equal 0, info_json["subscribers"], "channel should say there are no subscribers"
    
    sub.terminate
  end
  
  def multi_sub_url(pubs, prefix="/sub/multi/", delim="/")
    ids = pubs.map{|v| v.id}.shuffle
    "#{prefix}#{ids.join delim}"
  end
  
  class MultiCheck
    attr_accessor :id, :pub
    def initialize(id)
      self.id = id
      self.pub = Publisher.new url("/pub/#{self.id}")
    end
  end
  
  
  def test_channel_multiplexing(n=2, delimited=false)
    
    pubs = []
    n.times do |i|
      pubs << MultiCheck.new(short_id)
    end
    
    sub_url=delimited ? multi_sub_url(pubs, '/sub/split/', '_') : multi_sub_url(pubs)
    
    n = 15
    scrambles = 5
    subs = []
    scrambles.times do |i|
      sub = Subscriber.new(url(sub_url), n, quit_message: 'FIN', retry_delay: 1, timeout: 20)
      sub.on_failure { false }
      subs << sub
    end
    
    subs.each &:run
    
    pubs.each {|p| p.pub.post "FIRST from #{p.id}" }
    
    10.times do |i|
      pubs.each {|p| p.pub.post "hello #{i} from #{p.id}" }
    end
    
    sleep 1
    
    5.times do |i|
      pubs.first.pub.post "yes #{i} from #{pubs.first.id}"
    end
    
    pubs.each do |p| 
      10.times do |i|
        p.pub.post "hello #{i} from #{p.id}"
      end
    end
    
    latesubs = Subscriber.new(url(sub_url), n, quit_message: 'FIN')
    latesubs.on_failure { false }
    subs << latesubs
    latesubs.run
    
    sleep 1
    
    10.times do |i|
      pubs.each {|p| p.pub.post "hello again #{i} from #{p.id}" }
    end
    
    pubs.first.pub.post "FIN"
    subs.each &:wait
    sleep 1
    subs.each_with_index do |sub, sub_i|
      
      assert_equal 0, sub.errors.count, "Subscriber encountered #{sub.errors.count} errors: #{sub.errors.join ", "}"
      
      msgs=[]
      pubs.each { |p| msgs << p.pub.messages.messages }
      
      sub.messages.each do |msg|  
        matched = false
        for mm in msgs do
          if mm.first == msg.message
            matched = true
            mm.shift
            break
          end
        end
        assert_equal matched, true, "message not matched"
      end
      
      sub.terminate
    end
    
  end
  
  def test_channel_multiplexing_5
    test_channel_multiplexing 5
  end
  
  def test_channel_delimitered_multiplexing_15
    test_channel_multiplexing 15, true
  end
  
  def test_message_delivery
    pub, sub = pubsub
    sub.run
    sleep 0.2
    assert_equal 0, sub.messages.messages.count
    pub.post "hi there"
    assert_equal 201, pub.response_code, "publisher response code"
    sleep 0.2
    assert_equal 1, sub.messages.messages.count, "received message count"
    pub.post "FIN"
    assert_equal 201, pub.response_code, "publisher response code"
    sleep 0.2
    assert_equal 2, sub.messages.messages.count, "recelived messages count"
    assert sub.messages.matches? pub.messages
    sub.terminate
  end
  
  def test_publish_then_subscribe
    pub, sub = pubsub
    pub.post "hi there"
    sub.run
    pub.post "FIN"
    sub.wait
    assert_equal 2, sub.messages.messages.count
    assert sub.messages.matches? pub.messages
    sub.terminate
  end

  def test_authorized_channels
    #must be published to before subscribing
    n=5
    pub, sub = pubsub n, timeout: 6, sub: "sub/authorized/"
    sub.on_failure { false }
    sub.run
    sleep 1
    sub.wait
    assert_equal n, sub.finished
    
    assert sub.match_errors(/code 403/), "expected 403 for all subscribers, got #{sub.errors.pretty_inspect}"
    sub.reset
    
    pub.post %w( fweep )
    assert_match /20[12]/, pub.response_code.to_s
    sleep 0.1
    
    sub.run
    
    sleep 0.1
    pub.post ["fwoop", "FIN"] { assert_match /20[12]/, pub.response_code.to_s }
    
    
    sub.wait
    verify pub, sub
    sub.terminate
  end

  def test_deletion
    #delete active channel
    par=5
    pub, sub = pubsub par, timeout: 10
    sub.on_failure { false }
    sub.run
    sleep 0.2
    pub.delete
    sleep 0.1
    assert_equal 200, pub.response_code
    assert_equal par, pub.response_body.match(/subscribers:\s+(\d)/)[1].to_i, "subscriber count after deletion"
    sub.wait
    assert sub.match_errors(/code 410/), "Expected subscriber code 410: Gone, instead was \"#{sub.errors.first}\""

    #delete channel with no subscribers
    pub, sub = pubsub 5, timeout: 1
    pub.post "hello"
    assert_equal 202, pub.response_code
    pub.delete
    assert_equal 200, pub.response_code

    #delete nonexistent channel
    pub, sub = pubsub
    pub.nofail=true
    pub.delete
    assert_equal 404, pub.response_code
  end

  def test_no_message_buffer
    chan_id=SecureRandom.hex
    pub = Publisher.new url("/pub/nobuffer/#{chan_id}")
    sub=[]
    40.times do 
      sub.push Subscriber.new(url("/sub/broadcast/#{chan_id}"), 1, use_message_id: false, quit_message: 'FIN')
    end
    
    pub.post ["this message should not be delivered", "nor this one"]
    
    sub.each {|s| s.run}
    sleep 1
    pub.post "received1"
    sleep 1
    pub.post "received2"
    sleep 1
    pub.post "FIN"
    sub.each {|s| s.wait}
    sub.each do |s|
      assert s.errors.empty?, "There were subscriber errors: \r\n#{s.errors.join "\r\n"}"
      ret, err = s.messages.matches? ["received1", "received2", "FIN"]
      assert ret, err || "Messages don't match"
    end
  end
  
  def test_channel_isolation
    rands= %w( foo bar baz bax qqqqqqqqqqqqqqqqqqq eleven andsoon andsoforth feh )
    pub=[]
    sub=[]
    10.times do |i|
      pub[i], sub[i]=pubsub 15
      sub[i].run
    end
    pub.each do |p|
      rand(1..10).times do
        p.post rands.sample
      end
    end
    sleep 1
    pub.each do |p|
      p.post 'FIN'
    end
    sub.each do |s|
      s.wait
    end
    pub.each_with_index do |p, i|
      verify p, sub[i]
    end
    sub.each {|s| s.terminate }
  end
  
  def test_broadcast_3
    test_broadcast 3
  end
  
  def test_broadcast_20
    test_broadcast 20
  end
  
  def test_longpoll_multipart
    pub, sub = pubsub 1, sub: 'sub/multipart/', use_message_id: false
    
    pub.post "first", "text/x-foobar"
    pub.post ["1", "2", "3", "4"]
    sub.run
    sleep 0.5
    pub.post "FIN"
    sub.wait
   
    verify pub, sub
    sub.terminate
  end
  
  def test_longpoll_multipart_extended(range=30..35)
    range.each do |i|
      pub, sub = pubsub 1, sub: 'sub/multipart/', use_message_id: false, timeout: 3
      i.times do |n|
        pub.post "#{n+1}"
      end
      pub.post "FIN"
      sub.run
      sub.wait
      verify pub, sub
      sleep 0.1
    end
  end
  
  def test_multiplexed_longpoll_multipart
    chans= [short_id, short_id, short_id]
    pub, sub = pubsub 1, sub: "sub/multipart_multiplex/#{chans.join "/"}", pub: "pub/#{chans[1]}", channel: "", use_message_id: false
    
    pub.post "first", "text/x-foobar"
    pub.post ["1", "2", "3", "4"]
    sub.run
    sleep 0.5
    pub.post "FIN"
    sub.wait
   
    verify pub, sub
    sub.terminate
  end
  
  
  def test_broadcast(clients=400)
    pub, sub = pubsub clients
    pub.post "!!"
    sub.run #celluloid async FTW
    #sleep 2
    pub.post ["!!!!", "what is this", "it's nothing", "nothing at all really"]
    pub.post "FIN"
    sub.wait
    sleep 0.5
    verify pub, sub
    sub.terminate
  end
  
  #def test_broadcast_10000
  #  test_broadcast 10000
  #end
  
  def dont_test_subscriber_concurrency
    chan=SecureRandom.hex
    pub_first = Publisher.new url("pub/first#{chan}")
    pub_last = Publisher.new url("pub/last#{chan}")
    
    sub_first, sub_last = [], []
    { url("sub/first/first#{chan}") => sub_first, url("sub/last/last#{chan}") => sub_last }.each do |url, arr|
      3.times do
        sub=Subscriber.new(url, 1, quit_message: 'FIN', timeout: 20)
        sub.on_failure do |resp, req|
          false
        end
        arr << sub
      end
    end
    
    sub_first.each {|s| s.run; sleep 0.1 }
    assert sub_first[0].no_errors?
    sub_first[1..2].each do |s|
      assert s.errors?
      assert s.match_errors(/code 409/)
    end

    sub_last.each {|s| s.run; sleep 0.1 }
    assert sub_last[2].no_errors?
    sub_last[0..1].each do |s|
      assert s.errors?
      assert s.match_errors(/code 40[49]/)
    end

    pub_first.post %w( foo bar FIN )
    pub_last.post %w( foobar baz somethingelse FIN )
    
    sub_first[0].wait
    sub_last[2].wait
    
    verify pub_first, sub_first[0]
    verify pub_last, sub_last[2]
    
    sub_first[1..2].each{ |s| assert s.messages.count == 0 }
    sub_last[0..1].each{ |s| assert s.messages.count == 0 }
    [sub_first, sub_last].each {|sub| sub.each{|s| s.terminate}}
  end

  def test_queueing
    pub, sub = pubsub 1
    pub.post %w( what is this_thing andnow 555555555555555555555 eleven FIN ), 'text/plain'
    sleep 0.3
    sub.run
    sub.wait
    verify pub, sub
    sub.terminate
  end
  
  def test_long_message(kb=0.5)
    pub, sub = pubsub 10, timeout: 10
    sub.run
    sleep 0.2
    pub.post ["#{"q"*((kb * 1024)-3)}end", "FIN"]
    sub.wait
    verify pub, sub
    sub.terminate
  end
  
  unless OMIT_LONGMSG
    #[5, 9, 9.5, 9.9, 10, 11, 15, 16, 17, 18, 19, 20, 30,  50, 100, 200, 300, 600, 900, 3000].each do |n|
    [5, 10, 20, 200, 900].each do |n|
      define_method "test_long_message_#{n}Kb" do 
        test_long_message n
      end
    end
    
    def test_message_length_range
      pub, sub = pubsub 2, timeout: 15
      sub.run
      
      n=5
      while n <= 10000 do
        pub.post "T" * n
        n=(n*1.01) + 1
        sleep 0.001
      end
      pub.post "FIN"
      sub.wait
      verify pub, sub
      sub.terminate
    end
    
    def generic_test_long_buffed_messages(client=:longpoll)
      kb=2000
      #kb=2
      pub, sub = pubsub 1, sub: "/sub/broadcast/", timeout: 10, client: client
      #pub, sub = pubsub 1, sub: "/sub/websocket_only/", client: :websocket
      #sub.on_message do |msg|
      #  puts ">>>>>>>message: #{msg.message[0...10]}...|#{msg.message.length}|"
      #end
      sub.run
      sleep 1
      m1="#{"q"*((kb * 1024)-3)}end"
      m2="#{"r"*((kb * 1024)-3)}end"
      i=0
      15.times do
        i+=1
        pub.post "#{i}#{m1}"
        i+=1
        pub.post "#{i}#{m2}"
      end
      pub.post "FIN"
      sub.wait
      verify pub, sub
      pub.delete
      sub.terminate
    end

    [:longpoll, :multipart, :eventsource, :websocket, :chunked].each do |client|
      define_method "test_long_buffed_messages_#{client}" do
        generic_test_long_buffed_messages client
      end
    end
    
  end
  
  def test_message_timeout
    pub, sub = pubsub 1, pub: "/pub/2_sec_message_timeout/", timeout: 10
    pub.post %w( foo bar etcetera ) #these shouldn't get delivered
    pub.messages.clear
    sleep 3
    #binding.pry
    sub.run
    sleep 1
    pub.post %w( what is this even FIN )
    sub.wait
    verify pub, sub
    sub.terminate
  end
    
  def test_subscriber_timeout
    chan=SecureRandom.hex
    sub=Subscriber.new(url("sub/timeout/#{chan}"), 5, timeout: 10)
    sub.on_failure { false }
    pub=Publisher.new url("pub/#{chan}")
    sub.run
    sleep 0.1
    pub.post "hello"
    sub.wait
    verify pub, sub, false
    assert sub.match_errors(/code 408/)
    sub.terminate
  end
  
  def assert_header_includes(response, header, str)
    assert response.headers[header].include?(str), "Response header '#{header}: #{response.headers[header]}' must contain \"#{str}\", but does not."
  end
  
  def test_access_control_options
    chan=SecureRandom.hex
    
    request = Typhoeus::Request.new url("sub/broadcast/#{chan}"), method: :OPTIONS, headers: { 'Origin':'example.com' }
    resp = request.run
    
    assert_equal "*", resp.headers["Access-Control-Allow-Origin"]
    %w( GET ).each do |v| 
      assert_header_includes resp, "Access-Control-Allow-Methods", v
      assert_header_includes resp, "Allow", v
    end
    %w( If-None-Match If-Modified-Since Content-Type Cache-Control X-EventSource-Event ).each {|v| assert_header_includes resp, "Access-Control-Allow-Headers", v}
    
    request = Typhoeus::Request.new url("sub/broadcast/#{chan}"), method: :OPTIONS
    resp = request.run
    %w( GET ).each do |v| 
      assert_header_includes resp, "Allow", v
    end
    
    
    request = Typhoeus::Request.new url("pub/#{chan}"), method: :OPTIONS, headers: { 'Origin': "example.com" }
    resp = request.run
    assert_equal "*", resp.headers["Access-Control-Allow-Origin"]
    %w( GET POST DELETE ).each do |v| 
      assert_header_includes resp, "Access-Control-Allow-Methods", v
      assert_header_includes resp, "Allow", v
    end
    %w( Content-Type ).each {|v| assert_header_includes resp, "Access-Control-Allow-Headers", v}
    
    request = Typhoeus::Request.new url("pub/#{chan}"), method: :OPTIONS
    resp = request.run
    %w( GET POST DELETE ).each do |v| 
      assert_header_includes resp, "Allow", v
    end
  end
  
  def generic_test_access_control(opt)
    pub, sub = pubsub 1, extra_headers: { Origin: opt[:origin] }, pub: opt[:pub_url], sub: opt[:sub_url]
    
    sub.on_message do |msg, bundle|
      opt[:verify_sub_response].call(bundle) if opt[:verify_sub_response]
    end
    
    pub.post "FIN"
    sub.run
    sub.wait
    
    yield pub, sub if block_given?
    
    sub.terminate
  end
  
  def test_invalid_etag
    chan_id=short_id
    pub = Publisher.new url("/pub/#{chan_id}"), accept: 'text/json'
    
    pub.post "1. one!!"
    sleep 1
    pub.post "2. tooo"
    pub.post "3. throo"
    sleep 1
    pub.post "4. fooo"
    n = 0
    sub = Subscriber.new(url("/sub/multipart_multiplex/#{short_id}/#{short_id}/#{chan_id}"), 1, quit_message: 'FIN', retry_delay: 1, timeout: 20)
    sub.on_message do |msg, bundle|
      n=n+1
      if n == 2
        bundle.etag="null"
      end
    end
    
    sub.on_failure do |err|
      assert_match /code 400/, err
      false
    end
    
    sub.run
    sleep 1
    pub.post "5. faaa"
    pub.post "FIN"
    sub.wait
  end
  
  #def test_expired_messages_with_subscribers
  #  chan = short_id
  #  pub, sub = pubsub 1, pub: "/pub/2_sec_message_timeout/", sub: "/sub/intervalpoll/", client: :intervalpoll, timeout: 9000, channel: short_id
  #  sub.on_failure do |err|
  #    puts "retry?!!"
  #    true
  #  end
  #  sub.run
  #  pub.post ["foo", "bar"]
  #  
  #  sleep 5
  #  
  #  pub.post ["yeah", "what", "the"]
  #  sub.wait
  #end
  
  def dont_test_auth
    chan = short_id
    
    subs = [ :longpoll, :eventsource, :websocket, :multipart ]
    
    subs.each do |t|
      sub = Subscriber.new(url("sub/auth_fail/#{chan}"), 1, client: t)
      sub.on_failure { false }
      sub.run
      sub.wait
      assert(sub.errors?)
      assert /code 500/, sub.errors.first
      sub.terminate
    end
    
    auth_pid = Process.spawn("bundle exec ./authserver.rb -q")
    while true
      resp = Typhoeus.get("http://127.0.0.1:8053/", followlocation: true)
      break unless resp.return_code == :couldnt_connect
      sleep 0.20
    end
    
    subs.each do |t|
      sub = Subscriber.new(url("sub/auth_fail/#{chan}"), 1, client: t)
      sub.on_failure { false }
      sub.run
      sub.wait
      assert(sub.errors?)
      assert /code 403/, sub.errors.first
      sub.terminate
    end
    
    pub = Publisher.new url("pub/#{chan}")
    
    pub.post [ "wut", "waht", "FIN" ]
    
    subs.each do |t|
      sub = Subscriber.new(url("sub/auth/#{chan}"), 1, client: t, quit_message: 'FIN')
      sub.on_failure { false }
      sub.run
      sub.wait
      verify pub, sub
    end
    
    Process.kill 2, auth_pid
  end
  
  def test_access_control
    
    ver= proc do |bundle| 
      assert_equal "*", bundle.headers["Access-Control-Allow-Origin"] 
    end
    generic_test_access_control(origin: "example.com", verify_sub_response: ver) do |pub, sub|
      verify pub, sub
    end
    
    ver= proc do |bundle| 
      assert_equal "http://foo.bar", bundle.headers["Access-Control-Allow-Origin"] 
      %w( Last-Modified Etag ).each {|v| assert_header_includes bundle, "Access-Control-Expose-Headers", v}
    end
    generic_test_access_control(origin: "http://foo.bar", verify_sub_response: ver, sub_url: "sub/from_foo.bar/") do |pub, sub|
      verify pub, sub
    end
    
    #test forbidding stuff
    pub, sub = pubsub 1, extra_headers: { "Origin": "http://forbidden.com" }, pub: "pub/from_foo.bar/", sub: "sub/from_foo.bar/", timeout: 1
    
    pub.extra_headers={ "Origin": "http://foo.bar" }
    pub.post "yeah"
    
    assert_match /20[12]/, pub.response_code.to_s
    pub.extra_headers={ "Origin": "http://forbidden.com" }
    post_failed = false
    begin 
      pub.post "yeah"
    rescue Exception => e
      post_failed = true
      assert_match /request failed:\s+403/, e.message
    end
    assert post_failed
    
    sub.on_failure { false }
    
    sub.run
    sub.wait
    
    sub.errors.each do |err|
      assert_match /code 403/, err
    end
    sub.terminate
    
  end
  
  def test_gzip
    #bug: turning on gzip cleared the response etag
    pub, sub = pubsub 1, sub: "/sub/gzip/", gzip: true, retry_delay: 0.3
    sub.run
    sleep 0.1
    pub.post ["2", "123456789A", "alsdjklsdhflsajkfhl", "boq"]
    sleep 1
    pub.post "foobar"
    msg = ""
    200.times { msg << SecureRandom.hex }
    pub.post msg
    pub.post "FIN"
    sub.wait
    verify pub, sub
  end
  
  def test_issue_212 #https://github.com/slact/nchan/issues/212
    chan1 = short_id
    chan2 = short_id
    sub = Subscriber.new url("/sub/multi/#{chan1}/#{chan2}"), 1, quit_message: 'FIN', client: :eventsource, timeout: 3
    sub.on_failure { false }
    pub = Publisher.new url("/pub/#{chan1}")
    pub_nobuf = Publisher.new url("/pub/nobuffer/#{chan2}")
    sub.run
    sub.wait :ready

    pub.post %w(yes what this and also)
    sleep 1.1
    pub_nobuf.post "WHAT?!"
    pub.messages << pub_nobuf.messages.first
    pub.post %W(foo bar baz bzzzt FIN)

    sub.wait
    
    verify pub, sub
  end
  
  def test_changing_buffer_length
    chan = short_id
    sub = Subscriber.new url("sub/broadcast/#{chan}"), 30, quit_message: 'FIN'
    pub_delta = Publisher.new url("/pub/#{chan}")
    pub_snapshot = Publisher.new url("/pub_1message/#{chan}")
    
    pub_delta.post %W(foo bar baz bzzzt FIN)
    sub.run
    sub.wait
    verify pub_delta, sub
    sub.reset
    
    pub_snapshot.post %W(blah anotherblah)
    pub_snapshot.messages.clear
    pub_snapshot.post "this is the real thing right here"
    
    pub_delta.messages.clear
    pub_delta.messages << pub_snapshot.messages.first
    pub_delta.post %W(d1 d2 d3 and_other_deltas FIN)
    sub.run
    sub.wait
    
    verify pub_delta, sub
  end
end


