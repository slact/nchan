#!/usr/bin/ruby
require 'test/unit'
require 'securerandom'
require_relative 'pubsub.rb'
SERVER=ENV["PUSHMODULE_SERVER"] || "127.0.0.1"
PORT=ENV["PUSHMODULE_PORT"] || "8082"
#Typhoeus::Config.verbose = true
def url(part="")
  part=part[1..-1] if part[0]=="/"
  "http://#{SERVER}:#{PORT}/#{part}"
end
puts "Server at #{url}"
def pubsub(concurrent_clients=1, opt={})
    urlpart=opt[:urlpart] || 'broadcast'
    timeout = opt[:timeout]
    sub_url=opt[:sub] || "sub/broadcast/"
    pub_url=opt[:pub] || "pub/"
    chan_id = opt[:channel] || SecureRandom.hex
    sub = Subscriber.new url("#{sub_url}#{chan_id}"), concurrent_clients, timeout: timeout, use_message_id: opt[:use_message_id], quit_message: 'FIN', gzip: opt[:gzip], retry_delay: opt[:retry_delay], client: opt[:client]
    pub = Publisher.new url("#{pub_url}#{chan_id}")
    return pub, sub
end
def verify(pub, sub, check_errors=true)
  assert sub.errors.empty?, "There were subscriber errors: \r\n#{sub.errors.join "\r\n"}" if check_errors
  ret, err = sub.messages.matches?(pub.messages)
  assert ret, err || "Messages don't match"
  sub.messages.each do |msg|
    assert_equal sub.concurrency, msg.times_seen, "Concurrent subscribers didn't all receive a message."
  end
end

class PubSubTest < Test::Unit::TestCase
  def setup
    Celluloid.boot
  end
  
  def test_interval_poll
    pub, sub=pubsub 1, client: :intervalpoll, quit_message: 'FIN', retry_delay:0.1
    sub.run
    pub.post ["hello this", "is a thing"]
    sleep 1
    pub.post ["oh now what", "is this even a thing?"]
    sleep 1
    pub.post "FIN"
    sub.wait
    verify pub, sub
    sub.terminate
  end
  
  def test_message_delivery
    pub, sub = pubsub
    sub.run
    sleep 0.2
    assert_equal 0, sub.messages.messages.count
    pub.post "hi there"
    assert_equal 201, pub.response_code
    sleep 0.2
    assert_equal 1, sub.messages.messages.count
    pub.post "FIN"
    assert_equal 201, pub.response_code
    sleep 0.2
    assert_equal 2, sub.messages.messages.count
    assert sub.messages.matches? pub.messages
    sub.terminate
  end

  def test_authorized_channels
    #must be published to before subscribing
    pub, sub = pubsub 5, timeout: 1, sub: "sub/authorized/"
    sub.on_failure { false }
    sub.run
    sub.wait
    assert_equal 5, sub.finished
    assert sub.match_errors(/code 40[34]/)
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
    pub, sub = pubsub 5, timeout: 10
    sub.on_failure { false }
    sub.run
    sleep 0.2
    pub.delete
    sleep 0.2
    assert_equal 200, pub.response_code
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
    sleep 0.2
    pub.post "received1"
    sleep 0.2
    pub.post "received2"
    sleep 0.2 
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
  
  def test_broadcast(clients=400)
    pub, sub = pubsub clients
    pub.post "yeah okay"
    sub.run #celluloid async FTW
    sleep 0.5
    pub.post ["hello there", "what is this", "it's nothing", "nothing at all really"]
    pub.post "FIN"
    sub.wait
    verify pub, sub
    sub.terminate
  end
  
  #def test_broadcast_for_3000
  #  test_broadcast 3000
  #end
  
  def test_subscriber_concurrency
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
    pub, sub = pubsub 5
    pub.post %w( what is this_thing andnow 555555555555555555555 eleven FIN )
    sleep 0.3
    sub.run
    sub.wait
    verify pub, sub
    sub.terminate
  end
  
  def test_long_message(kb=1)
    pub, sub = pubsub 10, timeout: 10
    sub.run
    pub.post ["q" * kb * 1024, "FIN"]
    sub.wait
    verify pub, sub
    sub.terminate
  end
  
  def test_long_message_500kb
    test_long_message 500
  end
  
  def test_long_message_700kb
    test_long_message 700
  end
  
  def test_long_message_950kb
    test_long_message 950
  end
  
  def test_message_length_range
    pub, sub = pubsub 2, timeout: 6
    sub.run
    
    n=5
    while n <= 10000 do
      pub.post "T" * n
      n=n*1.01
      sleep 0.001
    end
    pub.post "FIN"
    sub.wait
    verify pub, sub
    sub.terminate
  end
  
  def test_message_timeout
    pub, sub = pubsub 10, pub: "/pub/2_sec_message_timeout/", timeout: 4
    pub.post %w( foo bar etcetera ) #these shouldn't get delivered
    pub.messages.clear
    sleep 3
    
    sub.run
    pub.post %w( what is this even FIN )
    sub.wait
    verify pub, sub
    sub.terminate
  end
  
  def test_subscriber_timeout
    chan=SecureRandom.hex
    sub=Subscriber.new(url("sub/timeout/#{chan}"), 2, timeout: 10)
    sub.on_failure { false }
    pub=Publisher.new url("pub/#{chan}")
    sub.run
    pub.post "hello"
    sub.wait
    verify pub, sub, false
    assert sub.match_errors(/code 304/)
  end
  
  def assert_header_includes(response, header, str)
    assert response.headers[header].include?(str), "Response header '#{header}:#{response.headers[header]}' must include \"#{str}\", but does not."
  end
  
  def test_options
    chan=SecureRandom.hex
    request = Typhoeus::Request.new url("sub/broadcast/#{chan}"), method: :OPTIONS
    resp = request.run
    assert_equal "*", resp.headers["Access-Control-Allow-Origin"]
    %w( GET OPTIONS ).each {|v| assert_header_includes resp, "Access-Control-Allow-Methods", v}
    %w( If-None-Match If-Modified-Since Origin ).each {|v| assert_header_includes resp, "Access-Control-Allow-Headers", v}
    
    request = Typhoeus::Request.new url("pub/#{chan}"), method: :OPTIONS
    resp = request.run
    assert_equal "*", resp.headers["Access-Control-Allow-Origin"]
    %w( GET POST DELETE OPTIONS ).each {|v| assert_header_includes resp, "Access-Control-Allow-Methods", v}
    %w( Content-Type Origin ).each {|v| assert_header_includes resp, "Access-Control-Allow-Headers", v}
  end
end


