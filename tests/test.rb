#!/usr/bin/ruby
require 'test/unit'
require 'securerandom'
require "./pubsub.rb"

def url(part)
  "http://127.0.0.1:8082/#{part}"
end
def pubsub(concurrent_clients=1, opt={})
    urlpart=opt[:urlpart] || 'broadcast'
    timeout = opt[:timeout]
    sub_url=opt[:sub] || "sub/broadcast/"
    pub_url=opt[:pub] || "pub/"
    chan_id = opt[:channel] || SecureRandom.hex
    sub = Subscriber.new url("#{sub_url}#{chan_id}"), concurrent_clients, timeout: timeout, quit_message: 'FIN'
    pub = Publisher.new url("#{pub_url}#{chan_id}")
    return pub, sub
end
def verify(pub, sub)
  assert sub.errors.empty?, "There were subscriber errors: \r\n#{sub.errors.join "\r\n"}"
  ret, err = sub.messages.matches?(pub.messages)
  assert ret, err || "Messages don't match"
  sub.messages.each do |msg|
    assert_equal msg.times_seen, sub.concurrency, "Concurrent subscribers didn't all receive a message."
  end
end

def start_test_nginx
  begin #kill current test-nginx
    oldpid = File.read "/tmp/nhpm-test-nginx.pid"
    oldpid.delete! "\n"
    binding.pry
    system "kill #{oldpid}"
    puts "killed already-running nginx test server"
  rescue
    puts "no test nginx server running (it seems...)"
  end
  pid = spawn "./nginx -p ./ -c ./nginx.conf"
  puts "Spawned nginx test server with PID #{pid}"
end


class PubSubTest < Test::Unit::TestCase
  def setup
    Celluloid.boot
  end
  def test_message_delivery
    pub, sub = pubsub
    sub.run
    assert_equal sub.messages.messages.count, 0
    pub.post "hi there"
    sleep 0.2
    assert_equal sub.messages.messages.count, 1
    pub.post "FIN"
    sleep 0.2
    assert_equal sub.messages.messages.count, 2
    assert sub.messages.matches? pub.messages
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
    pub.each do |p|
      p.post 'FIN'
    end
    sub.each do |s|
      s.wait
    end
    pub.each_with_index do |p, i|
      verify p, sub[i]
    end
  end
  
  def test_broadcast(clients=700)
    pub, sub = pubsub clients
    sub.run #celluloid async FTW
    pub.post ["hello there", "what is this", "it's nothing", "FIN"]
    sub.wait
    verify pub, sub
  end
  
  #def test_broadcast_for_3000
  #  test_broadcast 3000
  #end
  
  def test_subscriber_concurrency
    chan=SecureRandom.hex
    pub = Publisher.new url("pub/#{chan}")
    
    sub_first, sub_last = [], []
    { url("sub/first/#{chan}") => sub_first, url("sub/last/#{chan}") => sub_last }.each do |url, arr|
      3.times do
        sub=Subscriber.new(url, 1, quit_message: 'FIN', timeout: 120)
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

    #sub_last.each {|s| s.run; sleep 0.1 }
    #assert sub_last[2].no_errors?
    #sub_last[0..1].each do |s|
    #  assert s.errors?
    #  assert s.match_errors(/code 409/)
    #end

    pub.post %w( foo bar FIN )
    
    sub_first[0].wait
    #sub_last[2].wait
    
    verify pub, sub_first[0]
    #verify pub, sub_last[2]
    
    sub_first[1..2].each{ |s| assert s.messages.count == 0 }
    #sub_last[0..1].each{ |s| assert s.messages.count == 0 }
  end

  def test_queueing
    pub, sub = pubsub 5
    pub.post %w( what is this_thing andnow 555555555555555555555 eleven FIN )
    sleep 0.3
    sub.run
    sub.wait
    verify pub, sub
  end
  
  def test_long_message(kb=1)
    pub, sub = pubsub 10, timeout: 10
    sub.run
    pub.post ["q" * kb * 1024, "FIN"]
    sub.wait
    verify pub, sub
  end
  
  def test_500kb_message
    test_long_message 500
  end
  
  def test_700kb_message
    test_long_message 700
  end
  
  def test_950kb_message
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
    verify pub, sub
  end
  
  def test_message_timeout
    #config should be set to message_timeout=5sec
    pub, sub = pubsub 1, timeout: 5
    pub.post "foo"
    sleep 10
    pub.messages.remove_old
    sub.run
    pub.post "FIN"
    sub.wait
    verify pub, sub
  end
end


