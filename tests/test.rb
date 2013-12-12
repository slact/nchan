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
    rnd = SecureRandom.hex
    sub = Subscriber.new url("#{urlpart}/sub/#{rnd}"), concurrent_clients, timeout: timeout, quit_message: 'FIN'
    pub = Publisher.new url("#{urlpart}/pub/#{rnd}")
    return pub, sub
end
def verify(pub, sub)
  assert sub.errors.empty?, "There were subscriber errors: #{sub.errors.join "; "}"
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
  
  def test_broadcast
    num_clients = 700
    pub, sub = pubsub num_clients
    sub.run #celluloid async FTW
    pub.post ["hello there", "what is this", "it's nothing", "FIN"]
    sub.wait
    verify pub, sub
  end
  
  def test_queueing
    pub, sub = pubsub 5
    pub.post %w( what is this_thing andnow 555555555555555555555 eleven FIN )
    sub.run
    sub.wait
    verify pub, sub
  end
  
  def test_long_message(kb=1)
    pub, sub = pubsub 10, timeout: 5
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
    pub, sub = pubsub 10, timeout: 5
    sub.run
    
    n=5
    while n <= 10000 do
      pub.post "T" * n
      n=n*1.01
    end
    pub.post "FIN"
    verify pub, sub
  end
  
  def test_message_timeout
    #config should be set to message_timeout=5sec
    pub, sub = pubsub 1
    pub.post "foo"
    sleep 10
    pub.messages.remove_old
    sub.run
    pub.post "FIN"
    verify pub, sub
  end
end


