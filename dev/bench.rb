#!/bin/ruby
require "pry"
require_relative "pubsub.rb"
require "optparse"

client=:longpoll
threads = 100
par = 100

def short_id
  SecureRandom.hex.to_i(16).to_s(36)[0..5]
end

myid = short_id
$server="localhost:8082"
sub_uri="/sub/broadcast/#{myid}"
pub_uri="/pub/#{myid}"

SUB_TIMEOUT = 1000
START_MSG = "START"
QUIT_MSG = "FIN"

opt=OptionParser.new do |opts|
  opts.on("-S", "--server SERVER (#{$server})", "server and port."){|v| $server=v}
  opts.on("-t", "--threads NUM (#{threads})", "number of subscriber threads"){|v| threads = v.to_i}
  opts.on("-p", "--parallel NUM (#{par})", "number of subscribers per thread"){|v| par = v.to_i}
  opts.on("-l", "--client STRING (#{client})", "sub client"){|v| client = v.to_sym}
  opts.on("--sub-uri STRING (#{sub_uri})", "sub uri"){|v| sub_uri = v}
  opts.on("--pub-uri STRING (#{pub_uri})", "sub uri"){|v| pub_uri = v}
end
opt.banner="Usage: bench.rb [options]"
opt.parse!

def mkmsg(n=nil)
  "#{Time.now.to_f}"
end

def url(part="")
  part=part[1..-1] if part[0]=="/"
  "http://#{$server}/#{part}"
end

class BenchDB
  attr_accessor :db
  def initialize
    @db=[]
  end
  
  def add(start_time, snd_time, rcv_time, measured_time)
    if start_time && snd_time < start_time
      #use measured time if possible
      if measured_time
        @db << measured_time
      else
        @db << rcv_time - start_time
        #puts "approx time"
      end
    else
      @db << rcv_time - snd_time
      #puts "approx time"
    end
  end
  
  def analyze
    return "no data" if @db.count == 0
    
    sum=0
    @db.each{|v| sum += v}
    "#{@db.count} entries, avg: #{sum/@db.count}"
  end
  
  def to_s
    @db.join ", "
  end
end



def t
  Time.now.to_f
end

Typhoeus.before do |req|

  req.original_options[:start_time]=Time.now.to_f
end

class BenchSub
  attr_accessor :sub, :benchdb, :startmsg_received, :msg_received
  def initialize(url, parallel, client_type, benchdb)
    self.sub = Subscriber.new url, parallel, timeout: SUB_TIMEOUT, client: client_type, nomsg: true, nostore: true
    self.benchdb = benchdb
    self.startmsg_received = 0
    self.msg_received = 0
    sub.on_failure do |resp|
      puts "FAIL!!"
      binding.pry
    end
    sub.on_message do |msg, req|
      #puts "msg is: #{msg}"
      if msg == START_MSG
        self.startmsg_received +=1
      elsif msg == QUIT_MSG
        false
      else
        t_now = Time.now.to_f
        t_msg = msg.to_f
        if sub.client_class == Subscriber::LongPollClient
          t_start = req.time_requested
          measured_time = req.request_time
        elsif sub.client_class == Subscriber::EventSourceClient
          t_start = req.original_options[:last_msg_time] || req.original_options[:start_time]
          measured_time = nil 
          
        elsif sub.client_class == Subscriber::WebSocketClient
          t_start = req.last_message_time
          measured_time = nil 
        end
        self.msg_received += 1
        benchdb.add(t_start, t_msg, t_now, measured_time)
      end
    end
    
  end
  def run
    sub.run
  end
  
  def wait
    sub.wait
  end
end






pub_url = url(pub_uri)
sub_url = url(sub_uri)

benchmark = BenchDB.new

benches = []
threads.times do 
  benches << BenchSub.new(sub_url, par, client, benchmark)
end

pub = Publisher.new pub_url, nostore: true, nomsg: true, timeout: 30

num_msgs = 20
msgs = []

puts "will publish #{num_msgs} messages to #{pub_url}"


puts "subscribe to #{sub_url} & run #{threads} threads of #{par} #{client} subs"
benches.each &:run

sleep 20

pub.post START_MSG
num_msgs.times do
  pub.post mkmsg
end
pub.post QUIT_MSG

benches.each &:wait

puts "done."

benches.each do |b|
  puts "STARTMSG received #{b.startmsg_received} times"
  puts "MSG received #{b.msg_received} times"
end

puts benchmark.analyze

