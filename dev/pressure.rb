#!/usr/bin/ruby
require 'securerandom'
require 'rubygems'
require 'bundler/setup'
require 'nchan_tools/pubsub'
require 'digest/sha1'
require "optparse"
require "pry"

$server_url="http://127.0.0.1:8082"
$interval = 1
$jitter=0.2
$parallel_count = 100

opt=OptionParser.new do |opts|
  opts.on("--server SERVER (#{$server_url})", "server url."){|v| $server_url=v}
  opts.on("--interval SECONDS (#{$interval}sec)", "time between mesages per subscriber"){|v| $interval=v.to_f}
  opts.on("--jitter PERCENT (#{$jitter*100}%)", "message interval jitter") {|v| $jitter=v.to_f/100 }
  opts.on("--parallel NUMBER (#{$parallel_count})", "number of concurrent pubsub pairs") { |v| $parallel_count = v.to_i }
  opts.on("--verbose", "number of concurrent pubsub pairs") do |v|
    #verbose = true
    Typhoeus::Config.verbose = true
  end
end
opt.parse!


Celluloid.boot

class Tracker
  include Celluloid
  def initialize(interval = 2)
    reset!
    @interval = interval
    @errors = 0
  end
  
  def reset!
    @sent = 0
    @received = 0
    @prev = Time.now
  end
  
  def sent!
    @sent += 1
  end
  def received!
    @received += 1
  end
  
  def error!(msg)
    @errors += 1
    puts "!!ERROR!!: #{msg}"
  end
  
  def run
    while true do
      Celluloid.sleep @interval
      now = Time.now
      dt = now - @prev
      @prev = now
      printf "pub: %d msg/sec sub: %d msg/sec err: %d\n", (@sent/dt).round, (@received/dt).round, @errors
      reset!
    end
  end
end

class NPipe  
  include Celluloid
  
  def url(part="")
    part=part[1..-1] if part[0]=="/"
    "#{$server_url}/#{part}"
  end
  def short_id
    "#{SecureRandom.hex.to_i(16).to_s(36)[0..5]}"
  end
  attr_accessor :pub, :sub, :id, :alt_id
  def initialize(tracker, pipe_id, interval, jitter)
    @tracker=tracker
    @pipe_id=pipe_id
    @id = ("#{@pipe_id}Z1Z#{short_id}")
    @alt_id = ("#{@pipe_id}Z2Z#{short_id}")
    @pub = NchanTools::Publisher.new url("/pub/#{@id}"), accept: 'text/json', timeout: 1000000
    @sub = NchanTools::Subscriber.new(url("/sub/#{@id}"), 1, quit_message: 'FIN', client: :eventsource, timeout: 1000000)
    @sub.on_message do |msg, bundle| 
      if msg != @inflight
        @tracker.error! "message mismatch: expected \"#{@inflight}\", have \"#{msg}\""
      else
        @tracker.received!
      end
      @inflight = nil
    end
    @sub.on_failure do |err|
      @tracker.error! "Subscriber #{@pipe_id}: #{err}"
    end
    @n=0
    @interval = interval
    @interval_range = (interval*(1-jitter.to_f/2))..interval*(1+jitter.to_f/2)
  end
  def ready
    @sub.run
    @sub.wait :ready
  end
  def run
    Celluloid.sleep rand(0..@interval)
    while true do
      Celluloid.sleep rand(@interval_range)
      @tracker.error! "Message not delivered in time: #{@inflight}" if @inflight
      self.pub
      @n+=1
    end
  end
  
  def pub
    @inflight = "#{@id} hey what is this? #{@n}"
    begin
      if @pub.post @inflight then
        @tracker.sent!
      else
        @tracker.error! "Failed to send msg \"#{@inflight}\""
        @inflight = nil
      end
    rescue NchanTools::Publisher::PublisherError => e
      @tracker.error! "Failed to send msg \"#{@inflight}\": #{e.to_s}"
      @inflight = nil
    end
  end
end
  
tracker = Tracker.new
npipes = []
for n in 1..$parallel_count do
  npipe = NPipe.new tracker, n, $interval, $jitter
  
  npipe.ready
  npipes << npipe
end

npipes.each do |np|
  np.async.run
end

tracker.run

sleep  40
