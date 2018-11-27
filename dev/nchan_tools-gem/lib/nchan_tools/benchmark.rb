require 'nchan_tools/pubsub'
require 'securerandom'
require 'timers'
require 'json'

module NchanTools
class Benchmark
  CSV_COLUMNS_ALL=%i[servers runtime channels channels_K channels_M subscribers message_length messages_sent messages_send_confirmed messages_send_unconfirmed messages_send_failed messages_received messages_unreceived messages_send_rate messages_receive_rate messages_send_rate_per_channel messages_receive_rate_per_subscriber message_publishing_avg message_publishing_99th message_publishing_max message_publishing_stddev message_publishing_count message_delivery_avg message_delivery_99th message_delivery_max message_delivery_stddev message_delivery_count]
  CSV_COLUMNS_DEFAULT=%i[servers runtime channels subscribers message_length messages_sent messages_send_confirmed messages_send_unconfirmed messages_send_failed messages_received messages_unreceived messages_send_rate messages_receive_rate messages_send_rate_per_channel messages_receive_rate_per_subscriber message_publishing_avg message_publishing_99th message_publishing_max message_publishing_stddev message_publishing_count message_delivery_avg message_delivery_99th message_delivery_max message_delivery_stddev message_delivery_count]
  class BenchmarkError < StandardError
  end
  def initialize(urls, init_args=nil)
    @urls = urls
    @n = urls.count
    @initializing = 0
    @ready = 0
    @running = 0
    @finished = 0
    @subs = []
    @results = {}
    @failed = {}
    
    @init_args = init_args
    @histograms = {}
    
    subs = []
  end
  
  def run
    puts "connecting to #{@n} Nchan server#{@n > 1 ? "s" : ""}..."
    @urls.each do |url|
      sub = NchanTools::Subscriber.new(url, 1, client: :websocket, timeout: 900000, extra_headers: {"Accept" => "text/x-json-hdrhistogram"})
      sub.on_failure do |err|
        unless @results[sub]
          unless @results[sub.url]
            @failed[sub] = true
            abort err, sub
          end
        end
        false
      end
      sub.on_message do |msg|
        msg = msg.to_s
        case msg
        when /^READY/
          puts   "  #{sub.url} ok"
          @ready +=1
          if @ready == @n
            puts "start benchmark..."
            control :run
          end
        when /^RUNNING/
          puts   "  #{sub.url} running"
        when /^RESULTS\n/
          msg = msg[8..-1]
          parsed = JSON.parse msg
          
          #backwards-compatible histogram fields
          parsed["histograms"]||={}
          if parsed[:message_publishing_histogram] then
            parsed[:histograms]["message publishing"]=parsed[:message_publishing_histogram]
          end
          if parsed[:message_delivery_histogram] then
            parsed[:histograms]["message delivery"]=parsed[:message_delivery_histogram]
          end
          
          @results[sub.url] = parsed
          @results[sub.url]["raw"] = msg if @results[sub.url]
          sub.client.send_close
        when /^INITIALIZING/
          #do nothing
        else
          raise BenchmarkError, "unexpected server response: #{msg}"
        end
      end
      @subs << sub
      sub.run
      sub.wait :ready, 1
      if @failed[sub]
        puts "  #{sub.url} failed"
      else
        puts "  #{sub.url} ok"
      end
    end
    return if @failed.count > 0
    puts "initializing benchmark..."
    control :init
    self.wait
    puts "finished."
    puts ""
  end
  
  def wait
    @subs.each &:wait
  end
  
  def control(msg)
    if @init_args && (msg.to_sym ==:init || msg.to_sym ==:initialize)
      msg = "#{msg.to_s} #{@init_args.map{|k,v| "#{k}=#{v}"}.join(" ")}"
    end
    @subs.each { |sub| sub.client.send_data msg.to_s }
  end
  
  def abort(err, src_sub = nil)
    puts "  #{err}"
    @subs.each do |sub|
      sub.terminate unless sub == src_sub
    end
  end
  
  def hdrhistogram_stats(name, histogram)
    fmt = <<-END.gsub(/^ {6}/, '')
      %s
        min:                         %.3fms
        avg:                         %.3fms
        99%%ile:                      %.3fms
        max:                         %.3fms
        stddev:                      %.3fms
        samples:                     %d
    END
    fmt % [ name,
      histogram.min, histogram.mean, histogram.percentile(99.0), histogram.max, histogram.stddev, histogram.count
    ]
  end
  
  def results
    @channels = 0
    @runtime = []
    @subscribers = 0
    @message_length = []
    @messages_sent = 0
    @messages_send_confirmed = 0
    @messages_send_unconfirmed = 0
    @messages_send_failed = 0
    @messages_received = 0
    @messages_unreceived = 0
    @histograms = {}
    @results.each do |url, data|
      @channels += data["channels"]
      @runtime << data["run_time_sec"]
      @subscribers += data["subscribers"]
      @message_length << data["message_length"]
      @messages_sent += data["messages"]["sent"]
      @messages_send_confirmed += data["messages"]["send_confirmed"]
      @messages_send_unconfirmed += data["messages"]["send_unconfirmed"]
      @messages_send_failed += data["messages"]["send_failed"]
      @messages_received += data["messages"]["received"]
      @messages_unreceived += data["messages"]["unreceived"]
      if data["histograms"]
        data["histograms"].each do |name, str|
          name = name.to_sym
          hdrh = HDRHistogram.unserialize(str, unit: :ms, multiplier: 0.001)
          if @histograms[name]
            @histograms[name].merge! hdrh
          else
            @histograms[name] = hdrh
          end
        end
      end
    end
    
    @message_length = @message_length.inject(0, :+).to_f / @message_length.size
    @runtime = @runtime.inject(0, :+).to_f / @runtime.size
    
    fmt = <<-END.gsub(/^ {6}/, '')
      Nchan servers:                 %d
      runtime:                       %d
      channels:                      %d
      subscribers:                   %d
      subscribers per channel:       %.1f
      messages:
        length:                      %d
        sent:                        %d
        send_confirmed:              %d
        send_unconfirmed:            %d
        send_failed:                 %d
        received:                    %d
        unreceived:                  %d
        send rate:                   %.3f/sec
        receive rate:                %.3f/sec
        send rate per channel:       %.3f/min
        receive rate per subscriber: %.3f/min
    END
    out = fmt % [
      @n, @runtime, @channels, @subscribers, @subscribers.to_f/@channels,
      @message_length, @messages_sent, @messages_send_confirmed, @messages_send_unconfirmed, @messages_send_failed, 
      @messages_received, @messages_unreceived,
      @messages_sent.to_f/@runtime,
      @messages_received.to_f/@runtime,
      (@messages_sent.to_f* 60)/(@runtime * @channels),
      (@messages_received.to_f * 60)/(@runtime * @subscribers)
    ]
    @histograms.each do |name, histogram|
      out << hdrhistogram_stats("#{name} latency:", histogram)
    end
    
    puts out
  end
  
  def append_csv_file(file, columns=Benchmark::CSV_COLUMNS_DEFAULT)
    require "csv"
    write_headers = File.zero?(file) || !File.exists?(file)
    headers = columns
    vals = {
      servers: @n,
      runtime: @runtime,
      channels: @channels,
      channels_K: @channels/1000.0,
      channels_M: @channels/1000000.0,
      subscribers: @subscribers * @channels,
      message_length: @message_length,
      messages_sent: @messages_sent,
      messages_send_confirmed: @messages_send_confirmed,
      messages_send_unconfirmed: @messages_send_unconfirmed,
      messages_send_failed: @messages_send_failed,
      messages_received: @messages_received,
      messages_unreceived: @messages_unreceived,
      messages_send_rate: @messages_sent.to_f/@runtime,
      messages_receive_rate: @messages_received.to_f/@runtime,
      messages_send_rate_per_channel: (@messages_sent.to_f* 60)/(@runtime * @channels),
      messages_receive_rate_per_subscriber: (@messages_received.to_f * 60)/(@runtime * @subscribers * @channels)
    }
    @histograms.each do |name, histogram|
      vals["#{name}_avg".to_sym]=histogram.mean
      vals["#{name}_95th".to_sym]=histogram.percentile(95.0)
      vals["#{name}_99th".to_sym]=histogram.percentile(99.0)
      vals["#{name}_max".to_sym]=histogram.max
      vals["#{name}_stddev".to_sym]=histogram.stddev
      vals["#{name}_count".to_sym]=histogram.count
    end
    
    row = []
    headers.each { |header| row << (vals[header.to_sym] || "-")}
    
    csv = CSV.open(file, "a", {headers: headers, write_headers: write_headers})
    csv << row
    csv.flush
    csv.close
  end
end
end
