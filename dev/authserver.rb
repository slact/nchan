#!/bin/ruby
require 'rubygems'
require 'bundler/setup'
require "pry"
require 'celluloid/current'
require 'celluloid/logger'
require "rack"
require "reel/rack/cli"
require 'reel/rack'
require "optparse"

module ReelConnectionExtensions
  def respond(code, headers, body=nil)
    if code == :ignore
      @current_request = nil
      @parser.reset
    else
      super
    end
  end
end
class Reel::Connection
  prepend ReelConnectionExtensions
end

module ReelServeExtensions
  def status_symbol(status)
    if status == -1
      :ignore
    else
      super
    end
  end
end
class Reel::Rack::Server
  prepend ReelServeExtensions
end

class AuthServer
  attr_accessor :app
  
  def print_request(env, body=nil)
    if @opt[:verbose]
      out = []
      out << "  #{env["REQUEST_METHOD"]} #{env["PATH_INFO"]}#{env['QUERY_STRING']!="" ? "?#{env['QUERY_STRING']}" : ""}"
      if @opt[:very_verbose]
        out << "  Host: #{env["HTTP_HOST"]}"
        env.each do |k,v|
          if k != "  HTTP_HOST" && k =~ /^HTTP_/
            out << "  #{k.split("_").slice(1..-1).each(&:capitalize!).join('-')}: #{v}"
          end
        end
      end
      out << "  #{body}"
      puts out.join("\n")
    end
  end
  
  def initialize(opt={}, &block)
    @opt = opt || {}
    @opt[:Port] ||= 8053
    
    if block_given?
      opt[:callback] = block
    end
    
    @app = proc do |env|
      resp = []
      headers = {}
      code = 200
      body = env["rack.input"].read
      chunked = false
      
      print_request env, body
      
      case env["REQUEST_PATH"] || env["PATH_INFO"]
      when /^\/accel_redirect\/(\w+)\/(\w+)(\/(\w+))?/
        what=$1
        chid=$2
        chid2=$4
        if what == "sub"
          headers["X-Accel-Redirect"]="/sub/internal/#{chid}"
        elsif what == "sub_multi"
          headers["X-Accel-Redirect"]="/sub/multi/#{chid}/#{chid2}"
        elsif what == "sub_withcb"
          headers["X-Accel-Redirect"]="/sub/withcb/#{chid}"
        else
          headers["X-Accel-Redirect"]="/pub/#{chid}"
        end
        headers["X-Accel-Buffering"] = "no"
      when "/auth"
        #meh
      when "/auth_fail_sleepy"
        code = -1
      when "/auth_fail_weird"
        code = 406
        headers["X-Banana"]="too-ripe"
        headers["Content-Type"]="text/x-beef"
        resp << "I don't accept."
        resp << "That is all."
      when "/auth_fail"
        code = 403
      when "/sub"
        resp << "subbed"
      when "/500"
        code = 500
        resp << "Let's pretend there was a server error."
      when "/404"
        code = 500
        resp << "Let's pretend there was 404."
      when "/pub"
        resp << publisher_upstream_transform_message(body)
      when "/pub_chunked"
        transformed = publisher_upstream_transform_message(body)
        resp << transformed[0..3]
        resp << transformed[4..-1]
        chunked = true
      when "/pub_empty"
        #nothing
      else
        code = 404
        resp << (env["REQUEST_PATH"] || env["PATH_INFO"])
        resp << " not found"
      end
      
      if @opt[:callback]
        @opt[:callback].call(env)
      end
      
      headers["Content-Length"]=resp.join("").length.to_s unless chunked
      
      [ code, headers, resp ]
    end
    
    @opt = Rack::Handler::Reel::DEFAULT_OPTIONS.merge(@opt)
    @app = Rack::CommonLogger.new(@app, STDOUT) unless @opt[:quiet]
  end
  
  def run
    ENV['RACK_ENV'] = @opt[:environment].to_s if @opt[:environment]
    
    
    @supervisor = Reel::Rack::Server.supervise(as: :reel_rack_server, args: [@app, @opt])
    
    if __FILE__ == $PROGRAM_NAME
      begin
        sleep
      rescue Interrupt
        Celluloid.logger.info "Interrupt received... shutting down" unless @opt[:quiet] 
        @supervisor.terminate
      end
    end
  end
  
  def stop
    @supervisor.terminate if @supervisor
  end

  def publisher_upstream_transform_message(msg)
    "WEE! + #{msg}"
  end
end



if __FILE__ == $PROGRAM_NAME then
  opt = {}
  opt_parser=OptionParser.new do |opts|
    opts.on("-q", "--quiet", "Be quiet!"){ opt[:quiet] = true}
    opts.on("--very-verbose", "Be very loud."){ opt[:very_verbose] = true; opt[:verbose] = true}
    opts.on("-v", "--verbose", "Be loud."){ opt[:verbose] = true}
  end
  opt_parser.parse!
  
  auth = AuthServer.new opt
  auth.run
end
