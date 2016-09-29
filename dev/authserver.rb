#!/bin/ruby
require "pry"
require 'celluloid/current'
require 'celluloid/logger'
require "rack"
require "reel/rack/cli"
require 'reel/rack'
require "optparse"

class AuthServer
  if __FILE__ != $PROGRAM_NAME then
    include Celluloid::IO
  end
  attr_accessor :app
  def initialize(opt={})
    @opt = opt || {}
    @opt[:Port] ||= 8053
    
    @app = proc do |env|
      resp = []
      headers = {}
      code = 200
      body = env["rack.input"].read
      
      pp env if @opt[:verbose]
      
      case env["REQUEST_PATH"] || env["PATH_INFO"]
      when "/accel_redirect"  
        chid="foo"
        headers["X-Accel-Redirect"]="/sub/internal/#{chid}"
        headers["X-Accel-Buffering"] = "no"
      when "/auth"
        #meh
      when "/auth_fail"
        code = 403
      when "/pub"
        resp << "WEE! + #{body}"
        headers["Content-Length"]=resp.join("").length.to_s
      end
      
      [ code, headers, resp ]
    end
  end
  
  def run
    Rack::Handler::Reel.run(app, @opt)
  end
end



if __FILE__ == $PROGRAM_NAME then
  opt = {}
  opt_parser=OptionParser.new do |opts|
    opts.on("-q", "--quiet", "Be quiet!"){ opt[:quiet] = true}
    opts.on("-v", "--verbose", "Be loud."){ opt[:verbose] = true}
  end
  opt_parser.parse!
  
  auth = AuthServer.new opt
  auth.run
end
