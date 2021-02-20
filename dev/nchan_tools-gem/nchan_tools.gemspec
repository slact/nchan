
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "nchan_tools/version"

Gem::Specification.new do |spec|
  spec.name          = "nchan_tools"
  spec.version       = NchanTools::VERSION
  spec.authors       = ["Leo Ponomarev"]
  spec.email         = ["leo@nchan.io"]

  spec.summary       = %q{Development and testing utilities for Nchan}
  spec.description   = %q{publishing, subscribing, testing, and benchmarking utilities for Nchan.}
  spec.homepage      = "https://nchan.io"
  spec.license       = "WTFPL"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency 'typhoeus'
  spec.add_dependency 'json'
  spec.add_dependency 'oga'
  spec.add_dependency "celluloid"
  spec.add_dependency "celluloid-io"
  spec.add_dependency "HDRHistogram"
  spec.add_dependency "redis", "~>4.2.0"
  spec.add_dependency "async"
  spec.add_dependency "async-redis"
  
  spec.add_dependency "websocket-driver"
  spec.add_dependency 'websocket-extensions'
  spec.add_dependency "permessage_deflate"
  spec.add_dependency 'http_parser.rb'
  if Gem::Version.new(RUBY_VERSION) >= Gem::Version.new('2.2.2')
    spec.add_dependency 'http-2'
  end  
  spec.add_development_dependency "pry"
  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
end
