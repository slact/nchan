#!/usr/bin/ruby
require "date"


SRC_DIR="src"
CONFIG_IN="nchan_commands.rb"


README_FILE="README.md"

if ARGV[0]
  ROOT_DIR = ARGV[0]
  readme_output_path = ARGV[1]
  mysite = true
else
  ROOT_DIR = ".."
  readme_output_path = readme_path
  mysite = nil
end
readme_path = "#{ROOT_DIR}/#{README_FILE}"


puts "redocument #{readme_path} #{readme_output_path}"

current_release=(`git describe --abbrev=0 --tags`).chomp
current_release_date = (`git log -1 --format=%ai #{current_release}`).chomp
current_release_date = DateTime.parse(current_release_date).strftime("%B %-d, %Y")

class CfCmd #let's make a DSL!
  attr_accessor :cmds
  class OneOf
    def initialize(arg) 
      @arg=arg
    end
    def []=(k,v)
      @arg[k]=v
    end
    def [](val)
      ret=@arg[val]
      raise "Unknown value lookup #{val}" if ret.nil?
      ret
    end
  end
  
  class Cmd
    attr_accessor :name, :type, :set, :conf, :offset_name
    attr_accessor :contexts, :args, :legacy, :alt, :disabled
    attr_accessor :group, :default, :info, :value

    def initialize(name, func)
      self.name=name
      self.set=func
    end
    
    def group
      @group || :none
    end
    
    
    def to_md(opt={})
      lines = []
      if value.class == Array
        val = "#{value.join ' | '}"
      elsif value
        val = "#{value}"
      end
      
      if opt[:mysite]
        namestr = "<a name=\"#{name}\">#{name}</a>"
      else
        namestr = "**#{name}**"
      end
      
      if val
        lines << "- #{namestr} `[ #{val} ]`"
      else
        lines << "- #{namestr}"
      end
      
      if Range === args
        lines << "  arguments: #{args.first} - #{args.exclude_end? ? args.last - 1 : args.last}"
      elsif Numeric === args
        lines << "  arguments: #{args}"
      else
        raise "invalid args: #{args}"
      end
      
      if default
        lines << "  default: `#{Array === default ? default.join(' ') : default}`"
      end
      
      ctx_lookup = { main: :http, srv: :server, loc: :location }
      ctx = contexts.map do |c|
        ctx_lookup[c] || c;
      end
      lines << "  context: #{ctx.join ', '}"
      
      if legacy
        lines << "  legacy name: #{legacy}"
      end
      
      if info
        out = info.lines.map do |line|
          "  > #{line.chomp}  "
        end
        lines << out.join("\n")
      end
      
      lines.map! {|l| "#{l}  "}
      lines.join "\n"
    end
  end
  
  
  def initialize(&block)
    @cmds=[]
    instance_eval &block
  end
  
  def method_missing(name, *args)
    define_cmd name, *args
  end
  
  
  def define_cmd(name, valid_contexts, handler, conf, opt={})
    cmd=Cmd.new name, handler
    cmd.contexts= valid_contexts
    
    if Array === conf
      cmd.conf=conf[0]
      cmd.offset_name=conf[1]
    else
      cmd.conf=conf
    end
    
    cmd.legacy = opt[:legacy]
    cmd.alt = opt[:alt]
    cmd.disabled = opt[:disabled]
    
    cmd.args = opt.has_key?(:args) ? opt[:args] : 1
    cmd.group = opt[:group]
    cmd.value = opt[:value]
    cmd.default = opt[:default]
    cmd.info = opt[:info]
    
    @cmds << cmd
  end
 
end


class Order 
  def initialize(*args)
    @i=0
    @ord = {}
    args.each do |val|
      @ord[val] = @i
      @i+=1
    end
  end
  def [](val)
    @ord[val] || Float::INFINITY
  end
end

begin
  cf=eval File.read("#{ROOT_DIR}/#{SRC_DIR}/#{CONFIG_IN}")
rescue Exception => e
  STDERR.puts e.message.gsub(/^\(eval\)/, "#{SRC_DIR}/#{CONFIG_IN}")
  exit 1
end

cmds = cf.cmds

group_order=Order.new(:pubsub, :security, :storage, :meta, :none, :development)

cmds.sort! do |c1, c2|
  if c1.group != c2.group
    group_order[c1] - group_order[c2]
  else
    (c1.name < c2.name) ? -1 : 1
  end
end



cmds.map! do |cmd|
  cmd.to_md(:mysite => mysite)
end


config_documentation= cmds.join "\n\n"

text = File.read(readme_path)

config_heading = "## Configuration Directives"
new_contents = text.gsub(/(?<=^#{config_heading}$).*(?=^## )/m, "\n\n#{config_documentation}\n\n")

new_contents = new_contents.gsub(/(?<=^The latest Nchan release is )\S+\s+\([^)]+\)/, "#{current_release} (#{current_release_date})")

File.open(readme_output_path, "w") {|file| file.puts new_contents }

