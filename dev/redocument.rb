#!/usr/bin/ruby
require "date"
require "pry"

SRC_DIR="src"
CONFIG_IN="nchan_commands.rb"


README_FILE="README.md"

if ARGV[0]
  ROOT_DIR = ARGV[0]
  readme_path = "#{ROOT_DIR}/#{README_FILE}"
  readme_output_path = ARGV[1]
  mysite = true
else
  ROOT_DIR = ".."
  readme_path = "#{ROOT_DIR}/#{README_FILE}"
  readme_output_path = readme_path
  mysite = nil
end

current_release = nil
current_release_date = nil
Dir.chdir ROOT_DIR do
  current_release=(`git describe --abbrev=0 --tags`).chomp
  current_release_date = (`git log -1 --format=%ai #{current_release}`).chomp
  current_release_date = DateTime.parse(current_release_date).strftime("%B %-d, %Y")
end


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
    attr_accessor :contexts, :args, :legacy, :alt, :disabled, :undocumented
    attr_accessor :group, :default, :info, :value

    def initialize(name, func)
      self.name=name
      self.set=func
    end
    
    def group
      @group || :none
    end
    
    def descr(str, opt)
      if opt[:mysite]
        "<span class=\"description\">#{str}:</span>"
      else
        "#{str}:"
      end
    end
    
    def to_md(opt={})
      lines = []
      if value.class == Array
        val = "[ #{value.join ' | '} ]"
      elsif value
        val = "#{value}"
      end
      
      if opt[:mysite]
        namestr = "<a class=\"directive\" id=\"#{name}\" href=\"##{name}\">#{name}</a>"
      else
        namestr = "**#{name}**"
      end
      
      if val
        lines << "- #{namestr} `#{val}`"
      else
        lines << "- #{namestr}"
      end
      
      if Range === args
        lines << "  #{descr 'arguments', opt} #{args.first} #{opt[:mysite] ? "&ndash;" : "-"} #{args.exclude_end? ? args.last - 1 : args.last}"
      elsif Numeric === args
        lines << "  #{descr 'arguments', opt} #{args}"
      else
        raise "invalid args: #{args}"
      end
      
      if default
        lines << "  #{descr 'default', opt} `#{Array === default ? default.join(' ') : default}`"
      end
      
      ctx_lookup = { main: :http, srv: :server, loc: :location }
      ctx = contexts.map do |c|
        ctx_lookup[c] || c;
      end
      lines << "  #{descr 'context', opt} #{ctx.join ', '}"
      
      if legacy
        if Array === self.legacy
          lines << "  #{descr 'legacy names', opt} #{legacy.join ', '}"
        else
          lines << "  #{descr 'legacy name', opt} #{legacy}"
        end
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
    
    def to_txt(opt={})
      lines = []
      if value.class == Array
        val = "[ #{value.join ' | '} ]"
      elsif value
        val = "#{value}"
      end
      
      if val
        lines << "#{name} #{val}"
      else
        lines << "#{name}"
      end
      
      if Range === args
        lines << "  #{descr 'arguments', opt} #{args.first} - #{args.exclude_end? ? args.last - 1 : args.last}"
      elsif Numeric === args
        lines << "  #{descr 'arguments', opt} #{args}"
      else
        raise "invalid args: #{args}"
      end
      
      if default
        lines << "  #{descr 'default', opt} #{Array === default ? default.join(' ') : default}"
      end
      
      ctx_lookup = { main: :http, srv: :server, loc: :location }
      ctx = contexts.map do |c|
        ctx_lookup[c] || c;
      end
      lines << "  #{descr 'context', opt} #{ctx.join ', '}"
      
      if legacy
        if Array === self.legacy
          lines << "  #{descr 'legacy names', opt} #{legacy.join ', '}"
        else
          lines << "  #{descr 'legacy name', opt} #{legacy}"
        end
      end
      
      if info
        out = info.lines.map do |line|
          "   #{line.chomp}  "
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
    cmd.undocumented = opt[:undocumented]
    
    cmd.args = opt.has_key?(:args) ? opt[:args] : 1
    cmd.group = opt[:group]
    cmd.value = opt[:value]
    cmd.default = opt[:default]
    cmd.info = opt[:info]
    
    @cmds << cmd if !cmd.disabled && !cmd.undocumented
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

#if mysite
#  config_documentation = "<div class='configuration'><markdown>#{config_documentation}</markdown></div>"
#end

text = File.read(readme_path)

if mysite
  #remove first line
  text.sub!(/^<.*?>$\n\n?/m, "")
  #remove second line
  text.sub!(/^https:\/\/nchan.slact.net$\n\n?/m, "")
  
  # add an #about link
  text.prepend "<a id=\"about\"></a>\n"
  
  #add a table-of-contents div right before the first heading
  text.sub! /^#/, "<div class=\"tableOfContents\"></div>\n#"
end

config_heading = "## Configuration Directives"
text.gsub!(/(?<=^#{config_heading}$).*(?=^## )/m, "\n\n#{config_documentation}\n\n")

if mysite
  contrib_heading = "## Contribute"
  text.gsub!(/(^#{contrib_heading}$).*(?=^## )?/m, "")
end


text.gsub!(/(?<=^The latest Nchan release is )\S+\s+\([^)]+\)/, "#{current_release} (#{current_release_date})")

if mysite
  text.gsub!(/https:\/\/nchan\.slact\.net\//, "/")
end

File.open(readme_output_path, "w") {|file| file.puts text }

