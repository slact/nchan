#!/usr/bin/env ruby
require "date"
require "optparse"

opt = {
  out: :readme,
  nchapp: false,
  root: "..",
  newversion: nil
}

opt_parser=OptionParser.new do |opts|
  opts.on("--path (#{opt[:root]})", "Path to Nchan project root") {|v| opt[:root] = v}
  opts.on("--stdout", "Output to stdout instead of overwriting Readme.") { opt[:out] = :stdout }
  opts.on("--nchapp", "Generate Nchan documentation for Nchapp, the Nchan documentation app.") { opt[:nchapp] = true; }
  opts.on("--output FILE", "Output to given file instead of overwriting README.md") { |v| opt[:out] = v }
  opts.on("--release VERSION", "Document a new version release in Readme and changelog") { |v| opt[:newversion] = v }
end
opt_parser.parse!

readme_file = "#{opt[:root]}/README.md"
changelog_file = "#{opt[:root]}/changelog.txt"
cmds_file = "#{opt[:root]}/src/nchan_commands.rb"

#current_release = nil
#current_release_date = nil
#Dir.chdir ROOT_DIR do
#  current_release=(`git describe --abbrev=0 --tags`).chomp
#  current_release_date = (`git log -1 --format=%ai #{current_release}`).chomp
#  current_release_date = DateTime.parse(current_release_date).strftime("%B %-d, %Y")
#end


class CfCmd #let's make a DSL!
  class CommandError < StandardError
  end
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
      raise CommandError, "Unknown value lookup #{val}" if ret.nil?
      ret
    end
  end
  
  class Cmd
    attr_accessor :name, :type, :set, :conf, :offset_name
    attr_accessor :contexts, :args, :legacy, :alt, :disabled, :undocumented
    attr_accessor :group, :uri, :tags, :default, :info, :value
    
    attr_accessor :declaration_order
    
    
    class ConsumableOptions
      def initialize(opt)
        @opt=opt.dup
      end
      def [](k)
        @opt.delete k
      end
      def method_missing(method_name, *args, &block)
        @opt.send(method_name, *args, &block)
      end
      def leftovers?
        leftovers.length > 0
      end
      def leftovers
        @opt.keys
      end
    end
    
    def initialize(name, func)
      self.name=name.to_sym
      self.set=func
      @tags = []
    end
    
    def group
      @group || :none
    end
    
    def set_options!(opt)
      opt = ConsumableOptions.new(opt)
      
      @legacy = opt[:legacy]
      @alt = opt[:alt]
      @disabled = opt[:disabled]
      @undocumented = opt[:undocumented]
      
      @args = opt.has_key?(:args) ? opt[:args] : 1
      @group = opt[:group]
      @value = opt[:value]
      @default = opt[:default]
      @info = opt[:info]
      @tags = opt[:tags] || []
      @uri = opt[:uri]
      
      opt[:post_handler] #use it up
      
      if opt.leftovers?
        raise CommandError, "command '#{@name}' has invalid options: #{opt.leftovers.join ", "}"
      end
    end
    
    def descr(str, opt)
      if opt[:nchapp]
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
      
      if opt[:nchapp]
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
        lines << "  #{descr 'arguments', opt} #{args.first} #{opt[:nchapp] ? "&ndash;" : "-"} #{args.exclude_end? ? args.last - 1 : args.last}"
      elsif Numeric === args
        lines << "  #{descr 'arguments', opt} #{args}"
      else
        raise CommandError, "#{@name} command invalid args: #{args}"
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
      
      if uri
        url = opt[:nchapp] ? uri : (uri[0]=='/' ? "https://nchan.io#{uri}" : uri)
        lines << "  [more details](#{url})"
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
    @cmds_by_name = {}
    instance_eval &block
  end
  
  def find(command_name)
    @cmds_by_name[command_name.to_sym]
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
    
    cmd.set_options!(opt)
    
    cmd.declaration_order = @cmds.count
    @cmds_by_name[cmd.name] = cmd
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
    @ord[val.group.to_sym] || Float::INFINITY
  end
end

begin
  cf=eval File.read(cmds_file), nil, cmds_file
rescue Exception => e
  STDERR.puts e.message
  STDERR.puts e.backtrace.map{|v| "  #{v}"}
  exit 1
end

cmds = cf.cmds

group_order=Order.new(:channel, :publish, :subscribe, :pubsub, :hook, :security, :storage, :meta, :none, :development)

cmds.sort! do |c1, c2|
  [group_order[c1], c1.name] <=> [group_order[c2], c2.name]
end


#tags=[]
#cmds.each do |cmd|
#  tags+= cmd.tags
#end
#tags.uniq!
#puts tags.sort.join " "

text_cmds=cmds.map do |cmd|
  cmd.to_md(:nchapp => opt[:nchapp])
end


config_documentation= text_cmds.join "\n\n"

#if opt[:nchapp]
#  config_documentation = "<div class='configuration'><markdown>#{config_documentation}</markdown></div>"
#end

text = File.read(readme_file)

def relevant_commands(cmds)
  cmds.map! { |cmd| "<a class=\"directive\" href=\"##{cmd.name}\">#{cmd.name}</a>" }
    
  if cmds.count > 0
    "<div class=\"relevant-directives\">\n related configuration: #{cmds.join ", "}</div>"
  else 
    ""
  end
end

if opt[:nchapp]
  #remove first line
  text.sub!(/^<.*?>$\n\n?/m, "")
  #remove second line
  text.sub!(/^https:\/\/nchan.io$\n\n?/m, "")
  
  # add an #about link
  text.prepend "<a id=\"about\"></a>\n"
  
  #add a table-of-contents div
  #text.sub! /^<!--\s?toc\s?-->/, "<div class=\"tableOfContents\"></div>\n#"
  
  text.gsub! /<!--\s?tag:\s?(\S+)\s?-->/ do |whole|
    tag = Regexp.last_match[1]
    mycmds = cmds.select{|cmd| cmd.tags.member? tag}
    relevant_commands mycmds
  end
  
  text.gsub! /<!--\s?commands?:\s?(.+)\s?-->/ do |whole|
    cmds = Regexp.last_match[1]
    cmds = cmds.split(" ").map {|c| cf.find(c)}.compact
    relevant_commands cmds
  end
end

config_heading = "## Configuration Directives"
text.gsub!(/(?<=^#{config_heading}$).*(?=^## )/m, "\n\n#{config_documentation}\n\n")

if opt[:nchapp]
  contrib_heading = "## Contribute"
  text.gsub!(/(^#{contrib_heading}$).*(?=^## )?/m, "")
end

if opt[:newversion]
  now = DateTime.now
  text.gsub!(/(?<=^The latest Nchan release is )\S+\s+\([^)]+\)/, "#{opt[:newversion]} (#{now.strftime("%B %-d, %Y")})")

  changelog = File.read changelog_file
  
  l = changelog.lines.first
  changes_logged = l.match(/^\s+/) && true
  unless changes_logged
    prev_ver = l.match(/^(\d+\.\d+\.\d+\w*)/)
    unless prev_ver
      STDERR.puts "Invalid changelog.txt first line: expected a version, got something weird..."
      exit 1
    end
    prev_ver = prev_ver.to_s
    unless opt[:newversion] == prev_ver
      STDERR.puts "No changes logged to changelog.txt between previous version and #{opt[:newversion]}."
      STDERR.puts "Did someone forget to update the changelog?"
      exit 1
    else
      changelog = changelog.lines.drop(1).join
    end
  else
    prev_ver = nil
    changelog.lines.each do |l|
      prev_ver = l.match(/^(\d+\.\d+\.\d+\w*)/)
      if prev_ver
        prev_ver = prev_ver.to_s
        break
      end
    end
    if prev_ver
      if Gem::Version.new(opt[:newversion]) <= Gem::Version.new(prev_ver)
        STDERR.puts "New version #{opt[:newversion]} must be greater than old version #{prev_ver} in changelog.txt"
        exit 1
      end
    end
  end
  
  File.open changelog_file, 'w' do |f|
    now = DateTime.now
    f.print "#{opt[:newversion]} (#{now.strftime("%b#{now.strftime('%b') == now.strftime('%B') ? "" : "."} %-d %Y")})\n"
    f.print changelog
  end
  
end

if opt[:nchapp]
  text.gsub!(/https:\/\/nchan\.io\//, "/")
end

case opt[:out]
when :stdout
  puts text
else
  File.open(opt[:out] == :readme ? readme_file : opt[:out], "w") {|file| file.puts text }
end

