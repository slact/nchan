#!/usr/bin/ruby
require 'digest/sha1'
require "pry"

scripts={}

Dir[ "#{File.dirname(__FILE__)}/scripts/*.lua" ].each do |f|
  scripts[File.basename(f, ".lua") .to_sym]=IO.read f
end

def cquote(str)
  out=[]
  str.each_line do |l|
    l.sub! "\n", "\\n"
    l.gsub! '"', '\"'
    l.gsub! /^(.*)$/, "  \"\\1\""
    out << l
  end
  out.join "\n"
end

cout= <<EOF
//don't edit this please, it was auto-generated

typedef struct {
%s
} nhpm_redis_lua_scripts_t;

static nhpm_redis_lua_scripts_t nhpm_rds_lua_scripts = {
%s
};

static nhpm_redis_lua_scripts_t nhpm_rds_lua_hashes = {
%s
};

static nhpm_redis_lua_scripts_t nhpm_rds_lua_script_names = {
%s
};

EOF

struct=[]
name_table=[]
script_table=[]
hashed_table=[]
comments_table=[]

scripts.sort_by {|k,v| k}.each do |v| 
  name=v.first
  script=v.last

  name_table << "  \"#{name}\","

  str=[]
  for l in script.lines do
    cmt=l.match /^--(.*)/
    break unless cmt
    str << "  //#{cmt[1]}"
  end
  str << "  char *#{name};\n"
  struct << str.join("\n")

  script_table << "  //#{name}\n#{cquote(script)}"

  hashed_table << "  \"#{Digest::SHA1.hexdigest script}\""
end

if scripts.count > 0
  out=sprintf cout, struct.join("\n"), script_table.join(",\n\n"), hashed_table.join(",\n"), name_table.join("\n")
else
  out="//nothing here\n"
end

if ARGV[0]=="file"
  path="#{File.dirname(__FILE__)}/redis_lua_commands.h"
  File.write path, out
  puts "generated #{path}"
else
  puts out
end
