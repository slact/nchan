#!/usr/bin/ruby
require 'digest/sha1'

scripts={}

Dir[ "#{File.dirname(__FILE__)}/scripts/*.lua" ].each do |f|
  scripts[File.basename(f, ".lua") .to_sym]=IO.read f
end

def cquote(str) 
  new_str=str.gsub /"/, "\\\""
  new_str.gsub! /^(.*)$/, "  \"\\1\\n\""
  new_str
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

EOF

struct=[]
script_table=[]
hashed_table=[]
scripts.sort_by {|k,v| k}.each do |v| 
  name=v.first
  script=v.last
  
  struct << "  char *#{name};"
  
  script_table << cquote(script)
  
  hashed_table << "  \"#{Digest::SHA1.hexdigest script}\""  
end

if scripts.count > 0
  out=sprintf cout, struct.join("\n"), script_table.join(",\n\n"), hashed_table.join(",\n")
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
