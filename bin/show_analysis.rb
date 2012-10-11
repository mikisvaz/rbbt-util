#!/usr/bin/env ruby

require 'rbbt'
require 'rbbt/util/simpleopt'
require 'rbbt/workflow'
require 'pp'

def usage(task)
  puts task.usage
  exit -1
end

options = SOPT.get "-p--port*"

require 'sinatra'
require 'compass'
require 'rbbt/workflow/rest'


file = ARGV[0]

Sinatra::Application.port = options[:port] || 4567
Sinatra::Application.run = true
Sinatra::Application.views = '.'

WorkflowREST.setup

include RbbtHTMLHelpers
$nomenu = true
get '/' do
  workflow_render(file, nil, nil, params)
end
