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

  params.delete "captures"
  params.delete "splat"

  visualization_parameters = get_visualization_parameters(params)

  cache_type = params.delete(:_cache_type) || params.delete("_cache_type") || :async
  update = params.delete(:_update) || params.delete("_update") || nil

  cache("Show_analysis", :file => file, :update => update, :cache_type => cache_type, :params => params, :visualization_params => visualization_parameters) do
    workflow_render(file, nil, nil, params)
  end
end
