#{{{ MODULES AND HELPERS

register Sinatra::RbbtRESTWorkflow

Rbbt.etc.workflows.find.read.split("\n").each do |workflow|
  next if workflow.empty?
  Workflow.require_workflow workflow
  add_workflow Kernel.const_get(workflow), true
end if Rbbt.etc.workflows.find.exists?

register Sinatra::RbbtRESTMain
register Sinatra::RbbtRESTEntity
register Sinatra::RbbtRESTFileServer # Remove to prevent serving files
register Sinatra::RbbtRESTKnowledgeBase
helpers Sinatra::RbbtMiscHelpers

#{{{ SESSIONS

use Rack::Session::Cookie, :key => 'rack.session',
  :path => '/',
  :expire_after => 2592000,
  :secret => "#{self.to_s} secret!!"


#{{{ DIRECTORIES
global_var = Rbbt.var.sinatra
local_var = Rbbt.var.sinatra.app[$class_name]

set :cache_dir           , local_var.cache
set :persist_dir         , local_var.cache.persistence
set :persist_options     , {:persist => true, :persist_dir => :persist_dir}
set :file_dir            , local_var.files
set :permalink_dir       , local_var.permalink
set :favourites_dir      , local_var.favourites
set :favourite_lists_dir , local_var.favourite_lists
set :favourite_maps_dir  , local_var.favourite_maps

#set :finder, nil

#{{{ WORKFLOWS



