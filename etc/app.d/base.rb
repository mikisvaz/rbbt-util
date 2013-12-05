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
  :secret => 'StudyExplorer secret!!'


#{{{ DIRECTORIES
local_var = Rbbt.var.find(:current)
set :cache_dir           , local_var.sinatra.cache.find
set :persist_dir         , local_var.sinatra.cache.persistence.find
set :persist_options     , {:persist => true, :persist_dir => :persist_dir}
set :file_dir            , local_var.sinatra.files.find
set :permalink_dir       , local_var.sinatra.permalink.find
set :favourites_dir      , local_var.sinatra.favourites.find
set :favourite_lists_dir , local_var.sinatra.favourite_lists
set :favourite_maps_dir  , local_var.sinatra.favourite_maps

#{{{ WORKFLOWS



