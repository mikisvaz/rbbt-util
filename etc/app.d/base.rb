#{{{ MODULES AND HELPERS
register Sinatra::RbbtRESTMain
register Sinatra::RbbtRESTEntity
register Sinatra::RbbtRESTWorkflow
register Sinatra::RbbtRESTFileServer
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
set :file_dir            , local_var.sinatra.files.find
set :permalink_dir       , local_var.sinatra.permalink.find
set :favourites_dir      , local_var.sinatra.favourites.find
set :favourite_lists_dir , local_var.sinatra.favourite_lists
set :favourite_maps_dir  , local_var.sinatra.favourite_maps

#{{{ WORKFLOWS

if Rbbt.etc.workflows.find.exists?
 Rbbt.etc.workflows.find.read.split("\n").each do |workflow|
  Workflow.require_workflow workflow
  add_workflow Kernel.const_get(workflow), true
 end
end


