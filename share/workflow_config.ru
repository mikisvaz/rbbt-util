require 'rbbt'
require 'rbbt/resource'
$LOAD_PATH.unshift('lib') unless $LOAD_PATH.include?('lib')

def load_file(file)
  if Array === file
    file.each{|f| load_file f }
  elsif file.exists?
    Log.info("Loading: " << file)
    load file
  end
end
def app_eval(app, file)
  if Array === file
    file.each{|f| app_eval app, f }
  elsif file.exists?
    app.class_eval do
      Log.info("Loading: " << file)
      eval file.read, nil, file
    end
  end
end


#{{{ INIT
ENV["RBBT_LOG"] = Log.severity.to_s
require Rbbt.etc['app.d/init.rb'].find

#{{{ Workflow
workflow = Rbbt.etc['target_workflow'].read
wf = Workflow.require_workflow workflow, true


$title = wf.to_s
$app_name = app_name = wf.to_s + "REST"
$app = app = eval "class #{app_name} < Sinatra::Base; self end"

Rbbt.search_paths = Path::SEARCH_PATHS.merge(:workflow => File.join(wf.libdir, '{TOPLEVEL}','{SUBPATH}'))

etc_dir = Rbbt.etc
#etc_dir.search_paths = Path::SEARCH_PATHS.merge(:workflow => File.join(wf.libdir, '{TOPLEVEL}','{SUBPATH}'))


#{{{ PRE
load_file etc_dir['app.d/pre.rb'].find 

app.get '/' do
  redirect to(File.join('/', wf.to_s))
end

#{{{ BASE
app_eval app, etc_dir['app.d/base.rb'].find

app.add_workflow wf, :priority

#{{{ WORKFLOWS
app_eval app, etc_dir['app.d/workflows.rb'].find_all

#{{{ RESOURCES
load_file etc_dir['app.d/resources.rb'].find

#{{{ KNOWLEDGEBASE
load_file etc_dir['app.d/knowledge_bases.rb'].find

#{{{ REQUIRES
load_file etc_dir['app.d/requires.rb'].find

#{{{ ENTITIES
load_file etc_dir['app.d/entities.rb'].find

#{{{ ROUTES
app_eval app, etc_dir['app.d/routes.rb'].find_all

#{{{ FINDER
app_eval app, etc_dir['app.d/finder.rb'].find

#{{{ POST
load_file etc_dir['app.d/post.rb'].find_all

#{{{ PRELOAD
load_file etc_dir['app.d/preload.rb'].find_all

#{{{ PRELOAD
load_file Rbbt.etc['app.d/semaphores.rb'].find_all

if etc_dir['target_workflow_exports'].exists?
  exports = etc_dir['target_workflow_exports'].read.split("\n")
  exports.each do |task|
    wf.export task.to_sym
  end
end

if etc_dir['target_workflow_stream_exports'].exists?
  exports = etc_dir['target_workflow_stream_exports'].read.split("\n")
  exports.each do |task|
    wf.export_stream task.to_sym
  end
end

if etc_dir['target_workflow_async_exports'].exists?
  exports = etc_dir['target_workflow_async_exports'].read.split("\n")
  exports.each do |task|
    wf.export_asynchronous task.to_sym
  end
end

if etc_dir['target_workflow_sync_exports'].exists?
  exports = etc_dir['target_workflow_sync_exports'].read.split("\n")
  exports.each do |task|
    wf.export_synchronous task.to_sym
  end
end

if etc_dir['target_workflow_exec_exports'].exists?
  exports = etc_dir['target_workflow_exec_exports'].read.split("\n")
  exports.each do |task|
    wf.export_exec task.to_sym
  end
end

app.get '/reload_workflow' do
  if production?
    halt 500, "Not allowed in production" 
  end

  workflow = params[:workflow] if params[:workflow]
  wf_file = Workflow.local_workflow_filename(workflow)
  wf_dir = File.dirname(wf_file)
  $LOADED_FEATURES.delete_if do |path|
    Misc.path_relative_to(wf_dir, path)
  end
  load wf_file
  halt 200, "Workflow #{ workflow } reloaded"
end

#{{{ RUN
require 'rack'
use Rack::Deflater
run app

