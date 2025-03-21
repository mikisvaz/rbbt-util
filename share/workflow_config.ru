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
wf = Workflow.require_workflow workflow


$title = wf.to_s
$app_name = app_name = wf.to_s + "REST"
$app = app = eval "class #{app_name} < Sinatra::Base; self end"

Rbbt.search_paths = Path::path_maps.merge(:workflow => File.join(wf.libdir, '{TOPLEVEL}','{SUBPATH}'))

etc_dir = Rbbt.etc

#{{{ PRE
load_file etc_dir['app.d/pre.rb'].find 

app.get '/' do
  begin
    template_render('main', params, 'main', :cache_type => :asynchronous)
  rescue TemplateMissing
    redirect to(File.join('/', wf.to_s))
  end
end

#{{{ BASE
app_eval app, etc_dir['app.d/base.rb'].find

#{{{ WORKFLOWS
app_eval app, etc_dir['app.d/workflows.rb'].find_all

app.add_workflow wf, :priority

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
    if task.include?('#')
      wf_name, task_name = task.split("#")
      begin
        task_wf = Kernel.const_get wf_name
        task_wf.export task_name.to_sym
      rescue
      end
    else
      wf.export task.to_sym
    end
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

  begin
      workflow = params[:workflow] if params[:workflow]
      wf_file = Workflow.local_workflow_filename(workflow)
      wf_dir = File.dirname(wf_file)
      $LOADED_FEATURES.delete_if do |path|
          Misc.path_relative_to(wf_dir, path)
      end
      load wf_file
  rescue Exception
      if File.exist?(Rbbt.etc['target_workflow'].read.strip)
          load Rbbt.etc['target_workflow'].read.strip
      else
          raise $!
      end
  end

  halt 200, "Workflow #{ workflow } reloaded"
end

require 'tilt/sass'
class << Tilt::SassTemplate
  private
  def sass_options
    options.merge(:filename => eval_file, :line => line, :syntax => :scss, :load_paths => RbbtRESTHelpers.sass_resources)
  end
end

#{{{ RUN
require 'rack'
use Rack::Deflater
run app

