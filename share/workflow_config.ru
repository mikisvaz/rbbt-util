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

if Rbbt.etc['target_workflow_exports'].exists?
  exports = Rbbt.etc['target_workflow_exports'].read.split("\n")
  exports.each do |task|
    wf.export task.to_sym
  end
end

$title = wf.to_s
$class_name = class_name = wf.to_s + "REST"
$app = app = eval "class #{class_name} < Sinatra::Base; self end"

#{{{ PRE
load_file Rbbt.etc['app.d/pre.rb'].find 

app.get '/' do
  redirect to(File.join('/', wf.to_s))
end

#{{{ BASE
app_eval app, Rbbt.etc['app.d/base.rb'].find

app.add_workflow wf, :priority

#{{{ WORKFLOWS
app_eval app, Rbbt.etc['app.d/workflows.rb'].find_all

#{{{ RESOURCES
load_file Rbbt.etc['app.d/resources.rb'].find

#{{{ KNOWLEDGEBASE
load_file Rbbt.etc['app.d/knowledge_bases.rb'].find

#{{{ REQUIRES
load_file Rbbt.etc['app.d/requires.rb'].find

#{{{ ENTITIES
load_file Rbbt.etc['app.d/entities.rb'].find

#{{{ ROUTES
app_eval app, Rbbt.etc['app.d/routes.rb'].find_all

#{{{ FINDER
app_eval app, Rbbt.etc['app.d/finder.rb'].find

#{{{ POST
load_file Rbbt.etc['app.d/post.rb'].find_all

#{{{ PRELOAD
load_file Rbbt.etc['app.d/preload.rb'].find_all

#{{{ RUN
require 'rack'
use Rack::Deflater
run app

