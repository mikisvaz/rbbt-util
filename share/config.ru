require 'rbbt'
require 'rbbt/resource'
$LOAD_PATH.unshift('lib') unless $LOAD_PATH.include?('lib')

def load_file(file)
  if file.exists?
    Log.info("Loading: " << file)
    load file
  end
end

def app_eval(app, file)
  if file.exists?
    app.class_eval do
      Log.info("Loading: " << file)
      eval file.read, nil, file
    end
  end
end

#{{{ INIT
load_file Rbbt.etc['app.d/init.rb'].find

$class_name = class_name = File.basename(FileUtils.pwd)
$app = app = eval "class #{class_name} < Sinatra::Base; self end"

#{{{ PRE
load_file Rbbt.etc['app.d/pre.rb'].find 

#{{{ BASE
app_eval app, Rbbt.etc['app.d/base.rb'].find

#{{{ RESOURCES
load_file Rbbt.etc['app.d/resources.rb'].find

#{{{ ENTITIES
load_file Rbbt.etc['app.d/entities.rb'].find

#{{{ ROUTES
app_eval app, Rbbt.etc['app.d/routes.rb'].find

#{{{ FINDER
app_eval app, Rbbt.etc['app.d/finder.rb'].find

#{{{ POST
load_file Rbbt.etc['app.d/post.rb'].find 

#{{{ PRELOAD
load_file Rbbt.etc['app.d/preload.rb'].find 

#{{{ RUN
$title = class_name
require 'rack'
use Rack::Deflater
run app

