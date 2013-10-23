require 'rbbt/resource'

require 'rbbt'
load Rbbt.etc['app.d/init.rb'].find

class_name = File.basename(FileUtils.pwd)

app = eval "class #{class_name} < Sinatra::Base; self end"

app.class_eval do
  Log.info{"Loading: " << Rbbt.etc['app.d/base.rb'].find}
  eval Rbbt.etc['app.d/base.rb'].read, nil, Rbbt.etc['app.d/base.rb'].find
end


#{{{ RESOURCES
Log.info{"Loading: " << Rbbt.etc['app.d/resources.rb'].find}
load Rbbt.etc['app.d/resources.rb'].find

#{{{ ENTITIES
Log.info{"Loading: " << Rbbt.etc['app.d/entities.rb'].find}
load Rbbt.etc['app.d/entities.rb'].find

#{{{ FINDER
app.class_eval do
  Log.info{"Loading: " << Rbbt.etc['app.d/finder.rb'].find}
  eval Rbbt.etc['app.d/finder.rb'].read
end

#{{{ POST
Log.info{"Loading: " << Rbbt.etc['app.d/post.rb'].find if Rbbt.etc['app.d/post.rb'].exists?}
load Rbbt.etc['app.d/post.rb'].find if Rbbt.etc['app.d/post.rb'].exists?

#{{{ RUN
$title = class_name
require 'rack'
use Rack::Deflater
run app

