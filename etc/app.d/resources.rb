
EntityRESTHelpers.entity_resources.unshift Rbbt.www.views.find if Rbbt.www.views.exists?
RbbtRESTHelpers.template_resources.unshift Rbbt.www.views.find if Rbbt.www.views.exists?
  
load Rbbt.etc['app.d']['grid_system.rb'].find if Rbbt.etc['app.d']['grid_system.rb'].exists?

require 'sass-css-importer'

Sinatra::RbbtRESTMain.add_resource_path(Path.setup(ENV["RBBT_VIEWS_DIR"])) if ENV["RBBT_VIEWS_DIR"] and not ENV["RBBT_VIEWS_DIR"].empty?

Compass::Frameworks::ALL.each do |importer|
  next unless importer.respond_to? :path
  path = importer.stylesheets_directory
  RbbtRESTHelpers.add_sass_load_path path
end

RbbtRESTHelpers.add_sass_load_path './www/views/compass'

