EntityRESTHelpers.entity_resources.unshift Rbbt.www.views.find
RbbtRESTHelpers.template_resources.unshift Rbbt.www.views.find
RbbtRESTHelpers.add_sass_load_path "#{Gem.loaded_specs['compass'].full_gem_path}/frameworks/compass/stylesheets"
RbbtRESTHelpers.add_sass_load_path "#{Gem.loaded_specs['zurb-foundation'].full_gem_path}/scss/" 
RbbtRESTHelpers.add_sass_load_path "#{Gem.loaded_specs['modular-scale'].full_gem_path}/stylesheets/" 
RbbtRESTHelpers.javascript_resources << Path.setup("#{Gem.loaded_specs['zurb-foundation'].full_gem_path}/js/foundation")
RbbtRESTHelpers.javascript_resources << Path.setup("#{Gem.loaded_specs['zurb-foundation'].full_gem_path}/js/vendor")


