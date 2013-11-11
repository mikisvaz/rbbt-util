Rbbt.etc.requires.read.split("\n").each do |file|
  next if file.empty?
  Log.debug("requiring #{ file }")
  require file
end if Rbbt.etc.requires.exists?


Entity.entity_list_cache = Rbbt.var.find.sinatra.entity_lists
Entity.entity_map_cache = Rbbt.var.find.sinatra.entity_maps
Entity.entity_property_cache = Rbbt.var.find.sinatra.entity_properties

Rbbt.etc.entities.read.split("\n").each do |name|
  next if name.empty?
  mod = Kernel.const_get name
  Log.debug("Including Entity::REST for #{ name }")
  mod.module_eval do
    include Entity::REST
  end
end if Rbbt.etc.entities.exists?

$annotation_repo = Rbbt.var.find.sinatra.annotation_repo.find
(Rbbt.etc.persist_properties.yaml || {}).each do |name,list|
  next if name.empty?
  mod = Kernel.const_get name
  mod.module_eval do
   list.each do |elem|
     prop, type, repo = elem.split(",").collect{|e| e.strip}

     Log.debug("Persist #{name} #{prop}: #{[type, repo].compact * ", "}")
     if repo == 'repo'
      options = {:annotation_repo => $annotation_repo}
     else
      options = {}
     end
     persist prop, type, options
   end
  end
end if Rbbt.etc.persit_properties.exists?


