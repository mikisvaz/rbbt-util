#{{{ FINDER
finder = Finder.new
#if ENV['RBBT_FINDER']
  finder.add_instance(KEGG.pathways, :grep => '^hsa\|^#', :fields => ["Pathway Name"], :namespace => Organism.default_code("Hsa"), :fix => Proc.new{|l| l.sub(/ - Homo sapiens.*/,'')})
  finder.add_instance(Organism.lexicon(Organism.default_code("Hsa")), :persist => true, :namespace => Organism.default_code("Hsa"), :grep => '^LRG_', :invert_grep => true)
#end
set :finder, finder
Log.debug("Finder started with: #{finder.instances.length} instances")


