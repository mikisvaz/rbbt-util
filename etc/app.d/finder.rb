#{{{ FINDER
finder = Finder.new
if ENV['RBBT_FINDER']
  finder.add_instance(KEGG.pathways, :grep => '^hsa\|^#', :fields => ["Pathway Name"], :namespace => "Hsa/jun2011", :fix => Proc.new{|l| l.sub(/ - Homo sapiens.*/,'')})
  finder.add_instance(Organism.lexicon("Hsa/jun2011"), :persist => true, :namespace => "Hsa/jun2011", :grep => '^LRG_', :invert_grep => true)
end
set :finder, finder
Log.debug("Finder started with: #{finder.instances.length} instances")


