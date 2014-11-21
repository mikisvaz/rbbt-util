#{{{ FINDER
finder = Finder.new
if ENV['RBBT_FINDER']
  organism = Organism.default_code("Hsa")
  finder.add_instance(KEGG.pathways, :grep => '^hsa\|^#', :fields => ["Pathway Name"], :namespace => organism, :fix => Proc.new{|l| l.sub(/ - Homo sapiens.*/,'')}) if defined? KEGG
  finder.add_instance(Organism.lexicon(organism), :persist => true, :namespace => organism, :grep => Organism.blacklist_genes(organism).list, :invert_grep => true) if defined? Organism
end
set :finder, finder
Log.debug("Finder started with: #{finder.instances.length} instances")


