#{{{ FINDER
finder = Finder.new
if ENV['RBBT_FINDER']
  organism = Organism.default_code("Hsa")
  finder.add_instance(Organism.lexicon(organism), :persist => true, :namespace => organism, :grep => Organism.blacklist_genes(organism).list, :invert_grep => true) if defined? Organism

  mutation_hash = {"Genomic Mutation" => /\w+:\d+:[ACTG\-\+]+/}
  finder.add_instance(mutation_hash, :namespace => organism) 

  mi_hash = {"Mutated Isoform" => /ENSP\w+:.+/}
  finder.add_instance(mi_hash, :namespace => organism) 

  prot_hash = {"Ensembl Protein ID" => /ENSP\w+$/}
  finder.add_instance(prot_hash, :namespace => organism) 

  gene_hash = {"Ensembl Gene ID" => /ENSG\w+$/}
  finder.add_instance(gene_hash, :namespace => organism) 

  organism_hash = {"organism" => /[A-Z][a-z]{2}(?:\/[a-z]{3}20\d\d)?/}
  finder.add_instance(organism_hash, :namespace => organism) 

  snp_hash = {"SNP" => /^rs\d+$/}
  finder.add_instance(snp_hash, :namespace => organism) 
end
set :finder, finder
Log.debug("Finder started with: #{finder.instances.length} instances")


