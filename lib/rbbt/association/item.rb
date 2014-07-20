require 'rbbt/entity'

module AssociationItem
  extend Entity

  annotation :knowledge_base
  annotation :database
  annotation :reverse

  property :namespace => :both do
    knowledge_base.namespace
  end

  property :part => :array2single do
    self.clean_annotations.collect{|p| p.partition("~") }
  end

  property :target => :array2single do
    self.part.collect{|p| p[2]}
  end

  property :source => :array2single do
    self.clean_annotations.collect{|p| p[/[^~]+/] }
  end

  property :target_type => :both do
    type = reverse ? knowledge_base.source(database) : knowledge_base.target(database)
  end

  property :source_type => :both do
    reverse ? knowledge_base.target(database) : knowledge_base.source(database)
  end

  property :target_entity => :array2single do
    type = reverse ? knowledge_base.source(database) : knowledge_base.target(database)
    knowledge_base.annotate self.target, type, database #if self.target.any?
  end

  property :source_entity => :array2single do
    type = reverse ? knowledge_base.target(database) : knowledge_base.source(database)
    knowledge_base.annotate self.source, type #if self.source.any?
  end

  property :value => :array2single do
    value = (reverse ? knowledge_base.get_index(database).reverse : knowledge_base.get_index(database)).chunked_values_at self
    value.collect{|v| NamedArray.setup(v, knowledge_base.get_index(database).fields)}
  end

  property :info_fields => :both do
    knowledge_base.index_fields(database)
  end

  property :info => :array2single do
    fields = self.info_fields

    return [{}] * self.length if fields.nil? or fields.empty?

    value = self.value
    value.collect{|v|
      Hash[*fields.zip(v).flatten]
    }
  end

  property :tsv => :array do
    info_fields = self.info_fields
    fields = [self.source_type, self.target_type].concat info_fields
    type = [self.source_type, self.target_type] * "~"
    tsv = TSV.setup({}, :key_field => type, :fields => fields, :type => :list, :namespace => self.namespace)
    self.each do |match|
      tsv[match] = [match.source, match.target].concat match.info.values_at(*info_fields)
    end
    tsv
  end

  def self.incidence(pairs, key_field = nil)
    matrix = {}
    targets = []
    sources = []
    matches = {}

    pairs.each do |p|
      s, sep, t = p.partition "~"
      sources << s
      targets << t
      matches[s] ||= Hash.new{false}
      matches[s][t] = true
    end

    sources.uniq!
    targets = targets.uniq.sort

    matches.each do |s,hash|
      matrix[s] = hash.values_at(*targets)
    end

    defined?(TSV)? TSV.setup(matrix, :key_field => (key_field || "Source") , :fields => targets, :type => :list) : matrix
  end
end
