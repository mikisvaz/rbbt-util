require 'rbbt/entity'

module AssociationItem
  extend Object::Entity

  annotation :knowledge_base
  annotation :database
  annotation :reverse

  property :name => :single do
    [source_entity, target_entity].collect{|e| e.respond_to?(:name)? e.name || e : e } * "~"
  end 

  property :full_name => :single do
    database ? [database, name] * ":" : name
  end 

  property :invert => :both do
    if Array === self
      inverted = self.collect do |item|
        s,_sep,t= item.partition "~"
        new = [t,s] * _sep
      end
      self.annotate inverted
      inverted.reverse = ! reverse
      inverted
    else
      s,_sep,t= self.partition "~"
      inverted = self.annotate([t,s] * _sep)
      inverted.reverse = ! reverse
      inverted
    end
  end

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

  property :target_entity_type => :both do
    Entity.formats[target_type].to_s
  end

  property :source_entity_type => :both do
    Entity.formats[source_type].to_s
  end

  property :target_type => :both do
    knowledge_base.target(database)
  end

  property :source_type => :both do
    knowledge_base.source(database)
  end

  property :undirected => :both do
    knowledge_base.undirected(database)
  end

  property :target_entity => :array2single do
    type = knowledge_base.target(database)
    knowledge_base.annotate self.target, type, database #if self.target.any?
  end

  property :source_entity => :array2single do
    type = knowledge_base.source(database)
    knowledge_base.annotate self.source, type, database #if self.source.any?
  end

  property :index => :both do |database|
    @index ||= knowledge_base.get_index(database)
  end
  property :value => :array2single do
    index = index(database)
    value = index.chunked_values_at self
    value.collect{|v| NamedArray.setup(v, index.fields)}
  end

  property :info_fields => :both do
    knowledge_base.index_fields(database)
  end

  property :info => :array2single do
    fields = self.info_fields

    return [{}] * self.length if fields.nil? or fields.empty?

    value = self.value
    value.collect{|v|
      raise "No info for pair; not registered in index" if v.nil?
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
    tsv.entity_options = {:organism => namespace}
    knowledge_base.entity_options.each do |type,options|
      tsv.entity_options.merge! options
    end
    tsv
  end

  property :filter => :array do |*args,&block|
    keys = tsv.select(*args,&block).keys
    keys = self.annotate Annotated.purge(keys)
    keys
  end

  def self.incidence(pairs, key_field = nil, &block)
    matrix = {}
    targets = []
    sources = []
    matches = {}

    pairs.inject([]){|acc,m| acc << m; acc << m.invert if m.respond_to?(:undirected) and m.undirected; acc  }.each do |p|
      s, sep, t = p.partition "~"

      sources << s
      targets << t
      if block_given?
        matches[s] ||= Hash.new{nil}
        value = block.call p
        matches[s][t] = value unless value.nil? or (mv = matches[s][t] and value > mv)
      else
        matches[s] ||= Hash.new{false}
        matches[s][t] ||= true 
      end
    end

    sources.uniq!
    targets = targets.uniq.sort

    matches.each do |s,hash|
      matrix[s] = hash.values_at(*targets)
    end

    defined?(TSV)? TSV.setup(matrix, :key_field => (key_field || "Source") , :fields => targets, :type => :list) : matrix
  end

  def self.adjacency(pairs, key_field = nil, &block)
    incidence = incidence(pairs, key_field, &block)

    targets = incidence.fields
    adjacency = TSV.setup({}, :key_field => incidence.key_field, :fields => ["Target"], :type => :double)
    TSV.traverse incidence, :into => adjacency, :unnamed => true do |k,values|
      target_values = targets.zip(values).reject{|t,v| v.nil? }.collect{|t,v| [t,v]}
      next if target_values.empty?
      [k, Misc.zip_fields(target_values)]
    end
  end

  def self._select_match(orig, elem)
    if Array === orig and Array === elem
      (orig & elem).any?
    elsif Array === orig
      orig.include? elem
    elsif Array === elem
      elem.include? orig
    else
      elem === orif
    end
    false
  end

  def self.select(list, method = nil, &block)
    if method and method.any?
      list.select do |item|
        method.collect do |key,value|
          case key
          when :target
            _select_match item.target, value
          when :source
            _select_match item.source, value
          else
            orig = item.info[key]
            orig = orig.split(";;") if String orig
            _select_match orig, value 
          end
        end
      end
    else
      list.select(&block)
    end
  end
end
