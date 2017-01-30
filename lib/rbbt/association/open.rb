require 'rbbt/association/database'

module Association
  def self.version_file(file, namespace)
    old_file, file = file, file.sub('NAMESPACE', namespace) if namespace and String === file
    old_file.annotate file if Path === old_file
    file
  end
  
  def self.open(file, options = nil, persist_options = nil)
    options = options.nil? ? {} : options.dup
    persist_options = persist_options.nil? ?  Misc.pull_keys(options, :persist)  : persist_options.dup 

    options = Misc.add_defaults options, :zipped => true, :merge => true, :monitor => {:desc => "Opening database #{Misc.fingerprint file}"}
    options[:zipped] = false unless options[:merge]
    persist_options = Misc.add_defaults persist_options, :persist => true, :dir => Rbbt.var.associations
    persist = persist_options[:persist]

    file = version_file(file, options[:namespace]) if options[:namespace] and String === file

    data = Persist.persist_tsv(file, "Association Database", options, persist_options) do |data|
      file = file.call if Proc === file

      options = options.dup
      data.serializer = :double if data.respond_to? :serializer

      tsv = Association.database(file, options.merge(:persist => true, :unnamed => true, :data => data, :type => :double))

      #tsv.with_unnamed do
      #  tsv.with_monitor("Saving database #{Misc.fingerprint file}") do
      #    tsv.through do |k,v|
      #      data[k] = v
      #    end
      #  end
      #end

      data
    end
    data.entity_options = options[:entity_options] if options[:entity_options]
    data
  end
  
end
