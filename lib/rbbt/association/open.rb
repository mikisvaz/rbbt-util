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

    options = Misc.add_defaults options, :zipped => true, :monitor => {:desc => "Opening database #{Misc.fingerprint file}"}
    persist_options = Misc.add_defaults persist_options, :persist => true, :dir => Rbbt.var.associations
    persist = persist_options[:persist]

    file = version_file(file, options[:namespace]) if options[:namespace] and String === file

    data = Persist.persist_tsv(file, "Association Database", options, persist_options) do |data|
      file = file.call if Proc === file

      options = options.dup
      tsv = Association.database(file, options.merge(:persist => persist, :unnamed => true))
      tsv = tsv.to_double unless tsv.type == :double

      tsv.annotate data

      data.serializer = :double if data.respond_to? :serializer
      tsv.with_unnamed do
        tsv.with_monitor(options[:monitor]) do
          tsv.through do |k,v|
            data[k] = v
          end
        end
      end

      data
    end
    data.entity_options = options[:entity_options] if options[:entity_options]
    data
  end
  
end
