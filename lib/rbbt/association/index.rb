require 'rbbt/tsv'
require 'rbbt/association/open'

module Association
  def self.index(file, options = nil, persist_options = nil)
    options = options.nil? ? {} : options.dup
    persist_options = persist_options.nil? ?  Misc.pull_keys(options, :persist)  : persist_options.dup 

    persist_options = Misc.add_defaults persist_options.dup, :persist => true, :engine => "BDB"
    persist = persist_options[:persist]

    file = version_file(file, options[:namespace]) if options[:namespace] and String === file

    undirected = options[:undirected]
    Persist.persist_tsv(file, "Association Index", options, persist_options) do |data|
      recycle = options[:recycle]

      database = open(file, options, persist_options.dup)

      fields = database.fields
      source_field = database.key_field
      target_field = fields.first.split(":").last
      key_field = [source_field, target_field, undirected ? "undirected" : nil].compact * "~"

      TSV.setup(data, :key_field => key_field, :fields => fields[1..-1], :type => :list, :serializer => :list)

      data.key_field = key_field
      data.fields = fields[1..-1]
      data.type = :list
      data.serializer = :list 

      database.with_unnamed do
        database.through do |source, values|
          next if values.empty?
          next if source.nil? or source.empty?
          next if values.empty?

          targets, *rest = values

          size = targets ? targets.length : 0

          rest.each_with_index do |list,i|
            list.replace [list.first] * size if list.length == 1
          end if recycle and size > 1

          rest = Misc.zip_fields rest

          annotations = rest.length > 1 ?
            targets.zip(rest) :
            targets.zip(rest * targets.length) 

          annotations.each do |target, info|
            next if target.nil? or target.empty?
            key = [source, target] * "~"
            if data[key].nil? or info.nil?
              data[key] = info
            else
              old_info = data[key]
              info = old_info.zip(info).collect{|p| p * ";;" }
              data[key] = info
            end
          end
        end
      end

      data.close
      data
    end.tap do |data|
      data.read if not Hash === data and data.respond_to? :read
      Association::Index.setup data
      data
    end
  end
  module Index

    attr_accessor :source_field, :target_field, :undirected
    def parse_key_field
      @source_field, @target_field, @undirected = key_field.split("~")
    end

    def self.setup(repo)
      repo.extend Association::Index
      repo.parse_key_field
      repo.unnamed = true
      repo
    end

    def reverse
      @reverse ||= begin
                     if self.respond_to? :persistence_path
                       persistence_path = self.persistence_path
                       persistence_path = persistence_path.find if Path === persistence_path
                       reverse_filename = persistence_path + '.reverse'
                     else
                       raise "Can only reverse a TokyoCabinet::BDB dataset at the time"
                     end

                     self.read if self.respond_to? :read

                     if File.exists?(reverse_filename)
                       new = Persist.open_tokyocabinet(reverse_filename, false, serializer, TokyoCabinet::BDB)
                       new
                     else
                       FileUtils.mkdir_p File.dirname(reverse_filename) unless File.exists?(File.basename(reverse_filename))
                       new = Persist.open_tokyocabinet(reverse_filename, true, serializer, TokyoCabinet::BDB)
                       new.write
                       through do |key, value|
                         new_key = key.split("~").reverse.join("~")
                         new[new_key] = value
                       end
                       annotate(new)
                       new.key_field = key_field.split("~").values_at(1,0,2).compact * "~"
                       new.read
                     end

                     new.unnamed = true

                     Association::Index.setup new

                     new.undirected = undirected

                     new
                   end
    end

    def match(entity)
      return entity.inject([]){|acc,e| acc.concat match(e); acc } if Array === entity
      return [] if entity.nil?
      prefix(entity + "~")
    end

    def matches(entities)
      entities.inject(nil) do |acc,e| 
        m = match(e); 
        if acc.nil? or acc.empty?
          acc = m
        else
          acc.concat m
        end
        acc
      end
    end

    #{{{ Subset

    def subset(source, target)
      return [] if source.nil? or target.nil? or source.empty? or target.empty?

      if source == :all or source == "all"
        if target == :all or target == "all"
          return keys
        else
          matches = reverse.subset(target, source)
          return matches.collect{|m| r = m.partition "~"; r.reverse*"" }
        end
      end

      matches = source.uniq.inject([]){|acc,e| 
        if block_given?
          acc.concat(match(e))
        else
          acc.concat(match(e))
        end
      }

      return matches if target == :all or target == "all"

      target_matches = {}

      matches.each{|code| 
        s,sep,t = code.partition "~"
        next if undirected and t > s 
        target_matches[t] ||= []
        target_matches[t] << code
      }

      target_matches.values_at(*target.uniq).flatten.compact
    end

  end
end
