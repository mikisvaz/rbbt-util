module Entity

  def self.identifier_files(field)
    entity_type = Entity.formats[field]
    return [] unless entity_type and entity_type.include? Entity::Identified 
    entity_type.identifier_files
  end

  module Identified

    def self.included(base)
      base.annotation :format
      base.annotation :organism

      base.module_eval do
        def identity_type
          self.annotation_types.select{|m| m.include? Entity::Identified }.last
        end

        def identifier_files 
          files = identity_type.identifier_files.dup
          files.collect!{|f| f.annotate f.gsub(/\bNAMESPACE\b/, organism) } if annotations.include? :organism and self.organism
          files.reject!{|f| f =~ /\bNAMESPACE\b/ } 
          files
        end

        def identifier_index(format = nil, source = nil)
          Persist.memory("Entity index #{identity_type}: #{format} (from #{source || "All"})", :format => format, :source => source) do
            source ||= self.respond_to?(:format)? self.format : nil

            index = TSV.translation_index(identifier_files, format, source, :persist => true)
            raise "No index from #{ source } to #{ format }: #{Misc.fingerprint identifier_files}" if index.nil?
            index.unnamed = true
            index
          end
        end
      end

      base.property :to => :both do |target_format|

        target_format = case target_format
                        when :name
                          identity_type.name_format 
                        when :default
                          identity_type.default_format 
                        when :ensembl
                          identity_type.formats.select{|f| f =~ /Ensembl/}.first
                        else
                          target_format
                        end

        return self if target_format == format
        if Array === self
          self.annotate(identifier_index(target_format, self.format).values_at(*self))
        else
          self.annotate(identifier_index(target_format, self.format)[self])
        end.tap{|o| o.format = target_format unless o.nil? }
      end

      base.property :name => :both do
        to(:name)
      end

      base.property :default => :both do
        to(:name)
      end

      base.property :ensembl => :both do
        to(:ensembl)
      end

    end

  end

  attr_accessor :identifier_files, :formats, :default_format, :name_format, :description_format

  def add_identifiers(file, default = nil, name = nil, description = nil)
    if TSV === file
      all_fields = file.all_fields
    else
      if file =~ /NAMESPACE/
        all_fields = file.sub(/NAMESPACE/,'**').glob.collect do |f|
        TSV.parse_header(f).all_fields
        end.flatten.compact.uniq
      else
        all_fields = TSV.parse_header(file).all_fields
      end
    end

    self.format = all_fields
    @formats ||= []
    @formats.concat all_fields
    @formats.uniq!

    @default_format = default if default
    @name_format = name if name
    @description_format = description if description

    @identifier_files ||= []
    @identifier_files << file
    @identifier_files.uniq!


    self.include Entity::Identified unless Entity::Identified === self
  end

end
