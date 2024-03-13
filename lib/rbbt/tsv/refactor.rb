require_relative 'stream'
require 'scout/tsv'
require 'scout/open'
require_relative '../refactor'
module TSV
  extension_attr :monitor
  class << self

    alias original_open open

    def open(source, type = nil, options = nil)
      type, options = nil, type if options.nil? and (Hash === type or (String === type and type.include? "~"))
      options = TSV.str2options(options) if String === options and options.include? "~"
      options ||= {}
      options[:type] ||= type unless type.nil?
      if zipped = options.delete(:zipped)
        options[:one2one] = zipped
      end
      original_open(source, options)
    end
  end


  alias original_unzip unzip
  def unzip(field = 0, merge = false, sep = ":", delete = true, **kwargs)
    kwargs[:merge] ||= merge
    kwargs[:sep] ||= sep
    kwargs[:delete] ||= delete
    original_unzip(field, **kwargs)
  end

  alias original_reorder reorder
  def reorder(key_field = nil, fields = nil, merge: true, one2one: true, zipped: nil, **kwargs) 
    kwargs[:one2one] = zipped if one2one.nil?
    kwargs.delete :persist
    kwargs.delete :persist_data
    original_reorder(key_field, fields, **kwargs)
  end

  def swap_id(field = 0, merge = false, sep = ":", delete = true, **kwargs)
    kwargs[:merge] ||= merge
    kwargs[:sep] ||= sep
    kwargs[:delete] ||= delete
    change_id(field, **kwargs)
  end

  def swap_id(field, format, options = {}, &block)
    raise "Block support not implemented" if block_given?
    change_id(field, format, **options)
  end

  class << self
    alias original_range_index range_index
    alias original_pos_index pos_index
    def range_index(*args, filters: nil, **kwargs)
      if filters
        raise "Not implemented" if filters.length > 1
        method, value = filters.first
        method.sub!("field:", '')
        kwargs[:select] = {method => value}
      end
      original_range_index(*args, **kwargs)
    end

    def pos_index(*args, filters: nil, **kwargs)
      if filters
        raise "Not implemented" if filters.length > 1
        method, value = filters.first
        method.sub!("field:", '')
        kwargs[:select] = {method => value}
      end
      original_pos_index(*args, **kwargs)
    end

    #alias original_setup setup

    #def setup(*args, **kwargs, &block)
    #  if args.length == 2 && String === args.last
    #    str_setup(args.last, args.first)
    #  else
    #    original_setup(*args, **kwargs, &block)
    #  end
    #end
  end

  def self.header_lines(key_field, fields, entry_hash = nil)
    entry_hash = entry_hash || {}
    entry_hash = entry_hash.merge(:key_field => key_field, :fields => fields)
    TSV::Dumper.header entry_hash
  end
end

Rbbt.relay_module_method TSV, :get_stream, Open, :get_stream
Rbbt.relay_module_method TSV::Parser, :traverse, TSV, :parse
Rbbt.relay_module_method TSV, :zip_fields, NamedArray, :zip_fields

module TSV
  alias original_dumper_stream dumper_stream
  def dumper_stream(keys = nil, no_options = false, unmerge = false)
    original_dumper_stream(:keys => keys, unmerge: unmerge, preamble: no_options)
  end

  alias original_to_s to_s
  def to_s(keys = nil, no_options = false, unmerge = false)
    if FalseClass === keys or TrueClass === keys or Hash === keys
      no_options = keys
      keys = nil
    end

    if keys == :sort
      with_unnamed do
        keys = self.keys.sort
      end
    end


    options = {:keys => keys, unmerge: unmerge}
    case no_options
    when TrueClass, FalseClass
      options[:preamble] = !no_options
    when Hash
      options.merge!(no_options)
      
    end
    original_dumper_stream(options).read
  end
  alias tsv_sort sort

  def attach_same_key(tsv, fields = nil)
    fields = [fields] unless fields.nil? || Array === fields
    if fields
      self.attach tsv, :fields => fields
    else
      self.attach tsv
    end
  end

  def attach_index(tsv, index = nil)
    self.attach tsv, index: index
  end

  def self.merge_row_fields(input, output, options = {})
    Open.write(output, Open.collapse_stream(input, **options))
  end

  def self.merge_different_fields(stream1, stream2, output, options = {})
    Open.write(output, TSV.paste_streams([stream1, stream2], **options))
  end

  def merge_different_fields(other, options = {})
    TmpFile.with_file do |output|
      TSV.merge_different_fields(self, other, output, options)
      options.delete :sort
      tsv = TSV.open output, options
      tsv.key_field = self.key_field unless self.key_field.nil?
      tsv.fields = self.fields + other.fields unless self.fields.nil? or other.fields.nil?
      tsv
    end
  end

  def attach_source_key(other, key)
    attach other, other_key: key
  end

  def with_monitor(use_monitor = true)
    monitor_state = monitor
    monitor = use_monitor
    begin
      yield
    ensure
      monitor = monitor_state
    end
  end
end

