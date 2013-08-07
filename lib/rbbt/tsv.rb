require 'yaml'

require 'rbbt/util/misc'
require 'rbbt/util/named_array'
require 'rbbt/util/log'

require 'rbbt/persist'

require 'rbbt/tsv/util'
require 'rbbt/tsv/serializers'
require 'rbbt/tsv/parser'
require 'rbbt/tsv/accessor'
require 'rbbt/tsv/manipulate'
require 'rbbt/tsv/index'
require 'rbbt/tsv/attach'
require 'rbbt/tsv/filter'

module TSV
  class << self
    attr_accessor :lock_dir
    
    def lock_dir
      @lock_dir ||= Rbbt.tmp.tsv_open_locks.find
    end
  end

  def self.setup(hash, options = {})
    options = Misc.add_defaults options, :default_value => []
    default_value = Misc.process_options options, :default_value
    hash = Misc.array2hash(hash, default_value) if Array === hash
    hash.extend TSV

    IndiferentHash.setup(options)
    ENTRIES.each do |entry|
      hash.send("#{ entry }=", options[entry]) if options.include? entry
      hash.send("#{ entry }=", options[entry.to_sym]) if options.include? entry.to_sym
    end

    hash
  end

  # options shift if type.nil?
  def self.open(source, type = nil, options = nil)
    type, options = nil, type if options.nil? and Hash === type
    options ||= {}
    options[:type] ||= type unless type.nil?

    persist_options = Misc.pull_keys options, :persist

    filename = get_filename source
    serializer = Misc.process_options options, :serializer
    unnamed = Misc.process_options options, :unnamed
    entity_options = Misc.process_options options, :entity_options

    Log.debug "TSV open: #{ filename } - #{options.inspect}.#{unnamed ? " [unnamed]" : "[not unnamed]"}"

    data = nil

    lock_filename = filename.nil? ? nil : Persist.persistence_path(filename, {:dir => TSV.lock_dir})
    Misc.lock lock_filename  do
      data = Persist.persist_tsv source, filename, options, persist_options do |data|
        if serializer
          data.extend TSV unless TSV === data
          data.serializer = serializer
        end

        open_options = Misc.pull_keys options, :open

        stream = get_stream source, open_options
        parse stream, data, options

        data.filename = filename.to_s unless filename.nil?
        if data.identifiers.nil? and Path === filename and filename.identifier_file_path
          data.identifiers = filename.identifier_file_path.to_s
        end

        data
      end
    end

    data.unnamed = unnamed unless unnamed.nil?

    data.entity_options = entity_options

    if Path === source and data.identifiers
      data.identifiers = Path.setup(data.identifiers, source.pkgdir, source.resource)
    end

    data
  end

  def self.parse_header(stream, options = {})
    case
    when Path === stream 
      stream.open do |f|
        Parser.new f, options
      end
    when (String === stream and stream.length < 300 and Open.exists? stream)
      Open.open(stream) do |f|
        Parser.new f, options
      end
    else
      Parser.new stream, options
    end
  end

  def self.parse(stream, data, options = {})
    monitor, grep, invert_grep = Misc.process_options options, :monitor, :grep, :invert_grep

    parser = Parser.new stream, options

    if grep
      stream.rewind
      stream = Open.grep(stream, grep, invert_grep)
      parser.first_line = stream.gets
    end

    line = parser.rescue_first_line

    if TokyoCabinet::HDB === data and parser.straight
      data.close
      pos = stream.pos if stream.respond_to? :pos
      begin
        CMD.cmd("tchmgr importtsv '#{data.persistence_path}'", :in => stream, :log => false, :dont_close_in => true)
      rescue
        Log.debug("tchmgr importtsv failed for: #{data.persistence_path}")
        Log.debug($!.message)
        if stream.respond_to? :seek 
          stream.seek pos
        else
          #raise "tchmgr import failed and cannot restore stream"
        end
      end
      data.write
    end

    data.extend TSV unless TSV === data
    data.unnamed = true

    if data.serializer == :type
      data.serializer = case
                        when parser.cast.nil?
                          data.serializer = parser.type
                        when (parser.cast == :to_i and (parser.type == :list or parser.type == :flat))
                          data.serializer = :integer_array
                        when (parser.cast == :to_i and parser.type == :single)
                          data.serializer = :integer
                        when (parser.cast == :to_f and parser.type == :single)
                          data.serializer = :float
                        when (parser.cast == :to_f and (parser.type == :list or parser.type == :flat))
                          data.serializer = :float_array
                        end
    end

    if monitor and (stream.respond_to?(:size) or (stream.respond_to?(:stat) and stream.stat.respond_to? :size)) and stream.respond_to?(:pos)
      size = case
             when stream.respond_to?(:size)
               stream.size
             else
               stream.stat.size
             end
      desc = "Parsing Stream"
      step = 100
      if Hash === monitor
        desc = monitor[:desc] if monitor.include? :desc 
        step = monitor[:step] if monitor.include? :step 
      end
      progress_monitor = Progress::Bar.new(size, 0, step, desc)
    else
      progress_monitor = nil
    end

    while not line.nil? 
      begin
        progress_monitor.tick(stream.pos) if progress_monitor 

        raise Parser::SKIP_LINE if line.empty?

        line = Misc.fixutf8(line)
        line = parser.process line
        parts = parser.chop_line line
        key, values = parser.get_values parts
        values = parser.cast_values values if parser.cast?
        parser.add_to_data data, key, values
        line = stream.gets
      rescue Parser::SKIP_LINE
        begin
          line = stream.gets
          next
        rescue IOError
          break
        end
      rescue Parser::END_PARSING
        break
      rescue IOError
        break
      end
    end

    parser.setup data

    data.unnamed = false

    data
  end
end
