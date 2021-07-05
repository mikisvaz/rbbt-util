require 'rbbt/persist'
require 'rbbt/persist/tsv'

require 'rbbt/util/log'
require 'rbbt/util/misc'
require 'rbbt/util/named_array'

require 'rbbt/tsv/util'
require 'rbbt/tsv/serializers'
require 'rbbt/tsv/parser'
require 'rbbt/tsv/accessor'
require 'rbbt/tsv/manipulate'
require 'rbbt/tsv/index'
require 'rbbt/tsv/attach'
require 'rbbt/tsv/filter'
require 'rbbt/tsv/field_index'
require 'rbbt/tsv/parallel'
require 'rbbt/tsv/stream'
require 'rbbt/tsv/melt'
require 'rbbt/tsv/matrix'

module TSV
  class << self
    attr_accessor :lock_dir, :unnamed
    
    def lock_dir
      @lock_dir ||= Rbbt.tmp.tsv_open_locks.find
    end
  end

  def self.str2options(str)
    field_options,_sep, rest =  str.partition("#")
    key, fields_str = field_options.split("~")

    fields = fields_str.nil? ? [] : fields_str.split(/,\s*/)

    rest = ":type=" << rest if rest =~ /^:?\w+$/
    rest_options = rest.nil? ? {} : Misc.string2hash(rest)

    {:key_field => key, :fields => fields}.merge(rest_options)
  end

  def self.setup(hash, type = nil, options = nil)
    type, options = nil, type if options.nil? and (Hash === type or (String === type and type.include? "~"))
    options = TSV.str2options(options) if String === options and options.include? "~"
    options ||= {}
    options[:type] ||= type unless type.nil?

    options = Misc.add_defaults options, :default_value => [], :unnamed => TSV.unnamed
    default_value = Misc.process_options options, :default_value
    hash = Misc.array2hash(hash, default_value) if Array === hash
    hash.extend TSV

    IndiferentHash.setup(options)
    ENTRIES.each do |entry|
      hash.send("#{ entry }=", options[entry.to_s]) if options.include? entry.to_s
      hash.send("#{ entry }=", options[entry.to_sym]) if options.include? entry.to_sym
    end

    hash.unnamed = options[:unnamed]

    hash
  end

  # options shift if type.nil?
  def self.open(source, type = nil, options = nil)
    type, options = nil, type if options.nil? and (Hash === type or (String === type and type.include? "~"))
    options = TSV.str2options(options) if String === options and options.include? "~"
    options ||= {}
    options[:type] ||= type unless type.nil?

    persist_options = Misc.pull_keys options, :persist

    raise "TSV source is nil" if source.nil?

    filename = get_filename source
    serializer = Misc.process_options options, :serializer
    unnamed = Misc.process_options options, :unnamed
    entity_options = Misc.process_options options, :entity_options


    Log.debug "TSV open: #{ filename } - #{Misc.fingerprint options }.#{unnamed ? " [unnamed]" : "[not unnamed]"}"

    data = nil

    lock_filename = filename.nil? ? nil : Persist.persistence_path(filename + '.open', {:dir => TSV.lock_dir})
    Misc.lock lock_filename  do
      data = Persist.persist_tsv source, filename, options, persist_options do |data|
        if serializer
          data.extend TSV unless TSV === data
          data.serializer = serializer
        end

        tsv_grep = Misc.process_options options, :tsv_grep
        tsv_grep ||= Misc.process_options options, :grep
        open_options = Misc.pull_keys options, :open

        stream = get_stream source, options.merge(open_options)
        parse stream, data, options.merge(:tsv_grep => tsv_grep)

        if ! open_options[:noclose]
          stream.close unless stream.closed?
          stream.join if stream.respond_to?(:join)
        end

        data.filename = filename.to_s unless filename.nil?

        if data.identifiers.nil? and Path === filename and filename.identifier_file_path
          data.identifiers = filename.identifier_file_path.find if filename.identifier_file_path.exists?
        end

        data
      end
    end


    data.unnamed = unnamed unless unnamed.nil?

    data.entity_options = entity_options

    if Path === source && data.identifiers
      Path.setup(data.identifiers, source.pkgdir, source.resource)
    end

    if data.respond_to? :persistence_path
      data.read
      data
    else
      h = data.dup
      data.clear
      data.annotate h
    end
  end

  def self.parse_header(stream, options = {})
    case
    when Path === stream 
      stream.open do |f|
        f.no_fail = false if ConcurrentStream === f
        Parser.new f, options
      end
    when (String === stream and stream.length < 300 and (Open.exists? stream or Open.remote? stream))
      Open.open(stream) do |f|
        f.no_fail = false if ConcurrentStream === f
        Parser.new f, options
      end
    else
      filename = stream.respond_to?(:filename) ? stream.filename : Misc.fingerprint(stream)
      Log.debug("Parsing header of open stream: #{filename}")
      Parser.new stream, options
    end
  end

  def self.parse(stream, data, options = {})

    parser = Misc.process_options options, :parser
    parser = TSV::Parser.new stream, options if parser.nil?

    # dump with tchmgr
    if defined? TokyoCabinet and TokyoCabinet::HDB === data and parser.straight and
      data.close
      begin
        bin = 'tchmgr'
        CMD.cmd("#{bin} version", :log => false)
        FileUtils.mkdir_p File.dirname(data.persistence_path)
        CMD.cmd("#{bin} importtsv '#{data.persistence_path}'", :in => stream, :log => false, :dont_close_in => true)
      rescue
        Log.debug("tchmgr importtsv failed for: #{data.persistence_path}")
      end
      data.write
    end

    # make TSV
    data.extend TSV unless TSV === data
    data.unnamed = true

    # choose serializer
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
                        else
                          data.serializer = :marshal
                        end
    end

    parser.traverse(options) do |key,values|
      parser.add_to_data data, key, values
    end

    # setup the TSV
    parser.setup data

    data.unnamed = false

    data
  end
end
