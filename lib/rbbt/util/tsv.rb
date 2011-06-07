require 'rbbt/util/resource'
require 'rbbt/util/misc'
require 'rbbt/util/open'
require 'rbbt/util/tc_hash'
require 'rbbt/util/tmpfile'
require 'rbbt/util/log'
require 'rbbt/util/persistence'
require 'digest'
require 'fileutils'

require 'rbbt/util/tsv/parse'
require 'rbbt/util/tsv/accessor'
require 'rbbt/util/tsv/manipulate'
require 'rbbt/util/tsv/index'
require 'rbbt/util/tsv/attach'
require 'rbbt/util/tsv/resource'

class TSV

  ESCAPES = {
    "\n" => "[[NL]]",
    "\t" => "[[TAB]]",
  }

  def self.escape(text)
    ESCAPES.each do |char,replacement|
      text = text.gsub(char, replacement)
    end
    text
  end

  def self.unescape(text)
    ESCAPES.each do |char,replacement|
      text = text.gsub(replacement, char)
    end
    text
  end

  def self.headers(file, options = {})

    ## Remove options from filename
    if String === file and file =~/(.*?)#(.*)/ and File.exists? $1
      options = Misc.add_defaults options, Misc.string2hash($2) 
      file = $1
    end

    fields = case
             when Open.can_open?(file)
               Open.open(file, :grep => options[:grep]) do |f| TSV.parse_header(f, options[:sep], options[:header_hash]).values_at(0, 1).flatten end
             when File === file
               file = Open.grep(file, options[:grep]) if options[:grep]
               TSV.parse_header(file, options[:sep], options[:header_hash]).values_at(0, 1).flatten
             else 
               raise "File #{file.inspect} not found"
             end

    if fields.compact.empty?
      nil
    else
      fields
    end
  end

  def initialize(file = {}, type = nil, options = {})
    # Process Options
    
    if Hash === type
      options = type 
      type    = nil
    end

    ## Remove options from filename
    if String === file and file =~/(.*?)#(.*)/ and File.exists? $1
      options = Misc.add_defaults options, Misc.string2hash($2) 
      file = $1
    end

    options = Misc.add_defaults options, :persistence => false, :type => type, :in_situ_persistence => true

    # Extract Filename

    file, extra  = file if Array === file and file.length == 2 and Hash === file.last

    @filename = Misc.process_options options, :filename
    @filename ||= case
                  when Resource::Path === file
                    file
                  when (String === file and File.exists? file)
                    File.expand_path file
                  when String === file
                    file
                  when File === file
                    File.expand_path file.path
                  when TSV === file 
                    File.expand_path file.filename
                  when (Persistence::TSV === file and file.filename)
                    File.expand_path file.filename
                  else
                    file.class.to_s
                  end

    # Process With Persistence
    #     Use filename to identify the persistence
    #     Several inputs supported
    #         Filename or File: Parsed
    #         Hash: Encapsulated, empty info
    #         TSV: Duplicate
    case
    when block_given?
      @data, extra = Persistence.persist(file, :TSV, :tsv_extra, options.merge(:force_array => true)) do |file, options, filename| yield file, options, filename end
      extra.each do |key, values|
        self.send("#{ key }=".to_sym, values) if self.respond_to? "#{ key }=".to_sym 
      end if not extra.nil?
 
    else

      case
      when Array === file
        @data = Hash[file.collect{|v| 
          [v,[]]
        }]
        @data.key_field = key_field if key_field
        @data.fields = fields if fields
      when Hash === file 
        @data = file
        @data.key_field = key_field if key_field
        @data.fields = fields if fields
      when TSV === file
        @data = file.data
        @data.key_field = key_field if key_field
        @data.fields = fields if fields
      when Persistence::TSV === file
        @data = file
        %w(case_insensitive namespace identifiers datadir fields key_field type filename cast).each do |key|
          if @data.respond_to?(key.to_sym)  and self.respond_to?("#{key}=".to_sym)
            self.send "#{key}=".to_sym, @data.send(key.to_sym) 
          end
        end
      else
        in_situ_persistence = Misc.process_options(options, :in_situ_persistence)
        @data, extra = Persistence.persist(file, :TSV, :tsv_extra, options) do |file, options, filename, persistence_file|
          data, extra = nil

          if in_situ_persistence and persistence_file

            options.merge! :persistence_data => Persistence::TSV.get(persistence_file, true, :double)
          end

          begin
            case
              ## Parse source
            when Resource::Path === file #(String === file and file.respond_to? :open)
              data, extra = TSV.parse(file.open(:grep => options[:grep]) , options)
              extra[:namespace] ||= file.namespace
              extra[:datadir]   ||= file.datadir
            when StringIO === file
              data, extra = TSV.parse(file, options)
            when Open.can_open?(file)
              Open.open(file, :grep => options[:grep]) do |f|
                data, extra = TSV.parse(f, options)
              end
            when File === file
              path = file.path
              file = Open.grep(file, options[:grep]) if options[:grep]
              data, extra = TSV.parse(file, options)
            when IO === file
              file = Open.grep(file, options[:grep]) if options[:grep]
              data, extra = TSV.parse(file, options)
            when block_given?
              data 
            else
              raise "Unknown input in TSV.new #{file.inspect}"
            end

            extra[:filename] = filename
          rescue Exception
            FileUtils.rm persistence_file if persistence_file and File.exists?(persistence_file)
            raise $!
          end

          if Persistence::TSV === data
            %w(case_insensitive namespace identifiers datadir fields key_field type filename cast).each do |key| 
              if extra.include? key.to_sym
                if data.respond_to? "#{key}=".to_sym
                  data.send("#{key}=".to_sym, extra[key.to_sym])
                end
              end
            end 
            data.read
          end
 
          [data, extra]
        end
      end
    end

    if not extra.nil? 
      %w(case_insensitive namespace identifiers datadir fields key_field type filename cast).each do |key| 
        if extra.include? key.to_sym
          self.send("#{key}=".to_sym, extra[key.to_sym])
          #if @data.respond_to? "#{key}=".to_sym
          #  @data.send("#{key}=".to_sym, extra[key.to_sym])
          #end
        end
      end 
    end
  end

  def write
    @data.write if @data.respond_to? :write
  end

  def read
    @data.read if @data.respond_to? :read
  end

end
