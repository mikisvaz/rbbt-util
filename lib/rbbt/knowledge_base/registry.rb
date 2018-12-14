require 'rbbt/association'
require 'rbbt/association/item'

class KnowledgeBase

  def register(name, file = nil, options = {}, &block)
    if block_given?
      block.define_singleton_method(:filename) do name.to_s end
      Log.debug("Registering #{ name } from code block")
      @registry[name] = [block, options]
    else
      Log.debug("Registering #{ name }: #{ Misc.fingerprint file } #{Misc.fingerprint options}")
      @registry[name] = [file, options]
    end
  end

  def all_databases
    @registry.keys 
  end

  def fields(name)
    @fields[name] ||= get_index(name).fields
  end

  def description(name)
    @descriptions[name] ||= get_index(name).key_field.split("~")
  end

  def source(name)
    description(name)[0]
  end

  def target(name)
    description(name)[1]
  end

  def undirected(name)
    description(name)[2]
  end

  def get_index(name, options = {})
    name = name.to_s
    options[:organism] ||= options[:namespace] ||= self.namespace unless self.namespace.nil?
    @indices[[name, options]] ||= 
      begin 
        if options.empty?
          key = name.to_s
        elsif options[:key]
          key = options[:key]
          key = name if key == :name
        else
          fp = Misc.hash2md5(options)
          key = name.to_s + "_" + fp
        end

        Persist.memory("Index:" << [key, dir] * "@") do
          options = options.dup

          persist_dir = dir
          persist_file = persist_dir[key].find
          file, registered_options = registry[name]

          options = Misc.add_defaults options, registered_options if registered_options and registered_options.any?
          options = Misc.add_defaults options, :persist_file => persist_file, :persist_dir => persist_dir, :format => format, :persist => true

          if entity_options
            options[:entity_options] ||= {}
            entity_options.each do |type, info|
              options[:entity_options][type] ||= {}
              options[:entity_options][type] = Misc.add_defaults options[:entity_options][type], info
            end
          end

          persist_options = Misc.pull_keys options, :persist

          index = if persist_file.exists? and persist_options[:persist] and not persist_options[:update]
                    Log.low "Re-opening index #{ name } from #{ Misc.fingerprint persist_file }. #{options}"
                    Association.index(file, options, persist_options.dup)
                  else
                    options = Misc.add_defaults options, registered_options if registered_options
                    raise "Repo #{ name } not found and not registered" if file.nil?
                    Log.medium "Opening index #{ name } from #{ Misc.fingerprint file }. #{options}"
                    Association.index(file, options, persist_options.dup)
                  end

          index.namespace = self.namespace unless self.namespace

          index
        end
      end
  end

  def get_database(name, options = {})
    name = name.to_s

    options = options.dup
    if self.namespace == options[:namespace]
      options.delete(:namespace) 
    end
    if self.namespace == options[:organism]
      options.delete(:organism) 
    end
    @databases[[name, options]] ||= 
      begin 
        fp = Misc.fingerprint([name,options])

        if options.empty?
          key = name.to_s
        else
          fp = Misc.hash2md5(options)
          key = name.to_s + "_" + fp
        end

        options[:organism] ||= options[:namespace] ||= self.namespace unless self.namespace.nil?

        key += '.database'
        Persist.memory("Database:" << [key, dir] * "@") do
          options = options.dup

          persist_dir = dir
          persist_file = persist_dir[key].find
          file, registered_options = registry[name]

          options = Misc.add_defaults options, registered_options if registered_options and registered_options.any?
          options = Misc.add_defaults options, :persist_file => persist_file, :format => format, :persist => true

          if entity_options
            options[:entity_options] ||= {}
            entity_options.each do |type, info|
              options[:entity_options][type] ||= {}
              options[:entity_options][type] = Misc.add_defaults options[:entity_options][type], info
            end
          end

          persist_options = Misc.pull_keys options, :persist

          database = if persist_file.exists? and persist_options[:persist] and not persist_options[:update]
                       Log.low "Re-opening database #{ name } from #{ Misc.fingerprint persist_file }. #{options}"
                       Association.open(file, options, persist_options)
                     else
                       options = Misc.add_defaults options, registered_options if registered_options
                       raise "Repo #{ name } not found and not registered" if file.nil?
                       Log.medium "Opening database #{ name } from #{ Misc.fingerprint file }. #{options}"
                       Association.open(file, options, persist_options)
                     end

          database.namespace = self.namespace if self.namespace

          database
        end
      end
  end

  def index_fields(name)
    get_index(name).fields
  end

  def produce(name, *rest,&block)
    register(name, *rest, &block)
    get_index(name)
  end

  def info(name)

    source = self.source(name)
    target = self.target(name)
    source_type = self.source_type(name)
    target_type = self.target_type(name)
    fields = self.fields(name)
    source_entity_options = self.entity_options_for source_type, name
    target_entity_options = self.entity_options_for target_type, name
    undirected = self.undirected(name) == 'undirected'

    info = {
      :source => source,
      :target => target,
      :source_type => source_type,
      :target_type => target_type,
      :source_entity_options => source_entity_options,
      :target_entity_options => target_entity_options,
      :fields => fields,
      :undirected => undirected,
    }

    info
  end

end
