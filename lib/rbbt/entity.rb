require 'rbbt/annotations'
require 'rbbt/entity/identifiers'

module Entity 
  
  UNPERSISTED_PREFIX = "entity_unpersisted_property_"

  class MultipleEntity < TryAgain; end

  class DontPersist < Exception
    attr_accessor :payload
    def self.initialize(payload)
      @payload = payload
    end
  end

  class << self
    attr_accessor :formats, :entity_property_cache
  end
  FORMATS = begin
              hash = {}
              class << hash
                alias orig_include? include?

                attr_accessor :find_cache

                def find(value)
                  self.find_cache ||= {}
                  if self.find_cache.include? value
                    self.find_cache[value]
                  else
                    self.find_cache[value] = begin
                                               if orig_include? value
                                                 self.find_cache[value] = value
                                               else
                                                 found = nil
                                                 each do |k,v|
                                                   if value.to_s == k.to_s
                                                     found = k
                                                     break
                                                   elsif value.to_s =~ /\(#{Regexp.quote k}\)/
                                                     found = k
                                                     break
                                                   end
                                                 end
                                                 found
                                               end
                                             end
                  end
                end

                def [](value)
                  res = super
                  return res if res
                  key = find(value)
                  key ? super(key) : nil
                end

                def []=(key,value)
                  self.find_cache = nil
                  super
                end

                def include?(value)
                  find(value) != nil
                end
              end

              hash
            end

  def self.formats
    FORMATS
  end

  dir = (defined?(Rbbt) && defined?(Resource) && Resource === Rbbt ? Rbbt.var.entity_property : 'var/entity_property')
  self.entity_property_cache = dir

  def self.entity_property_cache=(dir)
    @entity_property_cache = dir
  end

  attr_accessor :all_formats, :all_properties
  def self.extended(base)
    base.extend Annotation
    Entity.formats[base.to_s] = base

    base.module_eval do
      attr_accessor :_ary_property_cache

      attr_accessor :template, :list_template, :action_template, :list_action_template, :keep_id

      def self.format=(formats)
        formats = [formats] unless Array === formats
        self.all_formats ||= []
        self.all_formats = self.all_formats.concat(formats).uniq
        formats.each do |format|
          Entity.formats[format] ||= self
        end
      end

      def _ary_property_cache
        @_ary_property_cache ||= {}
      end

      def base_entity
        self.annotation_types.select{|m| Entity === m}.last
      end

      def property(*args, &block)
        class << self; self; end.property(*args,&block)
      end

      def to_yaml(*args)
        self.clean_annotations.dup.to_yaml(*args)
      end


      def encode_with(coder)
        coder.scalar = clean_annotations
      end

      def consolidate
        self.inject(nil){|acc,e| 
          if acc.nil?
            acc = e
          else
            acc.concat e
          end
        }
      end

      def all_properties
        annotation_types.select{|m| Entity === m}.collect{|e| e.all_properties}.flatten.uniq
      end

      def self.property(name, &block)
        Log.debug "Defining property #{name} for #{self}"

        case
        when (Hash === name and name.size == 1)
          name, type = name.collect.first
        when (String === name or Symbol === name)
          type = :single
        else
          raise "Format of name ( => type) not understood: #{name.inspect}"
        end

        name = name.to_s unless String === name

        persisted_name = UNPERSISTED_PREFIX + name
        self.remove_method persisted_name if methods.include? persisted_name

        case type
        when :both
          define_method name, &block 
 
        when :single, :single2array
          single_name = "_single_" << name
          define_method single_name, &block 
          define_method name do |*args, &block|
            if Array === self
              res = self.collect{|e| e.send(single_name, *args)}
              res.first.annotate(res) if Annotated === res.first && type == :single2array
              res
            else
              self.send(single_name, *args, &block)
            end
          end
        when :array, :array2single
          ary_name = "_ary_" << name
          define_method ary_name, &block 

          define_method name do |*args, &block|
            case
            when Array === self
              self.send(ary_name, *args, &block)
            when (Array === self.container and not self.container_index.nil? and self.container.respond_to? ary_name)
              cache_code = Misc.hash2md5({:name => ary_name, :args => args})
              res = (self.container._ary_property_cache[cache_code] ||=  self.container.send(name, *args))
              if Hash === res
                res[self]
              else
                res[self.container_index]
              end
            else
              res = self.make_list.send(ary_name, *args, &block)
              Hash === res ? res[self] : res[0]
            end
          end
        when :multiple
          multi_name = "_multiple_" << name

          define_method multi_name do
            if self.instance_variable_get("@multiple_result_" + name.to_s)
              return self.instance_variable_get("@multiple_result_" + name.to_s)
            end
            raise MultipleEntity, "Entity property #{name} runs with multiple entities"
          end

          define_method name do |*args|

            if self.instance_variable_get("@multiple_result_" + name.to_s)
              return self.instance_variable_get("@multiple_result_" + name.to_s)
            end

            obj = if Array === self
                    self
                  elsif self.respond_to?(:container) && Array === self.container
                    self.container
                  else
                    self.make_list
                  end

            missing = []
            obj.each do |e|
              begin 
                res = e.send(multi_name)
                e.instance_variable_set("@multiple_result_" + name.to_s, res)
              rescue MultipleEntity
                missing << e
              end
            end

            res = missing.any? ? block.call(missing) : nil
            case res
            when Array
              missing.zip(res).each do |o,res|
                raise "Multiple function '#{name}' result 'nil' for element '#{o}'" if res.nil?
                o.instance_variable_set("@multiple_result_" + name.to_s, res)
                o.send(multi_name)
              end
            when Hash
              res.each do |o,res|
                raise "Multiple function '#{name}' result 'nil' for element '#{o}'" if res.nil?
                o.instance_variable_set("@multiple_result_" + name.to_s, res)
                o.send(multi_name)
              end
            end

            if Array === self
              res = self.collect{|o| o.send(multi_name)}
              self.instance_variable_set("@multiple_result_" + name.to_s, res)
              res
            else
              res = self.send(multi_name)
              self.instance_variable_set("@multiple_result_" + name.to_s, res)
              res
            end
          end

        else 
          raise "Type not undestood in property: #{ type }"
        end
        @all_properties ||= []
        @all_properties << name
      end

      def self.persist(method_name, type = nil, options = {})
        type = :memory if type.nil?
        options ||= {}
        options = Misc.add_defaults options, :dir => File.join(Entity.entity_property_cache, self.to_s, method_name.to_s)

        orig_method_name = method_name
        multi_name = "_multiple_" + method_name.to_s
        single_name = "_single_" + method_name.to_s

        method_name = multi_name if self.instance_methods.include?(multi_name.to_sym)
        method_name = single_name if self.instance_methods.include?(single_name.to_sym)

        orig_name = UNPERSISTED_PREFIX + method_name.to_s
        alias_method orig_name, method_name unless self.instance_methods.include? orig_name.to_sym

        define_method method_name do |*args|
          id = self.id
          persist_name = orig_method_name.to_s + ":" << (Array === id ? Misc.obj2digest(id) : id)

          persist_options = options
          persist_options = persist_options.merge(:other => {:args => args}) if args and args.any?

          begin
            Persist.persist(persist_name, type, persist_options.merge(:persist => true)) do
              self.send(orig_name, *args)
            end
          rescue DontPersist
            $!.payload
          end
        end
      end

      def self.unpersist(method_name)
        return unless orig_name = persisted?(method_name)
        alias_method method_name, orig_name
        remove_method orig_name
      end

      def self.persisted?(method_name)
        ["", "_multiple_", "_single_"].each do |type|
          orig_name = (UNPERSISTED_PREFIX + type + method_name.to_s).to_sym
          return orig_name if instance_methods.include?(orig_name)
        end
        return false
      end

      def self.with_persisted(method_name)
        persisted = persisted? method_name
        persist method_name unless persisted
        res = yield
        unpersist method_name unless persisted
        res
      end

    end 
  end
end
