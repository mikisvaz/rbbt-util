require 'rbbt-util'
require 'rbbt/resource/path'

module Rbbt::Config

  CACHE = IndiferentHash.setup({})

  GOT_KEYS=[]

  def self.add_entry(key, value, tokens)
    CACHE[key.to_s] ||= [] 
    CACHE[key.to_s] << [tokens, value]
  end

  def self.load_file(file)
    Log.debug "Loading file: #{ file }"
    TSV.traverse file, :type => :array do |line|
      next if line =~ /^#/
      key, value, *tokens = line.split(/\s/)
      tokens << "key:#{key}"

      self.add_entry(key, value, tokens) if key
    end
  end

  def self.load_config
    Rbbt.etc.config.find_all.each do |file|
      self.load_file(file)
    end
  end

  def self.set(values, *tokens)
    if not Hash === values
      values = {values => tokens.shift}
    end

    values.each do |key,value|
      add_entry key, value, tokens
    end
  end

  def self.token_priority(token)
    token, _sep, priority = token.to_s.partition("::")

    if priority.nil? || priority.empty?
      type, _sep, rest = token.partition(":")
      priority = case type
                 when "workflow"
                   4
                 when "task"
                   3
                 when "file"
                   2
                 when "line"
                   1
                 when "key"
                   20
                 else
                   10
                 end
    else
      priority = priority.to_i
    end

    [token, priority]
  end

  def self.match(entries, token)
    priorities = {}
    entries.each do |tokens, value|
      best_prio = nil
      tokens.each do |tok|
        tok, prio = token_priority tok
        best_prio = prio if best_prio.nil? or best_prio > prio
        next if prio > best_prio
        next unless tok == token
        priorities[prio] ||= []
        priorities[prio] << value
      end
    end if entries
    priorities
  end

  def self.get(key, *tokens)
    options = tokens.pop if Hash === tokens.last
    default = options.nil? ? nil : options[:default]

    tokens = tokens.flatten
    file, _sep, line = caller.reject{|l| 
      l =~ /rbbt\/(?:resource\.rb|workflow\.rb)/ or
        l =~ /rbbt\/resource\/path\.rb/ or
        l =~ /rbbt\/util\/misc\.rb/ or
        l =~ /accessor\.rb/ or
        l =~ /progress-monitor\.rb/ 
    }.first.partition(":")

    File.expand_path(file)

    tokens << ("file:" << file)
    tokens << ("line:" << file << ":" << line.sub(/:in \`.*/,''))

    entries = CACHE[key.to_s]
    priorities = {}
    tokens = tokens + ["key:" << key.to_s]
    tokens.each do |token|
      token_prio = match entries, token.to_s
      token_prio.each do |prio, values|
        priorities[prio] ||= []
        priorities[prio].concat(values)
      end
    end

    value = priorities.empty? ? default : priorities.collect{|p| p }.sort_by{|p,v| p}.first.last.last
    value = false if value == 'false'

    Log.debug "Value #{value.inspect} for config key '#{ key }': #{tokens * ", "}"
    GOT_KEYS << [key, value, tokens]

    value
  end

  self.load_config
end
