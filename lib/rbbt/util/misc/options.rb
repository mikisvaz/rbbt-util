module Misc

  def self.parse_cmd_params(str)
    return str if Array === str
    str.scan(/
             (?:["']([^"']*?)["']) |
             ([^"'\s]+)
    /x).flatten.compact
  end

  def self.positional2hash(keys, *values)
    if Hash === values.last
      extra = values.pop
      inputs = Misc.zip2hash(keys, values)
      inputs.delete_if{|k,v| v.nil? or (String === v and v.empty?)}
      inputs = Misc.add_defaults inputs, extra
      inputs.delete_if{|k,v| not keys.include?(k) and not (Symbol === k ? keys.include?(k.to_s) : keys.include?(k.to_sym))}
      inputs
    else
      Misc.zip2hash(keys, values)
    end
  end

  def self.array2hash(array, default = nil)
    hash = {}
    array.each do |key, value|
      value = default.dup if value.nil? and not default.nil?
      hash[key] = value
    end
    hash
  end

  def self.zip2hash(list1, list2)
    hash = {}
    list1.each_with_index do |e,i|
      hash[e] = list2[i]
    end
    hash
  end

  def self.process_to_hash(list)
    result = yield list
    zip2hash(list, result)
  end

  def self.hash2string(hash)
    hash.sort_by{|k,v| k.to_s}.collect{|k,v| 
      next unless %w(Symbol String Float Fixnum Integer Numeric TrueClass FalseClass Module Class Object).include? v.class.to_s
      [ Symbol === k ? ":" << k.to_s : k,
        Symbol === v ? ":" << v.to_s : v] * "="
    }.compact * "#"
  end

  def self.GET_params2hash(string)
    hash = {}
    string.split('&').collect{|item|
      key, value = item.split("=").values_at 0, 1
      hash[key] = value.nil? ? "" : CGI.unescape(value)
    }
    hash
  end

  def self.hash2GET_params(hash)
    hash.sort_by{|k,v| k.to_s}.collect{|k,v| 
      next unless %w(Symbol String Float Fixnum Integer Numeric TrueClass FalseClass Module Class Object Array).include? v.class.to_s
      v = case 
          when Symbol === v
            v.to_s
          when Array === v
            v * ","
          else
            CGI.escape(v.to_s).gsub('%2F','/')
          end
      [ Symbol === k ? k.to_s : k,  v] * "="
    }.compact * "&"
  end

  def self.add_GET_param(url, param, value)
    clean_url, param_str = url.split("?")
    hash = param_str.nil? ? {} : self.GET_params2hash(param_str)
    IndiferentHash.setup hash
    hash[param] = value
    clean_url << "?" << hash2GET_params(hash)
  end

  def self.hash_to_html_tag_attributes(hash)
    return "" if hash.nil? or hash.empty?
    hash.collect{|k,v| 
      case 
      when (k.nil? or v.nil? or (String === v and v.empty?))
        nil
      when Array === v
        [k,"'" << v * " " << "'"] * "="
      when String === v
        [k,"'" << v << "'"] * "="
      when Symbol === v
        [k,"'" << v.to_s << "'"] * "="
      when TrueClass === v
        [k,"'" << v.to_s << "'"] * "="
      when Numeric === v
        [k,"'" << v.to_s << "'"] * "="
      else
        nil
      end
    }.compact * " "
  end

  def self.html_tag(tag, content = nil, params = {})
    attr_str = hash_to_html_tag_attributes(params)
    attr_str = " " << attr_str if String === attr_str and attr_str != ""
    html = if content.nil?
      "<#{ tag }#{attr_str}/>"
    else
      "<#{ tag }#{attr_str}>#{ content }</#{ tag }>"
    end

    html
  end

  def self.add_defaults(options, defaults = {})
    options ||= {}
    case
    when Hash === options
      new_options = options.dup
    when String === options
      new_options = string2hash options
    else
      raise "Format of '#{options.inspect}' not understood. It should be a hash"
    end

    defaults.each do |key, value|
      next if options.include? key

      new_options[key] = value 
    end

    new_options
  end

  def self.process_options(hash, *keys)
    if keys.length == 1
      hash.include?(keys.first.to_sym) ? hash.delete(keys.first.to_sym) : hash.delete(keys.first.to_s) 
    else
      keys.collect do |key| hash.include?(key.to_sym) ? hash.delete(key.to_sym) : hash.delete(key.to_s) end
    end
  end

  def self.pull_keys(hash, prefix)
    new = {}
    hash.keys.each do |key|
      if key.to_s =~ /#{ prefix }_(.*)/
        case
        when String === key
          new[$1] = hash.delete key
        when Symbol === key
          new[$1.to_sym] = hash.delete key
        end
      else
        if key.to_s == prefix.to_s
          new[key] = hash.delete key
        end
      end
    end

    new
  end

  #def self.string2hash_old(string)

  #  options = {}
  #  string.split(/#/).each do |str|
  #    if str.match(/(.*)=(.*)/)
  #      option, value = $1, $2
  #    else
  #      option, value = str, true
  #    end

  #    option = option.sub(":",'').to_sym if option.chars.first == ':'
  #    value  = value.sub(":",'').to_sym if String === value and value.chars.first == ':'

  #    if value == true
  #      options[option] = option.to_s.chars.first != '!' 
  #    else
  #      options[option] = Thread.start do
  #        $SAFE = 0;
  #        case 
  #        when value =~ /^(?:true|T)$/i
  #          true
  #        when value =~ /^(?:false|F)$/i
  #          false
  #        when Symbol === value
  #          value
  #        when (String === value and value =~ /^\/(.*)\/$/)
  #          Regexp.new /#{$1}/
  #        else
  #          begin
  #            Kernel.const_get value
  #          rescue
  #            begin  
  #              raise if value =~ /[a-z]/ and defined? value
  #              eval(value) 
  #            rescue Exception
  #              value 
  #            end
  #          end
  #        end
  #      end.value
  #    end
  #  end

  #  options
  #end

  def self.string2hash(string)
    options = {}

    string.split('#').each do |str|
      key, sep, value = str.partition "="

      key = key[1..-1].to_sym if key[0] == ":"

      options[key] = true and next if value.empty?
      options[key] = value[1..-1].to_sym and next if value[0] == ":"
      options[key] = Regexp.new(/#{value[1..-2]}/) and next if value[0] == "/" and value[-1] == "/"
      options[key] = value[1..-2] and next if value =~ /^['"].*['"]$/
      options[key] = value.to_i and next if value =~ /^\d+$/
      options[key] = value.to_f and next if value =~ /^\d*\.\d+$/
      options[key] = true and next if value == "true"
      options[key] = false and next if value == "false"
      options[key] = value and next 

      options[key] = begin
                       saved_safe = $SAFE
                       $SAFE = 0
                       eval(value)
                     rescue Exception
                       value
                     ensure
                       $SAFE = saved_safe
                     end
    end

    return options

  end

end
