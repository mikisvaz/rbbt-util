require 'lockfile'
require 'net/smtp'
require 'digest/md5'
require 'cgi'
require 'zlib'
require 'rubygems/package'
require 'rbbt/util/tar'
require 'rbbt/util/misc/exceptions'
require 'rbbt/util/misc/concurrent_stream'
require 'rbbt/util/misc/indiferent_hash'
require 'rbbt/util/misc/pipes'
require 'rbbt/util/misc/format'
require 'rbbt/util/misc/omics'
require 'rbbt/util/misc/inspect'
require 'rbbt/util/misc/math'
require 'rbbt/util/misc/development'
require 'rbbt/util/misc/lock'


class Hash
  def chunked_values_at(keys, max = 5000)
    Misc.ordered_divide(keys, max).inject([]) do |acc,c|
      new = self.values_at(*c)
      new.annotate acc if new.respond_to? :annotate and acc.empty?
      acc.concat(new)
    end
  end
end

module LaterString
  def to_s
    yield
  end
end

module Misc

  def self.parse_cmd_params(str)
    return str if Array === str
    str.scan(/
             (?:["']([^"']*?)["']) |
             ([^"'\s]+)
    /x).flatten.compact
  end

  def self.pid_exists?(pid)
    return false if pid.nil?
    begin
      Process.getpgid(pid.to_i)
      true
    rescue Errno::ESRCH
      false
    end
  end

  def self.collapse_ranges(ranges)
    processed = []
    last = nil
    final = []
    ranges.sort_by{|range| range.begin }.each do |range|
      rbegin = range.begin
      rend = range.end
      if last.nil? or rbegin > last
        processed << [rbegin, rend]
        last = rend
      else
       new_processed = []
        processed.each do |pbegin,pend|
          if pend < rbegin
            final << [pbegin, pend]
          else
            eend = [rend, pend].max
            new_processed << [pbegin, eend]
            break
          end
        end
        processed = new_processed
        last = rend if rend > last
      end
    end

    final.concat processed
    final.collect{|b,e| (b..e)}
  end

  def self.total_length(ranges)
    Misc.collapse_ranges(ranges).inject(0) do |total,range| total += range.end - range.begin + 1 end
  end

  def self.random_sample_in_range(total, size)
    p = Set.new

    if size > total / 10
      template = (0..total - 1).to_a
      size.times do |i|
        pos = (rand * (total - i)).floor
        if pos == template.length - 1
          v = template.pop
        else
          v, n = template[pos], template[-1]
          template.pop
          template[pos] = n 
        end
        p << v
      end
    else
      size.times do 
        pos = nil
        while pos.nil? 
          pos = (rand * total).floor
          if p.include? pos
            pos = nil
          end
        end
        p << pos
      end
    end
    p
  end

  def self.sample(ary, size, replacement = false)
    if ary.respond_to? :sample
      ary.sample size
    else
      total = ary.length
      p = random_sample_in_range(total, size)
      ary.values_at *p
    end
  end


  def self.prepare_entity(entity, field, options = {})
    return entity unless defined? Entity
    return entity unless String === entity or Array === entity
    options ||= {}

    dup_array = options.delete :dup_array

    if Annotated === field or Entity.respond_to?(:formats) and Entity.formats.include? field
      params = options.dup

      params[:format] ||= params.delete "format"
      params.merge!(:format => field) unless params.include?(:format) and not ((f = params[:format]).nil? or (String === f and f.empty?))

      mod = Entity === field ? field : Entity.formats[field]
      entity = mod.setup(
        ((entity.frozen? and not entity.nil?) ? entity.dup : ((Array === entity and dup_array) ? entity.collect{|e| e.nil? ? e : e.dup} : entity) ),
        params
      ) 
    end

    entity
  end
 
  def self.ensembl_server(organism)
    date = organism.split("/")[1]
    if date.nil?
      "www.ensembl.org"
    else
      "#{ date }.archive.ensembl.org"
    end
  end


  def self.google_venn(list1, list2, list3, name1 = nil, name2 = nil, name3 = nil, total = nil)
    name1 ||= "list 1"
    name2 ||= "list 2"
    name3 ||= "list 3"

    sizes = [list1, list2, list3, list1 & list2, list1 & list3, list2 & list3, list1 & list2 & list3].collect{|l| l.length}

    total = total.length if Array === total

    label = "#{name1}: #{sizes[0]} (#{name2}: #{sizes[3]}, #{name3}: #{sizes[4]})"
    label << "|#{name2}: #{sizes[1]} (#{name1}: #{sizes[3]}, #{name3}: #{sizes[5]})"
      label << "|#{name3}: #{sizes[2]} (#{name1}: #{sizes[4]}, #{name2}: #{sizes[5]})"
      if total
        label << "| INTERSECTION: #{sizes[6]} TOTAL: #{total}"
      else
        label << "| INTERSECTION: #{sizes[6]}"
      end

    max = total || sizes.max
    sizes = sizes.collect{|v| (v.to_f/max * 100).to_i.to_f / 100}
    url = "https://chart.googleapis.com/chart?cht=v&chs=500x300&chd=t:#{sizes * ","}&chco=FF6342,ADDE63,63C6DE,FFFFFF&chdl=#{label}"
  end

  def self.consolidate(list)
    list.inject(nil){|acc,e|
      if acc.nil?
        acc = e
      else
        acc.concat e
        acc
      end
    }
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

  def self.send_email(from, to, subject, message, options = {})
    IndiferentHash.setup(options)
    options = Misc.add_defaults options, :from_alias => nil, :to_alias => nil, :server => 'localhost', :port => 25, :user => nil, :pass => nil, :auth => :login

    server, port, user, pass, from_alias, to_alias, auth = Misc.process_options options, :server, :port, :user, :pass, :from_alias, :to_alias, :auth

    msg = <<-END_OF_MESSAGE
From: #{from_alias} <#{from}>
To: #{to_alias} <#{to}>
Subject: #{subject}

#{message}
END_OF_MESSAGE

Net::SMTP.start(server, port, server, user, pass, auth) do |smtp|
  smtp.send_message msg, from, to
end
  end

  def self.counts(array)
    counts = {}
    array.each do |e|
      counts[e] ||= 0
      counts[e] += 1
    end

    counts
  end

  def self.proportions(array)
    total = array.length

    proportions = Hash.new 0

    array.each do |e|
      proportions[e] += 1.0 / total
    end

    class << proportions; self;end.class_eval do
      def to_s
        sort{|a,b| a[1] == b[1] ? a[0] <=> b[0] : a[1] <=> b[1]}.collect{|k,c| "%3d\t%s" % [c, k]} * "\n"
      end
    end

    proportions
  end


  def self.sorted_array_hits(a1, a2)
    e1, e2 = a1.shift, a2.shift
    counter = 0
    match = []
    while true
      break if e1.nil? or e2.nil?
      case e1 <=> e2
      when 0
        match << counter
        e1, e2 = a1.shift, a2.shift
        counter += 1
      when -1
        while not e1.nil? and e1 < e2
          e1 = a1.shift 
          counter += 1
        end
      when 1
        e2 = a2.shift
        e2 = a2.shift while not e2.nil? and e2 < e1
      end
    end
    match
  end

  def self.intersect_sorted_arrays(a1, a2)
    e1, e2 = a1.shift, a2.shift
    intersect = []
    while true
      break if e1.nil? or e2.nil?
      case e1 <=> e2
      when 0
        intersect << e1
        e1, e2 = a1.shift, a2.shift
      when -1
        e1 = a1.shift while not e1.nil? and e1 < e2
      when 1
        e2 = a2.shift
        e2 = a2.shift while not e2.nil? and e2 < e1
      end
    end
    intersect
  end

  def self.merge_sorted_arrays(a1, a2)
    e1, e2 = a1.shift, a2.shift
    new = []
    while true
      case
      when (e1 and e2)
        case e1 <=> e2
        when 0
          new << e1 
          e1, e2 = a1.shift, a2.shift
        when -1
          new << e1
          e1 = a1.shift
        when 1
          new << e2
          e2 = a2.shift
        end
      when e2
        new << e2
        new.concat a2
        break
      when e1
        new << e1
        new.concat a1
        break
      else
        break
      end
    end
    new
  end

  def self.binary_include?(array, elem)
    upper = array.size - 1
    lower = 0

    return -1 if upper < lower

    while(upper >= lower) do
      idx = lower + (upper - lower) / 2
      value = array[idx]

      case elem <=> value
      when 0
        return true
      when -1
        upper = idx - 1
      when 1
        lower = idx + 1
      else
        raise "Cannot compare #{[elem.inspect, value.inspect] * " with "}"
      end
    end

    return false
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

  def self.env_add(var, value, sep = ":", prepend = true)
    ENV[var] ||= ""
    return if ENV[var] =~ /(#{sep}|^)#{Regexp.quote value}(#{sep}|$)/
      if prepend
        ENV[var] = value + sep + ENV[var]
      else
        ENV[var] += sep + ENV[var]
      end
  end

  def self.do_once(&block)
    return nil if $__did_once
    $__did_once = true
    yield
    nil
  end

  def self.reset_do_once
    $__did_once = false
  end

  def self.insist(times = 3, sleep = nil, msg = nil)
    if Array === times
      sleep_array = times
      times = sleep_array.length
      sleep = sleep_array.shift
    end
    try = 0
    begin
      yield
    rescue
      if msg
        Log.warn("Insisting after exception: #{$!.message} -- #{msg}")
      else
        Log.warn("Insisting after exception: #{$!.message}")
      end
      if sleep and try > 0
        sleep sleep
        sleep = sleep_array.shift if sleep_array
      else
        Thread.pass
      end
      try += 1
      retry if try < times
      raise $!
    end
  end

  def self.try3times(&block)
    insist(3, &block)
  end

  def self.hash2string(hash)
    hash.sort_by{|k,v| k.to_s}.collect{|k,v| 
      next unless %w(Symbol String Float Fixnum Integer TrueClass FalseClass Module Class Object).include? v.class.to_s
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
      next unless %w(Symbol String Float Fixnum Integer TrueClass FalseClass Module Class Object Array).include? v.class.to_s
      v = case 
          when Symbol === v
            v.to_s
          when Array === v
            v * ","
          else
            CGI.escape(v.to_s)
          end
      [ Symbol === k ? k.to_s : k,  v] * "="
    }.compact * "&"
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
      when (Fixnum === v or Float === v)
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

  def self.path_relative_to(basedir, path)
    path = File.expand_path(path) unless path[0] == "/"
    basedir = File.expand_path(basedir) unless basedir[0] == "/"

    if path.index(basedir) == 0
      if basedir[-1] == "/"
        return path[basedir.length..-1]
      else
        return path[basedir.length+1..-1]
      end
    else
      return nil
    end
  end

  def self.hostname
    @hostanem ||= `hostname`.strip
  end


  def self.common_path(dir, file)
    file = File.expand_path file
    dir = File.expand_path dir

    return true if file == dir
    while File.dirname(file) != file
      file = File.dirname(file)
      return true if file == dir
    end

    return false
  end

  # WARN: probably not thread safe...
  def self.in_dir(dir)
    old_pwd = FileUtils.pwd
    res = nil
    begin
      FileUtils.mkdir_p dir unless File.exists? dir
      FileUtils.cd dir
      res = yield
    ensure
      FileUtils.cd old_pwd
    end
    res
  end

  def self.sensiblewrite(path, content = nil, &block)
    return if File.exists? path
    tmp_path = path + '.sensible_write'
    Misc.lock tmp_path  do
      if not File.exists? path
        FileUtils.rm_f tmp_path if File.exists? tmp_path
        begin
          case
          when block_given?
            File.open(tmp_path, 'w', &block)
          when String === content
            File.open(tmp_path, 'w') do |f| f.write content end
          when (IO === content or StringIO === content or File === content)
            File.open(tmp_path, 'w') do |f|  
              while block = content.read(2048); 
                f.write block
              end  
            end
          else
            File.open(tmp_path, 'w') do |f|  end
          end
          FileUtils.mv tmp_path, path
        rescue Exception
          Log.error "Exception in sensiblewrite: #{$!.message} -- #{ Log.color :blue, path }"
          FileUtils.rm_f tmp_path if File.exists? tmp_path
          FileUtils.rm_f path if File.exists? path
          raise $!
        end
      end
    end
  end

  def self.add_defaults(options, defaults = {})
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

  def self.string2const(string)
    return nil if string.nil?
    mod = Kernel

    string.to_s.split('::').each do |str|
      mod = mod.const_get str
    end

    mod
  end

  def self.string2hash_old(string)

    options = {}
    string.split(/#/).each do |str|
      if str.match(/(.*)=(.*)/)
        option, value = $1, $2
      else
        option, value = str, true
      end

      option = option.sub(":",'').to_sym if option.chars.first == ':'
      value  = value.sub(":",'').to_sym if String === value and value.chars.first == ':'

      if value == true
        options[option] = option.to_s.chars.first != '!' 
      else
        options[option] = Thread.start do
          $SAFE = 0;
          case 
          when value =~ /^(?:true|T)$/i
            true
          when value =~ /^(?:false|F)$/i
            false
          when Symbol === value
            value
          when (String === value and value =~ /^\/(.*)\/$/)
            Regexp.new /#{$1}/
          else
            begin
              Kernel.const_get value
            rescue
              begin  
                raise if value =~ /[a-z]/ and defined? value
                eval(value) 
              rescue Exception
                value 
              end
            end
          end
        end.value
      end
    end

    options
  end

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

    options = {}
    string.split(/#/).each do |str|
      if str.match(/(.*)=(.*)/)
        option, value = $1, $2
      else
        option, value = str, true
      end

      option = option.sub(":",'').to_sym if option.chars.first == ':'
      value  = value.sub(":",'').to_sym if String === value and value.chars.first == ':'

      if value == true
        options[option] = option.to_s.chars.first != '!' 
      else
        options[option] = Thread.start do
          $SAFE = 0;
          case 
          when value =~ /^(?:true|T)$/i
            true
          when value =~ /^(?:false|F)$/i
            false
          when Symbol === value
            value
          when (String === value and value =~ /^\/(.*)\/$/)
            Regexp.new /#{$1}/
          else
            begin
              Kernel.const_get value
            rescue
              begin  
                raise if value =~ /[a-z]/ and defined? value
                eval(value) 
              rescue Exception
                value 
              end
            end
          end
        end.value
      end
    end

    options
  end

  def self.field_position(fields, field, quiet = false)
    return field if Integer === field or Range === field
    raise FieldNotFoundError, "Field information missing" if fields.nil? && ! quiet
    fields.each_with_index{|f,i| return i if f == field}
    field_re = Regexp.new /^#{field}$/i
    fields.each_with_index{|f,i| return i if f =~ field_re}
    raise FieldNotFoundError, "Field #{ field.inspect } was not found" unless quiet
  end

  # Divides the array into +num+ chunks of the same size by placing one
  # element in each chunk iteratively.
  def self.divide(array, num)
    num = 1 if num == 0
    chunks = []
    num.to_i.times do chunks << [] end
    array.each_with_index{|e, i|
      c = i % num
      chunks[c] << e
    }
    chunks
  end

  # Divides the array into chunks of +num+ same size by placing one
  # element in each chunk iteratively.
  def self.ordered_divide(array, num)
    last = array.length - 1
    chunks = []
    current = 0
    while current <= last
      next_current = [last, current + num - 1].min
      chunks << array[current..next_current]
      current = next_current + 1
    end
    chunks
  end

  def self.append_zipped(current, new)
    current.each do |v|
      n = new.shift
      if Array === n
        v.concat new
      else
        v << n
      end
    end
    current
  end

  def self.zip_fields(array)
    return [] if array.empty? or (first = array.first).nil?
    first.zip(*array[1..-1])
  end
end

module PDF2Text
  def self.pdftotext(filename, options = {})
    require 'rbbt/util/cmd'
    require 'rbbt/util/tmpfile'
    require 'rbbt/util/open'


    TmpFile.with_file(Open.open(filename, options.merge(:nocache => true)).read) do |pdf_file|
      CMD.cmd("pdftotext #{pdf_file} -", :pipe => false, :stderr => true)
    end
  end
end
