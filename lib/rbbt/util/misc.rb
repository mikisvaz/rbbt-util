require 'lockfile'
require 'rbbt/util/chain_methods'
require 'rbbt/resource/path'
require 'rbbt/annotations'
require 'net/smtp'
require 'narray'

module Misc
  class FieldNotFoundError < StandardError;end

  COLOR_LIST = %w(red green blue black yellow pink purple)
  def self.colors_for(list)
    unused = COLOR_LIST.dup

    used = {}
    colors = list.collect do |elem|
      if used.include? elem
        used[elem]
      else
        color = unused.shift
        used[elem]=color
        color
      end
    end

    [colors, used]
  end

  def self.total_length(ranges)
    processed = []
    last = nil
    ranges.sort_by{|range| range.begin }.each do |range|
      if last.nil? or range.begin > last
        processed << range
        last = range.end
      else
        new_processed = []
        processed.each do |processed_range|
          if processed_range.end < range.begin
            new_processed << processed_range
          else
            eend = [range.end, processed_range.end].max
            new_processed << (processed_range.begin..eend)
            break
          end
        end
        processed = new_processed
        last = range.end if range.end > last
      end
    end

    processed.inject(0) do |total,range| total += range.end - range.begin + 1 end
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
    total = ary.length
    p = random_sample_in_range(total, size)
    ary.values_at *p
  end

  Log2Multiplier = 1.0 / Math.log(2.0)
  def self.log2(x)
    Math.log(x) * Log2Multiplier
  end

  def self.prepare_entity(entity, field, options = {})
    return entity unless String === entity or Array === entity
    options ||= {}
    dup_array = options.delete :dup_array
    entity = Entity.formats[field].setup(((entity.frozen? and not entity.nil?) ? entity.dup : ((Array === entity and dup_array) ? entity.collect{|e| e.nil? ? e : e.dup} : entity) ), options.merge({:format => field})) if defined?(Entity) and Entity.respond_to?(:formats) and Entity.formats.include? field
    entity
  end

  ARRAY_MAX_LENGTH = 10000
  STRING_MAX_LENGTH = ARRAY_MAX_LENGTH * 10

  def self.sanitize_filename(filename, length = 200)
    if filename.length > length
      if filename =~ /(\..{2,4})$/
        extension = $1
      else
        extension = ''
      end

      post_fix = " TRUNCATED at #{length} (#{filename.length})" + extension

      filename = filename[0..(length - post_fix.length - 1)] << post_fix
    else
      filename
    end
    filename
  end

  def self.remove_long_items(obj)
    case
    when (Array === obj and obj.length > ARRAY_MAX_LENGTH)
      remove_long_items(obj[0..ARRAY_MAX_LENGTH-2] << "TRUNCATED at #{ ARRAY_MAX_LENGTH } (#{obj.length})")
    when (Hash === obj and obj.length > ARRAY_MAX_LENGTH)
      remove_long_items(obj.compact[0..ARRAY_MAX_LENGTH-2] << ["TRUNCATED", "at #{ ARRAY_MAX_LENGTH } (#{obj.length})"])
    when (String === obj and obj.length > STRING_MAX_LENGTH)
      obj[0..STRING_MAX_LENGTH-1] << " TRUNCATED at #{STRING_MAX_LENGTH} (#{obj.length})"
    when Hash === obj
      new = {}
      obj.each do |k,v|
        new[k] = remove_long_items(v)
      end
      new
    when Array === obj
      obj.collect do |e| remove_long_items(e) end
    else
      obj
    end
  end

  def self.ensembl_server(organism)
    date = organism.split("/")[1]
    if date.nil?
      "www.ensembl.org"
    else
      "#{ date }.archive.ensembl.org"
    end
  end

  def self.filename?(string)
    String === string and string.length > 0 and string.length < 250 and File.exists?(string)
  end

  def self.max(list)
    max = nil
    list.each do |v|
      next if v.nil?
      max = v if max.nil? or v > max
    end
    max
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

  def self.sum(list)
    list.compact.inject(0.0){|acc,e| acc += e}
  end

  def self.mean(list)
    sum(list) / list.compact.length
  end

  def self.sd(list)
    return nil if list.length < 3
    mean = mean(list)
    Math.sqrt(list.compact.inject(0.0){|acc,e| d = e - mean; acc += d * d}) / (list.compact.length - 1)
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
      inputs.delete_if{|k,v| v.nil?}
      inputs = Misc.add_defaults inputs, extra
      inputs.delete_if{|k,v| not keys.include? k}
    else
      Misc.zip2hash(keys, values)
    end
  end

  def self.send_email(from, to, subject, message, options = {})
    IndiferentHash.setup(options)
    options = Misc.add_defaults options, :from_alias => nil, :to_alias => nil, :server => 'localhost', :port => 25, :user => nil, :pass => nil, :auth => :login
    IndiferentHash.setup(options)

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
    counts = Hash.new 0
    array.each do |e|
      counts[e] += 1
    end

    class << counts; self;end.class_eval do
      def to_s
        sort{|a,b| a[1] == b[1] ? a[0] <=> b[0] : a[1] <=> b[1]}.collect{|k,c| "%3d\t%s" % [c, k]} * "\n"
      end
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

  IUPAC2BASE = {
    "A" => ["A"],
    "C" => ["C"],
    "G" => ["G"],
    "T" => ["T"],
    "U" => ["U"],
    "R" => "A or G".split(" or "),
    "Y" => "C or T".split(" or "),
    "S" => "G or C".split(" or "),
    "W" => "A or T".split(" or "),
    "K" => "G or T".split(" or "),
    "M" => "A or C".split(" or "),
    "B" => "C or G or T".split(" or "),
    "D" => "A or G or T".split(" or "),
    "H" => "A or C or T".split(" or "),
    "V" => "A or C or G".split(" or "),
    "N" => %w(A C T G),
  }

  BASE2COMPLEMENT = {
    "A" => "T",
    "C" => "G",
    "G" => "C",
    "T" => "A",
    "U" => "A",
  }

  THREE_TO_ONE_AA_CODE = {
    "ala" =>   "A",
    "arg" =>   "R",
    "asn" =>   "N",
    "asp" =>   "D",
    "cys" =>   "C",
    "glu" =>   "E",
    "gln" =>   "Q",
    "gly" =>   "G",
    "his" =>   "H",
    "ile" =>   "I",
    "leu" =>   "L",
    "lys" =>   "K",
    "met" =>   "M",
    "phe" =>   "F",
    "pro" =>   "P",
    "ser" =>   "S",
    "thr" =>   "T",
    "trp" =>   "W",
    "tyr" =>   "Y",
    "val" =>   "V"
  }

  def self.fast_align(reference, sequence)
    init_gap = -1
    gap = -2
    diff = -2
    same = 2

    cols = sequence.length + 1
    rows = reference.length + 1

    a = NArray.int(cols, rows)


    for spos in 0..cols-1 do a[spos, 0] = spos * init_gap end
    for rpos in 0..rows-1 do a[0, rpos] = rpos * init_gap end

    for spos in 1..cols-1 do
      for rpos in 1..rows-1 do
        match = a[spos-1,rpos-1] + (sequence[spos-1] != reference[rpos-1] ? diff : same)
        skip_sequence = a[spos-1,rpos] + gap
        skip_reference = a[spos,rpos-1] + gap
        a[spos,rpos] = [match, skip_sequence, skip_reference].max
      end
    end

    start = Misc.max(a[-1,0..rows-1])
    start_pos = a[-1,0..rows-1].to_a.index start

    ref = ''
    seq = ''
    rpos = start_pos
    spos = cols - 1

    while spos > 0 and rpos > 0
      score = a[spos,rpos]
      score_match = a[spos-1,rpos-1]
      score_skip_reference = a[spos,rpos-1]
      score_skip_sequence = a[spos-1,rpos]

      case
      when score == score_match + (sequence[spos-1] != reference[rpos-1] ? diff : same)
        ref << reference[rpos-1]
        seq << sequence[spos-1]
        spos -= 1
        rpos -= 1
      when score == score_skip_reference + gap
        ref << reference[rpos-1]
        seq << '-'
        rpos -= 1
      when score == score_skip_sequence + gap
        seq << sequence[spos-1]
        ref << '-'
        spos -= 1
      else
        raise "stop"
      end
    end

    while (rpos > 0)
      ref << reference[rpos-1]
      seq = seq << '-'
      rpos -= 1    
    end

    while (spos > 0)
      seq << sequence[spos-1]
      ref = ref + '-'
      spos -= 1
    end

    [ref.reverse + reference[start_pos..-1], seq.reverse + '-' * (rows - start_pos - 1)]
  end
  def self.IUPAC_to_base(iupac)
    IUPAC2BASE[iupac]
  end

  def self.is_filename?(string)
    return true if Path === string
    return true if String === string and string.length < 265 and File.exists? string
    return false
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

  def self.array2hash(array)
    hash = {}
    array.each do |key, value|
      hash[key] = value
    end
    hash
  end

  def self.zip2hash(list1, list2)
    array2hash(list1.zip(list2))
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

  def self.benchmark(repeats = 1)
    require 'benchmark'
    res = nil
    begin
      measure = Benchmark.measure do
        repeats.times do
          res = yield
        end
      end
      puts "Benchmark for #{ repeats } repeats"
      puts measure
    rescue Exception
      puts "Benchmark aborted"
      raise $!
    end
    res
  end

  def self.profile
    require 'ruby-prof'
    RubyProf.start
    begin
      res = yield
    rescue Exception
      puts "Profiling aborted"
      raise $!
    ensure
      result = RubyProf.stop
      printer = RubyProf::FlatPrinter.new(result)
      printer.print(STDOUT, 0)
    end

    res
  end

  def self.memprof
    require 'memprof'
    Memprof.start
    begin
      res = yield
    rescue Exception
      puts "Profiling aborted"
      raise $!
    ensure
      Memprof.stop
      print Memprof.stats
    end

    res
  end

  def self.insist(times = 3)
    try = 0
    begin
      yield
    rescue
      try += 1
      retry if try < times
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

  def self.path_relative_to(basedir, path)
    path = File.expand_path(path)
    basedir = File.expand_path(basedir)

    if path =~ /#{Regexp.quote basedir}\/(.*)/
      return $1
    else
      return nil
    end
  end

  def self.lock(file, *args)
    return yield file, *args if file.nil?
    FileUtils.mkdir_p File.dirname(File.expand_path(file)) unless File.exists?  File.dirname(File.expand_path(file))

    res = nil

    lockfile = Lockfile.new(File.expand_path(file + '.lock'))

    lockfile.lock do
      res = yield file, *args
    end

    res
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

  def self.in_dir(dir)
    old_pwd = FileUtils.pwd
    res = nil
    begin
      FileUtils.mkdir_p dir unless File.exists? dir
      FileUtils.cd dir
      res = yield
    rescue
      raise $!
    ensure
      FileUtils.cd old_pwd
    end
    res
  end

  def self.fixutf8(string)
    if string.respond_to?(:valid_encoding?) and ! string.valid_encoding?
      @@ic ||= Iconv.new('UTF-8//IGNORE', 'UTF-8')
      @@ic.iconv(string)
    else
      string
    end
  end

  def self.sensiblewrite(path, content)
    Misc.lock path + '.sensible_write' do
      begin
        case
        when String === content
          File.open(path, 'w') do |f|  f.write content  end
        when (IO === content or StringIO === content)
          File.open(path, 'w') do |f|  while l = content.gets; f.write l; end  end
        else
          File.open(path, 'w') do |f|  end
        end
      rescue Interrupt
        FileUtils.rm_f path if File.exists? path
        raise "Interrupted (Ctrl-c)"
      rescue Exception
        FileUtils.rm_f path if File.exists? path
        raise $!
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

  def self.digest(text)
    Digest::MD5.hexdigest(text)
  end

  HASH2MD5_MAX_STRING_LENGTH = 1000
  HASH2MD5_MAX_ARRAY_LENGTH = 100
  def self.hash2md5(hash)
    str = ""
    keys = hash.keys
    keys = keys.clean_annotations if keys.respond_to? :clean_annotations
    keys = keys.sort_by{|k| k.to_s}

    keys.each do |k|
      next if k == :monitor or k == "monitor" or k == :in_situ_persistence or k == "in_situ_persistence"
      v = hash[k]
      case
      when TrueClass === v
        str << k.to_s << "=>true" 
      when FalseClass === v
        str << k.to_s << "=>false" 
      when Hash === v
        str << k.to_s << "=>" << hash2md5(v)
      when Symbol === v
        str << k.to_s << "=>" << v.to_s
      when (String === v and v.length > HASH2MD5_MAX_STRING_LENGTH)
        str << k.to_s << "=>" << v[0..HASH2MD5_MAX_STRING_LENGTH]
      when String === v
        str << k.to_s << "=>" << v
      when (Array === v and v.length > HASH2MD5_MAX_ARRAY_LENGTH)
        str << k.to_s << "=>[" << v[0..HASH2MD5_MAX_ARRAY_LENGTH] * "," << "]"
      when Array === v
        str << k.to_s << "=>[" << v * "," << "]"
      else
        v_ins = v.inspect

        case
        when v_ins =~ /:0x0/
          str << k.to_s << "=>" << v_ins.sub(/:0x[a-f0-9]+@/,'')
        else
          str << k.to_s << "=>" << v_ins
        end

      end
    end

    if str.empty?
      ""
    else
      Digest::MD5.hexdigest(str)
    end
  end

  def self.process_options(hash, *keys)
    if keys.length == 1
      hash.delete keys.first.to_sym
    else
      keys.collect do |key| hash.delete(key.to_sym) || hash.delete(key.to_s) end
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

  def self.string2hash(string)

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
    field_re = Regexp.new /#{field}/i
      fields.each_with_index{|f,i| return i if f =~ field_re}
    raise FieldNotFoundError, "Field #{ field.inspect } was not found" unless quiet
  end

  # Divides the array into +num+ chunks of the same size by placing one
  # element in each chunk iteratively.
  def self.divide(array, num)
    chunks = []
    num.times do chunks << [] end
    array.each_with_index{|e, i|
      c = i % num
      chunks[c] << e
    }
    chunks
  end

  def self.zip_fields(array)
    return [] if array.empty?
    array[0].zip(*array[1..-1])
  end

end

class RBBTError < StandardError
  attr_accessor :info

  alias old_to_s to_s
  def to_s
    str = old_to_s.dup
    if info
      str << "\n" << "Additional Info:\n---\n" << info << "---"
    end
    str
  end
end

module IndiferentHash
  extend ChainMethods
  self.chain_prefix = :indiferent

  def indiferent_get_brackets(key)
    case 
    when (Symbol === key and indiferent_clean_include? key)
      indiferent_clean_get_brackets(key)
    when (Symbol === key and indiferent_clean_include? key.to_s)
      indiferent_clean_get_brackets(key.to_s)
    when (String === key and indiferent_clean_include? key)
      indiferent_clean_get_brackets(key)
    when (String === key and indiferent_clean_include? key.to_sym)
      indiferent_clean_get_brackets(key.to_sym)
    else
      indiferent_clean_get_brackets(key) 
    end
  end

  def indiferent_values_at(*key_list)
    res = []
    key_list.each{|key| res << indiferent_get_brackets(key)}
    res
  end

  def indiferent_include?(key)
    case
    when Symbol === key
      indiferent_clean_include?(key) or indiferent_clean_include?(key.to_s) 
    when String === key
      indiferent_clean_include?(key) or indiferent_clean_include?(key.to_sym) 
    else
      indiferent_clean_include?(key)
    end
  end

  def indiferent_delete(value)
    if indiferent_clean_include? value.to_s
      indiferent_clean_delete(value.to_s) 
    else
      indiferent_clean_delete(value.to_sym) 
    end
  end

  def self.setup(hash)
    return hash if IndiferentHash === hash
    hash.extend IndiferentHash unless IndiferentHash === hash
    hash
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
