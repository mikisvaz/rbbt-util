module Misc
  ARRAY_MAX_LENGTH = 1000
  STRING_MAX_LENGTH = ARRAY_MAX_LENGTH * 100
  TSV_MAX_FIELDS=100
  TSV_MAX_ROWS=100

  def self.break_lines(text, char_size=80)
    text = text.gsub("\n", " ")
    lines = []
    line = []
    text.split(/([\s\-]+)/).each do |part|
      if line.join("").length + part.length > char_size
        lines << line * ""
        line = []
      end
      line << part
    end

    lines << line * ""

    lines.flatten.collect{|l| l.strip} * "\n"
  end

  def self.name2basename(file)
    sanitize_filename(file.gsub("/",'>').gsub("~", '-'))
  end

  def self.sanitize_filename(filename, length = 254)
    if filename.length > length
      if filename =~ /(\..{2,9})$/
        extension = $1
      else
        extension = ''
      end

      post_fix = "--#{filename.length}@#{length}_#{Misc.digest(filename)[0..4]}" + extension

      filename = filename[0..(length - post_fix.length - 1)] << post_fix
    else
      filename
    end
    filename
  end

  def self.fingerprint(obj)
    case obj
    when nil
      "nil"
    when (defined? Step and Step)
      "<Step:"  << (obj.short_path || Misc.fingerprint([obj.task.name, obj.inputs])) << ">"
    when TrueClass
      "true"
    when FalseClass
      "false"
    when Symbol
      ":" << obj.to_s
    when String
      if obj.length > 100
        "'" << obj.slice(0,30) << "<...#{obj.length}...>" << obj.slice(-10,30)<< "'"
      else 
        "'" << obj << "'"
      end
    when (defined? AnnotatedArray and AnnotatedArray)
      "<A: #{fingerprint Annotated.purge(obj)} #{fingerprint obj.info}>"
    when (defined? TSV and TSV::Parser)
      filename = obj.filename
      filename = "STDIN(#{rand})" if filename == '-'
      "<TSVStream:" + (filename || "NOFILENAME") + "--" << Misc.fingerprint(obj.options) << ">"
    when IO
      (obj.respond_to?(:filename) and obj.filename ) ? "<IO:" + (obj.filename || obj.inspect + rand(100000)) + ">" : obj.inspect
    when File
      "<File:" + obj.path + ">"
    when NamedArray
      "[<NamedArray: fields=#{fingerprint obj.fields} -- values=#{fingerprint obj[0..-1]}]"
    when Array
      if (length = obj.length) > 10
        "[#{length}--" <<  (obj.values_at(0,1, length / 2, -2, -1).collect{|e| fingerprint(e)} * ",") << "]"
      else
        "[" << (obj.collect{|e| fingerprint(e) } * ", ") << "]"
      end
    when (defined? TSV and TSV)
      obj.with_unnamed do
        "TSV:{"<< fingerprint(obj.all_fields|| []).inspect << ";" << fingerprint(obj.keys).inspect << "}"
      end
    when Hash
      if obj.length > 10
        "H:{"<< fingerprint(obj.keys) << ";" << fingerprint(obj.values) << "}"
      else
        new = "{"
        obj.each do |k,v|
          new << fingerprint(k) << '=>' << fingerprint(v) << ' '
        end
        if new.length > 1
           new[-1] =  "}"
        else
          new << '}'
        end
        new
      end
    when Float
      if obj.abs > 10
        "%.1f" % obj
      elsif obj.abs > 1
        "%.3f" % obj
      else
        "%.6f" % obj
      end
    else
      obj.to_s
    end
  end


  def self.remove_long_items(obj)
    case
    when IO === obj
      remove_long_items("IO: " + (obj.respond_to?(:filename) ? (obj.filename || obj.inspect) : obj.inspect ))
    when obj.respond_to?(:path)
      remove_long_items("File: " + obj.path)
    when TSV::Parser === obj
      filename = obj.filename
      filename = "STDIN(rand-#{rand(10000000)})" if filename == '-'
      remove_long_items("TSV Stream: " + filename + " -- " << Misc.fingerprint(obj.options))
    when TSV === obj
      tsv = obj
      fields = tsv.fields

      if obj.size > TSV_MAX_ROWS
        tsv = obj.head(TSV_MAX_ROWS)
        tsv["Truncated rows at #{TSV_MAX_ROWS} (#{obj.size})"] = nil
      end

      if fields && fields.length > TSV_MAX_FIELDS
        tsv = obj.slice(fields[0..TSV_MAX_ROWS-1])
        tsv.add_field "Truncated at #{TSV_MAX_ROWS} (#{fields.length})" do
          nil
        end
      elsif fields.nil?
        new = tsv.annotate({})
        tsv.each do |k,v|
          new[k] = Misc.remove_long_items(v)
        end
        tsv = new
      end

      tsv
    when (Array === obj and obj.length > ARRAY_MAX_LENGTH)
      remove_long_items(obj[0..ARRAY_MAX_LENGTH-2] << "TRUNCATED at #{ ARRAY_MAX_LENGTH }/#{obj.length}")
    when (Hash === obj and obj.length > ARRAY_MAX_LENGTH)
      remove_long_items(obj.collect.compact[0..ARRAY_MAX_LENGTH-2] << ["TRUNCATED", "at #{ ARRAY_MAX_LENGTH }/#{obj.length}"])
    when (String === obj and obj.length > STRING_MAX_LENGTH)
      obj[0..STRING_MAX_LENGTH-1] << " TRUNCATED at #{STRING_MAX_LENGTH}/#{obj.length}"
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

  def self.digest(text)
    Digest::MD5.hexdigest(text)
  end

  def self.sample_large_obj(obj, max = 100)
    length = obj.length
    head = obj[0..max/2]
    tail = obj[-max/2..-1]
    middle = (1..9).to_a.collect{|i| pos = (length / 10) * i + i; obj[pos-1..pos+1]}.flatten 
    if Array === obj 
      head + middle + tail + ["LENGTH: #{obj.length}"]
    else
      head << "..." << middle*"," << "..." << tail << "(#{obj.length})"
    end
  end

  HASH2MD5_MAX_STRING_LENGTH = 1000
  HASH2MD5_MAX_ARRAY_LENGTH = 100
  def self.hash2md5(hash)
    return "" if hash.nil? or hash.empty?

    str = ""
    keys = hash.keys
    keys = keys.clean_annotations if keys.respond_to? :clean_annotations
    keys = keys.sort_by{|k| k.to_s}

    if hash.respond_to? :unnamed
      unnamed = hash.unnamed
      hash.unnamed = true 
    end


    keys.each do |k|
      next if k == :monitor or k == "monitor" or k == :in_situ_persistence or k == "in_situ_persistence"
      _v = hash[k]
      _k = k
      v = TSV === _v ? _v : Annotated.purge(_v)
      k = Annotated.purge(k)

      case
      when TrueClass === v
        str << k.to_s << "=>true" 
      when FalseClass === v
        str << k.to_s << "=>false" 
      when TSV === v
        str << k.to_s << "=>" << obj2md5(v)
      when Hash === v
        str << k.to_s << "=>" << hash2md5(v)
      when Symbol === v
        str << k.to_s << "=>" << v.to_s
      when (String === v and v.length > HASH2MD5_MAX_STRING_LENGTH)
        #str << k.to_s << "=>" << v[0..HASH2MD5_MAX_STRING_LENGTH] << v[v.length-3..v.length+3] << v[-3..-1] << "; #{ v.length }"
        str << k.to_s << "=>" << v[0..HASH2MD5_MAX_STRING_LENGTH] << "; #{ v.length }"
      when String === v
        str << k.to_s << "=>" << v
      when (Array === v and v.length > HASH2MD5_MAX_ARRAY_LENGTH)
        #str << k.to_s << "=>[" << (v[0..HASH2MD5_MAX_ARRAY_LENGTH] + v[v.length-3..v.length+3] + v[-3..-1]) * "," << "; #{ v.length }]"
        str << k.to_s << "=>[" << v[0..HASH2MD5_MAX_ARRAY_LENGTH] * "," << "; #{ v.length }]"
      when TSV::Parser === v
        str << remove_long_items(v)
      when Array === v
        str << k.to_s << "=>[" << v * "," << "]"
      when File === v
        str << k.to_s << "=>[File:" << v.path << "]"
      else
        begin
          v_ins = v.inspect
        rescue
          v_ins = "#Object:" << v.object_id.to_s
        end

        case
        when v_ins =~ /:0x0/
          str << k.to_s << "=>" << v_ins.sub(/:0x[a-f0-9]+@/,'')
        else
          str << k.to_s << "=>" << v_ins
        end
      end

      if _v and defined? Annotated and Annotated === _v and not (defined? AssociationItem and AssociationItem === _v)
        info = _v.info
        info = Annotated.purge(info)
        str << "_" << hash2md5(info) 
      end
    end
    hash.unnamed = unnamed if hash.respond_to? :unnamed

    if str.empty?
      ""
    else
      digest(str)
    end
  end

  def self.txt_digest_str(txt)
    "digest: " << digest(txt)
  end

  def self.mtime_str(path)
    path = path.find if Path === path
    if File.exists? path
      "mtime: " << File.mtime(path).to_s
    else
      "mtime: not present"
    end
  end


  def self.step_file?(path)
    return true if defined?(Step) && Step === path.resource
    return false unless path.include?('.files/')
    parts = path.split("/")
    job = parts.select{|p| p =~ /\.files$/}.first
    if job
      i = parts.index job
      begin
        workflow, task = parts.values_at i - 2, i - 1
        return Kernel.const_get(workflow).tasks.include? task.to_sym
      rescue
      end
    end
    false
  end

  def self.obj2str(obj)
    _obj = obj
    obj = Annotated.purge(obj) if Annotated === obj

    str = case obj
          when nil
            'nil'
          when TrueClass
            'true'
          when FalseClass
            'false'
          when Hash
            "{"<< obj.collect{|k,v| obj2str(k) + '=>' << obj2str(v)}*"," << "}"
          when Symbol 
            obj.to_s
          when (defined?(Path) and Path)
            if defined?(Step) && Open.exists?(Step.info_file(obj))
              obj2str(Workflow.load_step(obj))
            elsif step_file?(obj)
              "Step file: " + obj
            else
              if obj.exists?
                if obj.directory?
                  files = obj.glob("**/*")
                  "directory: #{Misc.fingerprint(files)}"
                else
                  "file: " << Open.realpath(obj) << "--" << mtime_str(obj)
                end
              else
                obj + " (file missing)"
              end
            end
          when String
            if Misc.is_filename?(obj) and ! %w(. ..).include?(obj)
              obj2str Path.setup(obj.dup)
            else
              obj = obj.chomp if String === obj
              if obj.length > HASH2MD5_MAX_STRING_LENGTH
                sample_large_obj(obj, HASH2MD5_MAX_STRING_LENGTH) << "--" << txt_digest_str(obj)
              else
                obj
              end
            end
          when Array
            if obj.length > HASH2MD5_MAX_ARRAY_LENGTH
              "[" << sample_large_obj(obj, HASH2MD5_MAX_ARRAY_LENGTH).collect{|v| obj2str(v)} * "," << "]"
            else
              "[" << obj.collect{|v| obj2str(v)} * "," << "]"
            end
          when TSV::Parser
            remove_long_items(obj)
          when File 
            if obj.respond_to? :filename and obj.filename
              if defined?(Step) && Open.exists?(Step.info_file(obj.filename))
                obj2str(Workflow.load_step(obj.filename))
              else
                "<IO:" << obj.filename << "--" << mtime_str(obj.filename) << ">"
              end
            else
              "<IO:" << obj.path << "--" << mtime_str(obj.path) << ">"
            end
          when (defined? Step and Step)
            "<IO:" << obj.short_path << ">"
          when IO
            if obj.respond_to? :filename and obj.filename
              if defined?(Step) && Open.exists?(Step.info_file(obj.filename))
                obj2str(Workflow.load_step(obj.filename))
              else
                "<IO:" << obj.filename << "--" << mtime_str(obj.filename) << ">"
              end
            else

              if obj.respond_to? :obj2str
                obj.obj2str
              else
                class << obj
                  attr_accessor :obj2str
                end
                obj.obj2str = obj.inspect + rand(1000000).to_s
              end
            end
          else
            if obj.respond_to? :filename and obj.filename
              "<IO:" << obj.filename << "--" << mtime_str(obj.filename) << ">"
            else
              obj_ins = obj.inspect
              obj_str = if obj_ins =~ /:0x0/
                obj_ins.gsub(/:0x[a-f0-9]+/,'')
              else
                obj_ins
              end
            end
          end

    if defined? Annotated and Annotated === _obj and not (defined? AssociationItem and AssociationItem === _obj)
      info = Annotated.purge(_obj.info)
      str << "_" << obj2str(info) 
    end

    str
  end

  
  def self.obj2digest(obj)
    str = obj2str(obj)

    if str.empty?
      ""
    else
      digest(str)
    end
  end

  def self.obj2md5(obj)
    obj2digest(obj)
  end

  def self.file2md5(file)
    if File.exists?(file + '.md5')
      Open.read(file + '.md5')
    else
      md5 = CMD.cmd("md5sum '#{file}'").read.strip.split(" ").first
      begin
        Open.write(file + '.md5', md5)
      rescue
      end
      md5
    end
  end

  def self.get_filename(obj)
    if obj.respond_to? :filename
      obj.filename
    elsif obj.respond_to? :path
      obj.path
    elsif (Path === obj || (String === obj && Misc.is_filename?(obj)))
      obj
    else
      nil
    end
  end

  def self.scan_version_text(text, cmd = nil)
    cmd = "NOCMDGIVE" if cmd.nil? || cmd.empty?
    m = text.match(/(?:version.*?|#{cmd}.*?|#{cmd.to_s.split(/[-_.]/).first}.*?|v)((?:\d+\.)*\d+(?:-[a-z_]+)?)/i)
    return nil if m.nil?
    m[1]
  end
end
