module Misc
  ARRAY_MAX_LENGTH = 1000
  STRING_MAX_LENGTH = ARRAY_MAX_LENGTH * 10

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
      obj.path || Misc.fingerprint([obj.task.name, obj.inputs])
    when TrueClass
      "true"
    when FalseClass
      "false"
    when Symbol
      ":" << obj.to_s
    when String
      if obj.length > 100
        "'" << obj.slice(0,30) << "<...#{obj.length}...>" << obj.slice(-10,30) << " " << "'"
      else 
        "'" << obj << "'"
      end
    when (defined? AnnotatedArray and AnnotatedArray)
      "<A: #{fingerprint Annotated.purge(obj)} #{fingerprint obj.info}>"
    when (defined? TSV and TSV::Parser)
      "<TSVStream:" + (obj.filename || "NOFILENAME") + "--" << Misc.fingerprint(obj.options) << ">"
    when IO
      (obj.respond_to?(:filename) and obj.filename ) ? "<IO:" + (obj.filename || obj.inspect) + ">" : obj.inspect
    when File
      "<File:" + obj.path + ">"
    when Array
      if (length = obj.length) > 10
        "[#{length}--" <<  (obj.values_at(0,1, length / 2, -2, -1).collect{|e| fingerprint(e)} * ",") << "]"
      else
        "[" << (obj.collect{|e| fingerprint(e) } * ",") << "]"
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
          new << k.to_s << '=>' << fingerprint(v) << ' '
        end
        if new.length > 1
           new[-1] =  "}"
        else
          new << '}'
        end
        new
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
      remove_long_items("TSV Stream: " + obj.filename + " -- " << Misc.fingerprint(obj.options))
    when TSV === obj
      remove_long_items((obj.all_fields || []) + obj.keys.sort)
    when (Array === obj and obj.length > ARRAY_MAX_LENGTH)
      remove_long_items(obj[0..ARRAY_MAX_LENGTH-2] << "TRUNCATED at #{ ARRAY_MAX_LENGTH } (#{obj.length})")
    when (Hash === obj and obj.length > ARRAY_MAX_LENGTH)
      remove_long_items(obj.collect.compact[0..ARRAY_MAX_LENGTH-2] << ["TRUNCATED", "at #{ ARRAY_MAX_LENGTH } (#{obj.length})"])
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

  def self.digest(text)
    Digest::MD5.hexdigest(text)
  end

  HASH2MD5_MAX_STRING_LENGTH = 1000
  HASH2MD5_MAX_ARRAY_LENGTH = 100
  def self.hash2md5(hash)
    return "" if hash.nil? or hash.empty?

    hash = Annotated.purge(hash)

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
        v_ins = v.inspect

        case
        when v_ins =~ /:0x0/
          str << k.to_s << "=>" << v_ins.sub(/:0x[a-f0-9]+@/,'')
        else
          str << k.to_s << "=>" << v_ins
        end
      end

      if defined? Annotated and Annotated === v and not (defined? AssociationItem and AssociationItem === v)
        info = Annotated.purge(v.info)
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

end
