module Misc
  COLOR_LIST = %w(#BC80BD #CCEBC5 #FFED6F #8DD3C7 #FFFFB3 #BEBADA #FB8072 #80B1D3 #FDB462 #B3DE69 #FCCDE5 #D9D9D9)

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

  def self.format_seconds(time, extended = false)
    seconds = time.to_i
    str = [seconds/3600, seconds/60 % 60, seconds % 60].map{|t|  "%02i" % t }.join(':')
    str << ".%02i" % ((time - seconds) * 100) if extended
    str
  end

  def self.format_paragraph(text, size = 80, indent = 0, offset = 0)
    i = 0
    size = size + offset + indent
    re = /((?:\n\s*\n\s*)|(?:\n\s*(?=\*)))/
      text.split(re).collect do |paragraph|
      i += 1
      str = if i % 2 == 1
              words = paragraph.gsub(/\s+/, "\s").split(" ")
              lines = []
              line = " "*offset
              word = words.shift
              while word
                word = word[0..size-indent-offset-4] + '...' if word.length >= size - indent - offset
                while word and Log.uncolor(line).length + Log.uncolor(word).length <= size - indent
                  line << word << " "
                  word = words.shift
                end
                offset = 0
                lines << ((" " * indent) << line[0..-2])
                line = ""
              end
              (lines * "\n")
            else
              paragraph
            end
      offset = 0
      str
      end*""
  end

  def self.format_definition_list_item(dt, dd, size = 80, indent = 20, color = :yellow)
    dd = "" if dd.nil?
    dt = Log.color color, dt if color
    dt = dt.to_s  unless dd.empty?
    len = Log.uncolor(dt).length

    if indent < 0
      text = format_paragraph(dd, size, indent.abs-1, 0)
      text = dt << "\n" << text
    else
      offset = len - indent
      offset = 0 if offset < 0
      text = format_paragraph(dd, size, indent.abs+1, offset)
      text[0..len-1] = dt
    end
    text
  end

  def self.format_definition_list(defs, size = 80, indent = 20, color = :yellow, sep = "\n\n")
    entries = []
    defs.each do |dt,dd|
      text = format_definition_list_item(dt,dd,size,indent,color)
      entries << text
    end
    entries * sep 
  end

  def self.camel_case(string)
    return string if string !~ /_/ && string =~ /[A-Z]+.*/
      string.split(/_|(\d+)/).map{|e| 
        (e =~ /^[A-Z]{2,}$/ ? e : e.capitalize) 
      }.join
  end

  def self.camel_case_lower(string)
      string.split('_').inject([]){ |buffer,e| 
        buffer.push(buffer.empty? ? e.downcase : (e =~ /^[A-Z]{2,}$/ ? e : e.capitalize)) 
      }.join
  end

  def self.snake_case(string)
    return nil if string.nil?
    string = string.to_s if Symbol === string
    string.
      gsub(/([A-Z]{2,})([A-Z][a-z])/,'\1_\2').
      gsub(/([a-z])([A-Z])/,'\1_\2').
      gsub(/\s/,'_').gsub(/[^\w_]/, '').
      split("_").collect{|p| p.match(/[A-Z]{2,}/) ? p : p.downcase } * "_"
  end

  # source: https://gist.github.com/ekdevdes/2450285
  # author: Ethan Kramer (https://github.com/ekdevdes)
  def self.humanize(value, options = {})
    if options.empty?
      options[:format] = :sentence
    end

    values = value.to_s.split('_')
    values.each_index do |index|
      # lower case each item in array
      # Miguel Vazquez edit: Except for acronyms
      values[index].downcase! unless values[index].match(/[a-zA-Z][A-Z]/)
    end
    if options[:format] == :allcaps
      values.each do |value|
        value.capitalize!
      end

      if options.empty?
        options[:seperator] = " "
      end

      return values.join " "
    end

    if options[:format] == :class
      values.each do |value|
        value.capitalize!
      end

      return values.join ""
    end

    if options[:format] == :sentence
      values[0].capitalize! unless values[0].match(/[a-zA-Z][A-Z]/)

      return values.join " "
    end

    if options[:format] == :nocaps
      return values.join " "
    end
  end

  def self.fixascii(string)
    if string.respond_to?(:encode)
      self.fixutf8(string).encode("ASCII-8BIT") 
    else
      string
    end
  end

  def self.to_utf8(string)
    string.encode("UTF-16BE", :invalid => :replace, :undef => :replace, :replace => "?").encode('UTF-8')
  end

  def self.fixutf8(string)
    return nil if string.nil?
    return string if string.respond_to?(:encoding) && string.encoding.to_s == "UTF-8" && (string.respond_to?(:valid_encoding?) && string.valid_encoding?) ||
                     (string.respond_to?(:valid_encoding) && string.valid_encoding)

    if string.respond_to?(:encode)
      string.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: '')
    else
      require 'iconv'
      @@ic ||= Iconv.new('UTF-8//IGNORE', 'UTF-8')
      @@ic.iconv(string)
    end
  end

  def self.humanize_list(list)
    return "" if list.empty?
    if list.length == 1
      list.first
    else
      list[0..-2].collect{|e| e.to_s} * ", " << " and " << list[-1].to_s
    end
  end

  def self.parse_sql_values(txt)
    io = StringIO.new txt.strip

    values = []
    fields = []
    current = nil
    quoted = false
    while c = io.getc
      if quoted
        if c == "'"
          quoted = false
        else
          current << c
        end
      else
        case c
        when "("
          current = ""
        when ")"
          fields << current
          values << fields
          fields = []
          current = nil
        when ','
          if not current.nil?
            fields << current
            current = ""
          end
        when "'"
          quoted = true
        when ";"
          break
        else
          current << c
        end
      end
    end
    values
  end

end
