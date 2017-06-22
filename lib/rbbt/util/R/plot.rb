module R
  module SVG
    def self.ggplotSVG(data, script = nil, width = nil, height = nil, options = {})
      width ||= 3
      height ||= 3
      values = []

      options = options.dup

      sources = [:plot, Rbbt.share.Rlib["svg.R"].find(:lib), options[:source]].flatten.compact
      options.delete :source

      fast = options[:fast]

      if fast
        save_method = "rbbt.SVG.save.fast"
      else
        save_method = "rbbt.SVG.save"
      end

      if data
        data.each do |k,v|
          v = Array === v ? v : [v]
          next if v == "NA" or v.nil? or v.include? "NA" or v.include? nil
          values = v
          break
        end
        values = [values] unless Array === values
        field_classes = values.collect do |v| 
          v = v.first if Array === v
          case v
          when FalseClass, TrueClass
            "'logical'"
          when Numeric
            "'numeric'"
          when String
            if v.strip =~ /^[-+]?[\d\.]+$/
              "'numeric'"
            else
              "'character'"
            end
          when Symbol
            "'factor'"
          else
            ":NA"
          end
        end
        if field_classes.any?
          options[:R_open] ||= "colClasses=c('character'," + field_classes * ", " + ')'
        else
          options[:R_open] ||= "colClasses=c('character')"
        end

        TmpFile.with_file nil, true, :extension => 'svg' do |tmpfile|

          data.R <<-EOF, sources, options
  plot = { #{script} }

  #{save_method}('#{tmpfile}', plot, width = #{R.ruby2R width}, height = #{R.ruby2R height})
  data = NULL
          EOF

          Open.read(tmpfile).gsub(/(glyph\d+-\d+)/, '\1-' + File.basename(tmpfile))

        end
      else

        TmpFile.with_file nil, true, :extension => 'svg' do |tmpfile|
          R.run <<-EOF, sources, options
  plot = { #{script} }

  #{save_method}('#{tmpfile}', plot, width = #{R.ruby2R width}, height = #{R.ruby2R height})
  data = NULL
          EOF
          Open.read(tmpfile).gsub(/(glyph\d+-\d+)/, '\1-' + File.basename(tmpfile))
        end
      end
    end
  end

  module PNG
    def self.ggplotPNG(filename, data, script = nil, width = nil, height = nil, options = {})
      width ||= 3
      height ||= 3
      values = []

      sources = [:plot, options[:source]].flatten.compact

      data.each do |k,v|
        v = Array === v ? v : [v]
        next if v == "NA" or v.nil? or v.include? "NA" or v.include? nil
        values = v
        break
      end
      values = [values] unless Array === values
      field_classes = values.collect do |v| 
        case v
        when FalseClass, TrueClass
          "'logical'"
        when Numeric
          "'numeric'"
        when String
          if v.strip =~ /^[-+]?[\d\.]+$/
            "'numeric'"
          else
            "'character'"
          end
        when Symbol
          "'factor'"
        else
          ":NA"
        end
      end
      options[:R_open] ||= "colClasses=c('character'," + field_classes * ", " + ')'

      data.R <<-EOF, :plot, options
plot = { #{script} }

ggsave('#{filename}', plot, width = #{R.ruby2R width}, height = #{R.ruby2R height})
data = NULL
      EOF
    end

    def self.plot(filename, data = nil, script = nil, width = nil, height = nil, options = {}, &block)
      width ||= 200
      height ||= 200
      values = []

      script ||= ""
      if block_given?
        s = StringIO.new
        class << s
          def method_missing(name, *args)
            name = name.to_s
            if name[-1] == '='
              arg = args.first
              value = if String === arg
                        arg
                      else
                        R.ruby2R arg
                      end
              add("" << name[0..-2] << "=" << value)
            else
              args_strs = []
              args.each do |arg|
                value = if String === arg
                          arg
                        else
                          R.ruby2R arg
                        end
                args_strs << value
              end
              add("" << name << "(" << args_strs * ", " << ")")
            end
          end

          def add(line)
            self.write line << "\n"
          end
        end
        block.call(s)
        s.rewind
        script << "\n" << s.read
      end
      sources = [:plot, options[:source]].flatten.compact
      
      if data
        data.each do |k,v|
          v = Array === v ? v : [v]
          next if v == "NA" or v.nil? or v.include? "NA" or v.include? nil
          values = v
          break
        end 

        values = [values] unless values.nil? or Array === values

        field_classes = values.collect do |v| 
          case v
          when FalseClass, TrueClass
            "'logical'"
          when Numeric
            "'numeric'"
          when String
            if v.strip =~ /^[-+]?[\d\.]+$/
              "'numeric'"
            else
              "'character'"
            end
          when Symbol
            "'factor'"
          else
            ":NA"
          end
        end

        options[:R_open] ||= "colClasses=c('character'," + field_classes * ", " + ')' if field_classes.any?

        data.R <<-EOF, :plot, options
  png("#{ filename }", #{ width }, #{ height })
  { #{script} }
  dev.off()
  data = NULL
        EOF
      else
        R.run <<-EOF, :plot, options
  png("#{ filename }", #{ width }, #{ height })
  { #{script} }
  dev.off()
        EOF
      end
    end
  end
end
