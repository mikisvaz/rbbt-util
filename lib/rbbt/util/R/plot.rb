module R
  module SVG

    def self.plot(filename, data = nil, script = nil, width = nil, height = nil, options = {}, &block)
      width ||= 600
      height ||= 600
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
  rbbt.svg_plot("#{ filename }", width=#{ width }, height = #{ height }, function(){ #{script} })
  data = NULL
        EOF
      else
        R.run <<-EOF, :plot, options
  rbbt.svg_plot("#{ filename }", width=#{ width }, height = #{ height }, function(){ #{script} })
        EOF
      end
    end

    def self.ggplotSVG(*args)
      ggplot(*args)
    end

    def self.ggplot(data, script = nil, width = nil, height = nil, options = {})
      width ||= 2
      height ||= 2
      values = []

      options = options.dup

      sources = [:plot, :svg, options[:source]].flatten.compact
      options.delete :source

      entity_geom = options.delete :entity_geom

      field_classes = options[:field_classes]

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
          when Time
            "'Date'"
          when Symbol
            "'factor'"
          else
            ":NA"
          end
        end if field_classes.nil?

        if field_classes.any?
          options[:R_open] ||= "colClasses=c('character'," + field_classes * ", " + ')'
        else
          options[:R_open] ||= "colClasses=c('character')"
        end

        TmpFile.with_file nil, true, :extension => 'svg' do |tmpfile|

          if entity_geom
            data.R <<-EOF, sources, options
  plot = { #{script} }
  #{save_method}('#{tmpfile}', plot, width = #{R.ruby2R width}, height = #{R.ruby2R height}, entity.geom=#{R.ruby2R(entity_geom)}, data=data)
  data = NULL
            EOF
          else
            data.R <<-EOF, sources, options
  plot = { #{script} }

  #{save_method}('#{tmpfile}', plot, width = #{R.ruby2R width}, height = #{R.ruby2R height})
  data = NULL
            EOF
          end

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

    def self.eog(data, script = nil, width = nil, height = nil, options = {})
      TmpFile.with_file :extension => 'png' do |filename|
        ggplot(filename, data, script, width, height, options)
        `eog #{ filename }`
      end
    end

    def self.eog_plot(data, script = nil, width = nil, height = nil, options = {})
      TmpFile.with_file :extension => 'png' do |filename|
        plot(filename, data, script, width, height, options)
        `eog #{ filename }`
      end
    end

    def self.ggplotPNG(*args)
      ggplot(*args)
    end

    def self.ggplot(filename, data, script = nil, width = nil, height = nil, options = {})
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
rbbt.require('ggplot2')
plot = { #{script} }

ggsave('#{filename}', plot, width = #{R.ruby2R width}, height = #{R.ruby2R height})
data = NULL
      EOF
    end

    def self.plot(filename, data = nil, script = nil, width = nil, height = nil, options = {}, &block)
      width ||= 600
      height ||= 600
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
  rbbt.png_plot("#{ filename }", width=#{ width }, height = #{ height }, function(){ #{script} })
  data = NULL
        EOF
      else
        R.run <<-EOF, :plot, options
  rbbt.png_plot("#{ filename }", width=#{ width }, height = #{ height }, function(){ #{script} })
        EOF
      end
    end
  end

  module GIF

    def self.eog(data, frames = nil, script = nil, width = nil, height = nil, options = {})
      TmpFile.with_file :extension => 'gif' do |filename|
        ggplot(filename, data, frames, script, width, height, options)
        `eog #{ filename }`
      end
    end

    def self.eog_plot(data, frames = nil, script = nil, width = nil, height = nil, options = {})
      TmpFile.with_file :extension => 'gif' do |filename|
        plot(filename, data, frames, script, width, height, options)
        `eog #{ filename }`
      end
    end


    def self.ggplot(filename, data, frames = nil, script = nil, width = nil, height = nil, options = {})
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

      delay = options[:delay]
      delay = 10 if delay.nil?

      frames = data.keys if frames.nil?
      frames = (1..frames).to_a if Integer === frames

      data.R <<-EOF, :plot, options
frames = #{R.ruby2R frames}
for (frame in seq(1, length(frames))){
  frame.value = frames[frame]
  frame.str = sprintf("%06d", frame)
  plot = { #{script} }

  ggsave(paste('#{filename}', frame.str, 'tmp.png', sep="."), plot, width = #{R.ruby2R width}, height = #{R.ruby2R height})
}
data = NULL
      EOF

      CMD.cmd("convert #{filename}.*.tmp.png -set delay #{delay} -loop 0 #{filename} && rm #{filename}.*.tmp.png")
    end

    def self.plot(filename, data = nil, frames = nil, script = nil, width = nil, height = nil, options = {}, &block)
      width ||= 600
      height ||= 600
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
      
      delay = options[:delay]
      delay = 10 if delay.nil?

      frames = data.keys if frames.nil?
      frames = (1..frames).to_a if Integer === frames

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
frames = #{R.ruby2R frames}
for (frame in seq(1, length(frames))){
  frame.value = frames[frame]
  frame.str = sprintf("%06d", frame)
  plot = { #{script} }

  png(paste('#{filename}', frame.str, 'tmp.png', sep="."), #{ width }, #{ height })
  { #{script} }
  dev.off()
}
data = NULL
        EOF
      else
        R.run <<-EOF, :plot, options
frames = #{R.ruby2R frames}
for (frame in seq(1, length(frames))){
  frame.value = frames[frame]
  frame.str = sprintf("%06d", frame)
  png(paste('#{filename}', frame.str, 'tmp.png', sep="."), #{ width }, #{ height })
  { #{script} }
  dev.off()
}
        EOF
      end
      CMD.cmd("convert #{filename}.*.tmp.png -set delay #{delay} -loop 0 #{filename} && rm #{filename}.*.tmp.png")
    end
  end
end
