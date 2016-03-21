module R
  module SVG
    def self.ggplotSVG(data, script = nil, width = nil, height = nil, options = {})
      width ||= 3
      height ||= 3
      values = []

      options = options.dup

      sources = [:plot, Rbbt.share.Rlib["svg.R"].find(:lib), options[:source]].flatten.compact
      options.delete :source

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
          when Fixnum, Float
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

  rbbt.SVG.save('#{tmpfile}', plot, width = #{R.ruby2R width}, height = #{R.ruby2R height})
  data = NULL
          EOF

          Open.read(tmpfile).gsub(/(glyph\d+-\d+)/, '\1-' + File.basename(tmpfile))

        end
      else

        TmpFile.with_file nil, true, :extension => 'svg' do |tmpfile|
          R.run <<-EOF, sources, options
  plot = { #{script} }

  rbbt.SVG.save('#{tmpfile}', plot, width = #{R.ruby2R width}, height = #{R.ruby2R height})
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
        when Fixnum, Float
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

    def self.plot(filename, data, script = nil, width = nil, height = nil, options = {})
      width ||= 200
      height ||= 200
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
        when Fixnum, Float
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
    end
  end
end
