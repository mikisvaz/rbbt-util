require_relative '../refactor'
Rbbt.require_instead 'scout/tsv'
#require 'csv'
#
#module TSV
#  def self.csv(obj, options = {}) 
#    options = Misc.add_defaults IndiferentHash.setup(options.dup), :headers => true, :type => :list
#    headers = options[:headers]
#
#    noheaders = ! headers
#
#    type = options.delete :type
#    cast = options.delete :cast
#    merge = options.delete :merge
#    key_field = options.delete :key_field
#    fields = options.delete :fields
#    
#    if key_field || fields
#      orig_type = type
#      type = :double
#      merge = true
#    end
#
#    options[:headers] = false
#
#    csv = case obj
#          when Path
#            CSV.read obj.find.open, **options
#          when String
#            if Open.remote?(obj)
#              CSV.read Open.open(obj), **options
#            elsif Misc.is_filename?(obj)
#              CSV.read obj, **options
#            else
#              CSV.new obj, **options
#            end
#          else
#            CSV.new obj, **options
#          end
#
#    tsv = if noheaders
#            TSV.setup({}, :key_field => nil, :fields => nil, :type => type)
#          else
#            key, *csv_fields = csv.shift
#            TSV.setup({}, :key_field => key, :fields => csv_fields, :type => type)
#          end
#
#    csv.each_with_index do |row,i|
#      if noheaders
#        key, values = ["row-#{i}", row]
#      else
#        key, *values = row
#      end
#      
#      if cast
#        values = values.collect{|v| v.send cast }
#      end
#
#      case type
#      when :double, :flat
#        tsv.zip_new(key, values)
#      when :single
#        tsv[key] = values.first
#      when :list
#        tsv[key] = values
#      end
#    end
#
#    if key_field || fields
#      tsv = tsv.reorder(key_field, fields, :zipped => true, :merge => true)
#      if tsv.type != orig_type
#        tsv = case orig_type
#              when :list
#                tsv.to_list
#              when :single
#                tsv.to_single
#              when :list
#                tsv.to_list
#              when :flat
#                tsv.to_flat
#              end
#      end
#    end
#
#    tsv
#  end
#end
