#require 'rbbt/util/color'
#
#module Colorize
#  def self.from_name(color)
#    return color if color =~ /^#?[0-9A-F]+$/i
#    colors = Rbbt.share.color.color_names.tsv :type => :single
#    case color.to_s
#    when "white"
#      '#000'
#    when "black"
#      '#fff'
#    when 'green'
#      colors["green3"] 
#    when 'red'
#      colors["red3"] 
#    when 'yellow'
#      colors["gold1"] 
#    when 'blue'
#      colors["RoyalBlue"] 
#    else
#      colors[color.to_s] || color
#    end
#  end
#
#  def self.continuous(array, start = "#40324F", eend = "#EABD5D", percent = false) 
#    start_color = Color.new from_name(start)
#    end_color = Color.new from_name(eend)
#
#    if percent
#      array = array.collect{|v| n = v.to_f; n = n > 100 ? 100 : n; n < 0.001 ? 0.001 : n}
#    else
#      array = array.collect{|v| n = v.to_f; } 
#    end
#    max = array.max
#    min = array.min
#    range = max - min
#    array.collect do |v|
#      ratio = (v.to_f-min) / range
#      start_color.blend end_color, ratio
#    end
#  end
#  
#  def self.gradient(array, value, start = :green, eend = :red, percent = false)
#    index = array.index value
#    colors = continuous(array, start, eend, percent)
#    colors[index]
#  end
#
#  def self.rank_gradient(array, value, start = :green, eend = :red, percent = false)
#    index = array.index value
#    sorted = array.sort
#    array = array.collect{|e| sorted.index e}
#    colors = continuous(array, start, eend, percent)
#    colors[index]
#  end
#
#
#  def self.distinct(array)
#    colors = Rbbt.share.color["diverging_colors.hex"].list.collect{|c| Color.new c}
#
#    num = array.uniq.length
#    times = num / 12
#
#    all_colors = colors.dup
#    factor = 0.3 / times
#    times.times do
#      all_colors.concat  colors.collect{|n| n.darken(factor) }
#    end
#
#    value_color = Hash[*array.uniq.zip(all_colors).flatten]
#
#    value_color.values_at *array
#  end
#
#  def self.tsv(tsv, options = {})
#    values = tsv.values.flatten
#    if Numeric === values.first or (values.first.to_f != 0 and values[0] != "0" and values[0] != "0.0")
#      value_colors = Misc.process_to_hash(values){continuous(values)}
#    else
#      value_colors = Misc.process_to_hash(values){distinct(values)}
#    end
#
#    if tsv.type == :single
#      Hash[*tsv.keys.zip(value_colors.values_at(*values)).flatten]
#    else
#      Hash[*tsv.keys.zip(values.collect{|vs| value_colors.values_at(*vs)}).flatten]
#    end
#  end
#end
