require 'rbbt/util/color'

module Colorize
  def self.from_name(color)
    return color if color =~ /^#?[0-9A-F]+$/i
    case color.to_s
    when "white"
      '#000'
    when "black"
      '#fff'
    end
  end

  def self.continuous(array, start = :white, eend = :black) 
    start_color = Color.new from_name(start)
    end_color = Color.new from_name(eend)

    array = array.collect{|v| v.to_f}
    max = array.max
    min = array.min
    range = max - min
    array.collect do |v|
      start_color.blend end_color, (v - min) / range
    end
  end

  def self.distinct(array)
    colors = Rbbt.share.color["diverging_colors.hex"].list.collect{|c| Color.new c}

    num = array.uniq.length
    times = num / 12

    all_colors = colors.dup
    times.times do
      all_colors.concat  colors.collect{|n| n.darken(0.2) }
    end

    value_color = Hash[*array.uniq.zip(all_colors).flatten]

    value_color.values_at *array
  end

  def self.tsv(tsv)
    values = tsv.values.flatten
    if Fixnum === values.first or (values.first.to_f != 0 and values[0] != "0")
      value_colors = Misc.process_to_hash(values){continuous(values)}
    else
      value_colors = Misc.process_to_hash(values){distinct(values)}
    end

    if tsv.type == :single
      Hash[*tsv.keys.zip(value_colors.values_at(*values)).flatten]
    else
      Hash[*tsv.keys.zip(values.collect{|vs| value_colors.values_at(*vs)}).flatten]
    end
  end
end
