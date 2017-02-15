
module Misc

  Log2Multiplier = 1.0 / Math.log(2.0)
  Log10Multiplier = 1.0 / Math.log(10.0)
  def self.log2(x)
    Math.log(x) * Log2Multiplier
  end

  def self.log10(x)
    Math.log(x) * Log10Multiplier
  end

  def self.max(list)
    max = nil
    list.each do |v|
      next if v.nil?
      max = v if max.nil? or v > max
    end
    max
  end

  def self.sum(list)
    list.compact.inject(0.0){|acc,e| acc += e}
  end

  def self.mean(list)
    sum(list) / list.compact.length
  end

  def self.median(array)
    sorted = array.sort
    len = sorted.length
    (sorted[(len - 1) / 2] + sorted[len / 2]) / 2.0
  end


  def self.sd(list)
    return nil if list.length < 3
    mean = mean(list)
    list = list.compact
    list_length = list.length

    total_square_distance = 0
    list.each do |value|
      distance = value - mean
      total_square_distance += distance * distance
    end

    variance = total_square_distance / (list_length - 1)
    Math.sqrt(variance)
    #Math.sqrt(list.compact.inject(0.0){|acc,e| d = e - mean; acc += d * d; acc}) / (list.compact.length - 1)
  end

  def self.counts(array)
    counts = {}
    array.each do |e|
      counts[e] ||= 0
      counts[e] += 1
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

  def self.in_delta?(a, b, delta = 0.0001)
    (a.to_f - b.to_f).abs < delta
  end
end
