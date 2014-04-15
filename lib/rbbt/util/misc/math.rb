
module Misc

  Log2Multiplier = 1.0 / Math.log(2.0)
  def self.log2(x)
    Math.log(x) * Log2Multiplier
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

  def self.sd(list)
    return nil if list.length < 3
    mean = mean(list)
    Math.sqrt(list.compact.inject(0.0){|acc,e| d = e - mean; acc += d * d}) / (list.compact.length - 1)
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
end
