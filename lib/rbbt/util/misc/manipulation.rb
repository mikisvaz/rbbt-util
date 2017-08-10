module Misc
  def self.collapse_ranges(ranges)
    processed = []
    last = nil
    final = []
    ranges.sort_by{|range| range.begin }.each do |range|
      rbegin = range.begin
      rend = range.end
      if last.nil? or rbegin > last
        processed << [rbegin, rend]
        last = rend
      else
       new_processed = []
        processed.each do |pbegin,pend|
          if pend < rbegin
            final << [pbegin, pend]
          else
            eend = [rend, pend].max
            new_processed << [pbegin, eend]
            break
          end
        end
        processed = new_processed
        last = rend if rend > last
      end
    end

    final.concat processed
    final.collect{|b,e| (b..e)}
  end

  def self.total_length(ranges)
    self.collapse_ranges(ranges).inject(0) do |total,range| total += range.end - range.begin + 1 end
  end

  def self.sorted_array_hits(a1, a2)
    e1, e2 = a1.shift, a2.shift
    counter = 0
    match = []
    while true
      break if e1.nil? or e2.nil?
      case e1 <=> e2
      when 0
        match << counter
        e1, e2 = a1.shift, a2.shift
        counter += 1
      when -1
        while not e1.nil? and e1 < e2
          e1 = a1.shift 
          counter += 1
        end
      when 1
        e2 = a2.shift
        e2 = a2.shift while not e2.nil? and e2 < e1
      end
    end
    match
  end

  def self.intersect_sorted_arrays(a1, a2)
    e1, e2 = a1.shift, a2.shift
    intersect = []
    while true
      break if e1.nil? or e2.nil?
      case e1 <=> e2
      when 0
        intersect << e1
        e1, e2 = a1.shift, a2.shift
      when -1
        e1 = a1.shift while not e1.nil? and e1 < e2
      when 1
        e2 = a2.shift
        e2 = a2.shift while not e2.nil? and e2 < e1
      end
    end
    intersect
  end

  def self.merge_sorted_arrays(a1, a2)
    e1, e2 = a1.shift, a2.shift
    new = []
    while true
      case
      when (e1 and e2)
        case e1 <=> e2
        when 0
          new << e1 
          e1, e2 = a1.shift, a2.shift
        when -1
          new << e1
          e1 = a1.shift
        when 1
          new << e2
          e2 = a2.shift
        end
      when e2
        new << e2
        new.concat a2
        break
      when e1
        new << e1
        new.concat a1
        break
      else
        break
      end
    end
    new
  end

  def self.binary_include?(array, elem)
    array.bsearch {|e| e >= elem} == elem
  end

end
