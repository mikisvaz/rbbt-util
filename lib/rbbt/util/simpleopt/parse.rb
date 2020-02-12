require 'rbbt/util/simpleopt/accessor'

module SOPT
  def self.fix_shortcut(short, long)
    return short unless short and shortcuts.include?(short)

    current = shortcuts.select{|s,l| l == long}.collect{|s,l| s }.first
    return current if current

    chars = long.chars.to_a
    current = [chars.shift]
    short = current * ""

    if (shortcuts.include?(short) and not shortcuts[short] == long) 
      if long.index "-" or long.index "_"
        parts = long.split(/[_-]/)
        acc = parts.collect{|s| s[0] } * ""
        return acc unless shortcuts.include? acc
      elsif m = long.match(/(\d+)/)
        n = m[0]
        acc = long[0] + n
        return acc unless shortcuts.include? acc
      end
    end

    while shortcuts.include?(short) && shortcuts[short] != long 
      next_letter = chars.shift
      next_letter = chars.shift while %w(. - _).include?(next_letter)
      return nil if next_letter.nil?
      current << next_letter
      short = current * ""
    end

    return nil if shortcuts.include? short

    short
  end

  def self.register(short, long, asterisk, description)
    short = fix_shortcut(short, long)
    shortcuts[short] = long if short
    inputs << long
    input_shortcuts[long] = short
    input_descriptions[long] = description
    input_types[long] = asterisk ? :string : :boolean
  end

  def self.parse(opt_str)
    info = {}

    inputs = []
    if opt_str.include? "\n"
      re = /\n+/
    else
      re = /:/
    end
    opt_str.split(re).each do |entry|
      entry.strip!
      next if entry.empty?
      names, _sep, description = entry.partition /\s+/
      short, long, asterisk = names.match(/\s*(?:-(.+))?(?:--(.+?))([*])?$/).values_at 1,2,3 

      inputs << long
      register short, long, asterisk, description
    end
    inputs
  end
end
