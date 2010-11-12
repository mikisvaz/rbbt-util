require 'rbbt'
require 'open4'

module Misc

  def self.add_defaults(options, defaults = {})
    new_options = options.dup
    defaults.each do |key, value|
      new_options[key] = value if new_options[key].nil?
    end
    new_options
  end

  def self.string2hash(string)
    hash = {}
    string.split(',').each do |part|
      key, value = part.split('=>')
      hash[key] = value
    end
    
    hash
  end

  def self.sensiblewrite(path, content)
    if String === content
      File.open(path, 'w') do |f|  f.write content  end
    else
      File.open(path, 'w') do |f|  while ! content.eof; f.write content.readline; end  end
    end
  end

end



