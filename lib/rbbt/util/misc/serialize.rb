module Misc
  def self.load_yaml(yaml)
    case yaml
    when IO, StringIO
      if YAML.respond_to?(:unsafe_load)
        YAML.unsafe_load(yaml)
      else
        YAML.load(yaml)
      end
    when (defined?(Path) && Path)
      yaml.open do |io|
        load_yaml(io)
      end
    when String
      if Misc.is_filename?(yaml)
        File.open(yaml) do |io|
          load_yaml(io)
        end
      else
        load_yaml(StringIO.new(yaml))
      end
    else
      raise "Unkown YAML object: #{Misc.fingerprint yaml}"
    end
  end
end
