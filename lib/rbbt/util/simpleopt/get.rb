module SOPT
  def self.consume(args = ARGV)
    i = 0
    values = {}
    while i < args.length do
      current = args[i]
      break if current == "--"
      if m = current.match(/--?(.+?)(?:=(.+))?$/)
        key = $1
        value = $2

        input = inputs.include?(key)? key : shortcuts[key]

        if input.nil?
          i += 1
          next
        else
          args.delete_at i
        end
      else
        i += 1
        next
      end

      if input_types[input] == :string
        value = args.delete_at(i) if value.nil?
        values[input] = value
      else
        if value.nil? and %w(F false FALSE no).include?(args[i])
          Log.warn "Boolean values must are best specified as #{current}=[true|false], not #{ current } [true|false]. Token '#{args[i]}' following '#{current}' automatically assigned as value" 
          value = args.delete_at(i)
        end
        values[input] = %w(F false FALSE no).include?(value)? false : true
      end
    end

    IndiferentHash.setup values

    values
  end
  
  def self.get(opt_str)
    SOPT.parse(opt_str)
    SOPT.consume(ARGV)
  end

  def self.require(options, *parameters)
    parameters.flatten.each do |parameter|
      raise ParameterException, "Parameter '#{ Log.color :blue, parameter }' not given" if options[parameter].nil?
    end
  end
end
