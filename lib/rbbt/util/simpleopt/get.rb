module SOPT
  GOT_OPTIONS= IndiferentHash.setup({})
  def self.current_options=(options)
    @@current_options = options
  end
  def self.consume(args = ARGV)
    i = 0
    @@current_options ||= {}
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
        @@current_options[input] = value
      else
        if value.nil? and %w(F false FALSE no).include?(args[i])
          Log.warn "Boolean values are best specified as #{current}=[true|false], not #{ current } [true|false]. Token '#{args[i]}' following '#{current}' automatically assigned as value" 
          value = args.delete_at(i)
        end
        @@current_options[input] = %w(F false FALSE no).include?(value)? false : true
      end
    end

    IndiferentHash.setup @@current_options
    GOT_OPTIONS.merge! @@current_options

    @@current_options
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
