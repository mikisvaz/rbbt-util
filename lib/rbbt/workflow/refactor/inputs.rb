class Step

  def self.save_inputs(inputs, input_types, dir)
    inputs.each do |name,value|
      next if value.nil?
      type = input_types[name]
      type = type.to_s if type

      Task.save_input(dir, name, type, value)
    end.any?
  end

end

module Workflow
  def self.load_inputs(dir, names, types)
    inputs = IndiferentHash.setup({})
    names.zip(types) do |name, type|
      filename = File.join(directory, name.to_s) 
      value = Task.load_input_from_file(filename, name, type)
      inputs[name] = value unless value.nil?
    end
    inputs
  end
end
