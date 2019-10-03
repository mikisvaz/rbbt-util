module Workflow
  attr_accessor :example_dir

  def example_dir
    @example_dir ||= self.libdir.examples
  end

  def examples
    return {} unless self.libdir.examples.exists?
    examples = {}
    example_dir.glob("*/*").each do |example_dir|
      example = File.basename(example_dir)
      task_name = File.basename(File.dirname(example_dir))
      examples[task_name] ||= []
      examples[task_name] << example
    end
    IndiferentHash.setup examples
    examples
  end

  def example(task_name, example)
    task_info(task_name.to_sym)[:input_types].collect do |input,type|
      next unless example_dir[task_name][example][input].exists?
      [input, type, example_dir[task_name][example][input].find]
    end.compact
  end

  def self.load_inputs(dir, input_names, input_types)
    inputs = {}
    if File.exists?(dir) && ! File.directory?(dir)
      Log.debug "Loading inputs from #{dir}, not a directory trying as tar.gz"
      tarfile = dir
      digest = CMD.cmd("md5sum '#{tarfile}'").read.split(" ").first
      tmpdir = Rbbt.tmp.input_bundle[digest].find
      Misc.untar(tarfile, tmpdir) unless File.exists? tmpdir
      files = tmpdir.glob("*")
      if files.length == 1 && File.directory?(files.first)
        tmpdir = files.first
      end
      load_inputs(tmpdir, input_names, input_types)
    else
      dir = Path.setup(dir.dup)
      input_names.each do |input|
        file = dir[input].find
        file = dir.glob(input.to_s + ".*").reject{|f| f =~ /\.md5$/}.first if file.nil? or not file.exists?
        Log.debug "Trying #{ input }: #{file}"
        next unless file and file.exists?


        case input_types[input]
        when :file
          Log.debug "Pointing #{ input } to #{file}"
          if file =~ /\.read$/
            inputs[input.to_sym]  = Open.read(file)
          else
            inputs[input.to_sym]  = Open.realpath(file)
          end
        when :text
          Log.debug "Reading #{ input } from #{file}"
          inputs[input.to_sym]  = Open.read(file)
        when :array
          Log.debug "Reading array #{ input } from #{file}"
          inputs[input.to_sym]  = Open.read(file).split("\n")
        when :tsv
          Log.debug "Opening tsv #{ input } from #{file}"
          inputs[input.to_sym]  = TSV.open(file)
        when :boolean
          inputs[input.to_sym]  = (file.read.strip == 'true')
        else
          Log.debug "Loading #{ input } from #{file}"
          inputs[input.to_sym]  = file.read.strip
        end

      end
      IndiferentHash.setup(inputs)
    end
  end

  def example_inputs(task_name, example)
    inputs = {}
    IndiferentHash.setup(inputs)
    example(task_name, example).each do |input,type,file|

      case type
      when :tsv, :array, :text
        Log.debug "Pointing #{ input } to #{file}"
        inputs[input.to_sym]  = file
      when :boolean
        inputs[input.to_sym]  = (file.read.strip == 'true')
      else
        Log.debug "Loading #{ input } from #{file}"
        inputs[input.to_sym]  = file.read.strip
      end
    end
    inputs
  end


  def example_step(task_name, example, new_inputs = {})
    inputs = example_inputs(task_name, example)

    if new_inputs and new_inputs.any?
      IndiferentHash.setup(new_inputs)
      inputs = inputs.merge(new_inputs)
    end

    self.job(task_name, example, inputs)
  end
end
