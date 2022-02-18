module Workflow
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
        file = dir.glob(input.to_s + ".*").reject{|f| f =~ /\.md5$/}.first if file.nil? or not (File.symlink?(file) || file.exists?)
        Log.debug "Trying #{ input }: #{file}"
        next unless file and (File.symlink?(file) || file.exists?)

        type = input_types[input]

        type = :io if file.split(".").last == 'as_io'

        type = :path if file.split(".").last == 'as_path'

        type = :nofile if file.split(".").last == 'nofile'

        case type
        when :nofile
          inputs[input.to_sym]  = Open.realpath(file)
        when :path
          inputs[input.to_sym]  = Open.realpath(Open.read(file).strip)
        when :io
          inputs[input.to_sym] = Open.open(Open.realpath(file))
        when :file, :binary
          Log.debug "Pointing #{ input } to #{file}"
          if file =~ /\.yaml/
            inputs[input.to_sym]  = YAML.load(Open.read(file))
          else
            if File.symlink?(file)
              link_target = File.expand_path(File.readlink(file), File.dirname(file))
              inputs[input.to_sym]  = link_target
            else
              inputs[input.to_sym]  = Open.realpath(file)
            end
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
      inputs = IndiferentHash.setup(inputs)

      dir.glob("*#*").each do |od|
        name = File.basename(od)
        value = Open.read(od)
        Log.debug "Loading override dependency #{ name } as #{value}"
        inputs[name] = value.chomp
      end

      inputs
    end
  end

  def task_inputs_from_directory(task_name, directory)
    task_info = self.task_info(task_name)
    Workflow.load_inputs(directory, task_info[:inputs], task_info[:input_types])
  end

  def job_for_directory_inputs(task_name, directory, jobname = nil)
    inputs = task_inputs_from_directory(task_name, directory)
    job(task_name, jobname, inputs)
  end

end

class Step
  def self.save_inputs(inputs, input_types, input_options, dir)
    inputs.each do |name,value|
      type = input_types[name]
      type = type.to_s if type
      path = File.join(dir, name.to_s)

      path = path + '.as_io' if (IO === value || Step === value) && ! (input_options[name] && input_options[name][:nofile])
      Log.debug "Saving job input #{name} (#{type}) into #{path}"

      case
      when IO === value
        Open.write(path, value.to_s)
      when Step === value
        Open.ln_s(value.path, path)
      when type.to_s == "binary"
        if String === value && File.exists?(value)
          value = File.expand_path(value)
          Open.ln_s(value, path)
        elsif String === value && Misc.is_filename?(value, false)
          Open.write(path + '.as_path' , value)
        else
          Open.write(path, value, :mode => 'wb')
        end
      when type.to_s == "file"
        if String === value && File.exists?(value)
          value = File.expand_path(value)
          Open.ln_s(value, path)
        else
          value = value.collect{|v| v = "#{v}" if Path === v; v } if Array === value
          value = "#{value}" if Path === value
          Open.write(path + '.yaml', value.to_yaml)
        end
      when Array === value
        Open.write(path, value.collect{|v| Step === v ? v.path : v.to_s} * "\n")
      when IO === value
        if value.filename && String === value.filename && File.exists?(value.filename)
          Open.ln_s(value.filename, path)
        else
          Open.write(path, value)
        end
      else
        Open.write(path, value.to_s)
      end
    end.any?
  end

  def self.save_job_inputs(job, dir, options = nil)
    options = IndiferentHash.setup options.dup if options

    task_name = Symbol === job.overriden ? job.overriden : job.task_name
    workflow = job.workflow
    workflow = Kernel.const_get workflow if String === workflow
    if workflow
      task_info = IndiferentHash.setup(workflow.task_info(task_name))
      input_types = IndiferentHash.setup(task_info[:input_types])
      input_options = IndiferentHash.setup(task_info[:input_options])
      task_inputs = IndiferentHash.setup(task_info[:inputs])
      input_defaults = IndiferentHash.setup(task_info[:input_defaults])
    else
      task_info = IndiferentHash.setup({})
      input_types = IndiferentHash.setup({})
      task_inputs = IndiferentHash.setup({})
      task_options = IndiferentHash.setup({})
      input_defaults = IndiferentHash.setup({})
    end

    inputs = IndiferentHash.setup({})
    real_inputs = job.real_inputs || job.info[:real_inputs]
    job.recursive_inputs.zip(job.recursive_inputs.fields).each do |value,name|
      next unless task_inputs.include? name.to_sym
      next unless real_inputs.include? name.to_sym
      next if options && ! options.include?(name)
      next if value.nil?
      next if input_defaults[name] == value
      inputs[name] = value
    end

    if options && options.include?('override_dependencies')
      inputs.merge!(:override_dependencies => open[:override_dependencies])
      input_types = IndiferentHash.setup(input_types.merge(:override_dependencies => :array))
    end

    save_inputs(inputs, input_types, input_options, dir)

    inputs.keys
  end
end
