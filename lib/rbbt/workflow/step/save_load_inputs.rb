#module Workflow
#
#  def self.load_inputs(dir, input_names, input_types)
#    inputs = {}
#    if File.exist?(dir) && ! File.directory?(dir)
#      Log.debug "Loading inputs from #{dir}, not a directory trying as tar.gz"
#      tarfile = dir
#      digest = CMD.cmd("md5sum '#{tarfile}'").read.split(" ").first
#      tmpdir = Rbbt.tmp.input_bundle[digest].find
#      Misc.untar(tarfile, tmpdir) unless File.exist? tmpdir
#      files = tmpdir.glob("*")
#      if files.length == 1 && File.directory?(files.first)
#        tmpdir = files.first
#      end
#      load_inputs(tmpdir, input_names, input_types)
#    else
#      dir = Path.setup(dir.dup)
#      input_names.each do |input|
#        file = dir[input].find
#        file = dir.glob(input.to_s + ".*").reject{|f| f =~ /\.md5$/}.first if file.nil? or not (File.symlink?(file) || file.exists?)
#        Log.debug "Trying #{ input }: #{file}"
#        next unless file and (File.symlink?(file) || file.exists?)
#
#        type = orig_type = input_types[input]
#
#        type = :io if file.split(".").last == 'as_io'
#
#        type = :io_array if file.split(".").last == 'as_io_array'
#
#        type = :step if file.split(".").last == 'as_step'
#
#        type = :step_array if file.split(".").last == 'as_step_array'
#
#        type = :number_array if file.split(".").last == 'as_number_array'
#
#        type = :step_file if file.split(".").last == 'as_step_file'
#
#        type = :step_file_array if file.split(".").last == 'as_step_file_array'
#
#        type = :path if file.split(".").last == 'as_path'
#
#        type = :path_array if file.split(".").last == 'as_path_array'
#
#        type = :filename if file.split(".").last == 'as_filename'
#
#        type = :nofile if file.split(".").last == 'nofile'
#
#        case type
#        when :nofile
#          inputs[input.to_sym]  = Open.realpath(file)
#        when :path_array
#          inputs[input.to_sym]  = Open.read(file).strip.split("\n").collect{|p| Path.setup(p) }
#        when :path
#          inputs[input.to_sym]  = Path.setup(Open.read(file).strip.split("\n").first)
#        when :io
#          inputs[input.to_sym] = Open.open(Open.realpath(file))
#        when :io_array
#          inputs[input.to_sym] = Open.realpath(file).split("\n").collect{|f| Open.open(f)}
#        when :step_array
#          steps = Open.read(file).strip.split("\n").collect{|path| Workflow.load_step(path) }
#          inputs[input.to_sym] = steps
#        when :number_array
#          numbers = Open.read(file).strip.split("\n").collect{|num| num.to_f }
#          inputs[input.to_sym] = numbers
#        when :step
#          steps = Open.read(file).strip.split("\n").collect{|path| Workflow.load_step(path) }
#          inputs[input.to_sym] = steps.first
#        when :step_file
#          path = Open.read(file).strip
#          step_path, relative = path.match(/(.*)\.files\/(.*)/).values_at 1, 2
#          step = Step.new Path.setup(step_path).find
#          path = step.file(relative)
#          inputs[input.to_sym] = path
#        when :step_file_array
#          paths = Open.read(file).split("\n")
#          paths.each do |path| 
#            path.extend Path
#            step_path = path.match(/(.*)\.files/)[1]
#            path.resource = Step.new step_path
#          end
#          inputs[input.to_sym] = paths
#        when :file, :binary
#          Log.debug "Pointing #{ input } to #{file}"
#          if file =~ /\.yaml/
#            inputs[input.to_sym]  = Misc.load_yaml(file)
#          else
#            if File.symlink?(file)
#              link_target = File.expand_path(File.readlink(file), File.dirname(file))
#              inputs[input.to_sym]  = link_target
#            else
#              inputs[input.to_sym]  = Open.realpath(file)
#            end
#          end
#        when :text
#          Log.debug "Reading #{ input } from #{file}"
#          inputs[input.to_sym]  = Open.read(file)
#        when :array
#          Log.debug "Reading array #{ input } from #{file}"
#          inputs[input.to_sym]  = Open.read(file).split("\n")
#        when :tsv
#          Log.debug "Opening tsv #{ input } from #{file}"
#          inputs[input.to_sym]  = TSV.open(file)
#        when :boolean
#          case file.read.strip.downcase
#          when 'true'
#            inputs[input.to_sym]  = true
#          when 'false'
#            inputs[input.to_sym]  = false
#          end
#        when :integer
#          inputs[input.to_sym]  = file.read.to_i
#        when :float
#          inputs[input.to_sym]  = file.read.to_f
#        else
#          Log.debug "Loading #{ input } from #{file}"
#          inputs[input.to_sym]  = file.read.strip
#        end
#
#      end
#      inputs = IndiferentHash.setup(inputs)
#
#      dir.glob("*#*").each do |od|
#        name = File.basename(od)
#        name.sub!(/\.as_path$/,'')
#        value = Open.read(od)
#        Log.debug "Loading override dependency #{ name } as #{value}"
#        inputs[name] = value.chomp
#      end
#
#      inputs
#    end
#  end
#
#  def task_inputs_from_directory(task_name, directory)
#    task_info = self.task_info(task_name)
#    Workflow.load_inputs(directory, task_info[:inputs], task_info[:input_types])
#  end
#
#  def job_for_directory_inputs(task_name, directory, jobname = nil)
#    inputs = task_inputs_from_directory(task_name, directory)
#    job(task_name, jobname, inputs)
#  end
#end
#
#class Step
#  def self.save_input(name, value, type, dir)
#    path = File.join(dir, name.to_s)
#
#    case value
#    when Path
#      if Step === value.resource
#        step = value.resource
#        value = File.join('var/jobs', step.workflow.to_s, step.short_path + '.files', Misc.path_relative_to(step.files_dir, value))
#        path = path + '.as_step_file'
#      else
#        path = path + '.as_path'
#      end
#    when String
#      if Misc.is_filename?(value, true)
#        value = value.dup
#        value.extend Path
#        return save_input(name, value, type, dir)
#      end
#    when IO
#      path = path + '.as_io'
#    when Step
#      value = value.path
#      path = path + '.as_step'
#    when Array
#      case value.first
#      when Path
#        if Step === value.first.resource
#          path = path + '.as_step_file_array'
#        else
#          path = path + '.as_path_array'
#        end
#      when String
#        if Misc.is_filename?(value.first, true)
#          path = path + '.as_path_array'
#        end
#      when IO
#        path = path + '.as_io_array'
#      when Step
#        path = path + '.as_step_array'
#        value = value.collect{|s| s.path }
#      when Numeric
#        path = path + '.as_number_array'
#      end
#
#      value = value * "\n"
#    end
#
#    Log.debug "Saving job input #{name} (#{type}) into #{path}"
#
#    if IO === value && value.respond_to?(:filename) && value.filename
#      Open.write(path, value.filename)
#    elsif IO === value
#      Open.write(path, value)
#    else
#      Open.write(path, value.to_s)
#    end
#  end
#
#  def self.save_inputs(inputs, input_types, dir)
#    inputs.each do |name,value|
#      next if value.nil?
#      type = input_types[name]
#      type = type.to_s if type
#
#      save_input(name, value, type, dir)
#    end.any?
#  end
#
#  def self.save_job_inputs(job, dir, options = nil)
#    options = IndiferentHash.setup options.dup if options
#
#    task_name = job.original_task_name || job.task_name
#    workflow = job.original_workflow || job.workflow
#    workflow = Kernel.const_get workflow if String === workflow
#    if workflow
#      task_info = IndiferentHash.setup(workflow.task_info(task_name))
#      input_types = IndiferentHash.setup(task_info[:input_types])
#      input_options = IndiferentHash.setup(task_info[:input_options])
#      task_inputs = IndiferentHash.setup(task_info[:inputs])
#      input_defaults = IndiferentHash.setup(task_info[:input_defaults])
#    else
#      task_info = IndiferentHash.setup({})
#      input_types = IndiferentHash.setup({})
#      task_inputs = IndiferentHash.setup({})
#      task_options = IndiferentHash.setup({})
#      input_defaults = IndiferentHash.setup({})
#    end
#
#    inputs = IndiferentHash.setup({})
#    real_inputs = job.real_inputs || job.info[:real_inputs]
#    job.recursive_inputs.zip(job.recursive_inputs.fields).each do |value,name|
#      next unless task_inputs.include? name.to_sym
#      next unless real_inputs.include? name.to_sym
#      next if options && ! options.include?(name)
#      next if value.nil?
#      next if input_defaults[name] == value
#      inputs[name] = value
#    end
#
#    if options && options.include?('override_dependencies')
#      inputs.merge!(:override_dependencies => open[:override_dependencies])
#      input_types = IndiferentHash.setup(input_types.merge(:override_dependencies => :array))
#    end
#
#    save_inputs(inputs, input_types, dir)
#
#    inputs.keys
#  end
#end
