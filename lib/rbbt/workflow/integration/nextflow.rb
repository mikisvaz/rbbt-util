module Workflow

  OUTPUT_FIELDS=%w(outdir output)

  def self.parse_nextflow_schema(file)
    doc = Open.open(file){|f| JSON.parse(f.read) }
    description = doc["description"]

    properties = {}
    required = []

    properties[nil] = doc["properties"] if doc["properties"]
    required.concat doc["required"] if doc["required"]

    doc["definitions"].each do |section,section_info|
      next unless section_info["properties"]
      name = section_info["title"] || section
      properties[name] = section_info["properties"]
      required.concat section_info["required"] if section_info["required"] if section_info["required"]
    end if doc["definitions"]

    required = required.compact.flatten

    parameters = {}
    properties.each do |section,param_info|
      param_info.each do |name,info|
        input_options = {}
        type = info["type"]
        format = info["format"]
        input_desc = info["description"]
        input_section = info["description"]
        input_required = required.include?(name)
        input_options[:required] = true if input_required && ! OUTPUT_FIELDS.include?(name)
        if info.include?("enum")
          type = 'select'
          input_options[:select_options] = info["enum"]
        end
        parameters[name] = {type: type, format: format, description: input_desc, options: input_options, section: section}
      end
    end

    [description, parameters]
  end

  def self.nextflow_file_params(file)
    Open.read(file).scan(/params\.\w+/).collect{|p| p.split(".").last}.uniq
  end

  def self.nextflow_includes(file)
    Open.read(file).scan(/^include\s*{\s*([^\s]*?)\s+.*?}\s*from\s+["'](.*?)["'](?:\s*params.*)?/).collect{|p| p}.uniq
  end

  def self.nextflow_recursive_params(file)
    params = nextflow_file_params(file)
    dir = File.dirname(file)
    nextflow_includes(file).inject(params) do |params,info|
      name_str, included_file = info
      included_file = File.join(dir, included_file)
      included_file += '.nf' unless File.exist?(included_file) || ! File.exist?(included_file + '.nf')
      name_str.split(";").each do |name|
        name = name.strip
        begin
          include_params = nextflow_recursive_params(included_file).collect{|p| [p,name] * "-"}
          params += include_params
        rescue
        end
      end
      params
    end
  end

  def nextflow_file(file, name = nil, output = nil)
    name, output = nil, name if Hash === name

    if Hash === output
      result, output = output.collect.first
    else
      result = :text
    end

    dir = Path.setup(File.dirname(file))

    nextflow_schema = dir['nextflow_schema.json']

    description, params = Workflow.parse_nextflow_schema(nextflow_schema) if nextflow_schema.exists?

    file = file + '.nf' unless File.exist?(file) || ! File.exist?(file + '.nf')
    file = File.expand_path(file)
    name ||= File.basename(file).sub(/\.nf$/,'').gsub(/\s/,'_')
    Workflow.nextflow_recursive_params(file).each do |param|
      p,_sep, section = param.partition("-")
      if ! params.include?(p)
        params[p] = {type: :string, description: "Undocumented"}
      end
    end

    used_params = []
    desc description
    params.each do |name,info|
      input name.to_sym, info[:type], info[:description], nil, info[:options].merge(:noload => true)
    end
    task name => result do 
      work = file('work')
      profile = config :profile, :nextflow
      resume = config :resume, :nextflow
      config_file = config :config, :nextflow

      nextflow_inputs = {}

      inputs.zip(inputs.fields).collect do |v,f|
        v = if String === v && m = v.match(/^JOB_FILE:(.*)/)
              file(m[1]) 
            elsif v.nil?
              Rbbt::Config.get(['nextflow', f] * "_", 'default', f)
            else
              v
            end

        if f.to_s.include?("-") 
          p,_sep, section = f.to_s.partition("-")
          name = [section, p] * "."
        else
          name = f
        end
          
        case name.to_s
        when 'outdir'
          output = nextflow_inputs[name] = v || output || file('output')
        when 'output'
          output = nextflow_inputs[name] = v || output || self.tmp_path
        else
          nextflow_inputs[name] = v
        end
      end

      current_pwd = FileUtils.pwd
      Misc.in_dir file('stage') do

        cmd = "nextflow "

        cmd += " -C #{config_file}" if config_file

        cmd += " run"

        cmd += " -work-dir #{work} -ansi-log false"

        cmd += " -profile #{profile}" if profile

        cmd += " -resume" if resume == 'true'

        Dir.glob(current_pwd + "/*").each do |file|
          target = File.basename(file)
          Open.ln_s file, target unless File.exist?(target)
        end

        cmd("#{cmd} #{file}", nextflow_inputs.merge('add_option_dashes' => true))
      end

      if output && Open.exists?(output)
        if File.directory?(output)
          Dir.glob(output + "/**/*") * "\n"
        else
          output_file = output
          Open.link output, self.tmp_path
          nil
        end
      else
        work[File.join("*", "*", "*")].glob * "\n"
      end
    end
  end

  def nextflow_dir(path, output = nil)
    main = File.join(path, 'main.nf')
    nextflow_file main, File.basename(path), output
  end

  def nextflow_project(project, *args)
    CMD.cmd_log("nextflow pull #{project}")
    directory = File.join(ENV["HOME"], '.nextflow/assets', project)
    nextflow_dir directory, *args
  end

  def nextflow(path, *args)
    if File.directory?(path)
      nextflow_dir path, *args
    elsif File.exist?(path)
      nextflow_file path, *args
    else
      nextflow_project path, *args
    end
  end
end
