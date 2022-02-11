module Workflow
  def self.nextflow_file_params(file)
    Open.read(file).scan(/params\.\w+/).collect{|p| p.split(".").last}.uniq
  end

  def self.nextflow_includes(file)
    Open.read(file).scan(/^include\s*{\s*(.*?)(?:\s*as.*?)?}\s*from\s+"(.*?)"(?:\s*params.*)?/).collect{|p| p}.uniq
  end

  def self.nextflow_recursive_params(file)
    params = nextflow_file_params(file)
    dir = File.dirname(file)
    nextflow_includes(file).inject(params) do |params,info|
      name_str, included_file = info
      included_file = File.join(dir, included_file)
      included_file += '.nf' unless File.exists?(included_file) || ! File.exists?(included_file + '.nf')
      name_str.split(";").each do |name|
        name = name.strip
        include_params = nextflow_recursive_params(included_file).collect{|p| [p,name] * "-"}
        params += include_params
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

    file = file + '.nf' unless File.exists?(file) || ! File.exists?(file + '.nf')
    file = File.expand_path(file)
    name ||= File.basename(file).sub(/\.nf$/,'').gsub(/\s/,'_')
    params = Workflow.nextflow_recursive_params(file)

    params.each do |param|
      p,_sep, section = param.partition("-")
      if section.nil? || section.empty?
        input param, :string, "Nextflow param #{p}", nil, :nofile => true
      else
        input param, :string, "Nextflow param #{p} from import #{section}", nil, :nofile => true
      end
    end
    task name => result do 
      work = file('work')
      profile = config :profile, :nextflow

      new_inputs = inputs.zip(inputs.fields).collect do |v,f|
        if String === v && m = v.match(/^JOB_FILE:(.*)/)
          file(m[1]) 
        elsif v.nil?
          Rbbt::Config.get(['nextflow', f] * "_", 'default', f)
        else
          v
        end
      end
      
      inputs.replace new_inputs

      Misc.in_dir file('stage') do
        if profile
          cmd("nextflow run -work-dir #{work} -ansi-log false  -profile #{profile} #{file}", inputs.to_hash.merge('add_option_dashes' => true))
        else
          cmd("nextflow run -work-dir #{work} -ansi-log false #{file}", inputs.to_hash.merge('add_option_dashes' => true))
        end
      end

      output_file = file(output).glob.first if output
      output_file = work[File.join('*', '*', output)].glob.first if output && output_file.nil?

      if output_file.nil?
        work[File.join("*", "*", "*")].glob * "\n"
      else
        Open.link output_file, self.tmp_path
        #Open.rm_rf file('work')
        nil
      end
    end
  end

  def nextflow_dir(path, output = nil)
    main = File.join(path, 'main.nf')
    nextflow_file main, File.basename(path), output
  end

  def nextflow(path, *args)
    if File.directory?(path)
      nextflow_dir path, *args
    else
      nextflow_file path, *args
    end
  end
end
