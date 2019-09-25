module Workflow
  def nextflow_file(file, name = nil)
    file = file + '.nf' unless File.exists?(file) || ! File.exists?(file + '.nf')
    file = File.expand_path(file)
    name ||= File.basename(file).sub(/\.nf$/,'')
    params = Open.read(file).scan(/params\.\w+/).collect{|p| p.split(".").last}.uniq

    params.each do |param|
      input param, :string
    end
    task name => :text do 
      work = file('work')
      output = file('output')
      profile = config :profile, :nextflow
      Misc.in_dir output do
        if profile
          cmd("nextflow run -work-dir #{work} -name #{clean_name} -ansi-log false  -profile #{profile} #{file}", inputs.to_hash.merge('add_option_dashes' => true))
        else
          cmd("nextflow run -work-dir #{work} -name #{clean_name} -ansi-log false #{file}", inputs.to_hash.merge('add_option_dashes' => true))
        end
      end
    end
  end

  def nextflow_dir(path)
    main = File.join(path, 'main.nf')
    nextflow_file main, File.basename(path)
  end

  def nextflow(path)
    if File.directory?(path)
      nextflow_dir path
    else
      nextflow_file path
    end
  end
end
