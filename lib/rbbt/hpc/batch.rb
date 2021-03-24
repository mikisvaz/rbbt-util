module HPC
  class SBATCH < Exception; 
    attr_accessor :directory
    def initialize(directory)
      @directory = directory
    end
  end

  module TemplateGeneration
    def batch_options(job, options)
      IndiferentHash.setup(options)

      batch_options = IndiferentHash.setup({})

      keys = [
        :batch_dir,
        :batch_modules,
        :batch_name,
        :contain,
        :contain_and_sync,
        :copy_image,
        :drbbt,
        :env_cmd,
        :exclusive,
        :highmem,
        :manifest,
        :nodes,
        :queue,
        :singularity,
        :sync,
        :task_cpus,
        :time,
        :user_group,
        :wipe_container,
        :workdir,
      ]

      keys.each do |key|
        next if options[key].nil?
        batch_options[key] = Misc.process_options options, key
      end

      batch_dir = batch_options[:batch_dir]

      batch_name = File.basename(batch_dir)
      inputs_dir = File.join(batch_dir, 'inputs_dir')
      exec_cmd = exec_cmd(job, batch_options)
      rbbt_cmd = rbbt_job_exec_cmd(job, options)

      keys_from_config = [
        :queue,
        :highmem,
        :exclusive,
        :env_cmd,
        :user_group,
        :singularity
      ]

      keys_from_config.each do |key|
        next unless batch_options.include? key
        default_value = Rbbt::Config.get(key, "batch_#{key}", "batch")
        next if default_value.nil? 
        Misc.add_defaults batch_options, default_value
      end

      Misc.add_defaults batch_options, 
        :batch_name => batch_name,
        :exec_cmd => exec_cmd,
        :inputs_dir => inputs_dir, 
        :queue => 'debug',
        :nodes => 1, 
        :rbbt_cmd => rbbt_cmd,
        :step_path => job.path,
        :task_cpus => 1,
        :time => '2min', 
        :workdir => Dir.pwd 

      batch_options
    end

    def rbbt_job_exec_cmd(job, options)

      jobname  = job.clean_name
      workflow = job.workflow
      task     = job.task_name

      Misc.add_defaults options, :jobname => jobname

      task = Symbol === job.overriden ? job.overriden : job.task_name

      if job.overriden
        override_deps = job.rec_dependencies.
          select{|dep| Symbol === dep.overriden }.
          collect do |dep| 

          name = [dep.workflow.to_s, dep.task_name] * "#"
          [name, dep.path] * "="  
        end * ","

        options[:override_deps] = override_deps
      end

      # Save inputs into inputs_dir
      inputs_dir = Misc.process_options options, :inputs_dir
      saved = Step.save_job_inputs(job, inputs_dir) if inputs_dir
      options[:load_inputs] = inputs_dir if saved && saved.any?

      saved.each do |input|
        options.delete input
      end if saved

      cmds = CMD.process_cmd_options options.merge(:add_option_dashes => true)

      <<-EOF.strip
workflow task #{workflow} #{task} #{cmds}
EOF
    end

    def header(options)
      header =<<-EOF
#!/bin/bash
      EOF

      header
    end

    def meta_data(options)
      meta =<<-EOF
#MANIFEST: #{(options[:manifest] || []) * ", "}
#DEPENDENCIES: #{(options[:dependencies] || []) * ", "}
#EXEC_CMD: #{options[:exec_cmd]}
#CMD: #{options[:rbbt_cmd]}
#STEP_PATH: #{options[:step_path]}
      EOF

      meta = meta.split("\n").reject{|line| line =~ /: $/} * "\n"
      meta
    end

    def load_modules(modules = [])
      modules = modules.split(/,\s*/) if String === modules

      str = ""
      modules.each do |mod|
        str << "module load #{ mod }" << "\n"
      end if modules

      str
    end

    def batch_system_variables
      <<-EOF
let MAX_MEMORY="$(grep MemTotal /proc/meminfo|grep -o "[[:digit:]]*") / 1024"
      EOF
    end

    def prepare_environment(job, options = {})
      modules = options[:batch_modules]

      batch_system_variables + load_modules(modules)
    end

    def exec_cmd(job, options = {})
      env_cmd     = Misc.process_options options, :env_cmd
      development = Misc.process_options options, :development

      job_cmd             = self.rbbt_job_exec_cmd(job, options)

      if env_cmd
        exec_cmd = %(env #{env_cmd} rbbt)
      else
        exec_cmd = %(rbbt)
      end

      exec_cmd << "--dev '#{development}'" if development

      exec_cmd
    end

    def execute(options)
      exec_cmd, job_cmd = options.values_at :exec_cmd, :rbbt_cmd

      <<-EOF
step_path=$( 
      #{exec_cmd} #{job_cmd} --printpath
)
exit_status=$?

#{exec_cmd} workflow write_info --recursive --force=false --check_pid "$step_path" batch_job $BATCH_JOB_ID
#{exec_cmd} workflow write_info --recursive --force=false --check_pid "$step_path" batch_system $BATCH_SYSTEM
      EOF
    end

    def sync_environment(options = {})
      ""
    end

    def cleanup_environment(options = {})
      ""
    end

    def coda(batch_options)
      <<-EOF
echo $exit_status > #{File.join(batch_options[:batch_dir], 'exit.status')}
exit $exit_status
EOF
    end

    def job_template(job, options = {})
      batch_options = batch_options job, options

      header              = self.header(batch_options)

      meta_data           = self.meta_data(batch_options)

      prepare_environment = self.prepare_environment(batch_options)

      execute             = self.execute(batch_options)

      sync_environment    = self.sync_environment(batch_options)

      cleanup_environment = self.cleanup_environment(batch_options)

      coda                = self.coda(batch_options)

      <<-EOF
#{header}
#{meta_data}

# #{Log.color :green, "1. Prepare environment"}
#{prepare_environment}

# #{Log.color :green, "2. Execute"}
#{execute} 

# #{Log.color :green, "3. Sync and cleanup environment"}
#{sync_environment}
#{cleanup_environment}
#{coda}
      EOF
    end

    def prepare_submision(template, batch_dir, clean_batch_job = false, batch_dependencies = [])
      Open.mkdir batch_dir
      fcmd   = File.join(batch_dir, 'command.batch')
      fdep   = File.join(batch_dir, 'dependencies.list')
      fcfdep = File.join(batch_dir, 'canfail_dependencies.list')

      Open.write(fcmd, template)

      %w(std.out std.err job.id job.status dependencies.list canfail_dependencies.list exit.status sync.log inputs_dir).each do |filename|
        path = File.join(batch_dir, filename)
        Open.rm_rf path if File.exists? path
      end if clean_batch_job

      batch_dependencies = [] if batch_dependencies.nil?

      canfail_dependencies = batch_dependencies.select{|dep| dep =~ /^canfail:(\d+)/ }.collect{|dep| dep.partition(":").last}
      dependencies = batch_dependencies.reject{|dep| dep =~ /^canfail:(\d+)/ }

      Open.write(fdep, dependencies * "\n") if dependencies.any?
      Open.write(fcfdep, canfail_dependencies * "\n") if canfail_dependencies.any?

      fcmd
    end


    def run_job(job, options = {})
      system = self.to_s.split("::").last

      batch_base_dir, clean_batch_job, remove_batch_dir, procpath, tail, batch_dependencies, dry_run = Misc.process_options options, 
        :batch_base_dir, :clean_batch_job, :remove_batch_dir, :batch_procpath, :tail, :batch_dependencies, :dry_run,
        :batch_base_dir => File.expand_path(File.join('~/rbbt-batch')) 

      workflow = job.workflow
      task_name = job.task_name

      TmpFile.with_file(nil, remove_batch_dir, :tmpdir => batch_base_dir, :prefix => "#{system}_rbbt_job-#{workflow.to_s}-#{task_name}-") do |batch_dir|
        Misc.add_defaults options, 
          :batch_dir => batch_dir, 
          :inputs_dir => File.join(batch_dir, "inputs_dir")

        options[:procpath_performance] ||= File.join(batch_dir, "procpath##{procpath.gsub(',', '#')}") if procpath

        template = self.job_template(job, options.dup)

        fcmd = prepare_submision(template, options[:batch_dir], clean_batch_job, batch_dependencies)

        batch_job = run_template(batch_dir, dry_run)

        return batch_job unless tail

        t_monitor = Thread.new do
          self.follow_job(batch_dir, :STDERR)
        end
        self.wait_for_job(batch_dir)
        t_monitor.raise Aborted
        return unless Open.read(File.join(batch_dir, 'exit.status')).strip == '0'
        path = Open.read(File.join(batch_dir, 'std.out')).strip
        if Open.exists?(path) && job.path != path
          Log.info "Path of BATCH job #{path} is different from original job #{job.path}. Stablishing link."
          Open.ln path, job.path
          Open.ln path + '.info', job.path + '.info'  if Open.exists?(path + '.info')
          Open.ln path + '.files', job.path + '.files' if Open.exists?(path + '.files')
        end
        batch_job
 
      end
    end

    def follow_job(batch_dir, tail = true)
      fjob = File.join(batch_dir, 'job.id')
      fout = File.join(batch_dir, 'std.out')
      ferr = File.join(batch_dir, 'std.err')
      fexit = File.join(batch_dir, 'exit.status')
      fstatus = File.join(batch_dir, 'job.status')

      job = Open.read(fjob).strip if Open.exists?(fjob)

      if job && ! File.exists?(fexit)
        begin
          status_txt = job_status(job)
          STDERR.puts Log.color(:magenta, "Status [#{job.to_i}]:")
          STDERR.puts status_txt
          lines = status_txt.split("\n").length
        rescue
          if ! File.exists?(fexit)
            STDERR.puts Log.color(:magenta, "Job #{job.to_i} not done and not running. STDERR:")
            STDERR.puts Open.read(ferr)
          end
          return
        end
      end

      if File.exists?(fexit)
        exit_status = Open.read(fexit)
        if exit_status.to_i == 0
          STDERR.puts Log.color(:magenta, "Job #{job} done with exit_status 0. STDOUT:")
          STDERR.puts Open.read(fout)
        else
          STDERR.puts Log.color(:magenta, "Job #{job.to_i} done with exit_status #{exit_status}. STDERR:")
          STDERR.puts Open.read(ferr)
        end
        return
      end

      if tail
        Log.severity = 10
        while ! File.exists? fout
          if job
            STDERR.puts
            Log.clear_line(STDERR)
            STDERR.write Log.color(:magenta, "Waiting for Output")
            3.times do
              STDERR.write Log.color(:magenta, ".")
              sleep 1
            end
            status_txt = job_status(job)
            lines.times do
              Log.clear_line(STDERR)
            end
            Log.clear_line(STDERR)
            STDERR.puts Log.color(:magenta, "Status [#{job.to_i}]:")
            STDERR.puts status_txt
            lines = status_txt.split("\n").length
          end
        end
        STDERR.puts
        Log.clear_line(STDERR)
        STDERR.puts Log.color(:magenta, "Output:")
        begin
          status_txt = job_status(job)
          Open.write(fstatus, status_txt) unless status_txt.nil? || status_txt.empty?
          out = CMD.cmd("tail -f '#{fout}'", :pipe => true) if File.exists?(fout) and not tail == :STDERR
          err = CMD.cmd("tail -f '#{ferr}'", :pipe => true) if File.exists?(ferr)

          terr = Misc.consume_stream(err, true, STDERR) if err
          tout = Misc.consume_stream(out, true, STDOUT) if out

          sleep 3 while job_status(job).include? job.to_s
        rescue Aborted
        ensure
          begin
            terr.exit if terr
            tout.exit if tout
            err.close if err
            err.join if err
          rescue Exception
          end

          begin
            out.close if out
            out.join if out
          rescue Exception
          end
        end
      end
    end

    def wait_for_job(batch_dir, time = 1)
      fexit = File.join(batch_dir, 'exit.status')
      fjob = File.join(batch_dir, 'job.id')
      job = Open.read(fjob) if Open.exists?(fjob)

      while ! Open.exists?(fexit)
        sleep time
      end
    end

  end

  module BATCH
    extend HPC::TemplateGeneration
  end

end

