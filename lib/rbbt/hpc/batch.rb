module HPC
  class BATCH_DRY_RUN < Exception; 
    attr_accessor :directory
    def initialize(directory)
      @directory = directory
    end
  end

  def self.batch_system(batch_system = 'auto')
    case batch_system.to_s.downcase
    when 'slurm'
      HPC::SLURM
    when 'lsf'
      HPC::LSF
    when 'pbs'
      HPC::PBS
    when 'auto'
      $previous_commands = [] if $previous_commands.nil?
      case $previous_commands.last
      when 'slurm'
        HPC::SLURM
      when 'lsf'
        HPC::LSF
      when 'pbs'
        HPC::PBS
      else
        case Scout::Config.get(:batch_system, :batch, :batch_system, :hpc, :HPC, :BATCH).to_s.downcase
        when 'slurm'
          HPC::SLURM
        when 'lsf'
          HPC::LSF
        when 'pbd'
          HPC::PBS
        else
          case ENV["BATCH_SYSTEM"].to_s.downcase
          when 'slurm'
            HPC::SLURM
          when 'lsf'
            HPC::LSF
          when 'pbs'
            HPC::PBS
          end
        end
      end
    end
  end

  module TemplateGeneration
    def exec_cmd(job, options = {})
      env_cmd     = IndiferentHash.process_options options, :env_cmd
      development = IndiferentHash.process_options options, :development

      if contain = options[:contain]
        contain = File.expand_path(contain)
        env_cmd ||= ""
        env_cmd << " TMPDIR='#{contain}/.rbbt/tmp' "
      end

      if options[:singularity]

        group, user, user_group, scratch_group_dir, projects_group_dir = options.values_at :group, :user, :user_group, :scratch_group_dir, :projects_group_dir

        singularity_img, singularity_opt_dir, singularity_ruby_inline, singularity_mounts = options.values_at :singularity_img, :singularity_opt_dir, :singularity_ruby_inline, :singularity_mounts

        singularity_cmd = %(singularity exec -e -B "#{File.expand_path singularity_opt_dir}":/singularity_opt/ -B "#{File.expand_path singularity_ruby_inline}":"/.singularity_ruby_inline":rw ) 

        if singularity_mounts
          singularity_mounts.split(",").each do |mount|
            singularity_cmd += "-B #{ mount } "
          end
        end

        if contain && options[:hardened]
          singularity_cmd << %( -C -H "#{contain}" \
-B "/.singularity_ruby_inline":"#{contain}/.singularity_ruby_inline":rw 
-B "#{options[:batch_dir]}" \
-B /scratch/tmp \
          #{ group != user_group ? "-B /gpfs/projects/#{user_group}" : "" } \
-B #{scratch_group_dir} \
-B #{projects_group_dir} \
-B /apps/ \
-B ~/git:"#{contain}/git":ro \
          #{Open.exists?('~/.rbbt/software/opt/')? '-B ~/.rbbt/software/opt/:"/opt/":ro' : '' } \
-B ~/.rbbt:"#{contain}/home/":ro)
        end

        singularity_cmd << " #{singularity_img} "
      end

      if env_cmd
        exec_cmd = %(env #{env_cmd} rbbt)
      else
        exec_cmd = %(rbbt)
      end

      exec_cmd << "--dev '#{development}'" if development

      exec_cmd = singularity_cmd  + exec_cmd if singularity_cmd

      exec_cmd
    end

    def rbbt_job_exec_cmd(job, options)

      jobname  = job.clean_name
      workflow = job.workflow
      task     = job.task_name

      IndiferentHash.add_defaults options, :jobname => jobname

      task = job.task_name

      if job.overriden?
        override_deps = job.overriden_deps.
          collect do |dep| 
            o_workflow = dep.overriden_workflow || dep.workflow.to_s
            o_task_name = dep.overriden_task || dep.task.name
            name = [o_workflow, o_task_name] * "#"
          [name, dep.path] * "="  
        end.uniq * ","

        options[:override_deps] = override_deps unless override_deps.empty?
      end

      # Save inputs into inputs_dir
      inputs_dir = IndiferentHash.process_options options, :inputs_dir
      saved = job.save_inputs(inputs_dir)
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

    def batch_options(job, options)
      IndiferentHash.setup(options)

      batch_options = IndiferentHash.setup({})

      keys = [
        :queue,
        :account,
        :partition,
        :exclusive,
        :highmem,
        :time,
        :nodes,
        :task_cpus,
        :mem,
        :mem_per_cpu,
        :gres,
        :lua_modules,
        :conda,
        :contraints,
        :licenses,
        :batch_dir,
        :batch_name,
        :contain,
        :sync,
        :contain_and_sync,
        :copy_image,
        :drbbt,
        :env_cmd,
        :manifest,
        :user_group,
        :wipe_container,
        :workdir,
        :purge_deps,
        :singularity,
        :singularity_img,
        :singularity_mounts,
        :singularity_opt_dir,
        :singularity_ruby_inline
      ]

      keys.each do |key|
        next if options[key].nil?
        batch_options[key] = IndiferentHash.process_options options, key
      end

      batch_dir = batch_options[:batch_dir]

      batch_name = File.basename(batch_dir)
      inputs_dir = File.join(batch_dir, 'inputs_dir')

      keys_from_config = [
        :queue,
        :highmem,
        :exclusive,
        :env_cmd,
        :user_group,
        :singularity_img,
        :singularity_mounts,
        :singularity_opt_dir,
        :singularity_ruby_inline,
        :singularity
      ]

      keys_from_config.each do |key|
        next unless batch_options.include? key
        default_value = Scout::Config.get(key, "batch_#{key}", "batch")
        next if default_value.nil? 
        IndiferentHash.add_defaults batch_options, default_value
      end

      user = batch_options[:user] ||= ENV['USER'] || `whoami`.strip
      group = batch_options[:group] ||= File.basename(File.dirname(ENV['HOME']))
      batch_options[:scratch_group_dir] = File.join('/gpfs/scratch/', group)
      batch_options[:projects_group_dir] = File.join('/gpfs/projects/', group)

      batch_options[:singularity] = true if batch_options[:singularity_img]

      if batch_options[:contain_and_sync]
        if batch_options[:contain].nil?
          contain_base = Scout::Config.get(:contain_base_dir, :batch_contain, :batch, :default => "/scratch/tmp/rbbt-[USER]")
          contain_base = contain_base.sub('[USER]', user)
          random_file = TmpFile.random_name
          batch_options[:contain] = File.join(contain_base, random_file)
        end

        batch_options[:sync] ||= "~/.rbbt/var/jobs" 
        batch_options[:wipe_container] ||= 'post'
      end

      if batch_options[:contain] && ! batch_options[:hardened]
        options[:workdir_all] = batch_options[:contain]
      end

      IndiferentHash.add_defaults batch_options, 
        :batch_name => batch_name,
        :inputs_dir => inputs_dir, 
        :nodes => 1, 
        :step_path => job.path,
        :task_cpus => 1,
        :time => '2min', 
        :env_cmd => '_JAVA_OPTIONS="-Xms1g -Xmx${MAX_MEMORY}m"',
        :singularity_img => ENV["SINGULARITY_IMG"] || "~/rbbt.singularity.img",
        :singularity_ruby_inline => ENV["SINGULARITY_RUBY_INLINE"] || "~/.singularity_ruby_inline",
        :singularity_opt_dir => ENV["SINGULARITY_OPT_DIR"] || "~/singularity_opt",
        :workdir => Dir.pwd 

      exec_cmd = exec_cmd(job, batch_options)
      rbbt_cmd = rbbt_job_exec_cmd(job, options)

      IndiferentHash.add_defaults batch_options, 
        :exec_cmd => exec_cmd,
        :rbbt_cmd => rbbt_cmd

      batch_dir = batch_options[:batch_dir]

      IndiferentHash.add_defaults batch_options,
        :fout   => File.join(batch_dir, 'std.out'),
        :ferr   => File.join(batch_dir, 'std.err'),
        :fjob   => File.join(batch_dir, 'job.id'),
        :fdep   => File.join(batch_dir, 'dependencies.list'),
        :fcfdep => File.join(batch_dir, 'canfail_dependencies.list'),
        :fexit  => File.join(batch_dir, 'exit.status'),
        :fsync  => File.join(batch_dir, 'sync.log'),
        :fsexit  => File.join(batch_dir, 'sync.status'),
        :fenv  => File.join(batch_dir, 'env.vars'),
        :fcmd   => File.join(batch_dir, 'command.batch')

      batch_options
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

    def load_conda(env = nil)
      return "" if env.nil? || env.empty?

      <<-EOF
if ! type conda | grep function &> /dev/null; then
    if [ ! -z $CONDA_EXE ]; then
            source "$(dirname $(dirname $CONDA_EXE))/etc/profile.d/conda.sh" &> /dev/null
    fi
fi
conda activate #{ env }
      EOF
    end


    def batch_system_variables
      <<-EOF
let MAX_MEMORY="$(grep MemTotal /proc/meminfo|grep -o "[[:digit:]]*") / 1024"
      EOF
    end

    def prepare_environment(options = {})
      modules = options[:lua_modules]
      conda = options[:conda]

      prepare_environment = ""

      functions = ""

      if contain = options[:contain]
        contain = File.expand_path(contain)
        functions +=<<-EOF
function batch_erase_contain_dir(){
    rm -Rfv '#{contain}' 2>1 >> '#{options[:fsync]}'
}
        EOF

        prepare_environment +=<<-EOF
if ls -A '#{contain}' &> /dev/null ; then
  empty_contain_dir="false"
else
  empty_contain_dir="true"
fi
        EOF

        prepare_environment +=<<-EOF if options[:wipe_container] == 'force'
batch_erase_contain_dir()
        EOF
      end

      if sync = options[:sync]
        source = if options[:singularity]
                   File.join(options[:contain], '.rbbt/var/jobs')
                 elsif options[:contain]
                   File.join(options[:contain], 'var/jobs')
                 else
                   '~/.rbbt/var/jobs/'
                 end

        source = File.expand_path(source)
        sync = File.expand_path(sync)
        functions +=<<-EOF
function batch_sync_contain_dir(){
  mkdir -p "$(dirname '#{sync}')"
  rsync -avztAXHP --copy-unsafe-links "#{source}/" "#{sync}/" 2>1 >> '#{options[:fsync]}'
  sync_es="$?" 
  echo $sync_es > '#{options[:fsexit]}'
  find '#{sync}' -type l -ls | awk '$13 ~ /^#{sync.gsub('/','\/')}/ { sub("#{source}", "#{sync}", $13); print $11, $13 }' | while read A B; do rm $A; ln -s $B $A; done
}
        EOF
      end

      if options[:singularity]

        group, user, user_group, scratch_group_dir, projects_group_dir = options.values_at :group, :user, :user_group, :scratch_group_dir, :projects_group_dir

        singularity_img, singularity_opt_dir, singularity_ruby_inline = options.values_at :singularity_img, :singularity_opt_dir, :singularity_ruby_inline

        prepare_environment +=<<-EOF
# Load singularity modules
command -v singularity &> /dev/null || module load singularity
mkdir -p "#{File.expand_path singularity_opt_dir}"
        EOF

        if contain && options[:hardened]

          prepare_environment +=<<-EOF
# Prepare container for singularity
mkdir -p "#{contain}"/.rbbt/etc/

for dir in .ruby_inline git home; do
    mkdir -p "#{contain}"/$dir
done

for tmpd in persist_locks  produce_locks  R_sockets  sensiblewrite  sensiblewrite_locks  step_info_locks  tsv_open_locks; do
    mkdir -p "#{contain}/.rbbt/tmp/$tmpd"
done

# Copy environment 
cp ~/.rbbt/etc/environment #{contain}/.rbbt/etc/

# Set search_paths
echo "singularity: /singularity_opt/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" > #{contain}/.rbbt/etc/search_paths
echo "rbbt_user: /home/rbbt/.rbbt/{TOPLEVEL}/{SUBPATH}" >> #{contain}/.rbbt/etc/search_paths
echo "outside_home: #{contain}/home/{TOPLEVEL}/{SUBPATH}" >> #{contain}/.rbbt/etc/search_paths
echo "group_projects: #{projects_group_dir}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> #{contain}/.rbbt/etc/search_paths
echo "group_scratch: #{scratch_group_dir}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> #{contain}/.rbbt/etc/search_paths
echo "user_projects: #{projects_group_dir}/#{user}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> #{contain}/.rbbt/etc/search_paths
echo "user_scratch: #{scratch_group_dir}/#{user}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> #{contain}/.rbbt/etc/search_paths
          EOF
        end
      end

      batch_system_variables + load_modules(modules) + "\n" + load_conda(conda) + "\n"  + functions + "\n" + prepare_environment
    end

    def execute(options)
      exec_cmd, job_cmd, task_cpus = options.values_at :exec_cmd, :rbbt_cmd, :task_cpus

      script=<<-EOF
step_path=$( 
      #{exec_cmd} #{job_cmd} --printpath
)
exit_status=$?

[[ -z $BATCH_JOB_ID ]] || #{exec_cmd} workflow write_info --recursive --force=false --check_pid "$step_path" batch_job $BATCH_JOB_ID
[[ -z $BATCH_SYSTEM ]] || #{exec_cmd} workflow write_info --recursive --force=false --check_pid "$step_path" batch_system $BATCH_SYSTEM
#{exec_cmd} workflow write_info --recursive --force=false --check_pid "$step_path" batch_cpus #{task_cpus}
      EOF

      script
    end

    def sync_environment(options = {})
      sync_environment = ""

      if options[:sync]
        sync_environment +=<<-EOF
if [ $exit_status == '0' ]; then 
  batch_sync_contain_dir
else
  sync_es=$exit_status
fi
        EOF
      end

      sync_environment
    end

    def cleanup_environment(options = {})
      cleanup_environment = ""

      cleanup_environment +=<<-EOF if options[:purge_deps]
if [ $exit_status == '0' ]; then 
  #{options[:exec_cmd]} workflow forget_deps --purge --recursive_purge "$step_path" 2>1 >> '#{options[:fsync]}' 
fi
      EOF

      if options[:sync]
        if options[:wipe_container] == 'force'
          cleanup_environment +=<<-EOF
batch_erase_contain_dir
          EOF
        elsif options[:wipe_container] == 'post' || options[:wipe_container] == 'both'
          cleanup_environment +=<<-EOF
if [ $sync_es == '0' -a $empty_contain_dir == 'true' ]; then 
  batch_erase_contain_dir
fi
          EOF
        end
      end
      cleanup_environment
    end

    def coda(options)
      coda =<<-EOF
echo $exit_status > '#{options[:fexit]}'
      EOF

      if options[:sync]
        coda +=<<-EOF
if [ $sync_es == '0' ]; then
  exit $exit_status
else
  exit $sync_es
fi
        EOF
      else
        coda +=<<-EOF
exit $exit_status
        EOF
      end

      coda
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

# #{Log.color :green, "0. Meta-data"}
#{meta_data}

# #{Log.color :green, "1. Prepare environment"}
#{prepare_environment}
env > #{batch_options[:fenv]}

# #{Log.color :green, "2. Execute"}
#{execute} 

# #{Log.color :green, "3. Sync and cleanup environment"}
#{sync_environment}
#{cleanup_environment}

# #{Log.color :green, "4. Exit"}
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
        Open.rm_rf path if File.exist? path
      end if clean_batch_job

      batch_dependencies = [] if batch_dependencies.nil?

      canfail_dependencies = batch_dependencies.select{|dep| dep =~ /^canfail:(\d+)/ }.collect{|dep| dep.partition(":").last}
      dependencies = batch_dependencies.reject{|dep| dep =~ /^canfail:(\d+)/ }

      Open.write(fdep, dependencies * "\n") if dependencies.any?
      Open.write(fcfdep, canfail_dependencies * "\n") if canfail_dependencies.any?

      fcmd
    end

    def batch_dir_for_id(batch_base_dir, id)
      job_id_file = Dir.glob(File.join(batch_base_dir, '*/job.id')).select{|f| Open.read(f).strip == id.to_s }.first
      job_id_file ? File.dirname(job_id_file) : nil
    end

    def run_job(job, options = {})
      system = self.to_s.split("::").last

      batch_base_dir, clean_batch_job, remove_batch_dir, procpath, tail, batch_dependencies, dry_run, orchestration_rules_file = IndiferentHash.process_options options, 
        :batch_base_dir, :clean_batch_job, :remove_batch_dir, :batch_procpath, :tail, :batch_dependencies, :dry_run, :orchestration_rules,
        :batch_base_dir => File.expand_path(File.join('~/rbbt-batch')) 

      if (batch_job = job.info[:batch_job]) && job_queued(batch_job)
        Log.info "Job #{job.short_path} already queued in #{batch_job}"
        return batch_job, batch_dir_for_id(batch_base_dir, batch_job) 
      end

      if job.running?
        Log.info "Job #{job.short_path} already running in #{job.info[:pid]}"

        if job.info[:batch_job]
          return job.info[:batch_job], batch_dir_for_id(batch_base_dir, batch_job)
        else
          return 
        end
      end

      workflow = job.workflow
      task_name = job.task_name

      options = options.merge(HPC::Orchestration.job_rules(HPC::Orchestration.orchestration_rules(orchestration_rules_file), job)) if orchestration_rules_file

      workflows_to_load = job.rec_dependencies.select{|d| Step === d}.collect{|d| d.workflow }.compact.collect(&:to_s) - [workflow.to_s]

      TmpFile.with_file(nil, remove_batch_dir, :tmpdir => batch_base_dir, :prefix => "#{system}_rbbt_job-#{workflow.to_s}-#{task_name}-") do |batch_dir|
        IndiferentHash.add_defaults options, 
          :batch_dir => batch_dir, 
          :inputs_dir => File.join(batch_dir, "inputs_dir"),
          :workflows => workflows_to_load.any? ? workflows_to_load.uniq * "," : nil

        options[:procpath_performance] ||= File.join(batch_dir, "procpath##{procpath.gsub(',', '#')}") if procpath

        template = self.job_template(job, options.dup)

        fcmd = prepare_submision(template, options[:batch_dir], clean_batch_job, batch_dependencies)

        batch_job = run_template(batch_dir, dry_run)

        hold_dependencies(job, batch_job) unless dry_run

        return [batch_job, batch_dir] unless tail

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

        [batch_job, batch_dir]
      end
    end

    def hold_dependencies(job, batch_job)
      job.init_info
      job.set_info :batch_job, batch_job
      job.set_info :batch_system, self.batch_system
      job.dependencies.each do |dep|
        next unless dep.waiting?
        next if (dep_batch_job = dep.info[:batch_job]) && job_queued(dep_batch_job)

        hold_dependencies(dep, batch_job)
      end
    end

    def follow_job(batch_dir, tail = true)
      fjob = File.join(batch_dir, 'job.id')
      fout = File.join(batch_dir, 'std.out')
      ferr = File.join(batch_dir, 'std.err')
      fexit = File.join(batch_dir, 'exit.status')
      fstatus = File.join(batch_dir, 'job.status')

      job = Open.read(fjob).strip if Open.exists?(fjob)

      if job && ! File.exist?(fexit)
        begin
          status_txt = job_status(job)
          STDERR.puts Log.color(:magenta, "Status [#{job.to_i}]:")
          STDERR.puts status_txt
          lines = status_txt.split("\n").length
        rescue
          if ! File.exist?(fexit)
            STDERR.puts Log.color(:magenta, "Job #{job.to_i} not done and not running. STDERR:")
            STDERR.puts Open.read(ferr)
          end
          return
        end
      end

      if File.exist?(fexit)
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
        while ! File.exist? fout
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
          out = CMD.cmd("tail -f '#{fout}'", :pipe => true) if File.exist?(fout) and not tail == :STDERR
          err = CMD.cmd("tail -f '#{ferr}'", :pipe => true) if File.exist?(ferr)

          terr = Misc.consume_stream(err, true, STDERR) if err
          tout = Misc.consume_stream(out, true, STDOUT) if out

          sleep 3 while job_queued(job)
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

    def job_queued(job)
      job_status(job).split(/[\s\.]+/).include?(job.to_s)
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

