module HPC
  class SBATCH < Exception; 
    attr_accessor :directory
    def initialize(directory)
      @directory = directory
    end
  end

  module SLURM

    def self.template(args, options = {})

      development      = options.delete :drbbt
      singularity      = options.delete :singularity
      contain          = options.delete :contain
      sync             = options.delete :sync
      user_group       = options.delete :user_group
      contain_and_sync = options.delete :contain_and_sync
      wipe_container   = options.delete :wipe_container
      copy_image       = options.delete :copy_image
      exclusive        = options.delete :exclusive
      highmem          = options.delete :highmem

      slurm_step_path  = options.delete :slurm_step_path

      manifest         = options.delete :manifest

      queue            = options.delete(:queue) || Rbbt::Config.get('queue', :slurm_queue, :slurm, :SLURM, :default => 'bsc_ls')
      task_cpus        = options.delete(:task_cpus) || 1
      nodes            = options.delete(:nodes) || 1
      time             = options.delete(:time) || "0:02:00"

      inputs_dir       = options.delete :inputs_dir
      config_keys      = options.delete :config_keys

      user = ENV['USER'] || `whoami`.strip
      group = File.basename(File.dirname(ENV['HOME']))

      if contain_and_sync
        random_file = TmpFile.random_name
        contain = "/scratch/tmp/rbbt-#{user}/#{random_file}" if contain.nil?
        sync = "~/.rbbt/var/jobs" if sync.nil?
        wipe_container = "post" if wipe_container.nil?
      end

      contain = nil if contain == "" || contain == "none"
      sync = nil if sync == "" || sync == "none"

      contain = File.expand_path(contain) if contain

      name = options[:name] ||= Misc.obj2digest({:options => options.collect{|k,v| [k,v]}.sort_by{|k,v| k.to_s }, :args => args})
      options.delete(:name)
      slurm_basedir = options[:slurm_basedir] ||= File.expand_path(File.join('~/rbbt-slurm', name)) if slurm_basedir.nil?
      options.delete(:slurm_basedir)

      rbbt_cmd = args.reject{|e| e == '--' }.collect{|e| e.include?(" ")? '"' + e + '"' : e } * " "

      rbbt_cmd += " "  << options.collect do |o,v|
        o = o.to_s
        case v
        when TrueClass 
          '--' << o
        when FalseClass 
          '--' << o << "=false"
        else
          ['--' << o, "'#{v.to_s.gsub("'", '\'')}'"] * " "
        end
      end * " "

      rbbt_cmd << " --config_keys='#{config_keys.gsub("'", '\'')}'" if config_keys and not config_keys.empty?

      time = Misc.format_seconds Misc.timespan(time) unless time.include? ":"


      #{{{ PREPARE LOCAL LOGFILES

      Open.mkdir slurm_basedir

      fout = File.join(slurm_basedir, 'std.out')
      ferr = File.join(slurm_basedir, 'std.err')
      fjob = File.join(slurm_basedir, 'job.id')
      fexit = File.join(slurm_basedir, 'exit.status')
      fsync = File.join(slurm_basedir, 'sync.log')
      fsyncexit = File.join(slurm_basedir, 'sync.status')
      fcmd = File.join(slurm_basedir, 'command.slurm')

      #{{{ GENERATE TEMPLATE

      # HEADER
      header =<<-EOF
#!/bin/bash
#SBATCH --qos="#{queue}"
#SBATCH --job-name="#{name}"
#SBATCH --workdir="#{Dir.pwd}"
#SBATCH --output="#{fout}"
#SBATCH --error="#{ferr}"
#SBATCH --cpus-per-task="#{task_cpus}"
#SBATCH --time="#{time}"
#SBATCH --nodes="#{nodes}"
      EOF

      prep  = ""

      if highmem
        header +=<<-EOF
#SBATCH --constraint=highmem 
        EOF
      end

      if exclusive
        header +=<<-EOF
#SBATCH --exclusive 
        EOF
      end

      # ENV
      env = ""
      env +=<<-EOF
# Prepare env
[[ -f ~/config/load.sh ]] && source ~/config/load.sh
module load java

# Calculate max available memory
let "MAX_MEMORY=$SLURM_MEM_PER_CPU * $SLURM_CPUS_PER_TASK" || let MAX_MEMORY="$(grep MemTotal /proc/meminfo|grep -o "[[:digit:]]*") / 1024"
      EOF


      # RUN
      run = ""
      exec_cmd = %(env _JAVA_OPTIONS="-Xms1g -Xmx${MAX_MEMORY}m")


      if singularity
        #{{{ SINGULARITY
        
        singularity_exec = %(singularity exec -e -B $SINGULARITY_OPT_DIR:/singularity_opt/ -B /apps/)
        
        env +=<<-EOF
module load intel/2018.1
module load singularity
PROJECTS_ROOT="/gpfs/projects/bsc26/"
SINGULARITY_IMG="$PROJECTS_ROOT/rbbt.singularity.img"
SINGULARITY_OPT_DIR="$PROJECTS_ROOT/singularity_opt/"
SINGULARITY_RUBY_INLINE="$HOME/.singularity_ruby_inline"
mkdir -p "$SINGULARITY_RUBY_INLINE"
        EOF

        if contain
          scratch_group_dir = File.join('/gpfs/scratch/', group)
          projects_group_dir = File.join('/gpfs/projects/', group)

          prep +=<<-EOF

# Prepare container dir
CONTAINER_DIR="#{contain}"
mkdir -p $CONTAINER_DIR/.rbbt/etc/

for dir in .ruby_inline git home; do
    mkdir -p $CONTAINER_DIR/$dir
done

for tmpd in persist_locks  produce_locks  R_sockets  sensiblewrite  sensiblewrite_locks  step_info_locks  tsv_open_locks; do
    mkdir -p $CONTAINER_DIR/.rbbt/tmp/$tmpd
done

# Copy environment 
cp ~/.rbbt/etc/environment $CONTAINER_DIR/.rbbt/etc/

# Set search_paths
echo "singularity: /singularity_opt/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" > $CONTAINER_DIR/.rbbt/etc/search_paths
echo "rbbt_user: /home/rbbt/.rbbt/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
echo "outside_home: $CONTAINER_DIR/home/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
echo "group_projects: #{projects_group_dir}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
echo "group_scratch: #{scratch_group_dir}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
echo "user_projects: #{projects_group_dir}/#{user}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
echo "user_scratch: #{scratch_group_dir}/#{user}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
          EOF

          if user_group && group != user_group
            prep +=<<-EOF

# Add user_group search_path
echo "#{user_group}: /gpfs/projects/#{user_group}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
            EOF
          end

          if inputs_dir
            prep +=<<-EOF

# Copy inputs
[[ -d '#{inputs_dir}' ]] && cp -R '#{inputs_dir}' $CONTAINER_DIR/inputs
            EOF
            rbbt_cmd = rbbt_cmd.sub(inputs_dir, "#{contain}/inputs")
          end

          if copy_image
            prep +=<<EOF

# Copy image
rsync -avz "$SINGULARITY_IMG" "$CONTAINER_DIR/rbbt.singularity.img" 1>&2
SINGULARITY_IMG="$CONTAINER_DIR/rbbt.singularity.img"
EOF
          end

          if  wipe_container == "pre" || wipe_container == "both"
            if singularity
              prep +=<<-EOF

# Clean container pre
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -Rfv .rbbt/var/jobs &>> #{fsync}
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rbbt system clean -f &>> #{fsync}
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -Rfv tmp/ &>> #{fsync}
EOF
            else
              prep = ""
            end
          end
        end

        if contain
          singularity_exec << %( -C -H "$CONTAINER_DIR" \
-B /scratch/tmp \
#{ group != user_group ? "-B /gpfs/projects/#{user_group}" : "" } \
-B #{scratch_group_dir} \
-B #{projects_group_dir} \
-B "$SINGULARITY_RUBY_INLINE":"$CONTAINER_DIR/.ruby_inline":rw  \
-B ~/git:"$CONTAINER_DIR/git":ro \
#{Open.exists?('~/.rbbt/software/opt/')? '-B ~/.rbbt/software/opt/:"/opt/":ro' : '' } \
-B ~/.rbbt:"$CONTAINER_DIR/home/":ro \
"$SINGULARITY_IMG")
          exec_cmd << ' TMPDIR="$CONTAINER_DIR/.rbbt/tmp" '
        else
          singularity_exec += %( -B "$SINGULARITY_RUBY_INLINE":"$HOME/.ruby_inline":rw "$SINGULARITY_IMG" )
        end

        if development
          exec_cmd += " rbbt --dev='#{development}'"
        else
          exec_cmd += ' rbbt'
        end

        exec_cmd = singularity_exec + " " + exec_cmd
      else
        if development
          exec_cmd << " " << %(~/git/rbbt-util/bin/rbbt --dev=#{development})
        else
          exec_cmd << " " << 'rbbt'
        end

        if contain
          rbbt_cmd << " " << %(--workdir_all='#{contain.gsub("'", '\'')}/workdir')
        end
      end


      cmd =<<-EOF
#{exec_cmd} \\
#{rbbt_cmd}
EOF
      annotate_cmd =<<-EOF
#{exec_cmd} \\
workflow write_info --recursive --force=false --check_pid "$step_path" slurm_job $SLURM_JOB_ID 
EOF

      header +=<<-EOF if manifest
#MANIFEST: #{manifest * ", "}
      EOF

      header +=<<-EOF if slurm_step_path
#STEP_PATH: #{slurm_step_path}
      EOF

      header +=<<-EOF
#CMD: #{rbbt_cmd}
      EOF

      run +=<<-EOF

# Run command
step_path=$(#{cmd})

# Save exit status
exit_status=$?

# Annotate info with SLURM job_info
#{annotate_cmd}

EOF

      # CODA
      coda = ""
      if sync
        if singularity
          coda +=<<-EOF
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rbbt system clean all -q &>> #{fsync}
EOF
#        else
#          coda +=<<-EOF
#rbbt system clean all -q &>> #{fsync}
#EOF
        end

        if sync.include?("=>")
          source, _sep, sync = sync.partition("=>")
          source = source.strip
          sync = sync.strip
          source = File.join(File.expand_path(contain), source)
        else
          source = File.join(File.expand_path(contain), 'workdir/var/jobs')
        end

        target = File.expand_path(sync)
        coda +=<<-EOF

# Sync data to target location
if [ $exit_status == '0' ]; then 
  mkdir -p "$(dirname '#{target}')"
  rsync -avztAXHP --copy-unsafe-links "#{source}/" "#{target}/" &>> #{fsync} 
  sync_es="$?" 
  echo $sync_es > #{fsyncexit}
  find '#{target}' -type l -ls | awk '$13 ~ /^#{target.gsub('/','\/')}/ { sub("#{source}", "#{target}", $13); print $11, $13 }' | while read A B; do rm $A; ln -s $B $A; done
else
  sync_es="$exit_status" 
fi
EOF

        if  contain && (wipe_container == "post" || wipe_container == "both")
          prep =<<-EOF + prep
if ls -A '#{contain}' &> /dev/null ; then
    echo "ERROR: Container directory not empty, refusing to wipe. #{contain}" &>> #{fsync}
fi
          EOF
          if singularity
            coda +=<<-EOF
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -v /dev/shm/sem.*.{in,out,process} /dev/shm/sem.Session-PID.*.sem 2> /dev/null >> #{fsync}


# Clean container directory
#if [ $exit_status == '0' -a $sync_es == '0' ]; then 
    singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rbbt system clean -f &>> #{fsync}
    singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -Rfv .rbbt/var/jobs &>> #{fsync}
    singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -Rfv tmp/ &>> #{fsync}
#else
#    echo "ERROR: Process failed or results could not sync correctly. Contain directory not purged" &>> #{fsync}
#fi
EOF
          else
            coda +=<<-EOF
##{exec_cmd} system clean
#if [ $exit_status == '0' -a $sync_es == '0' ]; then 
    rm -Rfv #{contain} &>> #{fsync}
#else
#    echo "ERROR: Process failed or results could not sync correctly. Contain directory not purged" &>> #{fsync}
#fi
EOF

          end
        end
      end

      coda +=<<-EOF

# Write exit status to file
echo $exit_status > #{fexit}
EOF

      if sync
        coda +=<<-EOF 
if [ "$sync_es" == '0' ]; then 
  unset sync_es
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

      template = [header, env, prep, run, coda] * "\n"

      template
    end
    
    def self.issue_template(template, options = {})

      slurm_basedir = options[:slurm_basedir]
      dependencies = options.delete :slurm_dependencies
      dependencies = [] if dependencies.nil?

      canfail_dependencies = dependencies.select{|dep| dep =~ /^canfail:(\d+)/ }.collect{|dep| dep.partition(":").last}
      dependencies = dependencies.reject{|dep| dep =~ /^canfail:(\d+)/ }

      Open.mkdir slurm_basedir

      dry_run = options.delete :dry_run

      fout  = File.join(slurm_basedir, 'std.out')
      ferr  = File.join(slurm_basedir, 'std.err')
      fjob  = File.join(slurm_basedir, 'job.id')
      fdep  = File.join(slurm_basedir, 'dependencies.list')
      fcfdep  = File.join(slurm_basedir, 'canfail_dependencies.list')
      fexit = File.join(slurm_basedir, 'exit.status')
      fsync = File.join(slurm_basedir, 'sync.log')
      fcmd  = File.join(slurm_basedir, 'command.slurm')

      job = nil
      if options[:clean_job]
        [fcmd, fjob, fout, ferr, fsync, fexit].each do |file|
          Open.rm file if Open.exists? file
        end
      end

      return if Open.exists?(fexit)

      STDERR.puts Log.color(:magenta, "Issuing SLURM file: #{fcmd}")
      STDERR.puts template

      Open.write(fcmd, template) unless File.exists? fcmd
      if File.exists?(fjob)
        job = Open.read(fjob).to_i
      else
        if File.exists?(fout)
          return
        elsif dry_run
          STDERR.puts Log.color(:magenta, "To execute run: ") + Log.color(:blue, "sbatch '#{slurm_basedir}/command.slurm'")
          STDERR.puts Log.color(:magenta, "To monitor progress run (needs local rbbt): ") + Log.color(:blue, "rbbt mn --tail -w '#{slurm_basedir}'")
          raise HPC::SBATCH, slurm_basedir
        else
          Open.rm fsync
          Open.rm fexit
          Open.rm fout
          Open.rm ferr

          Open.write(fdep, dependencies * "\n") if dependencies.any?
          Open.write(fcfdep, canfail_dependencies * "\n") if canfail_dependencies.any?


          dep_str = '--dependency='
          normal_dep_str = dependencies.any? ? "afterok:" + dependencies * ":" : nil
          canfail_dep_str = canfail_dependencies.any? ? "afterany:" + canfail_dependencies * ":" : nil

          if normal_dep_str.nil? && canfail_dep_str.nil?
            dep_str = ""
          else
            dep_str += [normal_dep_str, canfail_dep_str].compact * ","
          end

          job = CMD.cmd("sbatch #{dep_str} '#{fcmd}'").read.scan(/\d+/).first.to_i
          Log.debug "SBATCH job id: #{job}"
          Open.write(fjob, job.to_s)
          job
        end
      end
    end

    def self.follow_job(slurm_basedir, tail = true)
      fjob = File.join(slurm_basedir, 'job.id')
      fout = File.join(slurm_basedir, 'std.out')
      ferr = File.join(slurm_basedir, 'std.err')
      fstatus = File.join(slurm_basedir, 'job.status')

      job = Open.read(fjob).strip if Open.exists?(fjob)

      if job
        status_txt = CMD.cmd("squeue --job #{job}").read
        STDERR.puts Log.color(:magenta, "Status [#{job.to_i}]:")
        STDERR.puts status_txt
        lines = status_txt.split("\n").length
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
            status_txt = CMD.cmd("squeue --job #{job}").read
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
          CMD.cmd("squeue --job #{job} > #{fstatus}")
          out = CMD.cmd("tail -f '#{fout}'", :pipe => true) if File.exists?(fout) and not tail == :STDERR
          err = CMD.cmd("tail -f '#{ferr}'", :pipe => true) if File.exists?(ferr)

          terr = Misc.consume_stream(err, true, STDERR) if err
          tout = Misc.consume_stream(out, true, STDOUT) if out

          sleep 3 while CMD.cmd("squeue --job #{job}").read.include? job.to_s
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

    def self.wait_for_job(slurm_basedir, time = 1)
      fexit = File.join(slurm_basedir, 'exit.status')
      fjob = File.join(slurm_basedir, 'job.id')
      job = Open.read(fjob) if Open.exists?(fjob)


      while ! Open.exists?(fexit)
        sleep time
      end
    end

    def self.run_job(job, options = {})
      options = IndiferentHash.setup(options.dup)
        
      dry_run          = options.delete :dry_run
      tail             = options.delete :tail
      dependencies     = options.delete :slurm_dependencies
      procpath         = options.delete :SLURM_procpath

      options[:jobname]   = job.clean_name
      options[:slurm_step_path] = job.path

      log_level         = options.delete :log
      log_level       ||= Log.severity

      workflow = job.workflow

      task = Symbol === job.overriden ? job.overriden : job.task_name

      if job.overriden
        override_deps = job.rec_dependencies.
          select{|dep| Symbol === dep.overriden }.
          collect do |dep| 

          name = [dep.workflow.to_s, dep.task_name] * "#"
          [name, dep.path] * "="  
        end * ","
      end

      remove_slurm_basedir = options.delete :remove_slurm_basedir 
      slurm_basedir = options.delete :SLURM_basedir
      slurm_basedir = "~/rbbt-slurm" if slurm_basedir.nil?
      TmpFile.with_file(nil, remove_slurm_basedir, :tmpdir => slurm_basedir, :prefix => "SLURM_rbbt_job-") do |tmp_directory|
        options[:slurm_basedir] ||= tmp_directory
        slurm_basedir = options[:slurm_basedir]
        inputs_dir = File.join(tmp_directory, 'inputs_dir')
        saved = Step.save_job_inputs(job, inputs_dir)

        cmd = ['workflow', 'task', workflow.to_s, task.to_s, '--printpath', '--log', log_level.to_s]

        cmd << "--procpath_performance='#{tmp_directory}/procpath##{procpath.gsub(',', '#')}'" if procpath

        cmd << "--override_deps='#{override_deps.gsub("'", '\'')}'" if override_deps and not override_deps.empty?

        cmd << "--load_inputs='#{inputs_dir}'" if saved && saved.any?

        template = self.template(cmd, options)
        jobid = self.issue_template(template, options.merge(:slurm_basedir => slurm_basedir, :dry_run => dry_run, :slurm_dependencies => dependencies))

        return jobid unless tail

        t_monitor = Thread.new do
          self.follow_job(slurm_basedir, :STDERR)
        end
        self.wait_for_job(slurm_basedir)
        t_monitor.raise Aborted
        return unless Open.read(File.join(slurm_basedir, 'exit.status')).strip == '0'
        path = Open.read(File.join(slurm_basedir, 'std.out')).strip
        if Open.exists?(path) && job.path != path
          Log.info "Path of SLURM job #{path} is different from original job #{job.path}. Stablishing link."
          Open.ln path, job.path
          Open.ln path + '.info', job.path + '.info'  if Open.exists?(path + '.info')
          Open.ln path + '.files', job.path + '.files' if Open.exists?(path + '.files')
        end
        jobid
      end
    end
  end

  def self.relay(job, options={})
    options = Misc.add_defaults options, :target => 'mn1', :search_path => 'user'
    done_deps = job.dependencies.select do |dep|
      dep.done? 
    end

    error_deps = job.dependencies.select do |dep|
      dep.error? && ! dep.recoverable_error?
    end

    (done_deps + error_deps).each do |dep|
      Step.migrate(dep.path, options[:search_path], options)
    end

  end
end

