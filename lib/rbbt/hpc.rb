require 'rbbt-util'
require 'rbbt/util/cmd'

module Marenostrum
  SERVER='mn1'
  class SBATCH < Exception; 
    attr_accessor :directory
    def initialize(directory)
      @directory = directory
    end
  end

  module SLURM

    def self.template(args, options = {})

      development      = options.delete :drbbt
      contain          = options.delete :contain
      sync             = options.delete :sync
      contain_and_sync = options.delete :contain_and_sync
      wipe_container   = options.delete :wipe_container
      copy_image       = options.delete :copy_image
      exclusive        = options.delete :exclusive
      highmem          = options.delete :highmem

      queue            = options.delete(:queue) || 'bsc_ls'
      task_cpus        = options.delete(:task_cpus) || 1
      nodes            = options.delete(:nodes) || 1
      time             = options.delete(:time) || "0:00:10"

      inputs_dir       = options.delete :inputs_dir
      config_keys      = options.delete :config_keys

      if contain_and_sync
        contain = "/scratch/tmp/rbbt" if contain.nil?
        sync = "~/.rbbt/var/jobs" if sync.nil?
      end

      contain = File.expand_path(contain) if contain

      singularity = true if contain || ! development


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
          ['--' << o, "'#{v}'"] * " "
        end
      end * " "

      rbbt_cmd << " --config_keys='#{config_keys}'" if config_keys and not config_keys.empty?


      time = Misc.format_seconds Misc.timespan(time) unless time.include? ":"


      #{{{ PREPARE LOCAL LOGFILES

      Open.mkdir slurm_basedir

      fout = File.join(slurm_basedir, 'std.out')
      ferr = File.join(slurm_basedir, 'std.err')
      fjob = File.join(slurm_basedir, 'job.id')
      fexit = File.join(slurm_basedir, 'exit.status')
      fsync = File.join(slurm_basedir, 'sync.log')
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

      header +=<<-EOF
#CMD: #{rbbt_cmd}
      EOF

      # ENV
      env = ""
      env +=<<-EOF
# Prepare env
[[ -f ~/config/load.sh ]] && source ~/config/load.sh
module load java

# Calculate max available memory
let "MAX_MEMORY=$SLURM_MEM_PER_CPU * $SLURM_CPUS_ON_NODE"
      EOF

      if singularity
        env +=<<-EOF
module load intel/2018.1
module load singularity
PROJECTS_ROOT="/gpfs/projects/bsc26/"
SINGULARITY_IMG="$PROJECTS_ROOT/rbbt.singularity.img"
SINGULARITY_OPT_DIR="$PROJECTS_ROOT/singularity_opt/"
SINGULARITY_RUBY_INLINE="$HOME/.singularity_ruby_inline"
mkdir -p "$SINGULARITY_RUBY_INLINE"
        EOF
      end

      if contain
        user = ENV['USER'] || `whoami`.strip
        group = File.basename(File.dirname(ENV['HOME']))
        scratch_group_dir = File.join('/gpfs/scratch/', group)
        projects_group_dir = File.join('/gpfs/projects/', group)

        env +=<<-EOF

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
echo "rbbt_user: /home/rbbt/.rbbt/{TOPLEVEL}/{SUBPATH}" > $CONTAINER_DIR/.rbbt/etc/search_paths
echo "home: $CONTAINER_DIR/home/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
echo "group_projects: $CONTAINER_DIR/projects/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
echo "group_scratch: $CONTAINER_DIR/scratch/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
echo "user_projects: $CONTAINER_DIR/projects/#{user}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
echo "user_scratch: $CONTAINER_DIR/scratch/#{user}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
echo "/scratch/tmp/rbbt/projects/rbbt/workflows/" > $CONTAINER_DIR/.rbbt/etc/workflow_dir

[[ -a "$CONTAINER_DIR/projects" ]] || ln -s '#{projects_group_dir}' "$CONTAINER_DIR/projects"
[[ -a "$CONTAINER_DIR/scratch" ]] || ln -s '#{scratch_group_dir}' "$CONTAINER_DIR/scratch"
        EOF
        
        if inputs_dir
          env +=<<-EOF

# Copy inputs
[[ -d '#{inputs_dir}' ]] && cp -R '#{inputs_dir}' $CONTAINER_DIR/inputs
          EOF
          rbbt_cmd = rbbt_cmd.sub(inputs_dir, "#{contain}/inputs")
        end

        if copy_image
          env +=<<EOF

# Copy image
rsync -avz "$SINGULARITY_IMG" "$CONTAINER_DIR/rbbt.singularity.img" 1>&2
SINGULARITY_IMG="$CONTAINER_DIR/rbbt.singularity.img"
EOF
        end

        if  wipe_container == "pre" || wipe_container == "both"
          env +=<<-EOF

# Clean container pre
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -Rfv .rbbt/var/jobs &>> #{fsync}
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rbbt system clean -f &>> #{fsync}
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -Rfv tmp/ &>> #{fsync}
EOF
        end
      end

      # RUN
      run = ""


      exec_cmd = %(env _JAVA_OPTIONS="-Xms1g -Xmx${MAX_MEMORY}m")

      if singularity
        singularity_exec = %(singularity exec -e -B $SINGULARITY_OPT:/singularity_opt/ -B /apps/)

        if contain
          singularity_exec << %( -C -H "$CONTAINER_DIR" \
-B /scratch/tmp \
-B "$SINGULARITY_RUBY_INLINE":"$CONTAINER_DIR/.ruby_inline":rw  \
-B ~/git:"$CONTAINER_DIR/git":ro \
-B ~/.rbbt/software/opt/:"/opt/":ro \
-B ~/.rbbt:"$CONTAINER_DIR/home/":ro \
-B #{scratch_group_dir} \
-B #{projects_group_dir} \
"$SINGULARITY_IMG")
          exec_cmd << ' TMPDIR="$CONTAINER_DIR/.rbbt/tmp" '
        else
          singularity_exec += %( -B "$SINGULARITY_RUBY_INLINE":"$HOME/.ruby_inline":rw "$SINGULARITY_IMG" )
        end

        if development
          exec_cmd += ' rbbt --dev=git'
        else
          exec_cmd += ' rbbt'
        end

        exec_cmd = singularity_exec + " " + exec_cmd
      else
        exec_cmd << %(~/git/rbbt-util/bin/rbbt --dev=~/git/)
      end


      cmd =<<-EOF
#{exec_cmd} \\
#{rbbt_cmd}
EOF

      run +=<<-EOF

# Run command
#{cmd}

# Save exit status
exit_status=$?

# Clean job.id, since we are done
rm #{fjob}
EOF

      # CODA
      coda = ""
      if sync
        if singularity
          coda +=<<-EOF
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rbbt system clean all -q &>> #{fsync}
EOF
        end

        if sync.include?("=>")
          source, _sep, sync = sync.partition("=>")
          source = source.strip
          sync = sync.strip
          source = File.join(File.expand_path(contain), source)
        else
          source = File.join(File.expand_path(contain), '.rbbt/var/jobs')
        end

        target = File.expand_path(sync)
        coda +=<<-EOF
rsync -avt "#{source}/" "#{target}/" &>> #{fsync} 
sync_es="$?" 
find '#{target}' -type l -ls | awk '$13 ~ /^#{target.gsub('/','\/')}/ { sub("#{source}", "#{target}", $13); print $11, $13 }' | while read A B; do rm $A; ln -s $B $A; done
EOF

        if  contain && (wipe_container == "post" || wipe_container == "both")
          coda +=<<-EOF
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rbbt system clean -f &>> #{fsync}
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -v /dev/shm/sem.*.{in,out,process} /dev/shm/sem.Session-PID.*.sem 2> /dev/null >> #{fsync}
if [ $sync_es == '0' ]; then 
    singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -Rfv .rbbt/var/jobs &>> #{fsync}
    singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -Rfv tmp/ &>> #{fsync}
else
    echo "WARNING: Results could not sync correctly. Job directory not purged"
fi
unset sync_es
EOF
        end
      end
      coda +=<<-EOF

# Write exit status to file
echo $exit_status > #{fexit}
unset exit_status
EOF

      template = [header, env, run, coda] * "\n"

      template
    end
    
    def self.issue_template(template, options = {})

      slurm_basedir = options[:slurm_basedir]
      Open.mkdir slurm_basedir

      dry_run = options.delete :dry_run

      fout  = File.join(slurm_basedir, 'std.out')
      ferr  = File.join(slurm_basedir, 'std.err')
      fjob  = File.join(slurm_basedir, 'job.id')
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
          raise Marenostrum::SBATCH, slurm_basedir
        else
          Open.rm fsync
          Open.rm fexit
          Open.rm fout
          Open.rm ferr
          job = CMD.cmd("sbatch '#{fcmd}'").read.scan(/\d+/).first.to_i
          Open.write(fjob, job.to_s)
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

          Misc.consume_stream(err, true, STDERR) if err
          Misc.consume_stream(out, true, STDOUT) if out

          sleep 3 while CMD.cmd("squeue --job #{job}").read.include? job.to_s
        ensure
          begin
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

      workflow = job.workflow
      task = job.task_name

      keep_slurm_basedir = options.delete :keep_SLURM_slurm_basedir 
      slurm_basedir = options.delete :SLURM_basedir
      slurm_basedir = "~/rbbt-slurm" if slurm_basedir.nil?
      TmpFile.with_file(nil, !keep_slurm_basedir, :tmpdir => slurm_basedir, :prefix => "SLURM_rbbt_job-") do |tmp_directory|
        options[:slurm_basedir] ||= File.join(tmp_directory, 'workdir')
        slurm_basedir = options[:slurm_basedir]
        inputs_dir = File.join(tmp_directory, 'inputs_dir')
        saved = Step.save_job_inputs(job, inputs_dir, options)
        if saved
          options[:inputs_dir] = inputs_dir
          cmd = ['workflow', 'task', workflow.to_s, task.to_s, '-pf', '--load_inputs', inputs_dir, '--log', (options[:log] || Log.severity).to_s]
        else
          cmd = ['workflow', 'task', workflow.to_s, task.to_s, '-pf', '--log', (options[:log] || Log.severity).to_s]
        end


        template = self.template(cmd, options)
        self.issue_template(template, options.merge(:slurm_basedir => slurm_basedir, :dry_run => dry_run))

        return unless tail

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
      end
    end
  end
end

if __FILE__ == $0
  Log.severity = 0
  iii Marenostrum::SLURM.run('ls', nil, nil, :qos => "debug", :user => 'bsc26892') if __FILE__ == $0
end


