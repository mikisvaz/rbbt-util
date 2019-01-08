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
      inputs_dir       = options.delete :inputs_dir

      if contain_and_sync
        contain = "/scratch/tmp/rbbt" if contain.nil?
        sync = "~/.rbbt/var/jobs" if sync.nil?
      end

      contain = File.expand_path(contain) if contain

      singularity = true if contain || ! development


      name = options[:name] ||= Misc.obj2digest({:options => options.collect{|k,v| [k,v]}.sort_by{|k,v| k.to_s }, :args => args})
      workdir = options[:workdir] ||= File.expand_path(File.join('~/rbbt-workdir', name)) if workdir.nil?

      rbbt_cmd = args.reject{|e| e == '--' }.collect{|e| e.include?(" ")? '"' + e + '"' : e } * " "

      queue = options[:queue] || 'bsc_ls'
      tasks = options[:tasks] || 1
      time = options[:time] || "0:00:10"

      time = Misc.format_seconds Misc.timespan(time) unless time.include? ":"


      #{{{ PREPARE LOCAL LOGFILES

      Open.mkdir workdir

      fout = File.join(workdir, 'std.out')
      ferr = File.join(workdir, 'std.err')
      fjob = File.join(workdir, 'job.id')
      fexit = File.join(workdir, 'exit.status')
      fsync = File.join(workdir, 'sync.log')
      fcmd = File.join(workdir, 'command.slurm')

      #{{{ GENERATE TEMPLATE

      # HEADER
      header =<<-EOF
#!/bin/bash
#SBATCH --qos="#{queue}"
#SBATCH --job-name="#{name}"
#SBATCH --workdir="#{Dir.pwd}"
#SBATCH --output="#{fout}"
#SBATCH --error="#{ferr}"
#SBATCH --ntasks="#{tasks}"
#SBATCH --time="#{time}"
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
      EOF

      if singularity
        env +=<<-EOF
module load intel/2018.1
module load singularity
module load samtools
SINGULARITY_IMG="$HOME/projects/rbbt.singularity.img"
SINGULARITY_RUBY_INLINE="$HOME/.singularity_ruby_inline"
mkdir -p "$SINGULARITY_RUBY_INLINE"
        EOF
      end

      if contain
        env +=<<-EOF

# Prepare container dir
CONTAINER_DIR="#{contain}"
mkdir -p $CONTAINER_DIR/.rbbt/etc/
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
echo "user_projects: $CONTAINER_DIR/projects/#{ENV['USER']}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
echo "user_scratch: $CONTAINER_DIR/scratch/#{ENV['USER']}/{PKGDIR}/{TOPLEVEL}/{SUBPATH}" >> $CONTAINER_DIR/.rbbt/etc/search_paths
echo "/scratch/tmp/rbbt/projects/rbbt/workflows/" > $CONTAINER_DIR/.rbbt/etc/workflow_dir
        EOF
        
        if inputs_dir
          env +=<<-EOF

# Copy inputs
cp -R '#{inputs_dir}' $CONTAINER_DIR/inputs
          EOF
          rbbt_cmd = rbbt_cmd.sub(inputs_dir, "#{contain}/inputs")
        end

        if copy_image
          env +=<<EOF
rsync -avz "$SINGULARITY_IMG" "$CONTAINER_DIR/rbbt.singularity.img"
SINGULARITY_IMG="$CONTAINER_DIR/rbbt.singularity.img"
EOF
        end

        if  wipe_container == "pre" || wipe_container == "both"
          env +=<<-EOF
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -Rfv .rbbt/var/jobs &>> #{fsync}
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rbbt system clean -f &>> #{fsync}
EOF
        end
      end

      # RUN
      run = ""

      if singularity
        if contain
          group = File.basename(File.dirname(ENV['HOME']))
          scratch_group_dir = File.join('/gpfs/scratch/', group)
          projects_group_dir = File.join('/gpfs/projects/', group)
          exec_cmd = %(singularity exec -e -C -H "$CONTAINER_DIR" -B "$SINGULARITY_RUBY_INLINE":"$CONTAINER_DIR/.ruby_inline":rw  -B ~/git:"$CONTAINER_DIR/git":ro -B #{scratch_group_dir}:"$CONTAINER_DIR/scratch":ro -B ~/.rbbt/software/opt/:"/opt/":ro  -B ~/.rbbt:"$CONTAINER_DIR/home/":ro -B #{projects_group_dir}:"$CONTAINER_DIR/projects":ro "$SINGULARITY_IMG" env TMPDIR="$CONTAINER_DIR/.rbbt/tmp" rbbt)
        else
          exec_cmd = %(singularity exec -e -B "$SINGULARITY_RUBY_INLINE":"$HOME/.ruby_inline":rw "$SINGULARITY_IMG" rbbt)
        end

        if development
          exec_cmd += ' --dev=git'
        end
      else
        exec_cmd = %(~/git/rbbt-util/bin/rbbt --dev=~/git/)
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

        coda +=<<-EOF
rsync -avt "#{File.join(File.expand_path(contain), '.rbbt/var/jobs')}/" "#{File.expand_path(sync)}/" &>> #{fsync} 
EOF

        if  contain && (wipe_container == "post" || wipe_container == "both")
          coda +=<<-EOF
sync_es="$?" 
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rbbt system clean -f &>> #{fsync}
singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -v /dev/shm/sem.*.{in,out,process} /dev/shm/sem.Session-PID.*.sem 2> /dev/null >> #{fsync}
if [ $sync_es == '0' ]; then 
    singularity exec -e -C -H "$CONTAINER_DIR" "$SINGULARITY_IMG" rm -Rfv .rbbt/var/jobs &>> #{fsync}
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

      workdir = options[:workdir]
      Open.mkdir workdir

      dry_run = options.delete :dry_run

      fout  = File.join(workdir, 'std.out')
      ferr  = File.join(workdir, 'std.err')
      fjob  = File.join(workdir, 'job.id')
      fexit = File.join(workdir, 'exit.status')
      fsync = File.join(workdir, 'sync.log')
      fcmd  = File.join(workdir, 'command.slurm')

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
          STDERR.puts Log.color(:magenta, "To execute run: sbatch '#{workdir}/command.slurm'")
          raise Marenostrum::SBATCH, workdir
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

    def self.follow_job(workdir, tail = true)
      fjob = File.join(workdir, 'job.id')
      fout = File.join(workdir, 'std.out')
      ferr = File.join(workdir, 'std.err')
      fstatus = File.join(workdir, 'job.status')

      job = Open.read(fjob) if Open.exists?(fjob)

      if job
        status_txt = CMD.cmd("squeue --job #{job}").read
        STDERR.puts Log.color(:magenta, "Status [#{job.to_i}]:")
        STDERR.puts status_txt
        lines = status_txt.split("\n").length
      end

      if tail
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
          err = CMD.cmd("tail -f '#{ferr}'", :pipe => true) if File.exists? ferr

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

    def self.wait_for_job(workdir, time = 1)
      fexit = File.join(workdir, 'exit.status')
      fjob = File.join(workdir, 'job.id')
      job = Open.read(fjob) if Open.exists?(fjob)


      while ! Open.exists?(fexit)
        sleep time
      end
    end

    def self.run_job(job, options = {})
      workflow = job.workflow
      task = job.task_name
      name = job.clean_name
      keep_workdir = options.delete :keep_SLURM_workdir 
      TmpFile.with_file(nil, !keep_workdir) do |tmp_directory|
        workdir = options[:workdir] ||= File.join(tmp_directory, 'workdir')
        inputs_dir = File.join(tmp_directory, 'inputs_dir')
        Step.save_job_inputs(job, inputs_dir)
        options[:inputs_dir] = inputs_dir
        cmd = ['workflow', 'task', workflow.to_s, task.to_s, '-pf', '-jn', name, '--load_inputs', inputs_dir, '--log', (options[:log] || Log.severity).to_s]

        %w(workflows requires remote_workflow_tasks override_deps).each do |key|
          next unless options[key]
          cmd += ["--#{key.to_s}", options[key]]
        end

        template = self.template(cmd, options)
        self.issue_template(template, options)
        t_monitor = Thread.new do
          self.follow_job(workdir, :STDERR)
        end
        self.wait_for_job(workdir)
        t_monitor.raise Aborted
        return unless Open.read(File.join(workdir, 'exit.status')).strip == '0'
        path = Open.read(File.join(workdir, 'std.out')).strip
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


