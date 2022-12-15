require 'rbbt/hpc/batch'

module HPC
  module LSF 
    extend HPC::TemplateGeneration
    extend HPC::Orchestration

    def self.batch_system
      "LSF"
    end

    def self.batch_system_variables
      <<-EOF
let TOTAL_PROCESORS="$(cat /proc/cpuinfo|grep ^processor |wc -l)"
let MAX_MEMORY_DEFAULT="$(grep MemTotal /proc/meminfo|grep -o "[[:digit:]]*") / ( (1024 * $TOTAL_PROCESORS) / $LSB_MAX_NUM_PROCESSORS )"
[ ! -z $LSB_MAX_MEM_RUSAGE ] && let MAX_MEMORY="$LSB_MAX_MEM_RUSAGE" || MAX_MEMORY="$MAX_MEMORY_DEFAULT"
export MAX_MEMORY_DEFAULT
export MAX_MEMORY
export BATCH_JOB_ID=$LSF_JOBID
export BATCH_SYSTEM=#{batch_system}
      EOF
    end

    def self.header(options = {})
      options        = options.dup

      queue          = Misc.process_options options, :queue
      task_cpus      = Misc.process_options options, :task_cpus
      time           = Misc.process_options options, :time
      nodes          = Misc.process_options options, :nodes
      workdir        = Misc.process_options options, :workdir
      exclusive      = Misc.process_options options, :exclusive

      batch_dir  = Misc.process_options options, :batch_dir
      batch_name = Misc.process_options options, :batch_name
      batch_name ||= File.basename(batch_dir)

      fout       = File.join(batch_dir, 'std.out')
      ferr       = File.join(batch_dir, 'std.err')

      time = Misc.format_seconds Misc.timespan(time) unless time.include? ":"

      time = time.split(":").values_at(0, 1) * ":"

      header =<<-EOF
#!/bin/bash
#BSUB -J "#{batch_name}"
#BSUB -cwd "#{workdir}"
#BSUB -oo "#{fout}"
#BSUB -eo "#{ferr}"
#BSUB -q "#{queue}"
#BSUB -n "#{task_cpus}"
#BSUB -W "#{time}"
      EOF

      header << "#BSUB -x" << "\n" if exclusive

      header
    end

    def self.run_template(batch_dir, dry_run)

      fout   = File.join(batch_dir, 'std.out')
      ferr   = File.join(batch_dir, 'std.err')
      fjob   = File.join(batch_dir, 'job.id')
      fdep   = File.join(batch_dir, 'dependencies.list')
      fcfdep = File.join(batch_dir, 'canfail_dependencies.list')
      fexit  = File.join(batch_dir, 'exit.status')
      fsync  = File.join(batch_dir, 'sync.log')
      fcmd   = File.join(batch_dir, 'command.batch')

      return if Open.exists?(fexit)

      STDERR.puts Log.color(:magenta, "Issuing LSF file: #{fcmd}")
      STDERR.puts Open.read(fcmd)

      if File.exist?(fjob)
        job = Open.read(fjob).to_i
      else

        dependencies = Open.read(fdep).split("\n") if File.exist? fdep
        canfail_dependencies = Open.read(fcfdep).split("\n") if File.exist? fcfdep

        normal_dep_list = dependencies && dependencies.any? ? dependencies.collect{|d| "post_done(#{d})"} : []
        canfail_dep_list = canfail_dependencies && canfail_dependencies.any? ? canfail_dependencies.collect{|d| "done(#{d})"} : []

        dep_list = normal_dep_list + canfail_dep_list

        if dep_list.any?
          dep_str = '-w "' + dep_list * " && " + '"'
        else
          dep_str = ""
        end

        cmd = "bsub #{dep_str} < '#{fcmd}'"

        if File.exist?(fout)
          return
        elsif dry_run
          STDERR.puts Log.color(:magenta, "To execute run: ") + Log.color(:blue, cmd)
          STDERR.puts Log.color(:magenta, "To monitor progress run (needs local rbbt): ") + Log.color(:blue, "rbbt lsf tail '#{batch_dir}'")
          raise HPC::SBATCH, batch_dir
        else
          Open.rm fsync
          Open.rm fexit
          Open.rm fout
          Open.rm ferr


          job = CMD.cmd(cmd).read.scan(/\d+/).first.to_i
          Log.debug "BSUB job id: #{job}"
          Open.write(fjob, job.to_s)
          job
        end
      end
    end

    def self.job_status(job = nil)
      if job.nil?
        CMD.cmd("bjobs -w").read
      else
        CMD.cmd("bjobs -w #{job}").read
      end
    end
  end
end

