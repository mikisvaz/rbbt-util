require 'rbbt/hpc/batch'
require 'rbbt/hpc/orchestrate'

module HPC
  module SLURM 
    extend HPC::TemplateGeneration
    extend HPC::Orchestration

    def self.batch_system
      "SLURM"
    end

    def self.batch_system_variables
      <<-EOF
let TOTAL_PROCESORS="$(cat /proc/cpuinfo|grep ^processor |wc -l)"
let MAX_MEMORY_DEFAULT="$(grep MemTotal /proc/meminfo|grep -o "[[:digit:]]*") / ( (1024 * $TOTAL_PROCESORS) / $SLURM_CPUS_PER_TASK )"
MAX_MEMORY="$MAX_MEMORY_DEFAULT"
[ ! -z $SLURM_MEM_PER_CPU ] && let MAX_MEMORY="$SLURM_MEM_PER_CPU * $SLURM_CPUS_PER_TASK" 
[ ! -z $SLURM_MEM_PER_NODE ] && MAX_MEMORY="$SLURM_MEM_PER_NODE"
export MAX_MEMORY_DEFAULT
export MAX_MEMORY
export BATCH_JOB_ID=$SLURM_JOB_ID
export BATCH_SYSTEM=#{batch_system}
      EOF
    end

    def self.header(options = {})
      options = options.dup

      queue      = Misc.process_options options, :queue
      account    = Misc.process_options options, :account
      partition  = Misc.process_options options, :partition
      task_cpus  = Misc.process_options options, :task_cpus
      time       = Misc.process_options options, :time
      nodes      = Misc.process_options options, :nodes
      workdir    = Misc.process_options options, :workdir
      exclusive  = Misc.process_options options, :exclusive
      highmem    = Misc.process_options options, :highmem
      licenses   = Misc.process_options options, :licenses
      constraint = Misc.process_options options, :constraint
      gres       = Misc.process_options options, :gres

      constraint     = [constraint, "highmem"].compact * "&" if highmem

      mem            = Misc.process_options options, :mem
      mem_per_cpu    = Misc.process_options options, :mem_per_cpu

      batch_dir  = Misc.process_options options, :batch_dir
      batch_name = Misc.process_options options, :batch_name

      fout       = File.join(batch_dir, 'std.out')
      ferr       = File.join(batch_dir, 'std.err')

      time = Misc.format_seconds Misc.timespan(time) unless time.include? ":"

      sbatch_params = {"job-name" => batch_name,
                       "qos" => queue,
                       "account" => account,
                       "partition" => partition,
                       "output" => fout,
                       "error" => ferr,
                       "cpus-per-task" => task_cpus,
                       "nodes" => nodes,
                       "time" => time,
                       "constraint" => constraint,
                       "exclusive" => exclusive,
                       "licenses" => licenses,
                       "gres" => gres,
                       "mem" => mem,
                       "mem-per-cpu" => mem_per_cpu,
      }


      header =<<-EOF
#!/bin/bash
      EOF

      sbatch_params.each do |name,value|
        next if value.nil? || value == ""
        if TrueClass === value
          header << "#SBATCH --#{name}" << "\n"
        elsif Array === value
          value.each do |v|
            header << "#SBATCH --#{name}=\"#{v}\"" << "\n"
          end
        else
          header << "#SBATCH --#{name}=\"#{value}\"" << "\n"
        end
      end

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

      Log.info "Issuing SLURM file: #{fcmd}"
      Log.debug Open.read(fcmd)

      if File.exist?(fjob)
        job = Open.read(fjob).to_i
      else

        dependencies = Open.read(fdep).split("\n") if File.exist? fdep
        canfail_dependencies = Open.read(fcfdep).split("\n") if File.exist? fcfdep

        normal_dep_str = dependencies && dependencies.any? ? "afterok:" + dependencies * ":" : nil
        canfail_dep_str = canfail_dependencies && canfail_dependencies.any? ? "afterany:" + canfail_dependencies * ":" : nil

        if normal_dep_str.nil? && canfail_dep_str.nil?
          dep_str = ""
        else
          dep_str = '--dependency=' + [normal_dep_str, canfail_dep_str].compact * ","
        end

        cmd = "sbatch #{dep_str} '#{fcmd}'"

        if File.exist?(fout)
          return
        elsif dry_run
          STDERR.puts Log.color(:magenta, "To execute run: ") + Log.color(:blue, "sbatch '#{fcmd}'")
          STDERR.puts Log.color(:magenta, "To monitor progress run (needs local rbbt): ") + Log.color(:blue, "rbbt slurm tail '#{batch_dir}'")
          raise HPC::BATCH_DRY_RUN, batch_dir
        else
          Open.rm fsync
          Open.rm fexit
          Open.rm fout
          Open.rm ferr

          job = CMD.cmd(cmd).read.scan(/\d+/).first.to_i
          Log.debug "SBATCH job id: #{job}"
          Open.write(fjob, job.to_s)
          job
        end
      end
    end

    def self.job_status(job = nil)
      if job.nil?
        CMD.cmd("squeue").read
      else
        begin
          CMD.cmd("squeue --job #{job}").read
        rescue
          ""
        end
      end
    end

  end
end

