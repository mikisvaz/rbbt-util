require 'rbbt-util'
require 'rbbt/util/cmd'

module Marenostrum
  SERVER='mn1'
  module SLURM
    TEMPLATE=<<-EOF


    EOF
    def self.issue(cmd, name = nil, time = nil, options = {})
      name = "rbbt-job-" << rand(10000).to_s if name.nil?
      time = "00:00:10" if time.nil?
      workdir = "rbbt-workdir/" << name

      new_options = Misc.add_defaults options, "job-name" => name, "workdir" => workdir,
        "output" => "log", 
        "error" => "log.err", 
        "ntasks" => "1", 
        "time" => time.to_s,
        "user" => ENV["USER"],
        "key_file" => File.join(ENV['HOME'], '.ssh/id_rsa.pub')

      options.merge!(new_options)
      IndiferentHash.setup(options)

      key_file, user = options.values_at :key_file, :user

      template = <<-EOF
#!/bin/bash
#{options.collect do |name,value|
      next if name == 'user'
      next if name == 'key_file'
name = "--" << name.to_s.sub(/^--/,'')
[name, '"' << value << '"'] * "="
  end.compact.collect{|str| "#SBATCH #{str}"} * "\n"
}
#{ cmd }
      EOF

      TmpFile.with_file do |slurm|
        res = nil
        begin
          Open.write(slurm, template)
          Log.medium("Issuing job:\n" + template)

          cmd = "ssh -i #{key_file} #{ user }@#{ SERVER } mkdir -p '#{ workdir }'; scp -i #{key_file} #{ slurm } #{ user }@#{ SERVER }:'#{ workdir }/#{ name }.slurm'; ssh -i #{key_file} #{ user }@#{ SERVER } sbatch #{ workdir }/#{ name }.slurm"
          Log.debug cmd
          res = CMD.cmd(cmd).read
        rescue
          raise "Could not issue job"
        end

        res.scan(/\d+/).first
      end
    end

    def self.query(id, options = {})
      key_file, user = options.values_at :key_file, :user

      res = nil
      begin
        cmd = "ssh -i #{key_file} #{ user }@#{ SERVER } squeue |grep '#{id}\\|JOBID'"
        Log.debug cmd
        res = CMD.cmd(cmd).read
      rescue
        raise "Could not query job: #{ id }" << $!.message
      end

      res
    end

    def self.done?(id, options = {})
      ! query(id, options).include? id
    end

    def self.gather(id, file = nil, options = {})
      key_file, user, workdir, output = options.values_at :key_file, :user, :workdir, :output

      TmpFile.with_file do |result|

        begin
          cmd = "scp -i #{key_file} #{ user }@#{ SERVER }:'#{workdir}/#{output}' '#{ result }' " 
          Log.debug cmd
          CMD.cmd(cmd)
          if file.nil?
            Open.read(result)
          else
            Misc.sensiblewrite(result, file)
          end
        rescue
          raise "Could not gather job: #{ id }: " << $!.message
        end
      end
    end

    def self.run(cmd, name = nil, time = nil, options = {})
      sleep_time = Misc.process_options options, :sleep

      id = issue(cmd, name, time, options)

      if sleep_time.nil?
        times = [1,2,3,5,10,30]
      else
        times = Array ===  sleep_time ? sleep_time.dup : [sleep_time]
      end

      while not done?(id, options)
        Log.debug "Waiting on #{ id }"
        sleep_time = times.shift || sleep_time
        sleep sleep_time
      end

      gather(id, nil, options)
    end
  end
end

Log.severity = 0 if __FILE__ == $0
iii Marenostrum::SLURM.run('ls', nil, nil, :qos => "debug", :user => 'bsc26892') if __FILE__ == $0


