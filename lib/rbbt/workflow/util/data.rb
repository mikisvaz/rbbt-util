require 'rbbt/workflow'
require 'rbbt/workflow/examples'

module Workflow
  module Data
    class DataNotFound < ScoutException; end

    def data_dir(directory)
      @@data_directory = directory
    end

    def get_datadir(clean_name)
      data_dir = File.join(@@data_directory, clean_name)
      raise DataNotFound, "Data dir not found for #{clean_name} in #{@@data_directory}" unless File.directory?(data_dir)
      if Path === @@data_directory
        @@data_directory.annotate data_dir
      else
        Path.setup(data_dir)
      end
    end

    def get_file(clean_name, type = nil, extension = nil)
      begin
        extension = nil if String === extension && extension.empty?
        file1 = File.join(@@data_directory, type.to_s, (extension.nil? ? clean_name.to_s : clean_name.to_s + ".#{extension}"))
        file2 = File.join(@@data_directory, clean_name.to_s, (extension.nil? ? type.to_s : type.to_s + ".#{extension}"))
        if Open.exists?(file1)
          data_file = file1
        elsif Open.exists?(file2)
          data_file = file2
        else
          raise DataNotFound, "Data type #{type} not found for #{clean_name} in #{@@data_directory}"
        end
      end

      if Path === @@data_directory
        @@data_directory.annotate data_file
      else
        Path.setup(data_file)
      end
    end

    def data_task(name, workflow, oname, *rest, &block)
      dep_task(name, workflow, oname, *rest) do |jobname, options,dependencies|
        begin
          task_info = workflow.nil? ? nil : workflow.task_info(oname) 
          options[:extension] ||= task_info[:extension]
          path = get_file jobname, name, options[:extension]
          job = Step.new path
          job.task_name = name
          job.result_type = options[:result_type] || options[:type] || options[:extension]
          job
        rescue DataNotFound
        end

        next job if job

        data_options = {}
        data_options = data_options.merge(Workflow.load_inputs(File.join(@@data_directory,"options"), task_info[:inputs], task_info[:input_types]))

        begin
          data_options = data_options.merge(Workflow.load_inputs(get_file(jobname, :options), task_info[:inputs], task_info[:input_types]))
        rescue DataNotFound
        end

        begin
          data_options = data_options.merge(Workflow.load_inputs(get_datadir(jobname).options, task_info[:inputs], task_info[:input_types]))
        rescue DataNotFound
        end

        begin
          task_info = workflow.nil? ? nil : workflow.task_info(oname) 
          data_options = block.call get_datadir(jobname), data_options, task_info
        rescue
          Log.exception $!
        end if block_given?

        case data_options
        when Step
          next data_options
        when Hash
          if data_options.include?(:inputs)
            data_options = data_options.merge(options)
            workflow = data_options[:workflow] if data_options[:workflow]
            oname = data_options[:task] if data_options[:task]
            inputs = options.merge(data_options[:inputs])
          else
            inputs = options.merge(data_options)
          end

          {:workflow => workflow, :task => oname, :jobname => jobname, :inputs => inputs}
        else
          raise "Cannot understand data_options: #{data_options}"
        end

      end
    end

    def data(name, options = {}, &block)
      dep do |jobname, job_options, dependencies|
        if block_given?
          block.call jobname, job_options.merge(options), dependencies
        else
          begin
            path = get_file jobname, name, options[:extension]
            job = Step.new path
            job.task_name = name
            job.result_type = options[:result_type] || options[:type] || options[:extension]
            job
          rescue DataNotFound
            {:task => name, :options => options, :jobname => jobname}
          end
        end
      end
    end
  end
end
#module Workflow
#  module Data
#    def data(directory)
#      @@data_directory = directory
#    end
#
#    def get_datadir(clean_name)
#      data_dir = File.join(@@data_directory, clean_name)
#      raise "Data dir not found #{data_dir}" unless File.directory?(data_dir)
#      if Path === @@data_directory
#        @@data_directory.annotate data_dir
#      else
#        Path.setup(data_dir)
#      end
#    end
#
#    def data_task(name, workflow, oname, *rest, &block)
#      dep_task(name, workflow, oname, *rest) do |jobname, options|
#        data_dir = self.get_datadir(jobname)
#        task_info = workflow.task_info(oname)
#
#        dir_options = Workflow.load_inputs(data_dir.options, task_info[:inputs], task_info[:input_types])
#        data_options = block.call data_dir, dir_options, task_info
#
#        case data_options
#        when Step
#          job = data_options
#        when Hash
#          if data_options.include?(:inputs)
#            data_options = data_options.merge(options)
#            workflow = data_options[:workflow] if data_options[:workflow]
#            oname = data_options[:task] if data_options[:task]
#            inputs = options.merge(data_options[:inputs])
#          else
#            inputs = options.merge(data_options)
#          end
#
#          job = workflow.job(oname, jobname, inputs)
#        else
#          raise "Cannot understand data_options: #{data_options}"
#        end
#
#        job
#      end
#    end
#  end
#
#end
