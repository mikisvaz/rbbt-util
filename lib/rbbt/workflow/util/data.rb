require 'rbbt/workflow'
require 'rbbt/workflow/examples'

module Workflow
  module Data
    def data(directory)
      @@data_directory = directory
    end

    def get_datadir(clean_name)
      data_dir = File.join(@@data_directory, clean_name)
      raise "Data dir not found #{data_dir}" unless File.directory?(data_dir)
      if Path === @@data_directory
        @@data_directory.annotate data_dir
      else
        Path.setup(data_dir)
      end
    end

    def data_task(name, workflow, oname, *rest, &block)
      dep_task(name, workflow, oname, *rest) do |jobname, options|
        data_dir = self.get_datadir(jobname)
        task_info = workflow.task_info(oname)
        dir_options = Workflow.load_inputs(data_dir.options, task_info[:inputs], task_info[:input_types])
        data_options = block.call data_dir, dir_options, task_info
        {:inputs => data_options.merge(options)}
      end
    end
  end

end
