module Workflow

  Rbbt.claim Rbbt.software.opt.jar["cromwell.jar"], :url, "https://github.com/broadinstitute/cromwell/releases/download/48/cromwell-48.jar"
  Rbbt.claim Rbbt.software.opt.jar["wdltool.jar"], :url, "https://github.com/broadinstitute/wdltool/releases/download/0.14/wdltool-0.14.jar"

  def run_cromwell(file, work_dir, options = {})
    cromwell_inputs_file = Misc.process_options options, :cromwell_inputs_file
    jar = Rbbt.software.opt.jar["cromwell.jar"].produce.find
    if cromwell_inputs_file
      CMD.cmd_log("java -jar '#{jar}' run '#{file}' --workflow-root='#{work_dir}' -i #{cromwell_inputs_file}", options.merge("add_option_dashes" => true))
    else
      CMD.cmd_log("java -jar '#{jar}' run '#{file}' --workflow-root='#{work_dir}'", options.merge("add_option_dashes" => true))
    end
  end

  def load_cromwell(file)
    jar = Rbbt.software.opt.jar["wdltool.jar"].produce.find
    inputs = JSON.load(CMD.cmd("java -jar '#{jar}' inputs '#{file}'"))

    workflow_inputs = {}
    inputs.each do |input,input_type|
      workflow, task, input_name = input.split(".")
      workflow_inputs[workflow] ||= {}

      if input_name.nil?
        input_name = task
      else
        input_name = [task, input_name] * "."
      end

      workflow_inputs[workflow][input_name] = input_type
    end

    workflow_inputs.each do |workflow,input_list|
      input_list.each do |input_name,input_type|

        input_type = input_type.split(" ").last.sub('?','')
        input_type_fixed = case input_type
                            when "File", "file"
                              :file
                            when "Int"
                              :integer
                            when /Array/
                              :array
                            else
                              input_type.downcase.to_sym
                            end

        desc = [workflow, input_name] * "."
        default = nil
        input input_name, input_type_fixed, desc, default, :nofile => true
      end

      task workflow => :string do |*args|
        cromwell = file
        options = {}
        Misc.in_dir(self.files_dir) do
          options["metadata-output"] = file('metadata.json')
          options["inputs"] = file('inputs')

          cromwell_inputs = {}
          self.inputs.to_hash.each do |input, value|
            next if value.nil?
            key = [workflow.to_s, input] * "."
            cromwell_inputs[key] = value
          end

          Open.write(file('inputs'), cromwell_inputs.to_json )
          Cromwell.run_cromwell(cromwell, self.files_dir, options)
        end
        Open.read(Dir.glob(File.join(files_dir, "/cromwell-executions/#{workflow}/*/call-*/execution/stdout")).first)
      end

    end
  end
end
