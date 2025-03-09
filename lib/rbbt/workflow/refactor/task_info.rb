#require_relative 'export'
#require_relative 'recursive'
#module Workflow
#  def task_info(name)
#    name = name.to_sym
#    task = tasks[name]
#    raise "No '#{name}' task in '#{self.to_s}' Workflow" if task.nil?
#    id = File.join(self.to_s, name.to_s)
#    @task_info ||= {}
#    @task_info[id] ||= begin 
#                         description = task.description
#                         result_description = task.result_description
#                         returns = task.returns
#
#                         inputs = rec_inputs(name).uniq
#                         input_types = rec_input_types(name)
#                         input_descriptions = rec_input_descriptions(name)
#                         input_use = rec_input_use(name)
#                         input_defaults = rec_input_defaults(name)
#                         input_options = rec_input_options(name)
#                         extension = task.extension
#                         export = case
#                                  when (synchronous_exports.include?(name.to_sym) or synchronous_exports.include?(name.to_s))
#                                    :synchronous
#                                  when (asynchronous_exports.include?(name.to_sym) or asynchronous_exports.include?(name.to_s))
#                                    :asynchronous
#                                  when (exec_exports.include?(name.to_sym) or exec_exports.include?(name.to_s))
#                                    :exec
#                                  when (stream_exports.include?(name.to_sym) or stream_exports.include?(name.to_s))
#                                    :stream
#                                  else
#                                    :none
#                                  end
#
#                         dependencies = tasks[name].deps
#                         { :id => id,
#                           :description => description,
#                           :export => export,
#                           :inputs => inputs,
#                           :input_types => input_types,
#                           :input_descriptions => input_descriptions,
#                           :input_defaults => input_defaults,
#                           :input_options => input_options,
#                           :input_use => input_use,
#                           :returns => returns,
#                           #:result_type => result_type,
#                           #:result_description => result_description,
#                           :dependencies => dependencies,
#                           :extension => extension
#                         }
#                       end
#  end
#end
#
#module Task
#  def result_description
#    ""
#  end
#
#  def result_type
#    @returns
#  end
#
#end
#
#
