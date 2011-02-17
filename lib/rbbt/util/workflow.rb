require 'rake'
require 'rbbt/util/rake'

module WorkFlow
  module Runner
    def data
      $_workflow_prereq
    end

    def input
      $_workflow_input
    end

    $_workflow_default_persistence = :string
    def default_persistence
      $_workflow_default_persistence
    end

    def default_persistence=(type)
      $_workflow_default_persistence = type
    end

    def step(step_name, options = nil)
      dependencies, options = case
                              when ((String === options or Symbol === options) and %w(string marshal yaml tsv tsv_string).include? options.to_s)
                                [nil, {:persistence_type => options}]
                              when Hash === options
                                [nil, options]
                              else
                                [options, {}]
                              end

      options = Misc.add_defaults options, :persistence_type => default_persistence
      persistence_type = Misc.process_options options, :persistence_type
      dependencies = Misc.process_options options, :dependencies if options.include? :dependencies

      re = Regexp.new(/(?:^|\/)#{Regexp.quote step_name.to_s}\/.*$/)

      @last_step             = nil unless defined? @last_step
      @last_persistence_type = nil unless defined? @last_persistence_type

      if dependencies.nil? && ! @last_step.nil?
        dependencies = @last_step
      end
      @last_step = step_name

      # Generate the Hash definition
      rule_def = case 
                 when dependencies.nil?
                   re
                 when String === dependencies || Symbol === dependencies
                   {re => lambda{|filename| filename.sub(step_name.to_s, dependencies.to_s) }}
                 when Array === dependencies
                   {re => lambda{|filename| dependencies.collect{|dep| filename.sub(step_name.to_s, dep.to_s) } }}
                 when Proc === dependencies
                   {re => dependencies}
                 end

      @last_step = step_name
      last_persistence_type, @last_persistence_type = @last_persistence_type, persistence_type

      rule rule_def do |t|
        Persistence.persist(t, "", persistence_type, :persistence_file => t.name) do |t, options|
          $_workflow_prereq = case
                   when (t.prerequisites.nil? or (Array === t.prerequisites and t.prerequisites.empty?))
                     nil
                   else
                     Persistence.persist(t.prerequisites.first, "", last_persistence_type, :persistence_file => t.prerequisites.first) do
                       raise "Error, this file should be produced already"
                     end
                   end
          Log.high "Executing step [#{ step_name }]"
          yield t, options
        end
      end
    end
  end

  def self.run(file = :default, workflow_input = nil, &block)
    $_workflow_input = workflow_input
    RakeHelper.run("Runtime", file) do
      yield
    end
  end

  def self.load(wf_file, file = :default, workflow_input = nil)
    $_workflow_input = workflow_input
    RakeHelper.run(wf_file, file)
  end
end

