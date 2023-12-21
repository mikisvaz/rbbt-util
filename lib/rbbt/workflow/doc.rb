module Workflow

  def self.doc_parse_first_line(str)
    if str.match(/^([^\n]*)\n\n(.*)/sm)
      str.replace $2
      $1
    else
      ""
    end
  end

  def self.doc_parse_up_to(str, pattern, keep = false)
    pre, _pat, _post = str.partition pattern
    if _pat
      [pre, (keep ? _pat << _post : _post)]
    else
      _post
    end
  end

  def self.doc_parse_chunks(str, pattern)
    parts = str.split(pattern)
    return {} if parts.length < 2
    tasks = Hash[*parts[1..-1].collect{|v| v.strip}]
    tasks.delete_if{|t,d| d.empty?}
    tasks
  end

  def self.parse_workflow_doc(doc)
    title = doc_parse_first_line doc
    description, task_info = doc_parse_up_to doc, /^# Tasks/i
    task_description, tasks = doc_parse_up_to task_info, /^##/, true
    tasks = doc_parse_chunks tasks, /^## (.*)/ 
    {:title => title.strip, :description => description.strip, :task_description => task_description.strip, :tasks => tasks}
  end

  def documentation_markdown
    return "" if @libdir.nil?
    file = @libdir['workflow.md'].find
    file = @libdir['README.md'].find unless file.exists?
    if file.exists?
      file.read
    else
      ""
    end
  end

  def load_documentation
    return if @documentation
    @documentation ||= Workflow.parse_workflow_doc documentation_markdown
    @documentation[:tasks].each do |task, description|
      if task.include? "#"
        workflow, task = task.split("#")
        workflow = begin
                     Kernel.const_get workflow
                   rescue
                     next
                   end
      else
        workflow = self
      end

      if workflow.tasks.include? task.to_sym
        workflow.tasks[task.to_sym].description = description
      else
        Log.low "Documentation for #{ task }, but not a #{ workflow.to_s } task" 
      end
    end
  end

  attr_accessor :documentation
  def documentation
    load_documentation if @documentation.nil?
    @documentation 
  end
end
