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
    tasks = doc_parse_chunks tasks, /## (.*)/ 
    {:title => title.strip, :description => description.strip, :task_description => task_description.strip, :tasks => tasks}
  end

  def documentation_markdown
    file = @libdir['workflow.md'].find
    if file.exists?
      file.read
    else
      ""
    end
  end

  def load_documentation
    @documentation = Workflow.parse_workflow_doc documentation_markdown
    @documentation[:tasks].each do |task, description|
      raise "Documentation for #{ task }, but not a #{ self.to_s } task" unless tasks.include? task.to_sym
      tasks[task.to_sym].description = description
    end
  end

  attr_accessor :documentation
  def documentation
    load_documentation if @documentation.nil?
    @documentation 
  end
end
