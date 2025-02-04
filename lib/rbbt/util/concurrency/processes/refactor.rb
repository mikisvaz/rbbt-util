require 'scout/work_queue'
class RbbtProcessQueue < WorkQueue

  self.define_method(:start_process, WorkQueue.instance_method(:process))

  def init(&block)
    @worker_proc = block
    start_process(&@callback)
  end

  def callback(&block)
    @callback = block
  end

  def process(obj)
    self.write(obj)
  end

  alias add_process add_worker
  alias remove_process remove_one_worker
  alias remove_process remove_one_worker
end
