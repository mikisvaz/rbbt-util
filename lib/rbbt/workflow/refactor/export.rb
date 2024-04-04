module Workflow

  annotation :asynchronous_exports, :synchronous_exports, :exec_exports, :stream_exports

  def asynchronous_exports
    @asynchronous_exports ||= []
  end

  def synchronous_exports
    @synchronous_exports ||= []
  end

  def exec_exports
    @exec_exports ||= []
  end

  def stream_exports
    @exec_exports ||= []
  end


  def all_exports
    asynchronous_exports + synchronous_exports + exec_exports + stream_exports
  end

  alias task_exports all_exports

  def unexport(*names)
    names = names.collect{|n| n.to_s} + names.collect{|n| n.to_sym}
    names.uniq!
    exec_exports.replace exec_exports - names if exec_exports
    synchronous_exports.replace synchronous_exports - names if synchronous_exports
    asynchronous_exports.replace asynchronous_exports - names if asynchronous_exports
    stream_exports.replace stream_exports - names if stream_exports
  end
  
  def export_exec(*names)
    unexport *names
    exec_exports.concat names
    exec_exports.uniq!
    exec_exports
  end

  def export_synchronous(*names)
    unexport *names
    synchronous_exports.concat names
    synchronous_exports.uniq!
    synchronous_exports
  end

  def export_asynchronous(*names)
    unexport *names
    asynchronous_exports.concat names
    asynchronous_exports.uniq!
    asynchronous_exports
  end

  def export_stream(*names)
    unexport *names
    stream_exports.concat names
    stream_exports.uniq!
    stream_exports
  end

  alias export export_asynchronous
end
