require_relative '../../refactor'
Rbbt.require_instead 'scout/exceptions'
class ClosedStream < StandardError; end
class RbbtException < StandardError; end
#class ParameterException < RbbtException; end
#
#class MissingParameterException < ParameterException
#  def initialize(parameter)
#    super("Missing parameter '#{parameter}'")
#  end
#end
#
#class FieldNotFoundError < StandardError;end
#class ClosedStream < StandardError; end
#
#class ProcessFailed < StandardError; 
#  attr_accessor :pid, :msg
#  def initialize(pid = Process.pid, msg = nil)
#    @pid = pid
#    @msg = msg
#    if @pid
#      if @msg
#        message = "Process #{@pid} failed - #{@msg}"
#      else
#        message = "Process #{@pid} failed"
#      end
#    else
#      message = "Failed to run #{@msg}"
#    end
#    super(message)
#  end
#end
#
#class SSHProcessFailed < StandardError
#  attr_accessor :host, :cmd
#  def initialize(host, cmd)
#    @host = host
#    @cmd = cmd
#    message = "SSH server #{host} failed cmd '#{cmd}'" 
#    super(message)
#  end
#end
#
#class ConcurrentStreamProcessFailed < ProcessFailed
#  attr_accessor :concurrent_stream
#  def initialize(pid = Process.pid, msg = nil, concurrent_stream = nil)
#    super(pid, msg)
#    @concurrent_stream = concurrent_stream
#  end
#end
#
#class Aborted < StandardError; end
#
#class TryAgain < StandardError; end
#
#class TryThis < StandardError
#  attr_accessor :payload
#  def initialize(payload = nil)
#    @payload = payload
#  end
#end
#
#class SemaphoreInterrupted < TryAgain; end
#class LockInterrupted < TryAgain; end
#
#class RemoteServerError < StandardError; end
#
#class DependencyError < Aborted
#  def initialize(msg)
#    if defined? Step and Step === msg
#      step = msg
#      new_msg = [step.path, step.messages.last] * ": "
#      super(new_msg)
#    else
#      super(msg)
#    end
#  end
#end
#
#class DependencyException < RbbtException
#class DependencyRbbtException < RbbtException
#  def initialize(msg)
#    if defined? Step and Step === msg
#      step = msg
#
#      new_msg = nil
#      new_msg = [step.path, step.messages.last] * ": "
#
#      super(new_msg)
#    else
#      super(msg)
#    end
#  end
#end
#
#class DontClose < Exception; end
#
#class KeepLocked < Exception
#  attr_accessor :payload
#  def initialize(payload)
#    @payload = payload
#  end
#end
#
#class KeepBar < Exception
#  attr_accessor :payload
#  def initialize(payload)
#    @payload = payload
#  end
#end
#
#class StopInsist < Exception
#  attr_accessor :exception
#  def initialize(exception)
#    @exception = exception
#  end
#end
