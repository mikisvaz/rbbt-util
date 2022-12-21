class RbbtException < StandardError; end
class ParameterException < RbbtException; end

class MissingParameterException < ParameterException
  def initialize(parameter)
    super("Missing parameter '#{parameter}'")
  end
end

class FieldNotFoundError < StandardError;end
class ClosedStream < StandardError; end

class ProcessFailed < StandardError; 
  def initialize(pid = Process.pid)
    @pid = pid
    @msg = "Process #{@pid} failed"
    super(@msg)
  end

end

class Aborted < StandardError; end

class TryAgain < StandardError; end

class TryThis < StandardError
  attr_accessor :payload
  def initialize(payload = nil)
    @payload = payload
  end
end

class SemaphoreInterrupted < TryAgain; end
class LockInterrupted < TryAgain; end

class RemoteServerError < StandardError; end

class DependencyError < Aborted
  def initialize(msg)
    if defined? Step and Step === msg
      step = msg
      new_msg = [step.path, step.messages.last] * ": "
      super(new_msg)
    else
      super(msg)
    end
  end
end

class DependencyRbbtException < RbbtException
  def initialize(msg)
    if defined? Step and Step === msg
      step = msg

      new_msg = nil
      new_msg = [step.path, step.messages.last] * ": "

      super(new_msg)
    else
      super(msg)
    end
  end
end

class DontClose < Exception; end

class KeepLocked < Exception
  attr_accessor :payload
  def initialize(payload)
    @payload = payload
  end
end

class KeepBar < Exception
  attr_accessor :payload
  def initialize(payload)
    @payload = payload
  end
end

class StopInsist < Exception
  attr_accessor :exception
  def initialize(exception)
    @exception = exception
  end
end
