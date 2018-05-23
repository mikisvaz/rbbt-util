class RbbtException < StandardError; end
class ParameterException < RbbtException; end
class FieldNotFoundError < RbbtException;end
class TryAgain < RbbtException; end
class ClosedStream < RbbtException; end

class ProcessFailed < RbbtException; end
class Aborted < RbbtException; end

class RemoteServerError < RbbtException; end

class DependencyError < Aborted
  def initialize(msg)
    if defined? Step and Step === msg
      step = msg
      workflow = step.path.split("/")[-3]
      new_msg = [workflow, step.short_path, step.messages.last] * " - "
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

