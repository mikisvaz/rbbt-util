class ParameterException < Exception; end
class FieldNotFoundError < Exception;end
class TryAgain < Exception; end
class ClosedStream < Exception; end

class ProcessFailed < Exception; end
class Aborted < Exception; end

class RemoteServerError < Exception; end

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
