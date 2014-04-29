class ParameterException < Exception; end
class FieldNotFoundError < Exception;end
class Aborted < Exception
  def initialize(*args)
    super(*args)
  end
end
class TryAgain < Exception; end
class ClosedStream < Exception; end
class ProcessFailed < Exception; end
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
