module RbbtPython
  class << self
    attr_accessor :thread
  end

  def self.thread
    @thread ||= defined?(@thread) ? @thread : nil
  end

  MUTEX= Mutex.new
  QUEUE_IN ||= Queue.new
  QUEUE_OUT ||= Queue.new
  def self.synchronize(&block)
    MUTEX.synchronize &block
  end

  def self.init_thread
    if defined?(self.thread) && (self.thread && ! self.thread.alive?)
      Log.warn "Reloading RbbtPython thread"
      self.thread.join
      self.thread = nil
    end

    self.thread ||= Thread.new do
      require 'pycall'
      RbbtPython.run 'sys' do
        RbbtPython.paths.each do |path|
          sys.path.append path
        end
        nil
      end
      begin
        while block = QUEUE_IN.pop
          break if block == :stop
          res = 
            begin
              module_eval(&block)
            rescue Exception
              Log.exception $!
              raise $!
            end

          QUEUE_OUT.push res
        end
      rescue Exception
        Log.exception $!
        raise $!
      ensure
        PyCall.finalize if PyCall.respond_to?(:finalize)
      end
    end
  end

  def self.run_in_thread(&block)
    self.synchronize do
      init_thread
      QUEUE_IN.push block
      QUEUE_OUT.pop
    end
  end

  def self.stop_thread
    self.synchronize do
      QUEUE_IN.push :stop
    end if self.thread && self.thread.alive?
    self.thread.join if self.thread
  end

  def self.run_direct(mod = nil, imports = nil, &block)
    iii [:run, mod, imports, block]
    if mod
      if Hash === imports
        pyimport mod, **imports
      elsif imports.nil?
        pyimport mod 
      else
        pyfrom mod, :import => imports
      end
    end 

    module_eval(&block)
  end

  def self.run_threaded(mod = nil, imports = nil, &block)
    run_in_thread do
        if Hash === imports
          pyimport mod, **imports
        elsif imports.nil?
          pyimport mod 
        else
          pyfrom mod, :import => imports
        end
    end if mod

    run_in_thread(&block)
  end

  def self.run_simple(mod = nil, imports = nil, &block)
    self.synchronize do
      RbbtPython.run_direct 'sys' do
        while path = RbbtPython.paths.pop
          sys.path.append path
        end
        nil
      end
      run_direct(mod, imports, &block)
    end
  end

  class << self
    alias run run_simple
  end
end
