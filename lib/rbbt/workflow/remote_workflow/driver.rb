require 'rbbt/workflow/remote_workflow/driver/rest'
require 'rbbt/workflow/remote_workflow/driver/ssh'

class RemoteWorkflow
  def self.fix_hash(hash, fix_values = false)
    fixed = {}
    hash.each do |key, value|
      fixed[key.to_sym] = case value
                          when TrueClass
                            value
                          when FalseClass
                            value
                          when Hash 
                            fix_hash(value)  
                          when (fix_values and String )
                            value.to_sym
                          when IO
                            value.read
                          when TSV::Dumper
                            value.stream
                          when Step
                            stream = get_stream(value)
                            stream || value.load
                          else
                            value
                          end
    end
    fixed
  end

  def self.parse_exception(text)
    klass, message = text.split " => "
    begin
      klass = Kernel.const_get klass
      return klass.new message
    rescue
      message
    end
  end

  def self.capture_exception
    begin
      yield
    rescue Exception => e
      raise e unless e.respond_to? :response
      begin
        ne = parse_exception e.response.to_s
        case ne
        when String
          raise e.class, ne
        when Exception
          raise ne
        else
          raise
        end
      rescue
        raise e
      end
      raise $!
    end
  end

  def self.fix_params(params)
    new_params = {}
    params.each do |k,v|
      if Array === v and v.empty?
        new_params[k] = "EMPTY_ARRAY"
      else
        new_params[k] = v
      end
    end
    new_params
  end

end
