require 'digest'

module CacheHelper
  CACHE_DIR = '/tmp/cachehelper'
  FileUtils.mkdir_p(CACHE_DIR) unless File.exist?(CACHE_DIR)

  LOG_TIME = false
  class CacheLocked < Exception; end

  def self.time(id)
    t = Time.now
    data = block.call
    STDERR.puts "#{ id } time: #{Time.now - t}"
    data
  end

  def self.cachedir=(dir)
    @@cachedir=dir
    FileUtils.mkdir_p(dir) unless File.exist?(dir)
  end

  def self.cachedir
    @@cachedir ||= CACHE_DIR
  end


  def self.reset
    FileUtils.rm Dir.glob(cachedir + '*')
  end
 
  def self.reset_locks
    FileUtils.rm Dir.glob(cachedir + '*.lock')
  end
 
 
  def self.build_filename(name, key)
    File.join(cachedir, name + ": " + Digest::MD5.hexdigest(key.to_s))
  end
 
  def self.do(filename, block)
    FileUtils.touch(filename + '.lock')

    if LOG_TIME
      data = time do
        block.call
      end
    else
      data = block.call
    end
    
    File.open(filename, 'w'){|f| f.write data}
    FileUtils.rm(filename + '.lock')
    return data
  end
 
  def self.clean(name)
    FileUtils.rm Dir.glob(File.join(cachedir, "#{ name }*"))
  end
 
  def self.cache_ready?(name, key)
    filename = CacheHelper.build_filename(name, key)
    File.exist?(filename)
  end
 
  def self.cache(name, key = [], wait = nil, &block)
    filename = CacheHelper.build_filename(name, key)
    begin
      case
      when File.exist?(filename)
        return File.open(filename){|f| f.read}
      when File.exist?(filename + '.lock')
        raise CacheLocked
      else
        if wait.nil?
          CacheHelper.do(filename, block)
        else
          Thread.new{CacheHelper.do(filename, block)}
          return wait
        end
 
      end
    rescue CacheLocked
      if wait.nil?
        sleep 30
        retry
      else
        return wait
      end
    rescue Exception
      FileUtils.rm(filename + '.lock') if File.exist?(filename + '.lock')
      raise $!
    end
  end
 
  def self.marshal_cache(name, key = [])
    Marshal::load( cache(name, key) do
      Marshal::dump(yield)
    end)
  end
end
