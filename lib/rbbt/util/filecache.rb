require 'fileutils'
require 'rbbt/util/misc'

# Provides caching functionality for files downloaded from the internet
module FileCache
  CACHEDIR = "/tmp/rbbt_cache" 
  begin
    FileUtils.mkdir(CACHEDIR)
  rescue
  end unless File.exist? CACHEDIR


  def self.cachedir=(cachedir)
    CACHEDIR.replace cachedir
    Open.mkdir CACHEDIR unless Open.exist? CACHEDIR
  end

  def self.cachedir
    CACHEDIR
  end

  def self.path(filename)
    filename = File.basename filename

    filename.match(/(.+)\.(.+)/)

    base = filename.sub(/\..+/,'')
    dirs = base.scan(/./).reverse.values_at(0,1,2,3,4).compact

    File.join(File.join(CACHEDIR, *dirs), filename) 
  end

  def self.add(filename, content)
    path = path(filename)
    
    FileUtils.makedirs(File.dirname(path), :mode => 0777)

    Misc.sensiblewrite(path, content)

    FileUtils.chmod 0666, path

    path
  end

  def self.found(filename)
    File.exist? FileCache.path(filename)
  end

  def self.get(filename)
    path = path(filename)

    return nil if ! File.exist? path

    File.open(path)
  end

  def self.del(filename)
    path = path(filename)

    FileUtils.rm path if File.exist? path
  end

  def self.cache_online_elements(ids, pattern = nil, &block)
    ids = [ids] unless Array === ids

    result_files = {}
    missing = []
    ids.each do |id|
      filename = pattern ? pattern.sub("{ID}", id.to_s) : id.to_s

      if FileCache.found(filename)
        result_files[id] = FileCache.path(filename)
      else
        missing << id
      end
    end

    yield(missing).each do |id, content|
      filename = pattern ? pattern.sub("{ID}", id.to_s) : id.to_s
      path = FileCache.path(filename)
      Open.write(path, content)
      result_files[id] = path
    end if missing.any?

    result_files
  end
end
