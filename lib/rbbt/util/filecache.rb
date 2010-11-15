require 'fileutils'
require 'rbbt/util/base'
require 'rbbt/util/misc'

# Provides caching functionality for files downloaded from the internet
module FileCache

  def self.path(filename)
    filename = File.basename filename

    filename.match(/(.+)\.(.+)/)

    base = filename.sub(/\..+/,'')
    dirs = base.scan(/./).values_at(0,1,2,3,4).compact.reverse

    File.join(File.join(Rbbt.cachedir, *dirs), filename) 
  end

  def self.add(filename, content)
    path = path(filename)
    
    FileUtils.makedirs(File.dirname(path), :mode => 0777)

    Misc.sensiblewrite(path, content)

    FileUtils.chmod 0666, path

    path
  end

  def self.get(filename)
    path = path(filename)

    return nil if ! File.exists? path

    File.open(path)
  end

  def self.del(filename)
    path = path(filename)

    FileUtils.rm path if File.exist? path
  end
end
