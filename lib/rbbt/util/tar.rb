require_relative '../refactor'
Rbbt.require_instead 'scout/misc/tar'
#require 'rubygems/package'
#require 'zlib'
#require 'fileutils'
# 
## SOURCE: https://gist.github.com/sinisterchipmunk/1335041
## Adapted for Rbbt 
#
#module Misc
#  # Creates a tar file in memory recursively
#  # from the given path.
#  #
#  # Returns a StringIO whose underlying String
#  # is the contents of the tar file.
#  def self.tar(path, tarfile = nil)
#    tarfile ||= StringIO.new("")
#
#    Gem::Package::TarWriter.new(tarfile) do |tar|
#      Dir[File.join(path, "**/*")].each do |file|
#        mode = File.stat(file).mode
#        relative_file = file.sub /^#{Regexp::escape path}\/?/, ''
#
#        if File.directory?(file)
#          tar.mkdir relative_file, mode
#        else
#          tar.add_file relative_file, mode do |tf|
#            File.open(file, "rb") { |f| tf.write f.read }
#          end
#        end
#      end
#    end
#
#    tarfile.rewind
#
#    tarfile
#  end
#
#  def self.tarize(path, gz = nil)
#    gz ||= StringIO.new('wb')
#
#    tar =  Misc.in_dir(path) do self.tar('.') end
#
#    tar.rewind
#
#    string = tar.string
#
#    z = Zlib::GzipWriter.new(gz)
#    z.write string
#    z.close
#
#    gz.reopen('read')
#    gz.rewind
#
#
#    gz
#  end
#
#  def self.tarize(path)
#    Misc.in_dir(path) do
#      CMD.cmd("tar cvfz - '.'", :pipe => true)
#    end
#  end
#
#  # gzips the underlying string in the given StringIO,
#  # returning a new StringIO representing the 
#  # compressed file.
#  def self.gzip(tarfile)
#    gz = StringIO.new("")
#    z = Zlib::GzipWriter.new(gz)
#    z.write tarfile.string
#    z.close # this is necessary!
#
#    # z was closed to write the gzip footer, so
#    # now we need a new StringIO
#    StringIO.new gz.string
#  end
#
#  # un-gzips the given IO, returning the
#  # decompressed version as a StringIO
#  def self.ungzip(tarfile)
#    z = Zlib::GzipReader.new(tarfile)
#    unzipped = StringIO.new(z.read)
#    z.close
#    unzipped
#  end
#
#  def self._untar_cmd(io, destination)
#    FileUtils.mkdir_p destination unless File.exist? destination
#    CMD.cmd_log("tar xvf - -C '#{destination}'", :in => io)
#    nil
#  end
#
#  # untars the given IO into the specified
#  # directory
#  def self.untar(io, destination)
#    io = io.find if Path === io
#    if String === io and File.exist?(io)
#      Open.open(io) do |f|
#        untar(f, destination)
#      end
#    else
#      return _untar_cmd(io, destination)
#    end
#  end
#end
#
#
#### Usage Example: ###
##
## include Util::Tar
## 
## io = tar("./Desktop")   # io is a TAR of files
## gz = gzip(io)           # gz is a TGZ
## 
## io = ungzip(gz)         # io is a TAR
## untar(io, "./untarred") # files are untarred
##
#
