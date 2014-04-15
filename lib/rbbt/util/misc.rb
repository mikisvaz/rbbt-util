require 'lockfile'
require 'net/smtp'
require 'digest/md5'
require 'cgi'
require 'zlib'
require 'rubygems/package'
require 'rbbt/util/tar'
require 'rbbt/util/misc/exceptions'
require 'rbbt/util/misc/concurrent_stream'
require 'rbbt/util/misc/indiferent_hash'
require 'rbbt/util/misc/pipes'
require 'rbbt/util/misc/format'
require 'rbbt/util/misc/omics'
require 'rbbt/util/misc/inspect'
require 'rbbt/util/misc/math'
require 'rbbt/util/misc/development'
require 'rbbt/util/misc/lock'
require 'rbbt/util/misc/options'
require 'rbbt/util/misc/system'
require 'rbbt/util/misc/objects'
require 'rbbt/util/misc/manipulation'

module Misc

end

module PDF2Text
  def self.pdftotext(filename, options = {})
    require 'rbbt/util/cmd'
    require 'rbbt/util/tmpfile'
    require 'rbbt/util/open'


    TmpFile.with_file(Open.open(filename, options.merge(:nocache => true)).read) do |pdf_file|
      CMD.cmd("pdftotext #{pdf_file} -", :pipe => false, :stderr => true)
    end
  end
end
