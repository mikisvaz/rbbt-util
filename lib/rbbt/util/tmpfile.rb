require 'fileutils'

module TmpFile

  TMPDIR = "/tmp/tmpfiles" 
  FileUtils.mkdir TMPDIR unless File.exist? TMPDIR

  def self.tmpdir=(tmpdir)
    TMPDIR.replace tmpdir
    FileUtils.mkdir TMPDIR unless File.exist? TMPDIR
  end

  def self.tmpdir
    TMPDIR
  end


  # Creates a random file name, with the given suffix and a random number
  # up to +max+
  def self.random_name(s = "", max = 10000000)
    n = rand(max)
    s << n.to_s
    s
  end

  # Creates a random filename in the temporary directory
  def self.tmp_file(s = "",max=10000000)
    File.join(TMPDIR, random_name(s,max))
  end

  def self.with_file(content = nil, erase = true)
    tmpfile = tmp_file

    File.open(tmpfile, 'w') do |f| f.write content end if content != nil

    result = yield(tmpfile)

    FileUtils.rm tmpfile if File.exists?(tmpfile) and erase

    result
  end
end
