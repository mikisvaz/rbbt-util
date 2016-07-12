require 'fileutils'

module TmpFile

  TMPDIR = "/tmp/#{ENV['USER']}/tmpfiles" 
  FileUtils.mkdir_p TMPDIR unless File.exist? TMPDIR

  def self.tmpdir=(tmpdir)
    TMPDIR.replace tmpdir
    FileUtils.mkdir_p TMPDIR unless File.exist? TMPDIR
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

  def self.with_file(content = nil, erase = true, options = {})
    tmpfile = tmp_file
    if options[:extension]
      tmpfile += ".#{options[:extension]}"
    end

    if IO === content
      Misc.consume_stream(content, false, tmpfile)
    else
      File.open(tmpfile, 'w') do |f| f.write content end if content != nil
    end

    result = yield(tmpfile)

    FileUtils.rm_rf tmpfile if File.exist?(tmpfile) and erase

    result
  end

  def self.with_dir(erase = true, options = {})
    tmpdir = tmp_file

    FileUtils.mkdir_p tmpdir

    result = yield(tmpdir)

    FileUtils.rm_rf tmpdir if File.exist?(tmpdir) and erase

    result
  end
end
