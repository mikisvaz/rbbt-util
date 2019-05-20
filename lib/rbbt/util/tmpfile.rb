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
  def self.random_name(s = "tmp-", max = 10000000)
    n = rand(max)
    s + n.to_s
  end

  # Creates a random filename in the temporary directory
  def self.tmp_file(s = "tmp-", max=10000000, dir = TMPDIR)
    File.expand_path(File.join(dir, random_name(s,max)))
  end

  def self.with_file(content = nil, erase = true, options = {})
    options, content, erase = content, nil, true if Hash === content
    options, erase = erase, true if Hash === erase 

    prefix = options[:prefix] || "tmp-"
    tmpdir = options[:tmpdir] || TMPDIR
    max = options[:max] || 10000000
    tmpfile = tmp_file prefix, max, tmpdir
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
    prefix = options[:prefix] || "tmpdir-"
    tmpdir = tmp_file prefix

    FileUtils.mkdir_p tmpdir

    result = yield(tmpdir)

    FileUtils.rm_rf tmpdir if File.exist?(tmpdir) and erase

    result
  end
end
