require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt/workflow'
require 'rbbt/util/tmpfile'
require 'test/unit'

module TestWF
  extend Workflow

  helper :user do
    "User"
  end 

  task :user => :string do
    user
  end

  str = "TEST"
  task :str => :string do
    str
  end

  dep :str
  task :reverse => :string do
    step(:str).load.reverse
  end

  dep :str
  task :downcase => :string do
    step(:str).load.downcase
  end

  dep :str
  input :times, :integer, "Times to repeat"
  task :repeat => :string do |times|
    [step(:str).load] * times * "\n"
  end

  input :number, :float, "Number to doble"
  def self.double(number)
    2 * number
  end
  task :double => :float

  desc <<-EOT
Returns numer * 2 lines containing TEST
  EOT
  dep :str
  dep :double
  task :repeat2 => :string do 
    [step(:str).load] * step(:double).load * "\n"
  end

  dep :str, :repeat, :repeat2
  task :double_dep => :array do
   [] << step(:str).load << step(:repeat).load << step(:repeat2).load
  end

  export_synchronous :double
  export_asynchronous :repeat2

  input :letter, :string, "Letter", "D"
  task :letter => :string do |l|
    l
  end

  dep :letter
  task :letter_repeat => :string do |l|
    ([step(:letter).load] * 2) * ""
  end

  dep :letter
  dep :letter_repeat, :letter => "A"
  task :two_letters => :string do
    dependencies.collect{|d| d.load } * ":"
  end

  task :stream => :array do
    Misc.open_pipe do |sin|
      5.times do |i|
        sin.puts "line #{ i }"
        sleep 1
      end
    end
  end

  dep :stream
  task :stream2 => :array do
    TSV.get_stream step(:stream)
  end

  input :name, :string, "Name"
  task :input_dep => :text do |name|
    <<-EOF
Hi #{name}:
This is the input text
for this dependency
    EOF
  end

  input :text, :text, "Input text"
  task :reverse_input_text => :text do |text|
    text.reverse
  end

  dep :input_dep
  dep :reverse_input_text, :text => :input_dep
  task :send_input_dep_to_reverse => :text do
    TSV.get_stream step(:reverse_input_text)
  end

  input :i, :string, "Input", "A"
  task :t1 => :string do |i|
    i
  end

  input :i, :string, "Input", "B"
  task :t2 => :string do |i|
    i
  end

  dep :t1, :i => "C"
  dep :t2
  task :t3 => :string do |i|
    step(:t1).load + step(:t2).load
  end

  input :name, :string, "Name", nil, :jobname => true
  task :call_name => :string do |name|
    "Hi #{name}"
  end

  input :num, :integer
  task :odd => :integer do |num|
    raise ParameterException, "Not odd" if num % 2 == 0
    num
  end

  dep :odd, :num => 10, :compute => :canfail 
  dep :odd, :num => 11, :compute => :canfail 
  dep :odd, :num => 12, :compute => :canfail 
  dep :odd, :num => 13, :compute => :canfail 
  task :sum_odds => :integer do 
    dependencies.inject(0) do |acc, dep|
      acc += dep.load unless dep.error?
      acc
    end
  end

  dep :sum_odds
  task :sum_odds_str => :string do
    "Sum odds: " << step(:sum_odds).load.to_s
  end

  input :file, :file, "Save file"
  task :reverse_file => :text do |file|
    Open.read(file).reverse
  end


end

TestWF.workdir = Rbbt.tmp.test.jobs.TestWF

class TestWorkflow < Test::Unit::TestCase

  
  def test_repo_marshal
    TmpFile.with_file do |tmpdir|
      tmpdir = Rbbt.tmp.repo_dir.find
      repo = File.join(tmpdir, 'repo')

      filename = 'file'
      Open.repository_dirs.push(repo)

      job = TestWF.job(:call_name, "Miguel")
      job.run

      obj = job.info
      Open.write(File.join(repo, filename), Marshal.dump(obj))
      new =Open.open(File.join(repo, filename)) do |f|
        Marshal.load(f)
      end

      assert_equal new, obj
    end

  end
  
  def test_in_repo
    job = TestWF.job(:call_name, "Miguel")
    assert_equal "Hi Miguel", job.run
    assert_equal "Miguel", job.clean_name
  end

  def test_as_jobname
    job = TestWF.job(:call_name, "Miguel")
    assert_equal "Hi Miguel", job.run
    assert_equal "Miguel", job.clean_name

    job = TestWF.job(:call_name, nil, :name => "Miguel")
    assert_equal "Hi Miguel", job.run
    assert_equal "Miguel", job.clean_name
  end

  def test_update_on_input_dependency_update
    Open.repository_dirs << File.join(ENV["HOME"],".rbbt/tmp/test/workflow")
    Log.severity = 0
    Misc.with_env "RBBT_UPDATE", "true" do
      send_input_dep_to_reverse_job = TestWF.job(:send_input_dep_to_reverse, nil, :name => "Miguel")
      send_input_dep_to_reverse_job.clean
      send_input_dep_to_reverse_job.run

      input_dep_job = send_input_dep_to_reverse_job.step(:input_dep)
      mtime_orig = Open.mtime send_input_dep_to_reverse_job.step(:reverse_input_text).path

      sleep 2
      input_dep_job.clean
      input_dep_job.run
      send_input_dep_to_reverse_job = TestWF.job(:send_input_dep_to_reverse, nil, :name => "Miguel")

      send_input_dep_to_reverse_job.run
      mtime_new = Open.mtime send_input_dep_to_reverse_job.step(:reverse_input_text).path
      assert mtime_orig < mtime_new
    end
  end

  def test_helper
    assert_equal "User", TestWF.job(:user, "Default", :number => 3).run
  end

  def test_job
    str = "TEST"
    job = TestWF.job(:repeat2, "Default", :number => 3).clean.fork
    while not job.done?
      sleep 1
    end

    raise job.messages.last if job.error?

    assert_equal ["TEST"] * 6 * "\n", job.load
  end

  def test_with_subdir
    str = "TEST"
    job = TestWF.job(:repeat2, "Default", :number => 3).fork
    while not job.done?
      sleep 1
    end

    raise job.messages.last if job.error?

    assert_equal ["TEST"] * 6 * "\n", job.load
  end

  def test_search
    str = "TEST"
    TestWF.jobs(:repeat2).each do |name|
      TestWF.load_name(:repeat2, name).clean
    end
    job1 = TestWF.job(:repeat2, "subdir/Default", :number => 3).clean.fork
    job2 = TestWF.job(:repeat2, "subdir/Other", :number => 3).clean.fork
    job3 = TestWF.job(:repeat2, "Default", :number => 3).clean.fork

    while not job1.done? and not job2.done? and not job3.done?
      sleep 1
    end

    assert_equal [job1.name, job2.name].sort, TestWF.jobs(:repeat2, "subdir/").sort
    assert_equal [job1.name].sort, TestWF.jobs(:repeat2, "subdir/Default")
    assert TestWF.jobs(:repeat2).include?(job1.name)
    assert TestWF.jobs(:repeat2).include?(job2.name)
    assert TestWF.jobs(:repeat2).include?(job3.name)
    assert TestWF.load_name(:repeat2, job3.name).done?
    assert_equal "TEST\nTEST\nTEST\nTEST\nTEST\nTEST", TestWF.load_name(:repeat2, TestWF.jobs(:repeat2).first).load
  end

  def test_double_dep
    assert_equal ["TEST", "TEST\nTEST", "TEST\nTEST\nTEST\nTEST"], TestWF.job(:double_dep, "foo", :times => 2, :number => 2).clean.run
  end

  def test_object_workflow
    a = ""
    a.extend Workflow
    a.task :foo => :string do
      "bar"
    end
    
    job = a.job(:foo)
    assert_equal 'bar', job.exec
  end

  def test_letter
    assert_equal "D", TestWF.job(:letter).run
    assert_equal "B", TestWF.job(:letter, nil, :letter => "B").run
    assert_equal "BB", TestWF.job(:letter_repeat, nil, :letter => "B").run
    job = TestWF.job(:two_letters, nil, :letter => "V")
    assert_equal "V:AA", job.run
  end

  def test_override_dep
    TmpFile.with_file("OTHER", false) do |file|
      assert TestWF.job(:repeat2, nil, :number => 3, "TestWF#str" => file).clean.run.include? "OTHER"
    end
  end

  def __test_stream
    io = TestWF.job(:stream).run(:stream)
    Misc.consume_stream(TSV.get_stream(io), false, STDOUT)
    nil
  end

  def __test_fork_stream
    job = TestWF.job(:stream)
    job.clean
    io = job.fork(:stream)
    Misc.consume_stream(TSV.get_stream(io), false, STDOUT)
    nil
  end

  def test_stream_order

    Log.with_severity 0 do
      job = TestWF.job(:stream2)
      job.recursive_clean
      job.produce

    end
  end

  def test_rec_input_use
    assert TestWF.rec_input_use(:double_dep).include?(:times)
    assert TestWF.rec_input_use(:double_dep)[:times].include?(TestWF)
    assert TestWF.rec_input_use(:double_dep)[:times][TestWF].include?(:repeat)
  end

  def test_shared_inputs
    assert_equal "CB", TestWF.job(:t3).run
    assert_equal "CB", TestWF.job(:t3).run
  end

  def test_transplant
    listed = '/home/user/.rbbt/var/jobs/TestWF/task1/Default'
    real = '/usr/local/var/rbbt/jobs/TestWF/task1/Default'
    other = '/home/user/.rbbt/var/jobs/TestWF/task2/Default'
    real_other = '/usr/local/var/rbbt/jobs/TestWF/task2/Default'

    assert_equal real_other, Workflow.transplant(listed, real, other)
    assert_equal real_other, Workflow.transplant(nil, real, other)
  end

  def test_relocate
    TmpFile.with_file do |tmpdir|
      listed = File.join(tmpdir, '/home/user/.rbbt/var/jobs/TestWF/task1/Default')
      real = File.join(tmpdir, '/usr/local/var/rbbt/jobs/TestWF/task1/Default')
      other = File.join(tmpdir, '/home/user/.rbbt/var/jobs/TestWF/task2/Default')
      real_other = File.join(tmpdir, '/usr/local/var/rbbt/jobs/TestWF/task2/Default')

      Open.write(real_other,'')
      assert_equal real_other, Workflow.relocate(real, other)
      assert_equal real_other, Workflow.relocate(real, other)
    end
  end

  def test_relocate_alt
    TmpFile.with_file do |tmpdir|
      listed = File.join(tmpdir, '/scratch/tmp/rbbt/.rbbt/var/jobs/Study/sample_gene_cnvs_focal/Bladder-TCC')
      real = File.join(tmpdir, '/home/bsc26/bsc26892/.rbbt/var/jobs/Study/sample_gene_cnvs_focal/Bladder-TCC')
      other = File.join(tmpdir, '/scratch/tmp/rbbt/scratch/bsc26892/rbbt/var/jobs/Sample/gene_cnv_status_focal/PCAWG')
      real_other = File.join(tmpdir, '/home/bsc26/bsc26892/.rbbt/var/jobs/Sample/gene_cnv_status_focal/PCAWG')
      Open.write(real_other,'')

      assert_equal real_other, Workflow.relocate(real, other)
    end
  end

  def test_delete_dep
    job = TestWF.job(:t3).recursive_clean
    job.run
    assert job.checks.select{|d| d.task_name.to_s == "t1" }.any?
    job = TestWF.job(:t3)
    job.step(:t1).clean
    assert job.checks.select{|d| d.task_name.to_s == "t1" }.empty?
    job = TestWF.job(:t3).recursive_clean
    job.run
    assert job.checks.select{|d| d.task_name.to_s == "t1" }.any?
    job = TestWF.job(:t3)
    sleep 1
    Open.touch job.step(:t1).path
    Misc.with_env "RBBT_UPDATE", "false" do
      assert job.updated?
    end
    Misc.with_env "RBBT_UPDATE", "true" do
      assert ! job.updated?
    end
  end

  def test_canfail
    job = TestWF.job(:sum_odds)
    assert_equal 24, job.run

    job = TestWF.job(:sum_odds_str)
    job.recursive_clean
    assert_equal "Sum odds: 24", job.run
  end

  def test_save_inputs
    TmpFile.with_file("Hi") do |file|
      job = TestWF.job(:reverse_file, nil, :file => file)
      TmpFile.with_file do |dir|
        Path.setup(dir)
        Step.save_job_inputs(job, dir)
        assert_equal Dir.glob(dir + "/*"), [dir.file.find]
      end
    end

    job = TestWF.job(:reverse_file, nil, :file => "code")
    TmpFile.with_file do |dir|
      Path.setup(dir)
      Step.save_job_inputs(job, dir)
      assert_equal Dir.glob(dir + "/*"), [dir.file.find + '.read']
      inputs  = Workflow.load_inputs(dir, [:file], :file => :file)
      assert_equal inputs, {:file => 'code'}
    end

  end

  def test_archive
    job = TmpFile.with_file("Hi") do |file|
      job = TestWF.job(:reverse_file, nil, :file => file)
      job.run
      job
    end
    TmpFile.with_file nil, true, :extension => 'tar.gz' do |targz|
      Step.archive([job], targz)
      TmpFile.with_file do |dir|
        dir = Path.setup(dir)
        Misc.untar targz, dir
        assert dir.glob("**/*").collect{|f| File.basename(f)}.include? job.name
      end
    end
  end
end
