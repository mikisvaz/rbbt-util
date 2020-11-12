require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')

require 'rbbt-util'
require 'rbbt/util/misc/lock'
require 'rbbt/workflow'

class TestLock < Test::Unit::TestCase
  def __test_stress
  end
end

if __FILE__ == $0
  def deleted(pid = Process.pid)
    begin
      txt = `ls -la /proc/#{pid}/fd |grep deleted`
      puts Log.color(:magenta, [pid, txt.split("\n")*", "] * ": ")
    rescue Exception
      Log.exception $!
    end
  end

  cpus = 10
  file = "/tmp/test.lock"

  pids = []
  cpus.times do 
    pids << Process.fork do
      while true do
        Lockfile.new file do
          Lockfile.new file + '.1' do
          end
          File.open(file){|f| puts f.read }
        end
        deleted
      end
    end
  end



  exit
  size = 1000000
  num = 1
  cpus = 5

  pdb = "http://interactome3d.irbbarcelona.org/pdb.php?dataset=human&type1=interactions&type2=pdb&pdb=Q99685-Q99685-EXP-3hju.pdb1-B-0-A-0.pdb"
  Workflow.require_workflow "Structure"
  TmpFile.with_file do |dir|
    Structure.workdir = dir
    Path.setup dir
    TSV.traverse (0..size).to_a, :cpus => cpus, :type => :array, :bar => true do |i|
      begin
        v = rand(num).to_s
        file = File.join(dir, "file-" << v.to_s)

        Misc.lock file + '.produce' do
          Misc.lock file  do
          ##job = Structure.job(:neighbour_map, v, :pdb => pdb)
          #job = Translation.example_step(:translate, "Example")
          #job.path = file
          #if job.done?
          #  job.clean if rand < 0.3
          #else
          #  job.run(true) 
          #end
          end
        end
        deleted Process.pid

      rescue Exception
        raise $!
      end
    end
  end
end


