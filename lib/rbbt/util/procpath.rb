require 'scout/cmd'
module ProcPath
  CMD.tool :procpath do
    'pip install procpath'
  end

  def self.record(pid, path, options = {})
    IndiferentHash.setup(options)
    options = IndiferentHash.add_defaults options, "interval" => 30

    cmd_options = %w(interval recnum reevalnum).inject({}){|acc,k| acc[k] = options[k]; acc}

    Log.debug "ProcPath recording #{pid} in #{path} (#{Log.fingerprint options})"
    procpath_thread = Thread.new do 
      begin
        procpath_pid = CMD.cmd_pid(:procpath, "record --database-file '#{path}' '$..children[?(@.stat.pid == #{pid})]'", cmd_options.merge(:nofail => true, :add_option_dashes => true))
      rescue Exception
        Log.exceptions $!
        Process.kill "INT", procpath_pid
      end
    end

    procpath_thread.report_on_exception = false

    Process.wait pid.to_i
    procpath_thread.raise Interrupt
  end

  def self.plot(path, output, options = {})
    IndiferentHash.setup(options)
    options = IndiferentHash.add_defaults options, "query-name" => 'rss', 'epsilon' => 0.5, "moving-average-window" => 10

    cmd_options = %w(query-name epsilon monitor-average-window title logarithmic after before custom-query-file custom-value-expr).inject({}){|acc,k| acc[k] = options[k]; acc}
    CMD.cmd_log(:procpath, "plot --database-file '#{path}' --plot-file '#{output}' ", cmd_options.merge(:nofail => true, :add_option_dashes => true))
  end

  def self.monitor(pid, path)
    database, options_str = path.split("#")
    options = options_str.nil? ? {} : IndiferentHash.string2hash(options_str)

    database = File.expand_path database
    Log.low "ProcPath monitor #{pid} in #{database} (#{Log.fingerprint options})"

    ProcPath.record(pid, database + '.sqlite3', options)
    ProcPath.plot(database + '.sqlite3', database + '.cpu.svg', options.merge("query-name" => 'cpu'))
    ProcPath.plot(database + '.sqlite3', database + '.rss.svg', options.merge("query-name" => 'rss'))
  end
end

