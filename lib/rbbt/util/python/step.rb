require 'rbbt/workflow/step'

class Step
  def python_file(file, options = {})
    CMD.cmd_log(:python, file, options)
  end

  def python_block(options = {}, &block)
    RbbtPython.run options, &block
  end

  def python(python = nil, options = {}, &block)
    begin
      RbbtPython.add_path self.workflow.libdir.python.find
    rescue
      Log.warn "Error loading libdir python for workflow '#{Misc.fingerprint self.workflow}'"
    end
    case python
    when Path
      python_file python.find, options
    when String
      if Open.exists?(python)
        python_file python
      else
        TmpFile.with_file do |dir|
          pkg = "pkg#{rand(100)}"
          Open.write File.join(dir, "#{pkg}/__init__.py"), code

          RbbtPython.add_path dir

          Misc.in_dir dir do
            yield pkg
          end
        end
      end
    else
      python_block(python, &block)
    end
  end
end

