begin
  require 'inline'
  continue = true
rescue Exception
  Log.warn "The RubyInline gem could not be loaded: semaphore synchronization will not work"
  continue = false
end

if continue
  module RbbtSemaphore
    inline(:C) do |builder|
      builder.prefix <<-EOF
  #include <unistd.h>
  #include <stdio.h>
  #include <stdlib.h>
  #include <semaphore.h>
  #include <time.h>
  #include <assert.h>
  #include <errno.h>
  #include <signal.h>
  #include <fcntl.h>
      EOF

      builder.c_singleton <<-EOF
  void create_semaphore(char* name, int value){
    sem_open(name, O_CREAT, S_IRWXU|S_IRWXG|S_IRWXO, value);
  }
      EOF
      builder.c_singleton <<-EOF
  void delete_semaphore(char* name){
    sem_unlink(name);
  }
      EOF

      builder.c_singleton <<-EOF
  int wait_semaphore(char* name){
    int ret;
    sem_t* sem;
    sem = sem_open(name, 0);
    ret = sem_wait(sem);
    sem_close(sem);
    return(ret);
  }
      EOF

      builder.c_singleton <<-EOF
  void post_semaphore(char* name){
    sem_t* sem;
    sem = sem_open(name, 0);
    sem_post(sem);
    sem_close(sem);
  }
      EOF
    end

    SEM_MUTEX = Mutex.new
    def self.synchronize(sem)
      ret = RbbtSemaphore.wait_semaphore(sem)
      raise Aborted if ret == -1
      begin
        yield
      ensure
        RbbtSemaphore.post_semaphore(sem)
      end
    end

    def self.with_semaphore(size, file = nil)
      if file.nil?
        file = "/" << Misc.digest(rand(1000000000000).to_s) if file.nil?
      else
        file = file.gsub('/', '_') if file
      end

      begin
        Log.debug "Creating semaphore (#{ size }): #{file}"
        RbbtSemaphore.create_semaphore(file, size)
        yield file
      ensure
        Log.debug "Removing semaphore #{ file }"
        RbbtSemaphore.delete_semaphore(file)
      end
    end

    def self.fork_each_on_semaphore(elems, size, file = nil)

      TSV.traverse elems, :cpus => size, :bar => "Fork each on semaphore: #{ Misc.fingerprint elems }", :into => Set.new do |elem|
        elems.annotate elem if elems.respond_to? :annotate
        begin
          yield elem
        rescue Interrupt
          Log.warn "Process #{Process.pid} was aborted"
        end
        nil
      end
      nil
    end

    def self.thread_each_on_semaphore(elems, size)
      mutex = Mutex.new
      count = 0
      cv = ConditionVariable.new
      wait_mutex = Mutex.new

      begin

        threads = []
        wait_mutex.synchronize do
          threads = elems.collect do |elem| 
            Thread.new(elem) do |elem|

              continue = false
              mutex.synchronize do
                while not continue do
                  if count < size 
                    continue = true
                    count += 1
                  end
                  mutex.sleep 1 unless continue
                end
              end

              begin
                yield elem
              rescue Interrupt
                Log.error "Thread was aborted while processing: #{Misc.fingerprint elem}"
                raise $!
              ensure
                mutex.synchronize do
                  count -= 1
                  cv.signal if mutex.locked?
                end
              end
            end
          end
        end

        threads.each do |thread| 
          thread.join 
        end
      rescue Exception
        Log.exception $!
        Log.info "Ensuring threads are dead: #{threads.length}"
        threads.each do |thread| thread.kill end
      end
    end
  end 
end

