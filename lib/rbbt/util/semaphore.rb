begin
  require 'inline'
  continue = true
rescue Exception
  Log.warn "The RubyInline gem could not be loaded: semaphore synchronization will not work"
  continue = false
end

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
void wait_semaphore(char* name){
  sem_t* sem;
  sem = sem_open(name, 0);
  sem_wait(sem);
  sem_close(sem);
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
    RbbtSemaphore.wait_semaphore(sem)
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
      Log.low "Creating semaphore (#{ size }): #{file}"
      RbbtSemaphore.create_semaphore(file, size)
      yield file
    ensure
      Log.low "Removing semaphore #{ file }"
      RbbtSemaphore.delete_semaphore(file)
    end
  end

  def self.fork_each_on_semaphore(elems, size, file = nil)
    with_semaphore(size, file) do |file|

      TSV.traverse elems, :cpus => size*2, :bar => "Fork each on semaphore: #{ file }" do |elem|
        elems.annotate elem if elems.respond_to? :annotate
        begin
          RbbtSemaphore.synchronize(file) do
            yield elem
          end
        rescue Interrupt
          Log.warn "Process #{Process.pid} was aborted"
        end
      end

    end
    true
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

