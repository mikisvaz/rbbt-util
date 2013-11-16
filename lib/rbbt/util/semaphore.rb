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
  sem_open(name, O_CREAT, S_IRWXU, value);
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
  sem = sem_open(name, O_EXCL);
  sem_wait(sem);
  sem_close(sem);
}
    EOF

    builder.c_singleton <<-EOF
void post_semaphore(char* name){
  sem_t* sem;
  sem = sem_open(name, O_EXCL);
  sem_post(sem);
  sem_close(sem);
}
    EOF
  end

  def self.with_semaphore(size, file = nil)
    file = Misc.digest(rand.to_s) if file.nil?
    file.gsub!('/', '_')
    begin
      RbbtSemaphore.create_semaphore(file, size)
      yield file
    ensure
      RbbtSemaphore.delete_semaphore(file)
    end
  end

  def self.fork_each_on_semaphore(elems, size, file = nil)
    with_semaphore(size, file) do |file|
      begin
        pids = elems.collect do |elem| 
          Process.fork do 
            RbbtSemaphore.wait_semaphore(file)
            begin
              yield elem
            ensure
              RbbtSemaphore.post_semaphore(file)
            end
          end
        end
        pids.each do |pid| Process.waitpid pid end
      rescue Exception
        Log.error "Killing children: #{pids.sort * ", " }"
        pids.each do |pid| begin Process.kill "INT", pid; rescue; end; RbbtSemaphore.post_semaphore(file) end
        Log.error "Ensuring children are dead: #{pids.sort * ", " }"
        pids.each do |pid| begin Process.waitpid pid; rescue; end; end
      end
    end
  end
end if continue

