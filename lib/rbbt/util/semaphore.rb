require 'inline'

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
      RbbtSemaphore.create_semaphore(file, 2)
      yield file
    ensure
      RbbtSemaphore.delete_semaphore(file)
    end
  end
end

if __FILE__ == $0
  s = "/tmp_semaphore"
  RbbtSemaphore.delete_semaphore(s)
  RbbtSemaphore.create_semaphore(s, 2)

  pids = []
  5.times do
    pids << Process.fork{
      begin
        RbbtSemaphore.wait_semaphore(s)
        10.times do
          puts "Process: #{Process.pid}"
          sleep rand * 2
        end
      ensure
        RbbtSemaphore.post_semaphore(s)
      end
    }
  end

  pids.collect{|p| Process.waitpid p}


  RbbtSemaphore.delete_semaphore(s)
end
