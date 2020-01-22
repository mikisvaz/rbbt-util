# This is used by rbbt-rest when issuing a new job

$rest_cache_semaphore = "/REST_SEMAPHORE"
parallel_rest_jobs = Rbbt::Config.get('parallel_rest_jobs', :parallel_rest_jobs, :rest_jobs, :default => 2)
begin
  RbbtSemaphore.delete_semaphore($rest_cache_semaphore)
ensure
  RbbtSemaphore.create_semaphore($rest_cache_semaphore, parallel_rest_jobs)
end
Log.debug("Created semaphore: #{$rest_cache_semaphore} with #{parallel_rest_jobs} size")
