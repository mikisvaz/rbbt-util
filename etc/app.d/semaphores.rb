# This is used by rbbt-rest when issuing a new job

$rest_cache_semaphore = ENV["RBBT_REST_CACHE_SEMAPHORE"] || "/REST_SEMAPHORE"
parallel_rest_jobs = Rbbt::Config.get('parallel_rest_jobs', :parallel_rest_jobs, :rest_jobs, :default => 2)

parallel_rest_jobs = parallel_rest_jobs.to_i if String === parallel_rest_jobs

begin
  ScoutSemaphore.delete_semaphore($rest_cache_semaphore)
ensure
  ScoutSemaphore.create_semaphore($rest_cache_semaphore, parallel_rest_jobs)
end
Log.debug("Created semaphore: #{$rest_cache_semaphore} with #{parallel_rest_jobs} size")
