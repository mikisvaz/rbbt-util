## ComputeDependency

The compute parameter can be a single symbol `:produce` or `:bootstrap`, or an
array where the first element is that symbol and the rest are parameters such
as the number of CPUs for the bootstrapping. If the array has the symbol
`:canfail` then jobs issuing an `RbbtException` will not make the dependent job
fail as well. To control respawning in bootstraped dependencies you can specify
`:norespawn`, `:respawn`, `:always_respawn`, the default is to always respawn.
