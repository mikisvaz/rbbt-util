require_relative '../../refactor'
require 'scout/misc'
require 'scout/open'

Rbbt.relay_module_method Misc, :lock, Open, :lock
Rbbt.relay_module_method Misc, :sensiblewrite, Open, :sensible_write
Rbbt.relay_module_method Misc, :sort_stream, Open, :sort_stream
