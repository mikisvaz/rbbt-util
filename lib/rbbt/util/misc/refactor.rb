require_relative '../../refactor'
require 'scout/misc'
require 'scout/open'
require_relative '../log/refactor'

Rbbt.relay_module_method Misc, :lock, Open, :lock
Rbbt.relay_module_method Misc, :sensiblewrite, Open, :sensible_write
Rbbt.relay_module_method Misc, :consume_stream, Open, :consume_stream
Rbbt.relay_module_method Misc, :sort_stream, Open, :sort_stream
Rbbt.relay_module_method Misc, :sanitize_filename, Path, :sanitize_filename
Rbbt.relay_module_method Misc, :collapse_stream, Open, :collapse_stream
