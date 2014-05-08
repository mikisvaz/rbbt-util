#!/usr/bin/env ruby

ENV["RBBT_LOG"] = "10"

require 'rbbt-util'
require 'rbbt/util/R'

STDOUT.write File.join(R::LIB_DIR, "util.R")

