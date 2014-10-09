require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/misc'
require 'rbbt/association'
require 'rbbt/association'
require 'rbbt/association/util'

class TestAssociationUtil < Test::Unit::TestCase

  def test_specs
    spec = Association.parse_field_specification "TG=~Associated Gene Name=>Ensembl Gene ID"
    assert_equal ["TG", "Associated Gene Name", "Ensembl Gene ID"], spec

    spec = Association.parse_field_specification "TG=~Associated Gene Name"
    assert_equal ["TG", "Associated Gene Name", nil], spec

    spec = Association.parse_field_specification "Associated Gene Name=>Ensembl Gene ID"
    assert_equal ["Associated Gene Name", nil, "Ensembl Gene ID"], spec
  end

  def test_normalize_specs
    spec = Association.normalize_specs "TG=~Associated Gene Name=>Ensembl Gene ID", %w(SG TG Effect directed?)
    assert_equal ["TG", "Associated Gene Name", "Ensembl Gene ID"], spec

    spec = Association.normalize_specs "Ensembl Gene ID", %w(SG TG Effect directed?)
    assert_equal ["Ensembl Gene ID", nil, nil], spec

    spec = Association.normalize_specs "Ensembl Gene ID"
    assert_equal ["Ensembl Gene ID", nil, nil], spec
  end

  def test_process_specs
    spec = Association.extract_specs %w(SG TG Effect directed?), :target => "TG=~Associated Gene Name=>Ensembl Gene ID"
    assert_equal ["TG", "Associated Gene Name", "Ensembl Gene ID"], spec[:target]
    assert_equal ["SG", nil, nil], spec[:source]

    spec = Association.extract_specs %w(SG TG Effect directed?)
    assert_equal ["SG", nil, nil], spec[:source]
    assert_equal ["TG", nil,nil], spec[:target]

    spec = Association.extract_specs %w(SG TG Effect directed?), :source => "TG"
    assert_equal ["TG", nil, nil], spec[:source]
    assert_equal ["SG", nil,nil], spec[:target]

    spec = Association.extract_specs %w(SG TG Effect directed?), :source => "SG"
    assert_equal ["SG", nil, nil], spec[:source]
    assert_equal ["TG", nil,nil], spec[:target]

    spec = Association.extract_specs %w(SG TG Effect directed?), :target => "SG"
    assert_equal ["TG", nil, nil], spec[:source]
    assert_equal ["SG", nil,nil], spec[:target]
  end

  def test_headers
    spec = Association.headers  %w(SG TG Effect directed?), %w(Effect)
    assert_equal 0, spec[0]
    assert_equal 1, spec[1][0]

    spec = Association.headers  %w(SG TG Effect directed?), %w(Effect), :source => "SG"
    assert_equal 0, spec[0]
    assert_equal 1, spec[1][0]

    spec = Association.headers  %w(SG TG Effect directed?), %w(Effect), :target => "TG"
    assert_equal 0, spec[0]
    assert_equal 1, spec[1][0]

    spec = Association.headers  %w(SG TG Effect directed?), %w(Effect), :target => "TG", :source => "SG"
    assert_equal 0, spec[0]
    assert_equal 1, spec[1][0]

    spec = Association.headers  %w(SG TG Effect directed?), %w(Effect), :source => "TG"
    assert_equal 1, spec[0]
    assert_equal 0, spec[1][0]

    spec = Association.headers  %w(SG TG Effect directed?), %w(Effect), :target => "SG"
    assert_equal 1, spec[0]
    assert_equal 0, spec[1][0]

    spec = Association.headers  %w(SG TG Effect directed?), %w(Effect), :target => "SG", :source => "TG"
    assert_equal 1, spec[0]
    assert_equal 0, spec[1][0]

    spec = Association.headers  %w(SG TG Effect directed?), %w(Effect), :target => "SG", :source => "TG=~Associated Gene Name=>Ensembl Gene ID"
    assert_equal 1, spec[0]
    assert_equal 0, spec[1][0]

    spec = Association.headers  %w(SG TG Effect directed?), %w(Effect), :target => "SG=~Associated Gene Name=>Ensembl Gene ID", :source => "TG=~Associated Gene Name=>Ensembl Gene ID"
    assert_equal 1, spec[0]
    assert_equal 0, spec[1][0]

    spec = Association.headers  %w(SG TG Effect directed?), %w(Effect), :target => "SG=~Associated Gene Name", :source => "TG=~Associated Gene Name=>Ensembl Gene ID"
    assert_equal 1, spec[0]
    assert_equal 0, spec[1][0]

    spec = Association.headers  %w(SG TG Effect directed?), %w(Effect), :target => "TG=~Associated Gene Name", :source => "SG=~Associated Gene Name", :target_format => "Ensembl Gene ID"
    assert_equal 0, spec[0]
    assert_equal "Ensembl Gene ID", spec[5]
    assert_equal nil, spec[4]


    spec = Association.headers  %w(SG TG Effect directed?), %w(Effect), :target => "TG=~Associated Gene Name", :source => "SG=~Associated Gene Name", :source_format => "Ensembl Gene ID"
    assert_equal 0, spec[0]
    assert_equal "Ensembl Gene ID", spec[4]
    assert_equal nil, spec[5]
  end
end
