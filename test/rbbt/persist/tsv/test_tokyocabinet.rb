require File.expand_path(File.dirname(__FILE__) + '/../../../test_helper')
require 'rbbt/persist'
require 'rbbt/annotations'
require 'rbbt/util/tmpfile'
require 'test/unit'


module TestAnnotationModule
  extend Annotation

  self.annotation :test_annotation
end

class TestPersistTSVTC < Test::Unit::TestCase

  def test_organism
    require 'rbbt/sources/organism'
    TmpFile.with_file do |tmp_file|
      tsv = Organism.identifiers("Hsa").tsv :key_field => "Associated Gene Name", :fields => ["Ensembl Gene ID"], :type => :single, :persist => true, :persist_engine => "HDB", :persist_dir => tmp_file
      assert_equal "ENSG00000141510", tsv["TP53"]
    end
  end

  def test_annotation_persist
    TmpFile.with_file do |tmp|
      entity1 = "Entity 1"
      entity2 = "Entity 2"

      TestAnnotationModule.setup(entity1, :test_annotation => "1")
      TestAnnotationModule.setup(entity2, :test_annotation => "2")

      annotations = [entity1, entity2]

      persisted_annotations = Persist.persist("Test", :annotations, :file => tmp) do
        annotations
      end

      assert_equal "Entity 1", persisted_annotations.first
      assert_equal "Entity 2", persisted_annotations.last
      assert_equal "1", persisted_annotations.first.test_annotation
      assert_equal "2", persisted_annotations.last.test_annotation

      persisted_annotations = Persist.persist("Test", :annotations, :file => tmp) do
        annotations
      end

      assert_equal "Entity 1", persisted_annotations.sort.first
      assert_equal "Entity 2", persisted_annotations.sort.last
      assert_equal "1", persisted_annotations.sort.first.test_annotation
      assert_equal "2", persisted_annotations.sort.last.test_annotation
    end
  end

  def test_annotation_persist_with_repetitions
    TmpFile.with_file do |tmp|
      entity1 = "Entity 1"
      entity2 = "Entity 2"
      entity2bis = "Entity 2"

      TestAnnotationModule.setup(entity1, :test_annotation => "1")
      TestAnnotationModule.setup(entity2, :test_annotation => "2")
      TestAnnotationModule.setup(entity2bis, :test_annotation => "2")

      annotations = [entity1, entity2, entity2bis]

      persisted_annotations = Persist.persist("Test", :annotations, :file => tmp) do
        annotations
      end

      assert_equal 3, persisted_annotations.length

      assert_equal "Entity 1", persisted_annotations.first
      assert_equal "Entity 2", persisted_annotations.last
      assert_equal "1", persisted_annotations.first.test_annotation
      assert_equal "2", persisted_annotations.last.test_annotation

      persisted_annotations = Persist.persist("Test", :annotations, :file => tmp) do
        annotations
      end

      assert_equal 3, persisted_annotations.length

      assert_equal "Entity 1", persisted_annotations.sort.first
      assert_equal "Entity 2", persisted_annotations.sort.last
      assert_equal "1", persisted_annotations.sort.first.test_annotation
      assert_equal "2", persisted_annotations.sort.last.test_annotation
    end
  end

  def test_bdb
    TmpFile.with_file do |tmp|
      repo = Persist.open_tokyocabinet(tmp, true, :double, TokyoCabinet::BDB)
      repo["test:string1"] = [["STR1"]]
      repo["test:string2"] = [["STR2"]]
      repo["other_test:string3"] = [["STR2"]]

      assert_equal ["test:string1", "test:string2"].sort, repo.range("test:" << 0.chr, false, "test:" << 255.chr, false).sort
      assert_equal ["other_test:string3"].sort, repo.range("other_test:" << 0.chr, false, "other_test:" << 255.chr, false).sort
    end
  end

  def test_annotation_persist_repo
    TmpFile.with_file do |tmp|
      repo = Persist.open_tokyocabinet(tmp, true, :list, TokyoCabinet::BDB)

      entity1 = "Entity 1"
      entity2 = "Entity 2"

      TestAnnotationModule.setup(entity1, :test_annotation => "1")
      TestAnnotationModule.setup(entity2, :test_annotation => "2")

      annotations = [entity1, entity2]

      persisted_annotations = Persist.persist("Test", :annotations, :annotation_repo => repo) do
        annotations
      end
      raise

      assert_equal "Entity 1", persisted_annotations.first
      assert_equal "Entity 2", persisted_annotations.last
      assert_equal "1", persisted_annotations.first.test_annotation
      assert_equal "2", persisted_annotations.last.test_annotation

      persisted_annotations = Persist.persist("Test", :annotations, :annotation_repo => repo) do
        annotations
      end

      assert_equal "Entity 1", persisted_annotations.sort.first
      assert_equal "Entity 2", persisted_annotations.sort.last
      assert_equal "1", persisted_annotations.sort.first.test_annotation
      assert_equal "2", persisted_annotations.sort.last.test_annotation
    end
  end

  def test_annotation_persist_repo_annotated_array
    TmpFile.with_file do |tmp|
      repo = Persist.open_tokyocabinet(tmp, true, :list, TokyoCabinet::BDB)

      entity1 = "Entity 1"
      entity2 = "Entity 2"

      annotations = [entity1, entity2]
      TestAnnotationModule.setup(annotations, :test_annotation => "1")
      annotations.extend AnnotatedArray

      persisted_annotations = Persist.persist("Test", :annotations, :annotation_repo => repo) do
        annotations
      end

      assert_equal "Entity 1", persisted_annotations.first
      assert_equal "Entity 2", persisted_annotations.last
      assert_equal "1", persisted_annotations.first.test_annotation
      assert_equal "1", persisted_annotations.last.test_annotation

      persisted_annotations = Persist.persist("Test", :annotations, :annotation_repo => repo) do
        annotations
      end

      persisted_annotations.extend AnnotatedArray

      assert_equal "Entity 1", persisted_annotations.sort.first
      assert_equal "Entity 2", persisted_annotations.sort.last
      assert_equal "1", persisted_annotations.sort.first.test_annotation
      assert_equal "1", persisted_annotations.sort.last.test_annotation
    end
  end

  def test_annotation_persist_repo_triple_array
    TmpFile.with_file(nil, false) do |tmp|
      repo = Persist.open_database(tmp, true, :list, "BDB")

      entity1 = "Entity 1"
      entity2 = "Entity 2"

      annotations = [entity1, entity2]
      TestAnnotationModule.setup(annotations, :test_annotation => "1")
      annotations.extend AnnotatedArray

      annotations_ary = [annotations]
      TestAnnotationModule.setup(annotations_ary, :test_annotation => "1")
      annotations_ary.extend AnnotatedArray

      persisted_annotations = Persist.persist("Test", :annotations, :annotation_repo => repo) do
        annotations_ary
      end
      assert AnnotatedArray === persisted_annotations

      assert_equal "Entity 1", persisted_annotations.first.first
      assert_equal "Entity 2", persisted_annotations.first.last
      assert_equal "1", persisted_annotations.first.first.test_annotation
      assert_equal "1", persisted_annotations.first.last.test_annotation

      persisted_annotations = Persist.persist("Test", :annotations, :annotation_repo => repo) do
        annotations_ary
      end

      assert AnnotatedArray === persisted_annotations

      assert_equal "Entity 1", persisted_annotations.sort.first.first
      assert_equal "Entity 2", persisted_annotations.sort.first.last
      assert_equal "1", persisted_annotations.sort.first.first.test_annotation
      assert_equal "1", persisted_annotations.sort.first.last.test_annotation

    end
  end

  def test_annotation_persist_repo_with_repetitions
    TmpFile.with_file do |tmp|
      repo = Persist.open_tokyocabinet(tmp, true, :list, TokyoCabinet::BDB)

      entity1 = "Entity 1"
      entity2 = "Entity 2"
      entity2bis = "Entity 2"

      TestAnnotationModule.setup(entity1, :test_annotation => "1")
      TestAnnotationModule.setup(entity2, :test_annotation => "2")
      TestAnnotationModule.setup(entity2bis, :test_annotation => "2")

      annotations = [entity1, entity2, entity2bis]

      persisted_annotations = Persist.persist("Test", :annotations, :annotation_repo => repo) do
        annotations
      end

      assert_equal 3, persisted_annotations.length

      assert_equal "Entity 1", persisted_annotations.first
      assert_equal "Entity 2", persisted_annotations.last
      assert_equal "1", persisted_annotations.first.test_annotation
      assert_equal "2", persisted_annotations.last.test_annotation

      persisted_annotations = Persist.persist("Test", :annotations, :annotation_repo => repo) do
        annotations
      end

      assert_equal 3, persisted_annotations.length

      assert_equal "Entity 1", persisted_annotations.sort.first
      assert_equal "Entity 2", persisted_annotations.sort.last
      assert_equal "1", persisted_annotations.sort.first.test_annotation
      assert_equal "2", persisted_annotations.sort.last.test_annotation
    end
  end

  def __test_fdb
    TmpFile.with_file do |tmp|
      repo = Persist.open_tokyocabinet(tmp, true, :list, TokyoCabinet::FDB)
      repo.write
      repo.put(1, "hola")
      repo.put(2, "adios")
      47000.times do |i|
        repo.put(i*500, "key: " << i.to_s)
      end
      repo.read
      Misc.profile do
        repo.values_at *repo.range("[100,1000]")
      end
      Misc.benchmark(60_000) do
        repo.values_at *repo.range("[100,1000]")
      end
    end
  end
end
