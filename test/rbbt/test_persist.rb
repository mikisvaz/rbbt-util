require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt/persist'
require 'rbbt/util/tmpfile'
require 'test/unit'

module TestAnnotation
  extend Annotation

  self.annotation :test_annotation
end

class TestPersist < Test::Unit::TestCase

  def _test_annotation_persist
    TmpFile.with_file do |tmp|
      entity1 = "Entity 1"
      entity2 = "Entity 2"

      TestAnnotation.setup(entity1, :test_annotation => "1")
      TestAnnotation.setup(entity2, :test_annotation => "2")

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

  def test_annotation_persist_with_repeitions
    TmpFile.with_file do |tmp|
      entity1 = "Entity 1"
      entity2 = "Entity 2"
      entity2bis = "Entity 2"

      TestAnnotation.setup(entity1, :test_annotation => "1")
      TestAnnotation.setup(entity2, :test_annotation => "2")
      TestAnnotation.setup(entity2bis, :test_annotation => "2")

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


  def _test_bdb
    TmpFile.with_file do |tmp|
      repo = Persist.open_tokyocabinet(tmp, true, :double, TokyoCabinet::BDB)
      repo["test:string1"] = [["STR1"]]
      repo["test:string2"] = [["STR2"]]
      repo["other_test:string3"] = [["STR2"]]

      assert_equal ["test:string1", "test:string2"].sort, repo.range("test:" << 0.chr, false, "test:" << 255.chr, false).sort
      assert_equal ["other_test:string3"].sort, repo.range("other_test:" << 0.chr, false, "other_test:" << 255.chr, false).sort
    end
  end

  def _test_annotation_persist_repo
    TmpFile.with_file do |tmp|
      repo = Persist.open_tokyocabinet(tmp, true, :list, TokyoCabinet::BDB)

      entity1 = "Entity 1"
      entity2 = "Entity 2"

      TestAnnotation.setup(entity1, :test_annotation => "1")
      TestAnnotation.setup(entity2, :test_annotation => "2")

      annotations = [entity1, entity2]

      persisted_annotations = Persist.persist("Test", :annotations, :annotation_repo => repo) do
        annotations
      end

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

  def _test_annotation_persist_repo_with_repetitions
    TmpFile.with_file do |tmp|
      repo = Persist.open_tokyocabinet(tmp, true, :list, TokyoCabinet::BDB)

      entity1 = "Entity 1"
      entity2 = "Entity 2"
      entity2bis = "Entity 2"

      TestAnnotation.setup(entity1, :test_annotation => "1")
      TestAnnotation.setup(entity2, :test_annotation => "2")
      TestAnnotation.setup(entity2bis, :test_annotation => "2")

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


  def _test_array_persist
    TmpFile.with_file do |tmp|
      10.times do
        assert_equal ["1", "2"],(Persist.persist("Test", :array, :file => tmp) do
          ["1", "2"]
        end)
      end
    end

    TmpFile.with_file do |tmp|
      10.times do
        assert_equal [],(Persist.persist("Test", :array, :file => tmp) do
          []
        end)
      end
    end

    TmpFile.with_file do |tmp|
      10.times do
        assert_equal ["1"],(Persist.persist("Test", :array, :file => tmp) do
          ["1"]
        end)
      end
    end
  end
end
