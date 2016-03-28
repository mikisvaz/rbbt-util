require 'rbbt/tsv'
require 'rbbt/persist'
module TCCache
  def self.open(file, type = :single)
    database = Persist.open_tokyocabinet(file, true, type, "HDB")
    database.extend TCCache
  end

  def cache(key)

    self.read_and_close do
      return self[key] if self.include? key
    end

    value = yield

    self.write_and_close do
      self[key] = value
    end

    value
  end
end
