require 'rbbt/tsv'
require 'rbbt/persist'
module TCCache
  def self.open(file, type = :single)
    database = Persist.open_tokyocabinet(file, true, type, "HDB")
    database.extend TCCache
  end

  def cache(key)
    if self.include? key
      return self[key]
    else
      self.write_and_read do
        self[key] = yield
      end
    end
  end
end
