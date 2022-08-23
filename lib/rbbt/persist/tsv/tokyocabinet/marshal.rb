module Persist
  module TCAdapter
    def marshal_dump
      [persistence_path, tokyocabinet_class]
    end
  end
end

class TokyoCabinet::BDB
  def marshal_load(values)
    persistence_path, tokyocabinet_class = values
    Persist::TCAdapter.open persistence_path, false, tokyocabinet_class
  end
end

class TokyoCabinet::HDB
  def marshal_load(values)
    persistence_path, tokyocabinet_class = values
    Persist::TCAdapter.open persistence_path, false, tokyocabinet_class
  end
end

