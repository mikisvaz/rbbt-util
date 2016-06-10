require 'lmdb'

module Persist

  module LMDBAdapter
    include Persist::TSVAdapter
    def self.open(path, write)

      database = CONNECTIONS[path] ||= begin
                                         dir = File.dirname(File.expand_path(path))
                                         file = File.basename(path)
                                         env = LMDB.new(dir, :mapsize => 1024 * 10000)
                                         database = env.database file, :create => write
                                         database
                                       end

      database.extend Persist::LMDBAdapter unless Persist::LMDBAdapter === database
      database.persistence_path ||= path

      database
    end

    def each
      cursor do |cursor|
        while pair = cursor.next
          yield *pair
        end
      end
      self
    end

    def collect
      res = []
      cursor do |cursor|
        while pair = cursor.next
          res = if block_given?
                  yield *pair
                else
                  pair
                end
        end
      end
      res
    end
  end

  def self.open_lmdb(path, write, serializer = nil)
    write = true unless File.exist? path

    FileUtils.mkdir_p File.dirname(path) unless File.exist?(File.dirname(path))

    database = Persist::LMDBAdapter.open(path, write)

    unless serializer == :clean
      TSV.setup database
      database.serializer = serializer || database.serializer
    end

    database
  end
end
