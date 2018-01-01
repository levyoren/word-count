require 'sqlite3'

class WordCountDB

  def initialize(db_name, logger)
    @logger = logger
    @db_name = db_name
    init_db
  end

  def load_word_map
    word_map = {}
    @logger.info {"load word_map from db"}
    @db.execute("select * from #{@db_name}") do |row|
      word_map[row.first] = row.last
    end
    @logger.info {"done loading word_map from db"}
    return word_map
  end

  def save_word_map(word_map)
    @logger.info {"save word_map into db"}
    word_map.each_slice(500) do |map_slice|
      @db.execute <<-EOC
        insert or replace into word_count values
        #{map_slice.map { |w, c| "('#{w}', #{c})" }.join(',') };
      EOC
    end
  end

  private

  def init_db
    @logger.info {"init #{@db_name} db"}
    @db = SQLite3::Database.new("#{@db_name}.sqlite3")
    @db.execute <<-EOC
      create table if not exists #{@db_name} (
        word varchar(32),
        count int
      );
    EOC
  rescue Exception => e
    @logger.error {"error initializing #{@db_name} db; #{e.class}: #{e.message}"}
    raise
  end
end
