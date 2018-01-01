require 'json'
require 'parallel'
require 'sqlite3'
require 'time'

class WordCount

  def initialize(input)
    @word_map = load_word_map_from_db
    @input = JSON.parse(File.read(input))
  end

  def run
    index_files
    query_words
  end

  private

  def load_word_map_from_db
    @db = SQLite3::Database.new("word_count.db")
    @db.execute <<-SQL
      create table if not exists word_count (
        word varchar(32),
        count int
      );
    SQL
    word_map = {}
    @db.execute("select * from word_count") do |row|
      word_map[row.first] = row.last
    end
    return word_map
  end

  def index_files
    # parallelize word count per file
    book_word_maps = Parallel.map(extract_file_paths) do |file_path|
      # map each word to a counter
      map_word(file_path).reduce({}) do |word_map, word_count|
        # reduce counters by word
        reduce_word(word_map, word_count.keys.first, word_count.values.first)
      end
    end
    # reduce counters of all files by word
    book_word_maps.reduce(@word_map) do |word_map, book_word_map|
      book_word_map.each do |word, count|
        reduce_word(word_map, word, count)
      end
      word_map
    end
    save_map
  end

  def query_words
    @input["query"].each do |word|
      query(word)
    end
  end

  def extract_file_paths
    @input["index"].flat_map do |file_pattern|
      Dir.glob(file_pattern).map do |extracted_file_path|
        extracted_file_path
      end
    end
  end

  # count every alphabetical word in file as a hash of the form { "word" => 1 }
  def map_word(file_path)
    File.read(file_path).split(/[\W\d_]+/).map do |word|
      { word.downcase => 1 }
    end
  end

  # add word count into the given word_map hash
  def reduce_word(word_map, word, count)
    word_map[word] ||= 0
    word_map[word] += count
    return word_map
  end

  def query(word)
    puts "#{word.inspect} count: #{@word_map[word.downcase] || 0}"
  end

  def save_map
    @db.execute("insert or replace into word_count values #{@word_map.map { |word, count| "('#{word}', #{count})" }.join(', ') }")
  end

end

puts "#{Time.now} | Started"
wc = WordCount.new(ARGV[0])
wc.run
puts "#{Time.now} | Done"
