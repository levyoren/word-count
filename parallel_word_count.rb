require 'json'
require 'logger'
require 'parallel'
require 'time'
require './word_count_db'

class WordCount

  def initialize(input_file_path)
    # @logger = Logger.new("word_count.log")
    @logger = Logger.new(STDOUT)
    @logger.level = Logger::INFO
    @input_file_path = input_file_path
  end

  def run
    begin
      input = JSON.parse(File.read(@input_file_path))
      @db = WordCountDB.new("word_count", @logger)
      @word_map = @db.load_word_map
    rescue Exception => e
      @logger.fatal {"#{e.class}: #{e.message}"}
      exit(1)
    end
    index_files(input["index"])
    query_words(input["query"])
  end

  private

  def extract_file_paths(file_list)
    file_paths = (file_list || []).flat_map do |file_pattern|
      Dir.glob(file_pattern).map do |extracted_file_path|
        extracted_file_path
      end
    end
    @logger.info {"extracted #{file_paths.size} file paths to index"}
    return file_paths
  end

  # count words per file in parallel and reduce counters of all files by word
  def index_files(file_list)
    Parallel.map(extract_file_paths(file_list)) do |file_path|
      map_reduce_file_by_word(file_path)
    end.reduce(@word_map) do |word_map, book_word_map|
      book_word_map.each do |word, count|
        reduce_word(word_map, word, count)
      end
      word_map
    end
    # save map to db
    @db.save_word_map(@word_map)
  end

  # map each word in file to a single counter and reduce counters by word
  def map_reduce_file_by_word(file_path)
    @logger.debug {"counting words in #{file_path.inspect}"}
    File.read(file_path).split(/[\W\d_]+/).map do |word|
      map_word(word)
    end.reduce({}) do |word_map, word_count|
      reduce_word(word_map, word_count.keys.first, word_count.values.first)
    end
  rescue Exception => e
    @logger.error {"error counting words in #{file_path.inspect}; skipping file"}
    return {}
  end

  # count every alphabetical word in file as a hash of the form { "word" => 1 }
  def map_word(word)
    { word.downcase => 1 }
  end

  # add word count into the given word_map hash
  def reduce_word(word_map, word, count)
    word_map[word] ||= 0
    word_map[word] += count
    return word_map
  end

  def query_words(word_list)
    (word_list || []).each do |word|
      @logger.debug {"querying count for #{word.inspect}"}
      puts "#{word.inspect} count: #{@word_map[word.downcase] || 0}"
    end
  end

end

puts "#{Time.now} | Started"
wc = WordCount.new(ARGV[0])
wc.run
puts "#{Time.now} | Done"
