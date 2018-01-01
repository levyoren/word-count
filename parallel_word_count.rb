require 'json'
require 'logger'
require 'parallel'
require './word_count_db'

class WordCount

  def initialize
    @logger = Logger.new(STDOUT)
    @logger.level = Logger::INFO
    init
  end

  def init
    begin
      @db = WordCountDB.new("word_count", @logger)
      @word_map = @db.load_word_map
    rescue Exception => e
      @logger.fatal {"#{e.class}: #{e.message}"}
      exit(1)
    end
  end

  # count words found in files given in file list (with wilcard support)
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

  # query words given in list and return a count map
  def query_words(word_list)
    counts = {}
    (word_list || []).each do |word|
      @logger.debug {"querying count for #{word.inspect}"}
      counts[word] = @word_map[word.downcase] || 0
    end
    counts
  end

  # read file paths and queries from json input file of the following format:
  # {
  #   "index": <list of local file paths to count words from>,
  #   "query": <list of words to query>,
  # }
  # return a count map for each word
  def from_input_file(input_file_path)
    input = JSON.parse(File.read(input_file_path))
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

  # map each word in file to a single counter and reduce counters by word
  def map_reduce_file_by_word(file_path)
    @logger.debug {"counting words in #{file_path.inspect}"}
    File.read(file_path).split(/[\W\d_]+/).map do |word|
      map_word(word)
    end.reduce({}) do |word_map, word_count|
      reduce_word(word_map, word_count.keys.first, word_count.values.first)
    end
  rescue Exception => e
    @logger.error {"encounted error counting words in #{file_path.inspect}; #{e.class}: #{e.message}; skipping file"}
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
end
