require 'json'

class WordCount

  WORD_MAP_PATH = "word_map.json"

  def initialize
    @word_map = JSON.parse(File.read(WORD_MAP_PATH)) rescue {}
  end

  # count words found in files given in file list (with wildcard support)
  def index_files(file_list)
    file_list.flat_map do |file_pattern|
      Dir.glob(file_pattern).map do |listed_file_path|
        count(File.read(listed_file_path, :encoding => "UTF8-MAC"))
      end
    end
    save_map
  end

  # query words given in list and return a count map
  def query_words(word_list)
    counts = {}
    word_list.each do |word|
      counts[word] = query(word)
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

  def count(content)
    content.split(/[\W\d_]+/).each do |word|
      lowercase_word = word.downcase
      @word_map[lowercase_word] ||= 0
      @word_map[lowercase_word] += 1
    end
  end

  def query(word)
    @word_map[word.downcase] || 0
  end

  def save_map
    File.write(WORD_MAP_PATH, JSON.pretty_generate(@word_map) + "\n")
  end

end
