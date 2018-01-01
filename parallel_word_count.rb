require 'json'
require 'parallel'
require 'time'

class WordCount

  WORD_MAP_PATH = "word_map.json"

  def initialize(input)
    @word_map = JSON.parse(File.read(WORD_MAP_PATH)) rescue {}
    @input = JSON.parse(File.read(input))
  end

  def run
    files = @input["index"].flat_map do |file_pattern|
      Dir.glob(file_pattern).map do |listed_file_path|
        listed_file_path
      end
    end
    Parallel.map(files) do |file_path|
      map_count(file_path).reduce({}) do |word_map, word_count|
        reduce_count(word_map, word_count.keys.first, word_count.values.first)
      end
    end.reduce(@word_map) do |word_map, book_word_map|
      book_word_map.each do |word, count|
        reduce_count(word_map, word, count)
      end
      word_map
    end
    save_map
    @input["query"].each do |word|
      query(word)
    end
  end

  private

  # count every alphabetical word in file as a hash of the form { "word" => 1 }
  def map_count(file_path)
    # puts "mapping words from #{file_path}..."
    File.read(file_path).split(/[\W\d_]+/).map do |word|
      { word.downcase => 1 }
    end
  end

  # add word count into the given word_map hash
  def reduce_count(word_map, word, count)
    word_map[word] ||= 0
    word_map[word] += count
    return word_map
  end

  def query(word)
    puts "#{word.inspect} count: #{@word_map[word.downcase] || 0}"
  end

  def save_map
    File.write(WORD_MAP_PATH, JSON.pretty_generate(@word_map) + "\n")
  end

end

puts "#{Time.now} | Started"
wc = WordCount.new(ARGV[0])
wc.run
puts "#{Time.now} | Done"
