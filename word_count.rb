require 'json'

class WordCount

  WORD_MAP_PATH = "word_map.json"

  def initialize(input)
    @word_map = JSON.parse(File.read(WORD_MAP_PATH)) rescue {}
    @input = JSON.parse(File.read(input))
  end

  def run
    @input["index"].each do |file_path|
      count(File.read(file_path))
    end
    @input["query"].each do |word|
      query(word)
    end
  end

  private

  def count(content)
    content.split(/\W+/).each do |word|
      lowercase_word = word.downcase
      @word_map[lowercase_word] ||= 0
      @word_map[lowercase_word] += 1
    end
    save_map
  end

  def query(word)
    puts "#{word.inspect} count: #{@word_map[word.downcase] || 0}"
  end

  def save_map
    File.write(WORD_MAP_PATH, JSON.pretty_generate(@word_map) + "\n")
  end

end

wc = WordCount.new(ARGV[0])
wc.run
