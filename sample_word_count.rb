require 'time'
require './parallel_word_count'
require './word_count'

def count_words_in_files_using_object(words, files, counter)
  puts "#{Time.now} | Started"
  counter.index_files(files)
  counters = counter.query_words(words)
  puts "#{Time.now} | Done"
  return counters
end

words = ["today", "yesterday", "tomorrow", "fortnight"]
files = ["./books/*.txt"]

puts count_words_in_files_using_object(words, files, WordCount.new)
puts count_words_in_files_using_object(words, files, ParallelWordCount.new)
