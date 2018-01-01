require 'time'
require './parallel_word_count'

puts "#{Time.now} | Started"
wc = WordCount.new
# wc.from_input_file("./input.json")
wc.index_files(["./books/www.gutenberg.org-pg444.txt"])
puts wc.query_words(["today", "yesterday", "tomorrow", "fortnight"])
puts "#{Time.now} | Done"
