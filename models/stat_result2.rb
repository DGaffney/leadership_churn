class StatResultTwo
  include MongoMapper::Document
  key :net_statistic, String
  key :hashtag, String
  key :strftime_template, String
  key :n, Integer
  key :tau, Float
  key :end_time, String
  key :index, Integer
  key :article_count, Integer
end