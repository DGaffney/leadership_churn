class BulkSurvivalAnalysis
  include MongoMapper::Document
  key :hashtag, String
  key :timestamp, Time
  key :strftime_template, String
  key :results, String
end
