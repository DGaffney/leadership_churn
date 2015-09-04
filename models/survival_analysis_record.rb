class SurvivalAnalysisRecord
  include MongoMapper::Document
  key :hashtag, String
  key :timestamp, Time
  key :strftime_template, String
  key :metric_value, Float
  key :metric_name, String
  key :metric_rank, Float
  key :total_seen_lifespan, Float
  key :total_posted_lifespan, Float
  key :has_posted_yet, Boolean
  key :first_seen, Time
  key :first_posted, Time
end