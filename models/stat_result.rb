class StatResult
  include MongoMapper::Document
  key :hashtag, String
  key :t_step, String
  key :end_time, Time
  key :analytic, String
  key :user_set, Array
end