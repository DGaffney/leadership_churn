class BulkSurvivalAnalysis
  include MongoMapper::Document
  key :hashtag, String
  key :timestamp, Time
  key :strftime_template, String
  key :results, Hash
  
  def self.survivor_roll_call(hashtag, strftime_template)
    self.where(hashtag: hashtag, strftime_template: strftime_template).order(:timestamp).each do |survivors|
    end
  end
end
