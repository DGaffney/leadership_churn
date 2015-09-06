class StoreSurvivalAnalysis
  include Sidekiq::Worker
  sidekiq_options :queue => :store_survival
  def perform(record)
    obj = BulkSurvivalAnalysis.first_or_create(hashtag: record["hashtag"], timestamp: record["timestamp"], strftime_template: record["strftime_template"])
    obj.results = record["results"]
    obj.save!
  end
end
# BulkSurvivalAnalysis.ensure_index([[:hashtag, 1], [:timestamp, 1], [:strftime_template, 1]])
# SurvivalAnalysisRecord.ensure_index([[:screen_name, 1], [:hashtag, 1], [:timestamp, 1], [:strftime, 1], [:metric_name, 1]])