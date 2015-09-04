class StoreSurvivalAnalysis
  include Sidekiq::Worker
  sidekiq_options :queue => :store_survival
  def perform(record)
    sar = SurvivalAnalysisRecord.first_or_create(screen_name: record["screen_name"], hashtag: record["hashtag"], timestamp: record["timestamp"], strftime: record["strftime_template"], metric_name: record["metric_name"])
    sar.metric_value = record["metric_value"]
    sar.metric_rank = record["metric_rank"]
    sar.total_seen_lifespan = record["total_seen_lifespan"]
    sar.total_posted_lifespan = record["total_posted_lifespan"]
    sar.has_posted_yet = record["has_posted_yet"]
    sar.first_seen = record["first_seen"]
    sar.first_posted = record["first_posted"]
    sar.save!
  end
end
# SurvivalAnalysisRecord.ensure_index([[:screen_name, 1], [:hashtag, 1], [:timestamp, 1], [:strftime, 1], [:metric_name, 1]])