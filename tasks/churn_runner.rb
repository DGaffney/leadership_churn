class ChurnRunner
  include Sidekiq::Worker
  sidekiq_options :queue => :churn_runner
  def perform(hashtag, strftime, end_time)
    TemporalAnalysis.run(hashtag, strftime, end_time)
  end
end