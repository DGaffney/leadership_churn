class ChurnRunner
  include Sidekiq::Worker
  sidekiq_options :queue => :churn_runner
  def perform(hashtag, strftime_template, analytic)
    NetworkAnalysisTStep.run_by_analytic(hashtag, strftime_template, analytic)
  end
end