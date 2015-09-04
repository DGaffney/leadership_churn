class SurvivalRunner
  include Sidekiq::Worker
  sidekiq_options :queue => :churn_runner
  def perform(hashtag, strftime_template, analytic)
    NetworkAnalysisTStep.rank_by_analytic(hashtag, strftime_template, analytic)
  end
end