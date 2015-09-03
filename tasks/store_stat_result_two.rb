class StoreStatResultTwo
  include Sidekiq::Worker
  def perform(net)
    month, day, year, hour = net["end_time"].split(/[- ]/)
    end_time = net["strftime_template"].include?("H") ? Time.parse("#{year}-#{month}-#{day} #{hour}:00:00") : Time.parse("#{year}-#{month}-#{day} 00:00:00")
    sr2 = StatResultTwo.first_or_create(hashtag: net["hashtag"], end_time: end_time, net_statistic: net["net_statistic"]||"indegree", strftime_template: net["strftime_template"])
    sr2.tau = net["tau"].nan? ? 0 : net["tau"]
    sr2.n = net["n"]
    sr2.index = net["index"]
    sr2.article_count = MediaCloudArticle.where(hashtag: net["hashtag"], :publish_date.lte => end_time).count
    sr2.alexa_mean = AlexaRank.mean(net["hashtag"], end_time)
    sr2.alexa_max = AlexaRank.max(net["hashtag"], end_time)
    sr2.alexa_sum = AlexaRank.sum(net["hashtag"], end_time)
    sr2.count_rank = net["count"]
    sr2.save!
  end
end
