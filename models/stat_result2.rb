class StatResultTwo
  include MongoMapper::Document
  key :net_statistic, String
  key :hashtag, String
  key :strftime_template, String
  key :n, Integer
  key :tau, Float
  key :end_time, String
  key :index, Integer
  key :article_count, Integer
  key :alexa_mean, Float
  key :alexa_max, Float
  key :alexa_sum, Float
  key :count_rank, Integer
  def self.hashtags
    ["baltimoreuprising", "blacklivesmatter", "crimingwhilewhite", "enoughisenough", "ericgarner", "fasttailedgirls", "fergusonreport", "girlslikeus", "michaelbrown", "mynypd", "opferguson", "shutitdown", "solidarityisforwhitewomen", "survivorprivilege", "theemptychair", "trayvonmartin", "whyistayed", "yesallwhitewomen", "yesallwomen", "youoksis"]
  end

  def self.to_csv(hashtag)
    csvs = {}
    self.where(hashtag: hashtag).order(:end_time).each do |sr2|
      hour = sr2.strftime_template.include?("H") ? "hour" : "day"
      if csvs[sr2.net_statistic.to_s+hour].nil?
        csvs[sr2.net_statistic.to_s+hour] ||= CSV.open("#{hashtag}_#{sr2.net_statistic}_#{hour}.csv", "w")
        csvs[sr2.net_statistic.to_s+hour] << ["statistic", "hashtag", "strftime_template", "n", "tau", "start_time", "end_time", "index", "article_count", "alexa_mean", "alexa_max", "alexa_sum"]
      end
      csvs[sr2.net_statistic.to_s+hour] << [sr2.net_statistic, sr2.hashtag, sr2.strftime_template, sr2.n, sr2.tau, sr2.end_time, sr2.end_time, sr2.index, sr2.article_count, sr2.alexa_mean, sr2.alexa_max, sr2.alexa_sum]
    end
    csvs.values.collect(&:close)
  end
  
  def self.to_csvs
    hashtags.each do |h|
      self.to_csv(h)
    end
  end
end