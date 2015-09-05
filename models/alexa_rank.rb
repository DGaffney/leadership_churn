class AlexaRank
  include MongoMapper::Document
  key :host, String
  key :value, Integer
  key :percentile, Float

  def self.percentiles
    ars = AlexaRank.to_a
    values = ars.collect(&:value).compact.sort.reverse
    ars.each do |alexa_rank|
      puts alexa_rank.id
      alexa_rank.percentile = values.reverse_percentile(alexa_rank.value) if alexa_rank.value
      alexa_rank.save
    end
  end
  
  def self.mean(hashtag, end_time)
    hosts = MediaCloudArticle.where(hashtag: hashtag, :publish_date.lte => end_time).fields(:url).collect{|mca| URI.parse(mca.url).host rescue nil}.compact
    ranks = Hash[AlexaRank.where(host: hosts).collect{|ar| [ar.host, ar.percentile]}]
    hosts.length == 0 ? 0 : hosts.collect{|host| ranks[host]}.sum/hosts.length
  end
  
  def self.max(hashtag, end_time)
    hosts = MediaCloudArticle.where(hashtag: hashtag, :publish_date.lte => end_time).fields(:url).collect{|mca| URI.parse(mca.url).host rescue nil}.compact
    ranks = Hash[AlexaRank.where(host: hosts).collect{|ar| [ar.host, ar.percentile]}]
    ranks.values.compact.sort.last
  end
  
  def self.sum(hashtag, end_time)
    hosts = MediaCloudArticle.where(hashtag: hashtag, :publish_date.lte => end_time).fields(:url).collect{|mca| URI.parse(mca.url).host rescue nil}.compact
    ranks = Hash[AlexaRank.where(host: hosts).collect{|ar| [ar.host, ar.percentile]}]
    hosts.collect{|host| ranks[host]}.sum
  end
end