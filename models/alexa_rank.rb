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
end