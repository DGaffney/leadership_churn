class AlexaRanker
  include Sidekiq::Worker
  sidekiq_options :queue => :alexa_ranker
  def perform(host)
    ar = AlexaRank.first_or_create(host: host)
    ar.value = Nokogiri.parse(RestClient.get("http://data.alexa.com/data?cli=10&url=#{host}")).search("REACH").first.attributes["RANK"].value.to_i rescue nil
    ar.save!
  end
end