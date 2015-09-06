class BulkSurvivalAnalysis
  include MongoMapper::Document
  key :hashtag, String
  key :timestamp, Time
  key :strftime_template, String
  key :results, Hash
  
  def self.survivor_roll_call(hashtag, strftime_template)
    volatility_max = MediaVolatility.maximum(hashtag, strftime_template)
    results = {}
    BulkSurvivalAnalysis.where(hashtag: hashtag, strftime_template: strftime_template).order(:timestamp).each do |survivors_record|
      volatility_value = MediaVolatility.values_at(hashtag, strftime_template, survivors_record.timestamp)
      survivors_record.results.each do |screen_name, values|
        if Time.parse(values["first_posted"]) < survivors_record.timestamp
          alexa_volatiltiy = volatility_value["alexa"].score rescue nil
          raw_volatility = volatility_value["raw"].score rescue nil
          results[screen_name] ||= {first_seen: Time.parse(values["first_seen"]), first_posted: Time.parse(values["first_posted"]), entry_volatility_raw: raw_volatility, entry_volatility_alexa: alexa_volatiltiy, pre_media_raw: survivors_record.timestamp < volatility_max["raw"].time, pre_media_alexa: survivors_record.timestamp < volatility_max["alexa"].time, metric_ranks: [], article_count: MediaCloudArticle.where(:publish_date.lte => survivors_record.timestamp, hashtag: hashtag).count}
          results[screen_name][:total_seen_lifespan] = values["total_seen_lifespan"]
          results[screen_name][:total_posted_lifespan] = values["total_posted_lifespan"]
          results[screen_name][:metric_ranks] << values["metric_value"]          
        end
      end
    end
    csv = CSV.open("#{hashtag}_#{strftime_template}_survival.csv", "w")
    dataset = results.map do |screen_name, row|
      r = row.merge(Hash[row[:metric_ranks].all_stats.collect{|k,v| ["rank_"+k.to_s, v]}])
      r[:screen_name] = screen_name
      r.delete(:metric_ranks)
      r
    end
    keys = dataset.collect(&:keys).flatten.uniq
    csv << keys
    dataset.each do |row|
      csv << keys.collect{|k| row[k]}
    end
    csv.close
  end
end

# hashtags.each do |hashtag|
#   strftimes.each do |strftime|
#     self.survivor_roll_call(hashtag, strftime)
#     puts hashtag
#   end
# end