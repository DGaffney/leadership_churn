class Timeliner
  def self.hashtags
    ["baltimoreuprising", "blacklivesmatter", "crimingwhilewhite", "enoughisenough", "ericgarner", "fasttailedgirls", "fergusonreport", "girlslikeus", "michaelbrown", "mynypd", "opferguson", "shutitdown", "solidarityisforwhitewomen", "survivorprivilege", "theemptychair", "trayvonmartin", "whyistayed", "yesallwhitewomen", "yesallwomen", "youoksis"]
  end

  def self.strftimes
    ["%m-%d-%Y %H", "%m-%d-%Y"]
  end

  def self.time_steps
    [60*60, 60*60*24]
  end
  
  def self.strftime_time_step_map
    Hash[self.strftimes.zip(self.time_steps)]
  end

  def self.media_cloud_timeline(hashtag, strftime)
    t_0 = AchtungTweet.where(hashtag: hashtag).order(:published_at).first.published_at
    set = {}
    MediaCloudArticle.where(hashtag: hashtag).each do |mca|
      set[((mca.publish_date-t_0)/self.strftime_time_step_map[strftime]).to_i] ||= 0
      set[((mca.publish_date-t_0)/self.strftime_time_step_map[strftime]).to_i] += 1
    end
    set
  end
  
  def self.run
    full = []
    self.strftimes.each do |strftime|
      result = {}
      self.hashtags.each do |hashtag|
        print "."
        result[hashtag] = self.media_cloud_timeline(hashtag, strftime)
      end
      self.to_csv(result, strftime)
      full << result
    end
  end
  
  def self.to_csv(result, strftime)
    csv = CSV.open(strftime+"_media_timeline.csv", "w")
    uniq_counts = result.values.collect(&:keys).flatten.uniq.reject{|x| x < 0}.sort
    csv << ["t_step", result.keys].flatten
    uniq_counts.each do |t_step|
      csv << [t_step, result.keys.collect{|k| result[k][t_step]||0}].flatten
    end
    csv.close
  end
end


# 
# class Timeliner
#   def self.hashtags
#     ["baltimoreuprising", "blacklivesmatter", "crimingwhilewhite", "enoughisenough", "ericgarner", "fasttailedgirls", "fergusonreport", "girlslikeus", "michaelbrown", "mynypd", "opferguson", "shutitdown", "solidarityisforwhitewomen", "survivorprivilege", "trayvonmartin", "whyistayed", "yesallwhitewomen", "yesallwomen", "youoksis"]
#   end
# 
#   def self.strftimes
#     ["%m-%d-%Y %H", "%m-%d-%Y"]
#   end
# 
#   def self.time_steps
#     [60*60, 60*60*24]
#   end
#   
#   def self.strftime_time_step_map
#     Hash[self.strftimes.zip(self.time_steps)]
#   end
# 
#   def self.media_cloud_timeline(hashtag, strftime)
#     t_0 = AchtungTweet.where(hashtag: hashtag).order(:published_at).first.published_at
#     set = {}
#     MediaCloudArticle.where(hashtag: hashtag).each do |mca|
#       host = URI.parse(mca.url).host rescue nil
#       next if host.nil?
#       set[((mca.publish_date-t_0)/self.strftime_time_step_map[strftime]).to_i] ||= 0
#       set[((mca.publish_date-t_0)/self.strftime_time_step_map[strftime]).to_i] += 1#AlexaRank.where(host: host).first.value rescue 0
#     end
#     set
#   end
#   
#   def self.run
#     self.strftimes.each do |strftime|
#       result = {}
#       self.hashtags.each do |hashtag|
#         print "."
#         result[hashtag] = self.media_cloud_timeline(hashtag, strftime)
#       end
#       self.to_csv(result, strftime)
#     end
#   end
#   
#   def self.to_csv(result, strftime)
#     csv = CSV.open(strftime+"_media_timeline.csv", "w")
#     uniq_counts = result.values.collect(&:keys).flatten.uniq.reject{|x| x < 0}.sort
#     csv << ["t_step", "average", "median", "standard_deviation"].flatten
#     uniq_counts.each do |t_step|
#       csv << [t_step, result.keys.collect{|k| self.normalized_val(result, k, t_step)}.average, result.keys.collect{|k| self.normalized_val(result, k, t_step)}.median, result.keys.collect{|k| self.normalized_val(result, k, t_step)}.standard_deviation].flatten
#     end
#     csv.close
#   end
#   
#   def self.normalized_val(result, hashtag, t_step)
#     total = result[hashtag].values.sum
#     up_to_point = result[hashtag].select{|k,v| k <= t_step}.values.sum
#     return up_to_point/total
#   end
# end
