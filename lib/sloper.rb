class Sloper
  def self.hourly_dataset
    if @hourly_dataset.nil?
      file = CSV.read("summed_up_counts_hourly.csv").collect{|x| x.collect(&:to_i)}
      sum = file.collect(&:last).sum
      @hourly_dataset = file.collect{|x| [x[0], x[1]/sum]}
    else
      @hourly_dataset
    end
  end

  def self.daily_dataset
    if @daily_dataset.nil?
      file = CSV.read("summed_up_counts.csv").collect{|x| x.collect(&:to_i)}
      sum = file.collect(&:last).sum
      @daily_dataset = file.collect{|x| [x[0], x[1]/sum]}
    else
      @daily_dataset
    end
  end

  def self.value_at(x)
    previous_point = self.dataset.select{|r| r[0] <= x}.last
    next_point = self.dataset.select{|r| r[0] > x}.first
    return nil if previous_point.nil? || next_point.nil?
    previous_point.last+(next_point.last-previous_point.last).to_f/(next_point.first-previous_point.first).to_f*(x-previous_point.first)
  end  
end
class MediaCloudCounter
  def self.hourly_dataset
    if @hourly_dataset.nil?
      file = CSV.read("summed_up_counts_hourly.csv").collect{|x| x.collect(&:to_i)}
      sum = file.collect(&:last).sum
      @hourly_dataset = file.collect{|x| [x[0], x[1]/sum]}
    else
      @hourly_dataset
    end
  end

  def self.daily_dataset
    if @daily_dataset.nil?
      file = CSV.read("summed_up_counts.csv").collect{|x| x.collect(&:to_i)}
      sum = file.collect(&:last).sum
      @daily_dataset = file.collect{|x| [x[0], x[1]/sum]}
    else
      @daily_dataset
    end
  end

  def self.hashtags
    ["baltimoreuprising", "blacklivesmatter", "crimingwhilewhite", "enoughisenough", "ericgarner", "fasttailedgirls", "fergusonreport", "girlslikeus", "michaelbrown", "mynypd", "opferguson", "shutitdown", "solidarityisforwhitewomen", "survivorprivilege", "theemptychair", "trayvonmartin", "whyistayed", "yesallwhitewomen", "yesallwomen", "youoksis"]
  end
  
  def self.strftime_windows
    {"%m-%d-%Y %H" => 3600, "%m-%d-%Y" => 3600*24}
  end
  
  def self.run
    results = {}
    self.hashtags.each do |hashtag|
      start_date = AchtungTweet.order(:published_at).where(hashtag: hashtag).first.published_at
      end_date = AchtungTweet.order(:published_at.desc).where(hashtag: hashtag).first.published_at
      self.strftime_windows.each do |strftime, window|
        results[hashtag] ||= {}
        results[hashtag][strftime] ||= {}
        MediaCloudArticle.order(:publish_date).where(hashtag: hashtag).each do |article|
          dataset = strftime.include?("H") ? self.hourly_dataset : self.daily_dataset
          alexa_val = article.alexa_percentile
          dataset.select{|r| r.first < (end_date-article.publish_date)/window}.each do |row|
            key = (article.publish_date+row.first*window).strftime(strftime)
            results[hashtag][strftime][key] ||= {}
            results[hashtag][strftime][key]["raw"] ||= 0
            results[hashtag][strftime][key]["raw"] += row.last
            results[hashtag][strftime][key]["alexa"] ||= 0
            results[hashtag][strftime][key]["alexa"] += row.last * alexa_val if alexa_val
          end
        end
      end
    end
    results.each do |hashtag, strftime_results|
      strftime_results.each do |strftime, result_set|
        result_set.each do |date, values|
          month, day, year, hour = date.split(/[- ]/)
          time = strftime.include?("H") ? Time.parse("#{year}-#{month}-#{day} #{hour}:00:00 +0000") : Time.parse("#{year}-#{month}-#{day} 00:00:00 +0000")
          values.each do |value_type, score|
            MediaVolatility.first_or_create(hashtag: hashtag, strftime: strftime, time: time, type: value_type, score: score)
          end
        end
      end
    end
  end
end
class MediaVolatility
  include MongoMapper::Document
  key :hashtag, String
  key :strftime, String
  key :time, Time
  key :type, String
  key :score, Float
end
