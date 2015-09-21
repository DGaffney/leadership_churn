class ComparativeLeaders
  include MongoMapper::Document
  include Sidekiq::Worker
  key :start_time, Time
  key :end_time, Time
  key :hashtag, String
  key :screen_name, String
  key :retweet_proportion, Float
  key :log_retweet_proportion, Float
  key :pre_media, Boolean
  key :index, Integer
  sidekiq_options :queue => :comparative_leaders
  def self.last_article_time(hashtag, offset=0)
    MediaCloudArticle.where(hashtag: hashtag, :publish_date.gte => self.first_tweet_time(hashtag, offset)).order(:publish_date.desc).offset(offset).first.publish_date
  end

  def self.first_article_time(hashtag, offset=0)
    MediaCloudArticle.where(hashtag: hashtag, :publish_date.gte => self.first_tweet_time(hashtag, offset)).order(:publish_date).offset(offset).first.publish_date
  end

  def self.last_tweet_time(hashtag, offset=0)
    AchtungTweet.where(hashtag: hashtag).order(:published_at.desc).offset(offset).first.published_at
  end

  def self.first_tweet_time(hashtag, offset=0)
    AchtungTweet.where(hashtag: hashtag).order(:published_at).offset(offset).first.published_at
  end

  def network_to_point(hashtag, time)
    edges = {}
    existing_participants = AchtungTweet.where(hashtag: hashtag, :published_at.lte => time).distinct(:screen_name)
    AchtungTweet.where(hashtag: hashtag, :published_at.lte => time).each do |at|
      extract_mentioned_screen_names(at.text).each do |alter|
        edges[alter] ||= []
        edges[alter] << at.screen_name
      end
    end
    edges.select{|k,v| existing_participants.include?(k)}
  end

  def self.network_before_article(hashtag)
    edges = {}
    existing_participants = AchtungTweet.where(hashtag: hashtag, :published_at.lte => self.first_article_time(hashtag)).distinct(:screen_name)
    AchtungTweet.where(hashtag: hashtag, :published_at.lte => self.first_article_time(hashtag)).each do |at|
      extract_mentioned_screen_names(at.text).each do |alter|
        edges[alter] ||= []
        edges[alter] << at.screen_name
      end
    end
    edges.select{|k,v| existing_participants.include?(k)}
  end

  def self.network_after_article(hashtag)
    edges = {}
    existing_participants = AchtungTweet.where(hashtag: hashtag, :published_at.gte => self.first_article_time(hashtag)).distinct(:screen_name)
    AchtungTweet.where(hashtag: hashtag, :published_at.gte => self.first_article_time(hashtag)).each do |at|
      extract_mentioned_screen_names(at.text).each do |alter|
        edges[alter] ||= []
        edges[alter] << at.screen_name
      end
    end
    edges.select{|k,v| existing_participants.include?(k)}
  end
  
  def self.top_indegree(edges, limit=100)
    edges.sort_by{|k,v| v.length}.reverse.first(limit).collect(&:first)
  end

  def self.times(hashtag)
    times = []
    (self.first_tweet_time(hashtag).to_datetime.to_i..self.last_tweet_time(hashtag).to_datetime.to_i).step(1.day) do |date|
      times << Time.at(date)
    end
    times
  end

  def perform(screen_name, hashtag, start_time, end_time, pre_media, index)
    cl = ComparativeLeaders.first_or_create(screen_name: screen_name, hashtag: hashtag, start_time: Time.parse(start_time), end_time: Time.parse(end_time), pre_media: pre_media)
    cl.retweet_proportion = calculate_mention_proportion(screen_name, hashtag, Time.parse(end_time))
    cl.log_retweet_proportion = cl.retweet_proportion == 0 ? 0 : Math.log(cl.retweet_proportion)
    cl.index = index
    cl.save!
  end

  def calculate_mention_proportion(screen_name, hashtag, time)
    mentions = AchtungTweet.where(hashtag: hashtag, :published_at.lte => time, :mentioned_users.in => [screen_name]).count.to_f
    total_mentions = AchtungTweet.collection.aggregate([{"$match" => {"hashtag" => hashtag, "published_at" => {"$lte" => time}}}, {"$project" =>  {"item" => 1, "mentioned_users" => { "$size" => "$mentioned_users" }}}, {"$group" => {"_id" => nil,"count" => {"$sum" => "$mentioned_users"}}}])[0]["count"]
    return mentions/total_mentions
  end

  def self.kickoff_all
    ["baltimoreuprising", "blacklivesmatter", "crimingwhilewhite", "enoughisenough", "ericgarner", "fasttailedgirls", "fergusonreport", "girlslikeus", "michaelbrown", "mynypd", "opferguson", "shutitdown", "solidarityisforwhitewomen", "survivorprivilege", "theemptychair", "trayvonmartin", "whyistayed", "yesallwhitewomen", "yesallwomen", "youoksis"].each do |hashtag|
      self.kickoff(hashtag)
    end
  end

  def self.kickoff(hashtag)
    pre_set = self.top_indegree(self.network_before_article(hashtag))
    post_set = self.top_indegree(self.network_after_article(hashtag))-pre_set
    times = times(hashtag)
    pre_set.each do |user|
      index = 0
      times[1..-1].each do |t|
        ComparativeLeaders.perform_async(user, hashtag, Time.parse(times.first.strftime("%Y-%m-%d 00:00:00 +0000")), Time.parse(t.strftime("%Y-%m-%d 00:00:00 +0000")), true, index)
        index += 1
      end
    end
    post_set.each do |user|
      index = 0
      times[1..-1].each do |t|
        ComparativeLeaders.perform_async(user, hashtag, Time.parse(times.first.strftime("%Y-%m-%d 00:00:00 +0000")), Time.parse(t.strftime("%Y-%m-%d 00:00:00 +0000")), false, index)
        index += 1
      end
    end
  end
end