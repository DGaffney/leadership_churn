class ComparativeLeaders
  include MongoMapper::Document
  include Sidekiq::Worker
  key :start_time, Time
  key :end_time, Time
  key :hashtag, String
  key :screen_name, String
  key :retweet_proportion, Float
  key :pre_media, Boolean
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

  def self.network_to_point(hashtag, time)
    edges = {}
    AchtungTweet.where(hashtag: hashtag, :published_at.lte => time).each do |at|
      extract_mentioned_screen_names(at.text).each do |alter|
        edges[alter] ||= []
        edges[alter] << at.screen_name
      end
    end
    edges
  end

  def self.network_before_article(hashtag)
    edges = {}
    AchtungTweet.where(hashtag: hashtag, :published_at.lte => self.first_article_time(hashtag)).each do |at|
      extract_mentioned_screen_names(at.text).each do |alter|
        edges[alter] ||= []
        edges[alter] << at.screen_name
      end
    end
    edges
  end

  def self.network_after_article(hashtag)
    edges = {}
    AchtungTweet.where(hashtag: hashtag, :published_at.gte => self.first_article_time(hashtag)).each do |at|
      extract_mentioned_screen_names(at.text).each do |alter|
        edges[alter] ||= []
        edges[alter] << at.screen_name
      end
    end
    edges
  end
  
  def self.top_indegree(edges, limit=100)
    edges.sort_by{|k,v| v.length}.reverse.first(limit).collect(&:first)
  end


  def times(hashtag)
    times = []
    (self.first_tweet_time(hashtag).to_datetime.to_i..self.last_tweet_time(hashtag).to_datetime.to_i).step(1.day) do |date|
      times << Time.at(date)
    end
    times
  end

  def perform(screen_name, hashtag, start_time, end_time, pre_media)
    cl = ComparativeLeaders.first_or_create(screen_name: screen_name, hashtag: hashtag, start_time: start_time, end_time: end_time, pre_media: pre_media)
    cl.retweet_proportion = calculate_retweet_proportion(screen_name, hashtag, start_time, end_time)
    cl.save!
  end
  
  def calculate_retweet_proportion(screen_name, hashtag, start_time, end_time)
    net = self.network_to_point(hashtag, time)
    counts = net.values.flatten.counts
    return counts[screen_name].to_f/counts.values.sum
  end

  def self.kickoff(hashtag)
    pre_set = self.top_indegree(self.network_before_article(hashtag))
    post_set = self.top_indegree(self.network_after_article(hashtag))-pre_set
    times = times(hashtag)
    pre_set.each do |user|
      times.each_with_index do |t, i|
        ComparativeLeaders.perform_async(user, hashtag, times.first, t, true)
      end
    end
  end
end