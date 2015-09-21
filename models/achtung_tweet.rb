class AddMentionedUsers
  include Sidekiq::Worker
  sidekiq_options :queue => :add_mentioned_users
  def perform(id)
    t = AchtungTweet.find(id)
    t.mentioned_users = extract_mentioned_screen_names(t.text)
    t.save!
  end
end
class AchtungTweet
  include MongoMapper::Document
  key :hashtag, String
  key :twitter_id, Integer
  key :user_id, Integer
  key :published_at, Time
  key :in_reply_to_twitter_id, Integer
  key :in_reply_to_user_id, Integer
  key :source, String
  key :truncated, Boolean
  key :geotag, Array
  key :location, String
  key :text, String
  key :profile_name, String
  key :screen_name, String
  key :mentioned_users, Array
  def self.row_keys
    [:twitter_id, :user_id, :published_at, :in_reply_to_twitter_id, :in_reply_to_user_id, :source, :truncated, :geotag, :location, :text, :profile_name, :screen_name]
  end

  def self.process_row(row, hashtag)
    content = Hash[AchtungTweet.row_keys.zip(row).collect{|k,v| [k, AchtungTweet.process_value(k,v)]}]
    obj = AchtungTweet.first_or_create(hashtag: hashtag, twitter_id: content[:twitter_id])
    AchtungTweet.row_keys.each do |k|
      obj.send(k.to_s+"=", content[k])
    end
    obj.save!
    obj
  end
  
  def self.process_value(field, value)
    case field
    when :twitter_id
      value.to_i
    when :user_id
      value.to_i
    when :published_at
      Time.parse(value)
    when :in_reply_to_twitter_id
      value.to_i
    when :in_reply_to_user_id
      value.to_i
    when :truncated
      value == "1"
    when :geotag
      value.empty? ? [] : JSON.parse(value) rescue []
    else
      value
    end
  end

  def self.process_file(filename)
    Iconv.new('UTF-8','LATIN1').iconv(File.read(filename)).split("\n").collect{|x| row = x.split("\t"); row if !x.empty? && row.length == 12}.compact.each do |row|
      AchtungTweet.process_row(row, filename.split("/").last.split("_").first.gsub("#", "").downcase)
    end
  end

  def self.read_file(hashtag)
    filename = Dir[File.dirname(__FILE__) + '/../data/twitter/*.tsv'].select{|x| x.downcase.include?(hashtag)}.first
    Iconv.new('UTF-8','LATIN1').iconv(File.read(filename)).split("\n").collect{|x| row = x.split("\t"); row if !x.empty? && row.length == 12}.compact.collect do |row|
      content = Hash[self.row_keys.zip(row).collect{|k,v| [k, self.process_value(k,v)]}]
      obj = self.new(hashtag: hashtag, twitter_id: content[:twitter_id])
      self.row_keys.each do |k|
        obj.send(k.to_s+"=", content[k])
      end
      obj
    end  
  end

  def self.process_files
    puts Dir[File.dirname(__FILE__) + '/../data/twitter/*.tsv']
    Dir[File.dirname(__FILE__) + '/../data/twitter/*.tsv'].each {|filename| self.process_file(filename) }
  end
end

