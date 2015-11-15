class AddMentionedUsers
  include Sidekiq::Worker
  sidekiq_options :queue => :add_mentioned_users
  def perform(id)
    t = AchtungTweet.find(id)
    t.mentioned_users = extract_mentioned_screen_names(t.text)
    t.save!
  end
end
class ProcessCSV
  include Sidekiq::Worker
  sidekiq_options :queue => :hashtag_csv
  def perform(filename)
    rows = CSV.read(filename);false
    hashtag = filename.split("/").last.split(".").first.downcase
    queries = []
    ready = rows.collect{|row| r = AchtungTweet.hashed_row(row, hashtag); queries << {twitter_id: r[:twitter_id], hashtag: r[:hashtag]}; r};false
    existing = AchtungTweet.where("$or" => queries).collect{|r| {twitter_id: r[:twitter_id], hashtag: r[:hashtag]}};false
    to_write = [];false
    ready.each do |row|
      to_write << row if !existing.include?({twitter_id: row[:twitter_id], hashtag: row[:hashtag]})
    end;false
    AchtungTweet.collection.insert(to_write.uniq)
  end
  
  def self.kickoff
    Dir[File.dirname(__FILE__) + '/home/dgaff/Code/leadership_churn/data/twitter_new/*.csv'].each do |filename|
      ProcessCSV.perform_async(filename)
    end
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

  def self.hashed_row(row, hashtag)
    content = Hash[AchtungTweet.row_keys.zip(row).collect{|k,v| [k, AchtungTweet.process_value(k,v)]}]
    content[:hashtag] = hashtag
    content[:mentioned_users] = extract_mentioned_screen_names(content[:text])
    content
  end

  def process_row(row, hashtag)
    @dataset ||= []
    content = Hash[AchtungTweet.row_keys.zip(row).collect{|k,v| [k, AchtungTweet.process_value(k,v)]}]
    content[:hashtag] = hashtag
    content[:mentioned_users] = extract_mentioned_screen_names(content[:text])
    @dataset << content
    if @dataset.length > 1000
      AchtungTweet.collection.insert(@dataset)
      @dataset = []
    end
    # obj = AchtungTweet.first_or_create(hashtag: hashtag, twitter_id: content[:twitter_id])
    # AchtungTweet.row_keys.each do |k|
    #   obj.send(k.to_s+"=", content[k])
    # end
    # obj.mentioned_users = extract_mentioned_screen_names(content[:text])
    # obj.save!
    # obj
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

  def process_file(filename, proper_csv=false)
    if proper_csv
      CSV.read(filename).each do |row|
        process_row(row, filename.split("/").last.split("_").first.gsub("#", "").split(".").first.downcase)
      end
    else
      Iconv.new('UTF-8','LATIN1').iconv(File.read(filename)).split("\n").collect{|x| row = x.split("\t"); row if !x.empty? && row.length == 12}.compact.each do |row|
        process_row(row, filename.split("/").last.split("_").first.gsub("#", "").downcase)
      end
    end
  end

  def self.read_file(hashtag, proper_csv=false)
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

  def process_files_true_csv_twitter_new
    puts Dir['/home/dgaff/Code/leadership_churn/data/twitter_new/*.csv']
    Dir['/home/dgaff/Code/leadership_churn/data/twitter_new/*.csv'].each {|filename| process_file(filename, true) }
  end  

  def process_files
    puts Dir[File.dirname(__FILE__) + '/../data/twitter/*.tsv']
    Dir[File.dirname(__FILE__) + '/../data/twitter/*.tsv'].each {|filename| process_file(filename) }
  end
  
  def self.process_big_filelist
    relevant_hashtags = File.read("/home/dgaff/Code/leadership_churn/data/hashtags.csv").split("\n").collect{|x| x.gsub("#", "")}
    `ls /home/dgaff/erhardt_paper`.split("\n").each do |file|
      puts file
      data = Iconv.new('UTF-8','LATIN1').iconv(File.read("/home/dgaff/erhardt_paper/"+file)).split("\n").collect{|x| row = x.split("\t"); row if !x.empty? && row.length == 12}.compact;false
      data.each do |row|
        extract_hashtags(row[9]).each do |hashtag|
          if relevant_hashtags.include?(hashtag.downcase)
          csv = CSV.open("/home/dgaff/Code/leadership_churn/data/twitter_new/"+hashtag+".csv", "a+")
          csv << row
          csv.close
        end
      end
    end
  end
end

