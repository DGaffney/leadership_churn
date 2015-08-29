class MediaCloudArticle
  include MongoMapper::Document
  key :hashtag, String
  key :language, String
  key :media_id, Integer
  key :media_name, String
  key :publish_date, Time
  key :stories_id, Integer
  key :url, String
  key :title, String
  key :guid, String
    
  def self.row_keys
    [:language, :media_id, :media_name, :publish_date, :stories_id, :url, :title, :guid]
  end

  def self.process_row(row, hashtag)
    content = Hash[self.row_keys.zip(row).collect{|k,v| [k, self.process_value(k,v)]}]
    obj = self.first_or_create(hashtag: hashtag, media_id: content[:media_id])
    self.row_keys.each do |k|
      obj.send(k.to_s+"=", content[k])
    end
    obj.save!
    obj
  end
  
  def self.process_value(field, value)
    case field
    when :media_id
      value.to_i
    when :stories_id
      value.to_i
    when :publish_date
      Time.parse(value)
    else
      value
    end
  end

  def self.process_file(filename)
    CSV.read(filename)[1..-1].each do |row|
      self.process_row(row, filename.split("/").last.gsub(".csv", ""))
    end
  end

  def self.process_files
    Dir[File.dirname(__FILE__) + '/../data/media_cloud/*.csv'].each {|filename| self.process_file(filename) }
  end
end

