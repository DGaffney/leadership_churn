class Churner
  def self.run
    self.hashtags.each do |hashtag|
      self.strftimes.each do |strftime|
        ChurnRunner.perform_async(hashtag, strftime)
      end
    end
  end

  def self.regime_calculator(regimes, i)
    if i > regimes[:first_tweet_index] && regimes[:first_article_index] && i < regimes[:first_article_index] && regimes[:last_article_index] && i < regimes[:last_article_index]
      "pre_media"
    elsif i > regimes[:first_tweet_index] && regimes[:first_article_index] && i > regimes[:first_article_index] && regimes[:last_article_index] && i < regimes[:last_article_index]
      "present_media"
    elsif i > regimes[:first_tweet_index] && regimes[:first_article_index] && i > regimes[:first_article_index] && regimes[:last_article_index] && i > regimes[:last_article_index]
      "post_media"
    end
  end

  def self.hashtags
    ["baltimoreuprising", "blacklivesmatter", "crimingwhilewhite", "enoughisenough", "ericgarner", "fasttailedgirls", "fergusonreport", "girlslikeus", "michaelbrown", "mynypd", "opferguson", "shutitdown", "solidarityisforwhitewomen", "survivorprivilege", "theemptychair", "trayvonmartin", "whyistayed", "yesallwhitewomen", "yesallwomen", "youoksis"]
  end

  def self.strftimes
    ["%m-%d-%Y %H", "%m-%d-%Y"]
  end

  def self.network_churn(strftime, hashtag)
    network = {}
    snapshots = []
    times = []
    article_ids = []
    count = 0
    first_tweet = AchtungTweet.where(hashtag: hashtag).order(:published_at).first
    first_article = MediaCloudArticle.where(hashtag: hashtag, :publish_date.gte => first_tweet.published_at).order(:publish_date).first
    last_article = MediaCloudArticle.where(hashtag: hashtag, :publish_date.gte => first_tweet.published_at).order(:publish_date.desc).first
    regimes = {first_tweet_index: 0}
    cur_date = first_tweet.published_at.strftime(strftime)
    prev_date = nil
    cur_count = 0
    prev_count = 0
    tweet_count = 0
    AchtungTweet.where(hashtag: hashtag).order(:published_at).each do |tweet|
      tweet_count += 1
      cur_date = tweet.published_at.strftime(strftime)
      extract_mentioned_screen_names(tweet.text).each do |alter|
        network[alter] ||= {}
        network[alter][tweet.screen_name] ||= 0
        network[alter][tweet.screen_name] += 1
        if regimes[:first_article_index].nil? && tweet.published_at > first_article.publish_date
          print "!"
          regimes[:first_article_index] = count 
        end
        if regimes[:last_article_index].nil? && tweet.published_at > last_article.publish_date
          print "!"
          regimes[:last_article_index] = count 
        end
        if prev_date != cur_date
          cur_count = network.keys.length
          print "|"
          article_ids << MediaCloudArticle.where(hashtag: hashtag, :publish_date.gte => first_tweet.published_at, :publish_date.lte => tweet.published_at).fields(:id).collect(&:id)
          snapshots << self.process_network(network, limit=100)
          count += 1
          times << cur_date
          if snapshots.length > 1
            snapshots.last.keys.flatten.uniq.each do |network_attr|
              t_minus_one, t = snapshots[-2..-1].collect{|snap| snap[network_attr]}
              result = {tau: KendallTau.run(t_minus_one, t), regime: Churner.regime_calculator(regimes, count), index: count, hashtag: hashtag, strftime: strftime, network_attr: network_attr, actual_set_t: t, actual_set_t_minus_one: t_minus_one, time: times.last}
              sr = StatResult.first_or_create(index: count, hashtag: hashtag, network_attr: network_attr, strftime: strftime)
              sr.tau = result[:tau]
              sr.tweet_count = tweet_count
              sr.regime = result[:regime]
              sr.actual_set_t = result[:actual_set_t]
              sr.actual_set_t_minus_one = result[:actual_set_t_minus_one]
              sr.article_ids = article_ids.last
              sr.time = result[:time]
              sr.t_minus_one_count = prev_count
              sr.t_count = cur_count
              sr.save!
            end
          end
          prev_count = cur_count
        end
        print "."
      end
      prev_date = cur_date
    end
    {network: network, snapshots: snapshots, regimes: regimes, times: times, article_ids: article_ids}
  end

  def self.write_ncol(network)
    rand = rand(1000000)
    file = File.open(File.dirname(__FILE__) + "/../tmp/network_#{rand}.ncol", "w")
    network.each do |k,v|
      v.keys.each do |alter|
        file.write([k,alter].join("\t")+"\n")
      end
    end
    file.close
    File.dirname(__FILE__) + "/../tmp/network_#{rand}.ncol"
  end

  def self.analyze_network(ncol_filename)
    rand = rand(1000000)
    outfile = File.dirname(__FILE__) + "/../tmp/network_#{rand}.ncol"
    `python #{File.dirname(__FILE__)}/../scripts/network_stats.py #{ncol_filename} #{outfile} true`
    outfile
  end
  
  def self.analyze_outfile(outfile_name, limit=100)
    print outfile_name
    results = Hash[CSV.read(outfile_name)]
    nodes = JSON.parse(results["vertices"].gsub("'", "\""))
    top_results = {}
    (results.keys-["vertices"]).each do |v|
      top_results[v] = top_by_score(nodes.zip(JSON.parse(results[v].gsub("nan", "0"))), limit)
    end
    top_results
  end

  def self.process_network(network, limit=100)
    ncol_file = self.write_ncol(network)
    out_file = self.analyze_network(ncol_file)
    result = self.analyze_outfile(out_file, limit)
    `rm #{ncol_file}`
    `rm #{out_file}`
    result
  end

  def self.top_by_score(score_set, limit=100)
    score_set.sort_by(&:last).reverse.first(limit).collect(&:first)
  end
end