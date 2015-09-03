class TemporalAnalysis
  def self.hashtags
    ["baltimoreuprising", "blacklivesmatter", "crimingwhilewhite", "enoughisenough", "ericgarner", "fasttailedgirls", "fergusonreport", "girlslikeus", "michaelbrown", "mynypd", "opferguson", "shutitdown", "solidarityisforwhitewomen", "survivorprivilege", "theemptychair", "trayvonmartin", "whyistayed", "yesallwhitewomen", "yesallwomen", "youoksis"]
  end

  def self.strftimes
    ["%m-%d-%Y %H", "%m-%d-%Y"]
  end
  
  def self.tee_off
    self.hashtags.each do |hashtag|
      self.strftimes.each do |strftime|
        time = AchtungTweet.where(hashtag: hashtag).order(:published_at).first.published_at
        offset = 0
        while time+offset < Time.now
          ChurnRunner.perform_async(hashtag, strftime, time+offset)
          offset += strftime.include?("H") ? 3600 : 3600*24
        end
      end
    end
  end

  def self.run(hashtag, t_step, end_time)
    net = self.network_frame(hashtag, Time.parse(end_time))
    return nil if net.empty?
    self.store_results(self.parse_analysis(self.analyze(self.network_to_ncol(net))), hashtag, Time.parse(end_time), t_step)
  end
  
  def self.store_results(results, hashtag, end_time, t_step)
    results.each do |analytic, user_set|
      s = StatResult.first_or_create(hashtag: hashtag, t_step: t_step, end_time: end_time, analytic: analytic)
      s.user_set = user_set
      s.save!
    end
  end

  def self.network_frame(hashtag, end_time)
    network = {}
    AchtungTweet.where(hashtag: hashtag, :published_at.lte => end_time).fields(:text, :screen_name).order(:published_at).each do |t|
      extract_mentioned_screen_names(t.text).each do |alter|
        network[t.screen_name] ||= []
        network[t.screen_name] << alter
      end
    end
    network
  end

  def self.analyze(outfile)
    rand = rand(10000000)
    statfile = File.dirname(__FILE__)+"/../tmp/stats_#{rand}.ncol"
    `python #{File.dirname(__FILE__)}/../scripts/network_stats.py #{outfile} #{statfile} true`
    `rm #{outfile}`
    statfile
  end

  def self.parse_analysis(statfile, limit=100)
    results = Hash[CSV.read(statfile)]
    nodes = JSON.parse(results["vertices"].gsub("'", "\""))
    top_results = {}
    (results.keys-["vertices"]).each do |v|
      top_results[v] = top_by_score(nodes.zip(JSON.parse(results[v].gsub("nan", "0"))), limit)
    end
    top_results
  end

  def self.network_to_ncol(network)
    rand = rand(10000000)
    outfile = File.dirname(__FILE__)+"/../tmp/network_#{rand}.ncol"
    f = File.open(outfile, "w")
    network.each do |k,v|
      v.each do |alter|
        f.write("#{k}\t#{alter}\n")
      end
    end
    f.close
    outfile
  end
  
  def self.top_by_score(score_set, limit=100)
    score_set.sort_by(&:last).reverse.first(limit).collect(&:first)
  end
end
