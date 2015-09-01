class NetworkAnalysisTStep
  def self.hashtags
    ["baltimoreuprising", "blacklivesmatter", "crimingwhilewhite", "enoughisenough", "ericgarner", "fasttailedgirls", "fergusonreport", "girlslikeus", "michaelbrown", "mynypd", "opferguson", "shutitdown", "solidarityisforwhitewomen", "survivorprivilege", "theemptychair", "trayvonmartin", "whyistayed", "yesallwhitewomen", "yesallwomen", "youoksis"]
  end

  def self.strftimes
    ["%m-%d-%Y %H", "%m-%d-%Y"]
  end
  
  def self.analytics
    ["indegree"]
  end

  def self.network_by_t_step(hashtag, strftime_template)
    network = {}
    cur_strftime = AchtungTweet.where(hashtag: hashtag).order(:published_at).first.published_at.strftime(strftime_template)
    prev_strftime = cur_strftime.dup
    tweets =AchtungTweet.where(hashtag: hashtag).fields(:published_at, :text, :screen_name).order(:published_at).to_a;false
    tweets.each do |t|
      prev_strftime = cur_strftime
      cur_strftime = t.published_at.strftime(strftime_template)
      extract_mentioned_screen_names(t.text).each do |alter|
        network[t.screen_name] ||= []
        network[t.screen_name] << alter
      end
      if prev_strftime != cur_strftime
        hash = {network: network.dup, strftime_template: strftime_template, hashtag: hashtag, end_time: cur_strftime}
        yield hash
      end
    end
  end

  def self.indegree(network, count="all")
    count == "all" ? network.values.flatten.counts.sort_by{|k,v| v}.reverse.collect(&:first) : network.values.flatten.counts.sort_by{|k,v| v}.reverse.collect(&:first)
  end
  
  def self.store_result(net)
    month, day, year, hour = net[:end_time].split(/[- ]/)
    end_time = net[:strftime_template].include?("H") ? Time.parse("#{year}-#{month}-#{day} #{hour}:00:00") : Time.parse("#{year}-#{month}-#{day} 00:00:00")
    sr2 = StatResultTwo.first_or_create(hashtag: net[:hashtag], end_time: end_time, net_statistic: net[:net_statistic], strftime_template: net[:strftime_template])
    sr2.tau = net[:tau].nan? ? 0 : net[:tau]
    sr2.n = net[:n]
    sr2.index = net[:index]
    sr2.article_count = MediaCloudArticle.where(hashtag: net[:hashtag], :publish_date.lte => end_time).count
    sr2.alexa_mean = AlexaRank.mean(net[:hasthag], end_time)
    sr2.alexa_max = AlexaRank.max(net[:hasthag], end_time)
    sr2.alexa_sum = AlexaRank.sum(net[:hasthag], end_time)
    sr2.save!
  end
  
  def self.run_by_analytic(hashtag, strftime_template, analytic)
    latest = {}
    i = 0
    self.network_by_t_step(hashtag, strftime_template) do |net|
      net[:index] = i
      i+=1
      if latest[:end_time] != net[:end_time] && !latest.empty?
        if latest[:network] == net[:network]
          net[:tau] = 1.0
        else
          indegrees = [latest[:network], net[:network]].collect{|n| n[:net_statistic] = analytic ; net[:n] = n.length ; self.send(analytic, n, 100)}
          if indegrees.first == indegrees.last
            net[:tau] = 1.0
          else
            net[:tau] = KendallTau.run(*indegrees)
          end
        end
      end
      latest = net
      print "."
      self.store_result(net) if net[:tau]
    end
  end
  
  def self.run_all
    self.hashtags.each do |hashtag|
      self.strftimes.each do |strftime|
        self.analytics.each do |analytic|
          puts [hashtag, strftime, analytic].inspect
          ChurnRunner.perform_async(hashtag, strftime, analytic)
        end
      end
    end
  end
end