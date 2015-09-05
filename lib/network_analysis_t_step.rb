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
    tweets = AchtungTweet.read_file(hashtag)
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
    count == "all" ? network.values.flatten.counts.sort_by{|k,v| v}.reverse.collect(&:first) : network.values.flatten.counts.sort_by{|k,v| v}.reverse.first(count).collect(&:first)
  end

  def self.full_indegree(network, count="all")
    count == "all" ? Hash[network.values.flatten.counts.sort_by{|k,v| v}.reverse] : Hash[network.values.flatten.counts.sort_by{|k,v| v}.reverse.first(count)]
  end

  def self.store_result(net)
    net.delete(:network)
    begin
      tau = net[:tau].nan? ? 0 : net[:tau]
      StoreStatResultTwo.perform_async(net.merge(tau: tau))
    rescue
      puts "!*"
      sleep(4)
      retry
    end
  end
  
  def self.rank_by_analytic(hashtag, strftime_template, analytic)
    #first seen
    witness_me = {}
    #first posted
    shiny_and_chrome = {}
    self.network_by_t_step(hashtag, strftime_template) do |net|
      month, day, year, hour = net[:end_time].split(/[- ]/)
      time = net[:strftime_template].include?("H") ? Time.parse("#{year}-#{month}-#{day} #{hour}:00:00 +0000") : Time.parse("#{year}-#{month}-#{day} 00:00:00 +0000")
      uniq_posted_accounts = net[:network].values.flatten.uniq
      all_accounts = uniq_posted_accounts|net[:network].keys
      uniq_posted_accounts.each do |acct|
        shiny_and_chrome[acct] ||= time
      end
      all_accounts.each do |acct|
        witness_me[acct] ||= time
      end
      i = 0
      self.full_indegree(net[:network]).each do |account, degree|
        i += 1
        record = {screen_name: account, hashtag: hashtag, timestamp: time, strftime_template: strftime_template, metric_value: degree, metric_name: analytic, metric_rank: i, first_seen: witness_me[account], first_posted: shiny_and_chrome[account], has_posted_yet: !shiny_and_chrome[account].nil?}
        record[:total_seen_lifespan] = time-record[:first_seen] if record[:first_seen]
        record[:total_posted_lifespan] = time-record[:first_posted] if record[:first_posted]
        StoreSurvivalAnalysis.perform_async(record) if SurvivalAnalysisRecord.first(screen_name: record[:screen_name], hashtag: record[:hashtag], timestamp: record[:timestamp], strftime: record[:strftime_template], metric_name: record[:metric_name]).nil?
      end
    end
  end

  def self.tau_by_analytic(hashtag, strftime_template, analytic)
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
      self.store_result(net.merge(count: 100)) if net[:tau]
    end
  end
  
  def self.run_all_tau
    self.hashtags.each do |hashtag|
      self.strftimes.each do |strftime|
        self.analytics.each do |analytic|
          puts [hashtag, strftime, analytic].inspect
          ChurnRunner.perform_async(hashtag, strftime, analytic)
        end
      end
    end
  end

  def self.run_all_survival
    self.hashtags.each do |hashtag|
      self.strftimes.each do |strftime|
        self.analytics.each do |analytic|
          puts [hashtag, strftime, analytic].inspect
          SurvivalRunner.perform_async(hashtag, strftime, analytic)
        end
      end
    end
  end
end
