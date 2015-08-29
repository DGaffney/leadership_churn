class KendallTau

  def self.run(first, second)
    p = self.concordant_count(first, second)
    q = self.discordant_count(first, second)
    t = self.distinct_of(first, second).count
    u = self.distinct_of(second, first).count
    (p - q) / Math.sqrt((p + q + t) * (p + q + u))
  end
  
  def self.concordant_count(first, second)
    first_perm = first.permutation(2).to_a
    second_perm = second.permutation(2).to_a
    first_perm.map do |pair|
      next if [first.index(pair.first), first.index(pair.second), second.index(pair.first), second.index(pair.second)].include?(nil)
      1 if (first.index(pair.first) > first.index(pair.second) && second.index(pair.first) > second.index(pair.second)) || (first.index(pair.first) < first.index(pair.second) && second.index(pair.first) < second.index(pair.second))
    end.compact.sum
  end
  
  def self.discordant_count(first, second)
    first_perm = first.permutation(2).to_a
    second_perm = second.permutation(2).to_a
    first_perm.map do |pair|
      next if [first.index(pair.first), first.index(pair.second), second.index(pair.first), second.index(pair.second)].include?(nil)
      1 if (first.index(pair.first) < first.index(pair.second) && second.index(pair.first) > second.index(pair.second)) || (first.index(pair.first) > first.index(pair.second) && second.index(pair.first) < second.index(pair.second))
    end.compact.sum
  end
  
  def self.distinct_of(list_a, list_b)
    list_a-list_b
  end
end
