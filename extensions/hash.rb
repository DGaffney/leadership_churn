class Hash
  def get(key)
    self[key.to_s]||self[key.to_sym]
  end
  
  def set(key, value)
    if self[key.to_s]
      self[key.to_s] = value
    elsif self[key.to_sym]
      self[key.to_sym] = value
    else
      self[key.to_sym] = value
    end
    self
  end
end