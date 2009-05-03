# Exception classes for the ruby-monetdb driver

class MonetDBQueryError < StandardError
  def initialize(e)
    $stderr.puts e
  end
end

class MonetDBDataError < StandardError
  def initialize(e)
    $stderr.puts e
  end
end

class MonetDBCommandError < StandardError
  def initialize(e)
    $stderr.puts e
  end
end

class MonetDBSocketError < StandardError
  def initialize(e)
    $stderr.puts e
  end
end