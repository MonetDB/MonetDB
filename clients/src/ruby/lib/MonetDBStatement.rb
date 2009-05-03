# Formats, handles and executes SQL/XQUERY statements.

class MonetDBStatement
  
  def initialize(connection = nil, lang = 'sql', version = '5')
    @connection = connection
    @lang = lang
    @version = version
  end
  
  # Send a query to a monetdb5 server
  def real_query(q)
    if @connection != nil
      @connection.send(format_command("reply size 1"))
      @connection.send(format_query(q))
      query = @connection.receive()
    else 
      raise MonetDBConnectionError, "Failed to perform query; connection not available"
    end
  end
  
  private
  # Formats a query <i>string</i> so that it can be parsed by the server
  def format_query(q)
    if @lang == @@LANG_SQL
        return "s" + q + ";\n"
    else
      raise LanguageNotSupported
    end
  end
  
  # Formats a <i>command</i> string so that it can be parsed by the server
  def format_command(x)
    return "X" + x + "\nX"
  end
end