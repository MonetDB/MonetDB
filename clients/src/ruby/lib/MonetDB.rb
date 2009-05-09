#module MonetDB

  require 'MonetDBConnection'
  require 'MonetDBData'
  require 'MonetDBExceptions'

  class MonetDB
    Q_TABLE               = "1" # SELECT operation
    Q_UPDATE              = "2" # INSERT/UPDATE operations
    Q_CREATE              = "3" # CREATE/DROP TABLE operations
  
    def initalize()
      @connection = nil 
    end
  
    #  Establish a new connection
    # * user: username (default is monetdb)
    # * passwd: password (default is monetdb)
    # * lang: language (default is sql) 
    # * host: server hostanme or ip  (default is localhost)
    # * port: server port (default is 50000)
    def connect(username = "monetdb", password = "monetdb", lang = "sql", host="127.0.0.1", port = 50000, db_name = "demo", auth_type = "SHA1")
      # TODO: handle pools of connections
      @username = username
      @password = password
      @lang = lang
      @host = host
      @port = port
      @db_name = db_name
      @auth_type = auth_type
      
      @connection = MonetDBConnection.new(user = @username, passwd = @password, lang = @lang, host = @host, port = @port)
      @connection.real_connect(@db_name, @auth_type)
    end
  
    # Send a <b> user submitted </b> query to the server and store the response
    def query(q = "", type_cast = false)
      if  @connection != nil 
        q = "s" + q
    
        @data = MonetDBData.new(@connection, type_cast = type_cast)
        @data.execute(q)    
      end
      return @data
    end
    
    # Return true if there exists a "connection" object
    def is_connected?
      if @connection == nil
        return false
      else 
        return true
      end
    end
  
    # Reconnect to the server
    def reconnect
      if @connection != nil
        self.close
        
        @connection = MonetDBConnection.new(user = @username, passwd = @password, lang = @lang, host = @host, port = @port)
        @connection.real_connect(@db_name, @auth_type)
      end
    end
  
    # Close an active connection
    def close()
      @connection.disconnect
      @connection = nil
    end
  end
#end
