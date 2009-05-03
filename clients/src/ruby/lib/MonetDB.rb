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
    def connect(user = "monetdb", passwd = "monetdb", lang = "sql", host="127.0.0.1", port = 50000, db_name = "demo", auth_type = "SHA1")
      # TODO: handle pools of connections
      @connection = MonetDBConnection.new(user = "monetdb", passwd = "monetdb", lang = "sql", host="127.0.0.1", port = 50000)
      @connection.real_connect(db_name, auth_type)
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
  
    # Close an active connection
    def close()
      @connection.disconnect
    end
  end
#end
