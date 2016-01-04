# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

# = Introduction
    #
    # A typical sequence of events is as follows:
    # Create a database instance (handle), invoke query using the database handle to send the statement to the server and get back a result set object.
    #
    # A result set object  has methods for fetching rows, moving around in the result set, obtaining column metadata, and releasing the result set.
    # A result set object is an instance of the MonetDBData class.
    #
    # Records can be returned as arrays and iterators over the set.
    #
    # A database handler (dbh) is and instance of the MonetDB class.
    #
    # = Connection management 
    #
    #  connect    -  establish a new connection
    #                * user: username (default is monetdb)
    #                * passwd: password (default is monetdb)
    #                * lang: language (default is sql) 
    #                * host: server hostanme or ip  (default is localhost)
    #                * port: server port (default is 50000)
    #                * db_name: name of the database to connect to
    #                * auth_type: hashing function to use during authentication (default is SHA1)
    #
    #  is_connected? - returns true if there is an active connection to a server, false otherwise 
    #  reconnect     - recconnect to a server
    #  close         - terminate a connection
    #  auto_commit?  - returns ture if the session is running in auto commit mode, false otherwise  
    #  auto_commit   - enable/disable auto commit mode.
    #
    #  query         - fire a query
    #
    # Currently MAPI protocols 8 and 9 are supported.
    #
    # = Managing record sets 
    #
    #
    # A record set is represented as an instance of the MonetDBData class; the class provides methods to manage retrieved data.
    #
    #
    # The following methods allow to iterate over data:
    # 
    # fetch          - iterates over the record set and retrieves one row at a time. Each row is returned as an array.
    # each_record    - works as ruby each method. The method takes a block as parameter and yields each record to this block. 
    # fetch_hash     - iterates over the record set and retrieves one row at a time. Each row is returned as a hash.
    # each_record_as_hash - works as ruby each method. The method takes a block as parameter and yields each record, as hash, to this block
    # fetch_all      - returns all rows as a two dimensional array
    # fetch_all_as_column_hash - returns all records as a hash with the column name as the keys and an array with all column values as values
    #
    # Information about the retrieved record set can be obtained via the following methods:
    #
    # num_rows       - returns the number of rows present in the record set
    # num_fields     - returns the number of fields (columns) that compose the schema
    # name_fields    - returns the (ordered) name of the schema's columns
    # type_fields    - returns the (ordered) types list of the schema's columns
    #
    # To release a record set MonetDBData#free can be used.
    #
    # = Type conversion
    #
    # All values from the database are converted to the closest ruby type, i.e: INTEGER to int, TIME to time, CLOB to string
    # Some of the more complex datatypes are not recognized, such as INTERVAL, these are converted to strings
    #
    # = Transactions 
    #
    # By default monetdb works in auto_commit mode. To turn this feature off MonetDB#auto_commit(flag=false) can be used.
    #
    # Once auto_commit has been disable it is possible to start transactions, create/delete savepoints, rollback and commit with 
    # the usual SQL statements.
    #
    # Savepoints IDs can be generated using the MonetDB#save method. To release a savepoint ID use MonetDB#release.
    # 
    # Savepoints can be accessed (as a stack) with the MonetDB#transactions method.
    #
    # demo.rb contains usage example of the above mentioned methods.

require_relative 'MonetDBConnection'
require_relative 'MonetDBData'
require_relative 'MonetDBExceptions'

class MonetDB
  DEFAULT_USERNAME = "monetdb"
  DEFAULT_PASSWORD = "monetdb"
  DEFAULT_LANG     = MonetDBConnection::LANG_SQL
  DEFAULT_HOST     = "127.0.0.1"
  DEFAULT_PORT     = 50000
  DEFAULT_DATABASE = "test"
  DEFAULT_AUTHTYPE = "SHA1"
  
  def initalize()
    @connection = nil 
  end

  # Establish a new connection.
  #                * username: username (default is monetdb)
  #                * password: password (default is monetdb)
  #                * lang: language (default is sql) 
  #                * host: server hostanme or ip  (default is localhost)
  #                * port: server port (default is 50000)
  #                * db_name: name of the database to connect to
  #                * auth_type: hashing function to use during authentication (default is SHA1)
  def connect(username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD, lang=DEFAULT_LANG, host=DEFAULT_HOST, port=DEFAULT_PORT, db_name=DEFAULT_DATABASE, auth_type=DEFAULT_AUTHTYPE)
    # TODO: handle pools of connections
    
    @username = username
    @password = password
    @lang = lang
    @host = host
    @port = port
    @db_name = db_name
    @auth_type = auth_type
    
    @connection = MonetDBConnection.new(user = @username, passwd = @password, lang = @lang, host = @host, port = @port)
    @connection.connect(@db_name, @auth_type)
  end

  # Establish a new connection using named parameters.
  #                * user: username (default is monetdb)
  #                * passwd: password (default is monetdb)
  #                * language: lang (default is sql) 
  #                * host: host to connect to  (default is localhost)
  #                * port: port to connect to (default is 50000)
  #                * database: name of the database to connect to
  #                * auth_type: hashing function to use during authentication (default is SHA1)
  #
  # Conventionally named parameters are passed as an hash.
  #
  # Ruby 1.8:
  # MonetDB::conn({ :user => "username", :passwd => "password", :database => "database"})
  #
  # Ruby 1.9:
  # MonetDB::conn(user: "username", passwd: "password", database: "database")
  def conn(options)      
    user        = options[:user] || DEFAULT_USERNAME
    passwd      = options[:passwd] || DEFAULT_PASSWORD
    language    = options[:language] || DEFAULT_LANG
    host        = options[:host] || DEFAULT_HOST
    port        = options[:port] || DEFAULT_PORT
    database    = options[:database] || DEFAULT_DATABASE
    auth_type   = options[:auth_type] || DEFAULT_AUTHTYPE
    
    connect(user, passwd, language, host, port, database, auth_type)
  end

  # Send a <b> user submitted </b> query to the server and store the response.
  # Returns and instance of MonetDBData.
  def query(q="")
    if  @connection != nil 
      @data = MonetDBData.new(@connection)
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
      @connection.connect(db_name = @db_name, auth_type = @auth_type)
    end
  end
  
  # Turn auto commit on/off
  def auto_commit(flag=true)
    @connection.set_auto_commit(flag)
  end

  # Returns the current auto commit  (on/off) settings.
  def auto_commit?
    @connection.auto_commit?
  end
  
  # Returns the name of the last savepoint in a transactions pool
  def transactions
    @connection.savepoint
  end
  
  # Create a new savepoint ID
  def save
    @connection.transactions.save
  end
  
  # Release a savepoint ID
  def release
    @connection.transactions.release
  end
  
  # Close an active connection
  def close()
    @connection.disconnect
    @connection = nil
  end
end
