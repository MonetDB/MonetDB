# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

# MonetDB Active Record adapter
# monetdb_adapter.rb

# The code is an adaption of the adapter developer by Michalis Polakis (2008), to work on top of the pure ruby MonetDB 
# interface

require 'active_record/connection_adapters/abstract_adapter'
require 'MonetDB'

module ActiveRecord
  class Base
    # Establishes a connection to the database that's used by all Active Record objects
    def self.monetdb_connection(config) 
      require_library_or_gem('MonetDB')
      
      # extract connection parameters
      config 	= config.symbolize_keys

      host = config[:host] || "127.0.0.1"
      port = config[:port] || 50000
      username 	= config[:username] || "monetdb"
      password	= config[:password] || "monetdb"
    
      # Use "sql" as default language if none is specified
      lang = config[:lang] || "sql"
      
      # use empty string as database name if none is specified
      database = config[:database] || ""
      
      dbh = MonetDB.new
      ConnectionAdapters::MonetDBAdapter.new(dbh, logger, [host, port, username, password, database, lang], config)
    end
 end


    
 module ConnectionAdapters
  class MonetDBColumn < Column
    # Handles the case where column type is int but default
    # column value is the next value of a sequence(string).
    # By overriding this function, extract_default in
    # schema_definitions does not return a fixnum(0 or 1) but 
    # the correct default value.
    def type_cast(value)
      if( value.nil? )
        return nil
      elsif( type == :integer && value =~/next value for "sys"."seq_(\d)*"/ )
        return value
      else
        super
      end
    end

    private

      def simplified_type(field_type)
          case field_type
            when /int|smallint/i
              :integer
            when /real|double/i
              :float
            when /datetime/i
              :timestamp
            when /timestamp/i
              :timestamp
            when /char/i, /varchar/i
              :string
            when /bigint/i
              :bigint
            else
              super
          end
        end
  
  end #end of MonetDBColumn class

  class TableDefinition
    # Override so that we handle the fact that MonetDB 
    # doesn't support "limit" on integer column.
    # Otherwise same implementation
    def column(name, type, options = {})
      column = self[name] || ColumnDefinition.new(@base, name, type)
        
      if(type.to_sym != :integer and type.to_sym != :primary_key)
        column.limit = options[:limit] || native[type.to_sym][:limit] if options[:limit] or native[type.to_sym]
      end

      column.precision = options[:precision]
      column.scale = options[:scale]
      column.default = options[:default]
      column.null = options[:null]
      
      @columns << column unless @columns.include? column
      self
    end

  end

  class MonetDBAdapter < AbstractAdapter
    def initialize(connection, logger,   connection_options, config)
      super(connection, logger)
      @connection_options, @config = connection_options, config
      connect
    end
 
    def adapter_name #:nodoc:
      'MonetDB'
    end
  
    # Functions like rename_table, rename_column and 
    # change_column cannot be implemented in MonetDB.
    def supports_migrations?
      true
    end
    
    # testing savepoints in progress
    def supports_savepoints? #:nodoc:
      true
    end
    
    def support_transaction? #:nodoc:
      false
    end
    
    def supports_ddl_transactions?
      false
    end

    def native_database_types
        {
          :primary_key => "int NOT NULL auto_increment PRIMARY KEY",
          :string      => { :name => "varchar", :limit => 255 },
          :text        => { :name => "clob" },
          :integer     => { :name => "int"},
          :float       => { :name => "float" },
          :decimal     => { :name => "decimal" },
          :datetime    => { :name => "timestamp" },
          :timestamp   => { :name => "timestamp" },
          :time        => { :name => "time" },
          :date        => { :name => "date" },
          :binary      => { :name => "blob" },
          :boolean     => { :name => "boolean" },
          :bigint      => { :name => "bigint" }	
        }
    end


    #MonetDB does not support using DISTINCT withing COUNT
    #by default
    def supports_count_distinct?
      false
    end

    #----------CONNECTION MANAGEMENT------------------

    # Check if the connection is active
    def active?
      if @connection != nil
        @connection.is_connected?
      end
      
      return false
    end

    # Close this connection and open a new one in its place.
    def reconnect!
      if @connection != nil
        #@connection.reconnect
        false
      end
    end

    def disconnect!
     #@connection.auto_commit(flag=true)
     @connection.close
    end

    # -------END OF CONNECTION MANAGEMENT----------------



    # ===============SCHEMA DEFINITIONS===========#
    
    def binary_to_string(value) 
      res = ""
      value.scan(/../).each { |i| res << i.hex.chr }
      res
    end
    
    # ===========END OF SCHEMA DEFINITIONS========#




    #===============SCHEMA STATEMENTS===========#
    # The following schema_statements.rb functions are not supported by MonetDB (19/5/2008).
    #
    # -rename_table : not such functionality by MonetDB's API. Altering some 
    #  administratives' tables values has no result.
    #
    # -rename_column : Could be possible if I make a new_name column copy the
    # data from the old column there and then drop that column. But I would 
    # also have to take care of references and other constraints. Not sure if
    # this is desired.
    # 
    # -change_column : Alteration of a column's datatype is not supported. 
    # NOTE WE MAY BE ABLE TO "CHANGE" A COLUMN DEFINITION IF WE DROP THE COLUMN
    # AND CREATE A NEW ONE WITH THE OLD NAME. THIS COULD WORK AS LONG AS WE DON'T
    # LOSE ANY DATA.


    
    # Sets a new default value for a column.
    # ===== Examples =====
    #  change_column_default(:suppliers, :qualification, 'new')
    #  change_column_default(:accounts, :authorized, 1)
    def change_column_default(table_name, column_name, default)
      sql = "ALTER TABLE #{table_name} ALTER COLUMN #{quote_column_name(column_name)} SET DEFAULT" #{quote(default)}"

      if( default.nil? || (default.casecmp("NULL")==0) )
        sql << " NULL"
      else
        sql << quote(default)
      end
      p "SQL: " + sql + '\n'
      hdl = execute(sql) 
    end

    def remove_index(table_name, options = {})
      hdl = execute("DROP INDEX #{index_name(table_name, options)}")
    end

    # MonetDB does not support limits on certain data types
    # Limit is supported for the {char, varchar, clob, blob, time, timestamp} data types.
    def type_to_sql(type, limit = nil, precision = nil, scale = nil)
      return super if limit.nil?

      # strip off limits on data types not supporting them
      if [:integer, :double, :date, :bigint].include? type
        type.to_s
      else
        super
      end
    end

    # Returns an array of Column objects for the table specified by +table_name+.
    def columns(table_name, name = nil) 
      return [] if table_name.to_s.strip.empty?
      table_name = table_name.to_s if table_name.is_a?(Symbol)
      table_name = table_name.split('.')[-1] unless table_name.nil?

      hdl = execute("	SELECT name, type, type_digits, type_scale, \"default\", \"null\"  FROM _columns 	WHERE table_id in (SELECT id FROM _tables WHERE name = '#{table_name}')" ,name)
      
      num_rows = hdl.num_rows
      return [] unless num_rows >= 1
      
      result = []       

     
      while row = hdl.fetch_hash do
        col_name = row['name']
        col_default = row['default']
        # If there is no default value, it assigns NIL
        col_default = nil if (col_default && col_default.upcase == 'NULL')
        # Removes single quotes from the default value
        col_default.gsub!(/^'(.*)'$/, '\1') unless col_default.nil?

	      # A string is returned so we must convert it to boolean
        col_nullable = row['null']
	
	      if( col_nullable.casecmp("true") == 0 )
	        col_nullable = true
	      elsif( col_nullable.casecmp("false") == 0 )
          col_nullable = false
	      end

        col_type = row['type']
        type_digits = row['type_digits']
        type_scale = row['type_scale']

        
        # Don't care about datatypes that aren't supported by
        # ActiveRecord, like interval. 
        # Also do nothing for datatypes that don't support limit
        # like integer, double, date, bigint
        if(col_type == "clob" || col_type == "blob")
            if(type_digits.to_i > 0)
              col_type << "(#{type_digits})"
            end
        elsif(col_type == "char" ||
              col_type == "varchar" || 
              col_type == "time" || 
              col_type == "timestamp"
             )
          col_type << "(#{type_digits})"
        elsif(col_type == "decimal")
          if(type_scale.to_i ==  0)
            col_type << "(#{type_digits})"
          else
            col_type << "(#{type_digits},#{type_scale})"
          end
        end

        # instantiate a new column and insert into the result array
        result << MonetDBColumn.new(col_name, col_default, col_type, col_nullable)

      end
      
      #  check that free has been correctly performed
      hdl.free
      
      return result
    end

    # Adds a new column to the named table.
    # See TableDefinition#column for details of the options you can use.
    def add_column(table_name, column_name, type, options = {})
      if( (type.to_sym == :decimal) && (options[:precision].to_i+options[:scale].to_i > 18) )
        raise StandardError, "It is not possible to have a decimal column where Precision + Scale > 18 . The column will not be added to the table!"
	      return
      else
	      super
      end
    end    

    # Return an array with all non-system table names of the current 
    # database schema
    def tables(name = nil)
      cur_schema =  select_value("select current_schema",name)
      select_values("	SELECT t.name FROM sys._tables t, sys.schemas s 
			WHERE s.name = '#{cur_schema}' 
				AND t.schema_id = s.id 
				AND t.system = false",name)
    end

    # Returns an array of indexes for the given table.
    def indexes(table_name, name = nil)
      sql_query =  "	SELECT distinct i.name as index_name, k.\"name\", k.nr
	 		FROM
				idxs i, _tables t, objects k 
	 		WHERE
				i.type = 0 AND i.name not like '%pkey' 
				AND i.id = k.id AND t.id = i.table_id 
				AND t.name = '#{table_name.to_s}'
	 		ORDER BY i.name, k.nr;"
      result = select_all(sql_query,name);

      cur_index = nil
      indexes = []

      result.each do |row|
        if cur_index != row['index_name'] 
          indexes << IndexDefinition.new(table_name, row['index_name'], false, [])
          cur_index = row['index_name']
        end 	
  
        indexes.last.columns << row['name']
      end

      indexes 
    end

    # ===========END OF SCHEMA STATEMENTS========#

    # ===========QUOTING=========================#
    def quote(value, column = nil)
      if value.kind_of?(String) && column && column.type == :binary && column.class.respond_to?(:string_to_binary)
        s = column.class.string_to_binary(value).unpack("H*")[0]
        "BLOB '#{s}'"
      else
        super
      end
    end
   
    def quote_column_name(name) #:nodoc:
      "\"#{name.to_s}\""
    end
    
    def quote_table_name(name) #:nodoc:
      quote_column_name(name).gsub('.', '"."')
    end

    # If the quoted true is 'true' MonetDB throws a string cast exception
    def quoted_true
      "true"
    end

    # If the quoted false is 'false' MonetDB throws a string cast exception
    def quoted_false
      "false"
    end

    # ===========END-OF-QUOTING==================#


    # =========DATABASE=STATEMENTS===============#
   
    # Returns an array of arrays containing the field values.
    # Order of columns in tuple arrays is not guaranteed.
    def select_rows(sql, name = nil)
      result = select(sql, name)
      result.map{ |v| v.values}
    end

    def execute(sql, name = nil)
      # This substitution is needed. 
      sql =  sql.gsub('!=', '<>')
      sql += ';'
      #log(sql, name) do
         hdl = @connection.query(sql) 
      #end
    end 

    # Begins the transaction.
    def begin_db_transaction
      hdl = execute("START TRANSACTION")
    end

    # Commits the transaction (ends TRANSACTIOM). 
    def commit_db_transaction
      hdl = execute("COMMIT")
    end

    # Rolls back the transaction. Must be
    # done if the transaction block raises an exception or returns false (ends TRANSACTIOM).
    def rollback_db_transaction
      hdl = execute("ROLLBACK")
    end
    
    def current_savepoint_name
      @connection.transactions || 0
    end
    
    # Create a new savepoint
    def create_savepoint
     @connection.save
     execute("SAVEPOINT #{current_savepoint_name}")
    end

    # rollback to the last savepoint
    def rollback_to_savepoint
      execute("ROLLBACK TO SAVEPOINT #{current_savepoint_name}")
    end

    # release current savepoint
    def release_savepoint
      execute("RELEASE SAVEPOINT #{current_savepoint_name}")
    end
    

    def add_lock!(sql, options)
      @logger.info "Warning: MonetDB :lock option '#{options[:lock].inspect}' not supported. Returning unmodified sql statement!" if @logger && options.has_key?(:lock)
      sql
    end
   
    def empty_insert_statement(table_name)
	    # Ensures that the auto-generated id  value will not violate the primary key constraint.
	    # comment out for production code(?)
	    #make_sure_pk_works(table_name, nil)
      #"INSERT INTO #{quote_table_name(table_name)}"
    end
   #=======END=OF=DATABASE=STATEMENTS=========#


    protected
      # Returns an array of record hashes with the column names as keys and
      # column values as values.
      def select(sql, name = nil)
        hdl = execute(sql,name) 
       
        fields = []
        result = []

        if( (num_rows = hdl.num_rows) > 0 )
          fields = hdl.name_fields
          
          # Must do a successful mapi_fetch_row first
             
            row_hash={}
            fields.each_with_index do |f, i| 
              row_hash[f] = hdl.fetch_column_name(f)
            end
            result << row_hash
          
        end
        
        result
      end

      # Executes the update statement and returns the number of rows affected.
      def update_sql(sql, name = nil)
	      hdl = execute(sql,name)
        # affected_rows = hdl.affected_rows
        return false
      end
      
      # Returns the last auto-generated ID from the affected table.
      def insert_sql(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil)
	    # Ensures that the auto-generated id  value will not violate the 
	    # primary key constraint. Read the comments of make_sure_pk_works
	    # and documentation for further information.
	    # comment out for production code(?)
	      # table_name = extract_table_name_from_insertion_query(sql)
	      # make_sure_pk_works(table_name,name)	
        hdl = execute(sql, name)
        # last_auto_generated_id = hdl.get_last_auto_generated_id
      end
      
      # Some tests insert some tuples with the id values set. In other words, the sequence
      # is not used to generate a value for the primary key column named id. When a new tuple
      # it to be inserted, where the id value is not set explicitly, a primary key violation will
      # be raised because the generated from the sequence value is the same as one of the existing
      # id values. This happens in unit tests quite often. So this function serves that the unit tests
      # pass. However it is very expensive( sends 4 queries to the server) and probably not suitable for
      # production code. Check the implementation for further info/details.
      def make_sure_pk_works(table_name,name)
	    # Ensure the auto-generated id will not violate the primary key constraint.
      # This is expensive and it's used so that the tests pass. Comment out for production code(?).
	    # Assume that table name has one primary key column named id that is associated with a sequence,
	    # otherwise return
	      hdl = nil
	      sequence_name = extract_sequence_name( select_value("select \"default\" from _columns where table_id in (select id from _tables where name = '#{table_name}') and name='id';") )	

	      return if sequence_name.blank?	

	      max_id = select_value("select max(id) from #{table_name}").to_i
	      next_seq_val = select_value("select next value for #{sequence_name}").to_i

	      if( max_id > next_seq_val )
	        hdl = execute("ALTER SEQUENCE #{sequence_name} RESTART WITH #{max_id+1}" ,name)
	      else
	        hdl = execute("ALTER SEQUENCE #{sequence_name} RESTART WITH #{next_seq_val+1}",name)
	      end
      end

      # Auxiliary function that extracts the table name from an insertion query
      # It's called by insert_sql in order to assist at make_sure_pk_works.
      # Ideally, if make_sure_pk_works is commented out for production code, this
      # function will be never called.
      def extract_table_name_from_insertion_query(sql)
        $1 if sql =~ /INSERT INTO "(.*)" \(/
      end

      # Auxiliary function that extracts the sequence name.
      # It's called by make_sure_pk_works.
      # Ideally, if make_sure_pk_works is commented out for production code, this
      # function will be never called.
      def extract_sequence_name(seqStr)
	      $1 if seqStr =~ /\."(.*)"/
      end

    private
      def connect
        @connection.connect(user = @connection_options[2], passwd =  @connection_options[3], lang = @connection_options[5], host =  @connection_options[0], port =  @connection_options[1], db_name =  @connection_options[4], auth_type = "SHA1")  if @connection
      end 
  end
end
end
