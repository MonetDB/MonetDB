# Models a MonetDB RecordSet

require 'time'
require 'ostruct'
require 'MonetDBStatement'

require "bigdecimal"

class MonetDBData < MonetDBStatement
  Q_TABLE               = "1" # SELECT operation
  Q_UPDATE              = "2" # INSERT/UPDATE operations
  Q_CREATE              = "3" # CREATE/DROP TABLE operations

  MONET_HEADER_OFFSET   = 2

  @@DEBUG               = false
 
  def initialize(connection, type_cast)
    @connection = connection
    
    # Structure containing the header+results set for a fired Q_TABLE query
    @Q_TABLE_instance = OpenStruct.new
    @Q_TABLE_instance.query = {}
    @Q_TABLE_instance.header = {}
    @Q_TABLE_instance.record_set = []
    @Q_TABLE_instance.index = 0 # Position of the last returned record
     
    # Number of entries cached in a record set
    @dataCacheSize = 10
    
    
    # The number of rows to retrieve after an 'export' operation.
    @dataOffset = 10
    
    # The position from which to start retrieving rows from a recordset 
    @dataStartIndex = 0
    
    # Convert SQL data types to Ruby
    @type_cast = type_cast
  end
  
  # Free memory used to store the record set
  def free()
    @connection = nil    
    @Q_TABLE_instance = nil
    @dataOffset = 10
    @dataStartIndex = 0

  end
  
  # Fire a query and return the server response
  def execute(q)    
    # Before firing a query, set the max reply length to one record
    command = format_command("reply_size 1")
    @connection.encode_message(command).each { |msg|
      @connection.socket.write(msg)
    }
    
    # server response to command
    is_final, chunk_size = @connection.recv_decode_hdr()
    if chunk_size > 0
      raise MonetDBCommandError
    end
  
    bc = 0
    @connection.encode_message(q).each { |msg|
      bc += @connection.socket.write(msg)
      if @@DEBUG
        print msg + " " + bc.to_s + " bytes transmitted\n"
      end
    }
      
    record_set = Array.new
    header = Array.new
    first_rows = Array.new
    
    # Fetch the row header and first recordset entry (requested by setting the reply size to 1)
    is_final, chunk_size = @connection.recv_decode_hdr()
    read_bytes = 0
    if is_final == true
      # process one line at a time
      while (read_bytes < chunk_size )
        row = @connection.socket.readline
        read_bytes += row.length
        
        if row != ""
          
          if row[0].chr == "!"
            raise MonetDBQueryError, row
          elsif row[0].chr == "%"
            # process header data
            header << row

          elsif row[0].chr == "&" 
             @Q_TABLE_instance.query = parse_header_query(row)
          else
            # process tuples, store the actual data in an array (each tuple is an array itself)
            first_rows << parse_tuples(row)
            
          end
        end
      end
    end
    
    # Store infromation regarding a table
    @Q_TABLE_instance.header = parse_header_table(header)
    first_rows.each do
      record_set << parse_tuples(row)
    end
      
    
    #reutrn the actual data
    is_final = false
    
    # initialize cursors variables
    row_count = @Q_TABLE_instance.query['rows'].to_i #total number of rows in table
    row_index = @dataStartIndex + 1 # current row to retrieve (NOTE: one row as already been retrieved due to setting reply_size to 1)
    row_offset = @dataOffset # number of rows to retrieve per each block
    
    if @Q_TABLE_instance.query['type'] == Q_TABLE and @Q_TABLE_instance.query['id'] != ""
      @connection.encode_message(format_command("export " + @Q_TABLE_instance.query['id'] + " " + row_index.to_s + " " +  row_offset.to_s)).each do |msg|
      #@connection.encode_message(format_command("export " + @Q_TABLE_instance.query['id'] + " " + ((block * @dataCacheSize) + blockOffset).to_s + " " +  @dataCacheSize.to_s)).each do |msg|
        @connection.socket.write(msg)
      end
    end
    
    # Retrieve the table data
    # Fetch the row header and first recordset entry (requested by setting the reply size to 1)
    #  is_final, chunk_size = @connection.recv_decode_hdr()
    #  data = @connection.socket.recv(chunk_size)
    #  puts data
    # for row in data do
    while row_index <= row_count
      
      if ( row_index % row_offset ) == 0
        if row_index + row_offset > row_count
          row_offset = row_count - row_index
          if @DEBUG
            puts "Offset: " + row_offset.to_s
            puts "Rows: " + row_count.to_s
            puts "IDX: " + row_index.to_s
          end
        else
          @dataOffset *= 10
          row_offset += @dataOffset
        end
  
        # puts "Offset: " + row_offset.to_s
  
        @connection.encode_message(format_command("export " + @Q_TABLE_instance.query['id'] + " " + row_index.to_s  + " " + row_offset.to_s)).each do |msg|
          @connection.socket.write(msg)
        end
      end
      
      # Process the records one line at a time, store them as string. Type conversion will be performed "on demand" by the user.
      row = @connection.socket.readline
      
      # puts "IDX: " + row_index.to_s + "   " + row
      if row != ""
        if row[MONET_HEADER_OFFSET] == nil
          
        elsif row[MONET_HEADER_OFFSET].chr == "%"
         # puts "HEADER"
         # puts row
         # process header data
         # header << row
          
        elsif row[MONET_HEADER_OFFSET].chr == "&" 
          # puts "HEADER"
          # puts row
          # @Q_TABLE_instance.query = parse_header_query(row[2...row.length])
        else 
          # process tuples, store the actual data in an array (each tuple is an array itself)
          record_set << parse_tuples(row[MONET_HEADER_OFFSET...row.length])
          row_index += 1
        end
      end
     #  puts  row_index
     # end
    end
    
   
        
    # Make the data immutable
    @Q_TABLE_instance.record_set = record_set.freeze
    @Q_TABLE_instance.freeze
  end
  
  # Returns the record set entries hashed by column name orderd by column position
   def fetch_all_hash()
     columns = {}
     @Q_TABLE_instance.header["columns_name"].each do |col_name|
       columns[col_name] = fetch_column_name(col_name)
     end

     return columns
   end

   def fetch_hash()
     index = @Q_TABLE_instance.index
     if index > @Q_TABLE_instance.query['rows'].to_i 
       return false
     else
       columns = {}
       @Q_TABLE_instance.header["columns_name"].each do |col_name|
         position = @Q_TABLE_instance.header["columns_order"].fetch(col_name)

         columns[col_name] = @Q_TABLE_instance.record_set[index][position]

       end
       @Q_TABLE_instance.index += 1
       return columns
     end
   end

   # Returns the values for the column 'field'
   def fetch_column_name(field="")
     position = @Q_TABLE_instance.header["columns_order"].fetch(field)

     col = Array.new
     # Scan the record set by row
     @Q_TABLE_instance.record_set.each do |row|
       col << row[position]
     end

     return col
   end


   def fetch()
     index = @Q_TABLE_instance.index
     if index > @Q_TABLE_instance.query['rows'].to_i 
       return false
     else
       @Q_TABLE_instance.index += 1
       return @Q_TABLE_instance.record_set[index]
     end
   end

   # Cursor method that retrieves all the records present in a table and stores them in a cache.
   def fetch_all()
     if @Q_TABLE_instance.query['type'] == Q_TABLE   
       @Q_TABLE_instance.index = @Q_TABLE_instance.query['rows'].to_i
       return @Q_TABLE_instance.record_set
     else
       raise MonetDBDataError, "There is no record set currently available"
     end

   end
  
   # Returns the number of rows in the record set
   def num_rows()
      return @Q_TABLE_instance.query['rows'].to_i
   end

   # Returns the number of fields in the record set
   def num_fields()
     return @Q_TABLE_instance.query['columns'].to_i
   end

   # Returns the (ordered) name of the columns in the record set
   def name_fields()
     return @Q_TABLE_instance.header['columns_name']
   end
  
  private
  
  # Parses the data returned by the server and stores the content of header and record set 
  # for a Q_TABLE query in two (immutable) arrays. The Q_TABLE instance is then reperesented
  # by the OpenStruct variable @Q_TABLE_instance with separate fields for 'header' and 'record_set'.
  #
  def parse_tuples(row)
    # remove trailing and ending "[ ]"
    row = row.gsub(/^\[\s+/,'')
    row = row.gsub(/\t\]\n$/,'')
    
    row = row.split(/\t/)
    
    processed_row = Array.new
    
    # index the field position
    position = 0
    while position < row.length
      field = row[position].gsub(/,$/, '')
      if @type_cast == true
        if @Q_TABLE_instance.header["columns_type"] != nil
          name = @Q_TABLE_instance.header["columns_name"][position]
          if @Q_TABLE_instance.header["columns_type"][name] != nil
            type = @Q_TABLE_instance.header["columns_type"].fetch(name)
          end
          
        #  field = self.type_cast(field, type)
        field = type_cast(field, type)
 
        end
      end
      
      processed_row << field
      position += 1
    end
    return processed_row
  end
  
  # Parses a query header and returns information about the query.
  def parse_header_query(row)
    type = row[1].chr
    if type == Q_TABLE
      # Performing a SELECT: store informations about the table size, query id, total number of records and returned.
      rows = row.split(' ')[2]
      columns = row.split(' ')[3]
      returned = row.split(' ')[4]
      id = row.split(' ')[1]
      
      return { "id" => id, "type" => type, "rows" => rows, "columns" => columns, "returned" => returned }.freeze 
    else
      return {"type" => type}.freeze
    end
  end
  
  # Parses a Q_TABLE header and returns information about the schema.
  def parse_header_table(header_t)
    if @Q_TABLE_instance.query["type"] == Q_TABLE
      if header_t != nil
        name_t = header_t[0].split(' ')[1].gsub(/,$/, '')
        name_cols = Array.new
      
        header_t[1].split('%')[1].gsub(/'^\%'/, '').split('#')[0].split(' ').each do |col|
          name_cols << col.gsub(/,$/, '')
        end
      
        type_cols = { }
        header_t[2].split('%')[1].gsub(/'^\%'/, '').split('#')[0].split(' ').each_with_index do |col, i|
          if  col.gsub(/,$/, '') != nil
            type_cols[ name_cols[i] ] = col.gsub(/,$/, '') 
          end
        end
      
        length_cols = { }
        header_t[3].split('%')[1].gsub(/'^\%'/, '').split('#')[0].split(' ').each_with_index do |col, i|
          length_cols[ name_cols[i] ] = col.gsub(/,$/, '')
        end
      
        columns_order = {}
        name_cols.each_with_index do |col, i|
          columns_order[col] = i
        end
      
        return {"table_name" => name_t, "columns_name" => name_cols, "columns_type" => type_cols, 
          "columns_length" => length_cols, "columns_order" => columns_order}.freeze
      end
    end
  end
  
 
  # Cursor method that retrieves the next set of records. Previously cached records are discarded.
  # - row: current position
  # - offset: number of rows to read
  def fetch_next(row, offset)
  end
  
  # Cursor method that returns the record preceding <i>row</i> from a cached record set.
  # - row: target position
  def fetch_prev(row)
  end
  
  # Cursor method that returns the record following <i>row</i> from a cached record set.
  # - row: target position
  def fetch_succ(row)
  end
  
  # Cursor method that returns the record at position <i>row</i> from a cached record set.
  # If 'row' is not specified, the method returns the next row in the record set
  # - row: target position. 
  # - type_conversion: cast the results type according to the schema specifications;
  def fetch_row(row = '-1', type_conversion = false)
    if row == '-1'
      row = @Q_TABLE_instance.index
    end
    @Q_TABLE_instance.index = row.to_i + 1
    return @Q_TABLE_instance.record_set[row.to_i]
  end
  
  # Cursor method that returns a range of records from a cached record set.
  # - from: beginning of the range
  # - to: end of the range
  def fetch_rows(from, to)
  end
  
  # Cursor method that returns the first record from a cached record set.
  def fetch_first()
  end
  
  # Cursor method that returns the last record from a cached record set.
  def fetch_last()
  end
  
  def debug_columns_order()
    @Q_TABLE_instance.header["columns_order"].each { |pos|
      puts pos
    }
  end
  
  def debug_columns_type()
    @Q_TABLE_instance.header["columns_type"].each { |pos|
      puts pos
    }
  end
  
  def debug_columns_length()
    @Q_TABLE_instance.header["columns_length"].each { |pos|
      puts pos
    }
  end
  
  # MonetDB - Ruby types mapping
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
  
  # Converts the stored Q_TABLE fields into the actual data type specified in the schema
  # 
  def type_cast(field = "", cast = nil)
    
    if field == "NULL"
      return nil
    elsif cast == "varchar"
      return field[0...255].gsub(/^"/,'').gsub(/"$/,'').gsub(/\\\"/, '"')
    elsif cast == "int"
      return BigDecimal.new(field, 32).to_i
    elsif cast == "smallint"
      return BigDecimal.new(field, 16).to_i
    elsif cast == "bigint"
      return BigDecimal.new(field, 64).to_i
    elsif cast == "numeric"
      return BigDecimal.new(field, 32).to_i
    elsif cast == "decimal"
      return BigDecimal.new(field, 32).to_i
    elsif cast == "text"
      return field.gsub(/^"/,'').gsub(/"$/,'').gsub(/\"/, '')
    elsif cast == "real"
      return Float.new(field)
    elsif cast == "double"
      return Float.new(field)
    elsif cast == "time"
      return Time.gm(field)
    elsif cast == "date"
      return Date.civi(field)
    elsif cast == "blob"
      return field.gsub(/^"/,'').gsub(/"$/,'')
    elsif cast == "boolean"
      if field == "0"
        return true
      else 
        return false
      end
    elsif cast == "bigint"
      return BigInteger(field)
    end
  end
  
  def monetdb2str(field)
  end
  
end