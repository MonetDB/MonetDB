# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2008-2015 MonetDB B.V.

# Models a MonetDB RecordSet
require 'time'
require 'ostruct'

require "bigdecimal"

require 'MonetDBConnection'

require 'logger'

class MonetDBData 
  @@DEBUG               = false
 
  def initialize(connection)
    @connection = connection
    @lang = @connection.lang

    # Structure containing the header+results set for a fired Q_TABLE query     
    @header = []
    @query  = {}
    
    @record_set = []
    @index = 0 # Position of the last returned record
    
    
    @row_count = 0
    @row_offset = 10
    @row_index = Integer(MonetDBConnection::REPLY_SIZE)
  end
  
  # Fire a query and return the server response
  def execute(q)
   # fire a query and get ready to receive the data      
    @connection.send(format_query(q))
    data = @connection.receive
    
    return if data == nil
    
    record_set = "" # temporarly store retrieved rows
    record_set = receive_record_set(data)

    if (@lang == MonetDBConnection::LANG_SQL)
      rows = receive_record_set(data)
      # the fired query is a SELECT; store and return the whole record set
      if @action == MonetDBConnection::Q_TABLE
        @header = parse_header_table(@header)
        @header.freeze
      
        if @row_index.to_i < @row_count.to_i
          block_rows = ""
          while next_block
            data = @connection.receive
            block_rows += receive_record_set(data)
          end
          record_set += block_rows
        end
      end

      # ruby string management seems to not properly understand the MSG_PROMPT escape character.
      # In order to avoid data loss the @record_set array is built once that all tuples have been retrieved
      @record_set = record_set.split("\t]\n")
      
      if @record_set.length != @query['rows'].to_i
        raise MonetDBQueryError, "Warning: Query #{@query['id']} declared to result in #{@query['rows']} but #{@record_set.length} returned instead"
      end
    end
    @record_set.freeze  
  end
  
  # Free memory used to store the record set
  def free()
    @connection = nil    
    
    @header = []
    @query = {}

    @record_set = []
    @index = 0 # Position of the last returned record


    @row_index = Integer(MonetDBConnection::REPLY_SIZE)
    @row_count = 0
    @row_offset = 10
    
  end
  
  # Returns the record set entries hashed by column name orderd by column position
  def fetch_all_hash()
     columns = {}
     @header["columns_name"].each do |col_name|
       columns[col_name] = fetch_column_name(col_name)
     end

     return columns
   end

  def fetch_hash()
     if @index >= @query['rows'].to_i 
       return false
     else
       columns = {}
       @header["columns_name"].each do |col_name|
         position = @header["columns_order"].fetch(col_name)
         row = parse_tuple(@record_set[@index])
         columns[col_name] = row[position]
       end
       @index += 1
       return columns
     end
   end

  # Returns the values for the column 'field'
  def fetch_column_name(field="")
     position = @header["columns_order"].fetch(field)

     col = Array.new
     # Scan the record set by row
     @record_set.each do |row|
       col << parse_tuple(row)[position]
     end

     return col
   end


  def fetch()
    result = ""

     if @index >= @query['rows'].to_i
       result = false
     else
       result = parse_tuple(@record_set[@index])
       @index += 1
     end

     return result
   end

  # Cursor method that retrieves all the records present in a table and stores them in a cache.
  def fetch_all()
     if @query['type'] == MonetDBConnection::Q_TABLE 
        rows = Array.new
        @record_set.each do |row| 
           rows << parse_tuple(row)
        end
        @index = Integer(rows.length)
      else
        raise MonetDBDataError, "There is no record set currently available"
      end

      return rows
   end
  
  # Returns the number of rows in the record set
  def num_rows()
      return @query['rows'].to_i
   end

  # Returns the number of fields in the record set
  def num_fields()
     return @query['columns'].to_i
   end

  # Returns the (ordered) name of the columns in the record set
  def name_fields()
    return @header['columns_name']
  end
  
  # Returns the (ordered) name of the columns in the record set
  def type_fields
    return @header['columns_type']
  end
  
  private
  
  # store block of data, parse it and store it.
  def receive_record_set(response)
    rows = ""
    response.each_line do |row|   
      if row[0].chr == MonetDBConnection::MSG_QUERY      
        if row[1].chr == MonetDBConnection::Q_TABLE
          @action = MonetDBConnection::Q_TABLE
          @query = parse_header_query(row)
          @query.freeze
          @row_count = @query['rows'].to_i #total number of rows in table            
        elsif row[1].chr == MonetDBConnection::Q_BLOCK
          # strip the block header from data
          @action = MonetDBConnection::Q_BLOCK
          @block = parse_header_query(row)          
        elsif row[1].chr == MonetDBConnection::Q_TRANSACTION
          @action = MonetDBConnection::Q_TRANSACTION
        elsif row[1].chr == MonetDBConnection::Q_CREATE
          @action = MonetDBConnection::Q_CREATE
        end
      elsif row[0].chr == MonetDBConnection::MSG_INFO
        raise MonetDBQueryError, row
      elsif row[0].chr == MonetDBConnection::MSG_SCHEMA_HEADER
        # process header data
        @header << row
      elsif row[0].chr == MonetDBConnection::MSG_TUPLE
        rows += row
      elsif row[0] == MonetDBConnection::MSG_PROMPT
        return rows
      end
    end 
    return rows # return an array of unparsed tuples
  end
  
  def next_block
    if @row_index == @row_count
      return false
    else
      # The increment step is small to better deal with ruby socket's performance.
      # For larger values of the step performance drop;
      #
      @row_offset = [@row_offset, (@row_count - @row_index)].min
      
      # export offset amount
      @connection.set_export(@query['id'], @row_index.to_s, @row_offset.to_s)    
      @row_index += @row_offset    
      @row_offset += 1
    end    
      return true
      
  end
  
  # Formats a query <i>string</i> so that it can be parsed by the server
  def format_query(q)
    if @lang == MonetDBConnection::LANG_SQL
        return "s" + q + "\n;"
    else
      raise LanguageNotSupported, @lang
    end
  end
  
  # Parses the data returned by the server and stores the content of header and record set 
  # for a Q_TABLE query in two (immutable) arrays. The Q_TABLE instance is then reperesented
  # by the OpenStruct variable @Q_TABLE_instance with separate fields for 'header' and 'record_set'.
  #
  #def parse_tuples(record_set)
  #  processed_record_set = Array.new
    
  #  record_set.split("]\n").each do |row|
      
      # remove trailing and ending "[ ]"
  #    row = row.gsub(/^\[\s+/,'')
  #    row = row.gsub(/\t\]\n$/,'')
    
  #    row = row.split(/\t/)
    
  #    processed_row = Array.new
    
      # index the field position
  #    position = 0
  #    while position < row.length
  #      field = row[position].gsub(/,$/, '')
        
  #      if @type_cast == true
  #        if @header["columns_type"] != nil
  #          name = @header["columns_name"][position]
  #          if @header["columns_type"][name] != nil
  #            type = @header["columns_type"].fetch(name)
  #          end

          #  field = self.type_cast(field, type)
  #        field = type_cast(field, type)

  #        end
  #      end
      
  #      processed_row << field.gsub(/\\/, '').gsub(/^"/,'').gsub(/"$/,'').gsub(/\"/, '')
  #      position += 1
  #    end
  #    processed_record_set << processed_row
  #  end
  #  return processed_record_set
  #end
  
  # parse one tuple as returned from the server
  def parse_tuple(tuple)
    fields = Array.new
    # remove trailing  "["
    tuple = tuple.gsub(/^\[\s+/,'')
    
    tuple.split(/,\t/).each do |f|
      fields << f.gsub(/\\/, '').gsub(/^"/,'').gsub(/"$/,'').gsub(/\"/, '')
    end
    
    return fields.freeze
  end
  
  # Parses a query header and returns information about the query.
  def parse_header_query(row)
    type = row[1].chr
    if type == MonetDBConnection::Q_TABLE
      # Performing a SELECT: store informations about the table size, query id, total number of records and returned.
      id = row.split(' ')[1]
      rows = row.split(' ')[2]
      columns = row.split(' ')[3]
      returned = row.split(' ')[4]
      
      header = { "id" => id, "type" => type, "rows" => rows, "columns" => columns, "returned" => returned }
    elsif  type == MonetDBConnection::Q_BLOCK
      # processing block header
    
      id = row.split(' ')[1]
      columns = row.split(' ')[2]
      remains = row.split(' ')[3]
      offset = row.split(' ')[4]
      
      header = { "id" => id, "type" => type, "remains" => remains, "columns" => columns, "offset" => offset }
    else
      header = {"type" => type}
    end
    
    return header.freeze
  end
  
  # Parses a Q_TABLE header and returns information about the schema.
  def parse_header_table(header_t)
    if @query["type"] == MonetDBConnection::Q_TABLE
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
end

# Overload the class string to convert monetdb to ruby types.
class String
  def getInt
    self.to_i
  end
  
  def getFloat
    self.to_f
  end
  
  def getString
    #data = self.reverse
    # parse the string starting from the end; 
    #escape = false
    #position = 0
    #for i in data
    #  if i == '\\' and escape == true
    #      if data[position+1] == '\\' 
    #      data[position+1] = ''
    #      escape = true
    #    else
    #      escape = false
    #    end
    #  end
    #  position += 1
    #end
    #data.reverse
    self.gsub(/^"/,'').gsub(/"$/,'')
  end
  
  def getBlob
    # first strip trailing and leading " characters 
    self.gsub(/^"/,'').gsub(/"$/,'')
    
    # convert from HEX to the origianl binary data.
    blob = ""
    self.scan(/../) { |tuple| blob += tuple.hex.chr }
    return blob
  end
  
  # ruby currently supports only time + date frommatted timestamps;
  # treat TIME and DATE as strings.
  def getTime
    # HH:MM:SS
    self.gsub(/^"/,'').gsub(/"$/,'')
  end
  
  def getDate
    self.gsub(/^"/,'').gsub(/"$/,'')
  end
  
  def getDateTime
    #YYYY-MM-DD HH:MM:SS
    date = self.split(' ')[0].split('-')
    time = self.split(' ')[1].split(':')
    
    Time.gm(date[0], date[1], date[2], time[0], time[1], time[2])
  end
  
  def getChar
    # ruby < 1.9 does not have a Char datatype
    begin
      c = self.ord
    rescue
      c = self
    end
    
    return c 
  end
  
  def getBool
      if ['1', 'y', 't', 'true'].include?(self)
        return true
      elsif ['0','n','f', 'false'].include?(self)
        return false
      else 
        # unknown
        return nil
      end
  end
  
  def getNull
    if self.upcase == 'NONE'
      return nil
    else
      raise "Unknown value"
    end
  end
end
