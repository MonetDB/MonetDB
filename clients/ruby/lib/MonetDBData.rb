# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

# Models a MonetDB RecordSet
require 'time'
require 'date'
require 'ostruct'

require "bigdecimal"

require_relative 'MonetDBConnection'

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
  
  # Returns the record set entries hashed by column name orderd by column position
  def fetch_all_as_column_hash
     columns = {}
     @header["columns_name"].each do |col_name|
       columns[col_name] = fetch_by_column_name(col_name)
     end

     return columns
   end

  # returns a record hash (i.e: { id: 1, name: "John Doe", age: 42 } )
  def fetch_hash
    return false if @index >= @query['rows'].to_i 

    record_hash = record_hash(parse_tuple(@record_set[@index]))
    @index += 1
    return record_hash
  end

  # loops through all the hashes of the records and yields them to a given block
  def each_record_as_hash
    @record_set.each do |record| 
      parsed_record = parse_tuple(record)
      yield(record_hash(parsed_record))
    end
  end

  # Returns the values for the column 'field'
  def fetch_by_column_name(column_name="")
    position = @header["columns_order"].fetch(column_name)

    column_values = []
    @record_set.each do |row|
      column_values << parse_tuple(row)[position]
    end

    return column_values
  end

  # fetches a single record, updates the iterator index
  def fetch
    return false if @index >= @query['rows'].to_i

    result = parse_tuple(@record_set[@index])
    @index += 1

    return result
  end

  # resets the internal iterator index used by fetch and fetch_hash
  def reset_index
    @index = 0
  end

  # loops through all records and yields to a given block paramter
  def each_record
    raise MonetDBDataError, "There is no record set currently available" unless @query['type'] == MonetDBConnection::Q_TABLE 
    @record_set.each { |record| yield(parse_tuple(record)) }
  end

  # Cursor method that returns all the records
  def fetch_all
    result = []
    each_record do |record|
      result.push(record)
    end
    return result
  end
  
  # Returns the number of rows in the record set
  def num_rows
      return @query['rows'].to_i
   end

  # Returns the number of fields in the record set
  def num_fields
     return @query['columns'].to_i
   end

  # Returns the (ordered) name of the columns in the record set
  def name_fields
    return @header['columns_name']
  end
  
  # Returns the (ordered) name of the columns in the record set
  def type_fields
    return @header['columns_type']
  end
  
  # ===================
          private
  # ===================
  
  # store block of data, parse it and store it.
  def receive_record_set(response)
    rows = ""
    response.each_line do |row|
      case row[0]
      when MonetDBConnection::MSG_QUERY then parse_query(row)
      when MonetDBConnection::MSG_INFO then raise MonetDBQueryError, row
      when MonetDBConnection::MSG_SCHEMA_HEADER then @header << row
      when MonetDBConnection::MSG_TUPLE then rows += row
      when MonetDBConnection::MSG_PROMPT then return rows
      end
    end
    return rows # return an array of unparsed tuples
  end

  def parse_query(row)
    case row[1]
      when MonetDBConnection::Q_TABLE
        @action = MonetDBConnection::Q_TABLE
        @query = parse_header_query(row)
        @query.freeze
        @row_count = @query['rows'].to_i #total number of rows in table        
      when MonetDBConnection::Q_BLOCK
        @action = MonetDBConnection::Q_BLOCK # strip the block header from data
        @block = parse_header_query(row)     
      when MonetDBConnection::Q_TRANSACTION
        @action = MonetDBConnection::Q_TRANSACTION
      when MonetDBConnection::Q_CREATE
        @action = MonetDBConnection::Q_CREATE
    end
  end
  
  def record_hash(record)
    result = {}

    @header["columns_name"].each do |column_name|
       position = @header["columns_order"].fetch(column_name)
       result[column_name] = record[position]
     end

    return result
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
  
  # parse one tuple as returned from the server
  def parse_tuple(tuple)
    fields = []
    # remove trailing  "["
    tuple = tuple.gsub(/^\[\s+/,'')
    tuple.split(/,\t/).each_with_index do |field, index|
      field_value = convert_type(field, index)
      fields << field_value
    end
    
    return fields.freeze
  end

  # converts the given value the correct type
  def convert_type(value, index)
    return nil if "NULL" == value.upcase
    return case type_fields.values[index]
        when "int", "tinyint", "smallint", "bigint", "hugeint" then value.to_i
        when "double", "real", "decimal" then value.to_f
        when "boolean" then value.downcase == true
        when "date" then Date.parse(value)
        when "time" then Time.parse(value, Time.new("2000-01-01"))
        when "timestamp" then DateTime.parse(value)
        when "timestamptz" then DateTime.parse(value)
        else value.gsub(/\\/, '').gsub(/^"/,'').gsub(/"$/,'').gsub(/\"/, '')
      end
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
