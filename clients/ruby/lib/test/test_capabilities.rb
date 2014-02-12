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
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

# unit test suite for monetdb.
# connects to the 'ruby_test' database and runs test on the server capabilities and SQL language.
# Create first a database with the command:
# $ monetdb create ruby_test
# $ monetdb start ruby_start
#
# Tests examples have been taken from the python and java internfaces and mysql driver.

require 'MonetDB'
require 'test/unit'

require 'time'
require 'date'


class TC_MonetDBCapabilities < Test::Unit::TestCase
  # ruby rand function does not support MIN..MAX bounds.
  # This alias adds that feature.
  alias original_rand rand
  def rand(arg1=nil, arg2=nil) 
    if !arg1.kind_of?(Enumerable) && arg2 == nil 
      original_rand(arg1)
    elsif arg1.kind_of? Enumerable
      as_array = arg1.to_a
      as_array[original_rand(as_array.length)]
    elsif arg1 != nil
      arg1 + original_rand(arg2)
    end
  end
  
  # check the existance of a table
  def table_exists?(table='test_ruby')    
    begin
      res = @db.query("select * from #{table} where 1=0")
      return true
    rescue
      return false
    end
  end
  
  def drop_table(table='test_ruby')
    res = @db.query("DROP TABLE #{table}")
  end
  
  def setup
    @db = MonetDB.new
    @db.connect(user = "monetdb", passwd = "monetdb", lang = "sql", host="localhost", port = 50000, db_name = "ruby_test", auth_type = "SHA1")
    
  end

  def teardown
    @db.close
  end
  
  # CREATE TABLE test
  def test_create_table(table='test_ruby', cols = [ "First_Name varchar(255)", "Second_Name varchar(255)"])
    
    if table_exists?(table)
      drop_table(table)
    end
    
    colsdef =  ""
    cols.each do |c| colsdef += c + ',' end
      
    colsdef = colsdef.chop # remove last ',' character
    
    
    res = @db.query('CREATE TABLE ' + table + ' (' + colsdef + ')') 
  end
  
  
  # perform an inserstion of 'data' into 'table' and check the resulting 
  # length
  def test_data_integrity(table='test_ruby', data=["Gabriele", "MODENA"])
    test_create_table
        
    values = ""
    data.each do |d| values += '\'' + d.to_s + '\'' + ',' end
    values = values.chop # remove last ',' character
    
    insert = 'INSERT INTO ' + table +  ' VALUES ('  + values + ' )' 
    
    @db.query(insert)
    
    res = @db.query("SELECT * FROM #{table}")
    rows = res.fetch_all
    
    assert_equal(res.num_rows, rows.size)
  end
  
  # test TRANSACTION, COMMIT, ROLLBACK and SAVEPOINT in auto_commit=off mode
  def test_transactions(table="test_monetdb_transactions",  columndefs=['col1 INT', 'col2 VARCHAR(255)'])
    test_create_table(table, columndefs)
     
    data = [1, 'aa'] 
    values = ""
     
    data.each do |d| values += '\'' + d.to_s + '\'' + ',' end
    values = values.chop # remove last ',' character 
     
    insert = "INSERT INTO " + table + " VALUES " + " ( " + values +  " )"
     
    @db.query('START TRANSACTION')
    @db.auto_commit(flag=false) # if @db.auto_commit?
    @db.query(insert)

    @db.query("COMMIT")     
     
    res = @db.query('SELECT * FROM ' + table)
    rows_committed = res.fetch_all
    res.free
     
    # create a save point
    @db.save
    @db.query("SAVEPOINT #{@db.transactions} ;")
     
    @db.query(insert)
     
    # rollback to savepoint
    @db.query("ROLLBACK TO SAVEPOINT #{@db.transactions};")
    @db.release
     
    res = @db.query('SELECT * FROM ' + table)
    rows_rolled_back = res.fetch_all
    res.free
          
    assert_equal(rows_committed, rows_rolled_back)
    
    # restore autocommit for remaining tests
    @db.auto_commit(flag=true)    
  end
  
  # tests on datatypes conversion
  def test_char(table="test_monetdb_char", coldefs=["char_field CHAR(1)"])
      test_create_table(table, coldefs)
      char = 'a'

      @db.query("INSERT INTO " + table  + " VALUES ( '" + char  +"' ) ")

      res = @db.query("SELECT char_field FROM " + table + " where char_field = '" + char +"'")
      stored_string = res.fetch_hash
            
      assert_equal(char, stored_string['char_field'])      
  end
  
  def test_smallint(table="test_monetdb_smallint", coldefs=["int_field SMALLINT"])   
      test_create_table(table, coldefs)
       
      original_num = rand(-32768, 32767)
      num = original_num.to_s
            
      @db.query("INSERT INTO " + table  + " VALUES ('" + num  +"') ")

      res = @db.query("SELECT int_field FROM " + table + " where int_field = '" + num +"'")

      stored_string = res.fetch_hash
      
      assert_equal(num.to_i, stored_string['int_field'].getInt)      
  end
 
  def test_int(table="test_monetdb_int", coldefs=["int_field INT"])
      test_create_table(table, coldefs)
      
      original_num = rand((2 ** 31 -1))
      num = original_num.to_s

      @db.query("INSERT INTO " + table  + " VALUES ('" + num  +"') ")

      res = @db.query("SELECT int_field FROM " + table + " where int_field = '" + num +"'")

      stored_string = res.fetch_hash
      
      assert_equal(original_num, stored_string['int_field'].getInt)      
  end

  def test_bigint(table="test_monetdb_bigint", coldefs=["int_field BIGINT"])
      test_create_table(table, coldefs)
      
      original_num = rand((2 ** 63 -1))
      num = original_num.to_s
     
      @db.query("INSERT INTO " + table  + " VALUES ('" + num  +"') ")

      res = @db.query("SELECT int_field FROM " + table + " where int_field = '" + num +"'")

      stored_string = res.fetch_hash

      assert_equal(original_num, stored_string['int_field'].getInt)      
   end

   def test_real(table="test_monetdb_real", coldefs=["float_field REAL"])
     test_create_table(table, coldefs)
     
     original_num = 1.6065851e+20    
     num = original_num.to_s

     @db.query("INSERT INTO " + table  + " VALUES ('" + num  +"') ")

     res = @db.query("SELECT float_field FROM " + table + " where float_field = '" + num +"'")

     stored_string = res.fetch_hash

     assert_equal(original_num, stored_string['float_field'].getFloat)       
   end

   def test_double(table="test_monetdb_double", coldefs=["float_field DOUBLE"])
     test_create_table(table, coldefs)
     
     original_num = 1.6065851e+22     
     num = original_num.to_s

     @db.query("INSERT INTO " + table  + " VALUES ('" + num  +"') ")

     res = @db.query("SELECT float_field FROM " + table + " where float_field = '" + num +"'")

     stored_string = res.fetch_hash

     assert_equal(original_num, stored_string['float_field'].getFloat)       
   end

   def test_boolean(table="test_monetdb_boolean", coldefs=["bool_field BOOLEAN"] )
     test_create_table(table, coldefs)
     
     original_bool = false
     bool = original_bool.to_s
          
     @db.query("INSERT INTO " + table  + " VALUES ('" + bool  +"') ")

     res = @db.query("SELECT bool_field FROM " + table + " where bool_field = #{bool}")
     stored_string = res.fetch_hash
     assert_equal(original_bool, stored_string['bool_field'].getBool)     
   end
   
   def test_datetime(table="test_monetdb_datetime", coldefs=["dt_field TIMESTAMP"])
     test_create_table(table, coldefs)
     
     timestamp = "2009-07-01 15:34:33"
     
     date = timestamp.split(' ')[0].split('-')
     time = timestamp.split(' ')[1].split(':')

     dt = Time.gm(date[0], date[1], date[2], time[0], time[1], time[2])     
     
     @db.query("INSERT INTO " + table  + " VALUES ('" + timestamp  +"') ")

     res = @db.query("SELECT dt_field FROM " + table + " where dt_field = '" + timestamp +"'")
     stored_string = res.fetch_hash
     assert_equal(dt, stored_string['dt_field'].getDateTime)      
   end
   
   def test_date(table="test_monetdb_date", coldefs=["dt_field DATE"])
     test_create_table(table, coldefs)
     
     timestamp = "2009-07-01"
    
     @db.query("INSERT INTO " + table  + " VALUES ('" + timestamp  +"') ")

     res = @db.query("SELECT dt_field FROM " + table + " where dt_field = '" + timestamp +"'")
     stored_string = res.fetch_hash
     assert_equal(timestamp, stored_string['dt_field'].getDate)          
   end
    
   def test_time(table="test_monetdb_time", coldefs=["dt_field TIME"])
     test_create_table(table, coldefs)
     
     timestamp = "15:34:33"
     
     @db.query("INSERT INTO " + table  + " VALUES ('" + timestamp  +"') ")

     res = @db.query("SELECT dt_field FROM " + table + " where dt_field = '" + timestamp +"'")
     stored_string = res.fetch_hash
     assert_equal(timestamp, stored_string['dt_field'].getTime)     
   end
   
  def test_blob(table="test_monetdb_blob",  coldefs = ["blob_field BLOB"])
    test_create_table(table, coldefs) 
    
    blob = '0000000A146F777BB46B8FBD46AD503A54629C51'

    @db.query("INSERT INTO " + table  + " VALUES ('" + blob + "') ")
     
    res = @db.query("SELECT blob_field FROM " + table + " where blob_field = '#{blob}'")

    stored_string = res.fetch_hash
    assert_equal(blob, stored_string['blob_field'])    
  end 
   
  def test_utf8(table="test_monetdb_utf8", coldefs=["utf8_field varchar(100000)"])
    test_create_table(table, coldefs)
    
    utf8_string = "€¿®µ¶¹€¿®µ¶¹€¿®µ¶¹"
    
    @db.query("INSERT INTO " + table  + " VALUES ( '#{utf8_string}' ) ")
        
    res = @db.query("SELECT utf8_field FROM #{table} where utf8_field = '#{utf8_string}' ")
    stored_string = res.fetch_hash
  
    assert_equal(utf8_string, stored_string['utf8_field'])    
  end   
  
  # test MonetDB::conn() named parameters connection method.
  def test_conn_with_named_parameters    
    db = MonetDB.new()
        
    db.conn({ :user => "monetdb", :passwd => "monetdb", :port => 50000, :host => "localhost", :database => "ruby_test"})
    assert_equal(true, db.is_connected?)
    db.close
  end
  
end
