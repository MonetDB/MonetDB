# unit test suite for monetdb.
# connects to the 'ruby' database and runs test on the server capabilities and SQL language.
# Create first a database with the command:
# $ monetdb create ruby
# $ monetdb run ruby
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
  
  def setup
    @db = MonetDB.new
    @db.connect(user = "monetdb", passwd = "monetdb", lang = "sql", host="localhost", port = 50000, db_name = "ruby", auth_type = "SHA1")
    
  end

  def teardown
    @db.close
  end
  
  # CREATE TABLE test
  def test_create_table(table='test_ruby', cols = [ "First_Name varchar(255)", "Second_Name varchar(255)"])
    cols_def =  ""
    cols.each do |c| cols_def += c + ',' end
    cols_def = cols_def.chop # remove last ',' character
    
    res = @db.query('CREATE TABLE ' + table + ' (' + cols_def + ')') 
  end
  
  # check the existance of a table
  def test_table_exists(table='test_ruby')
    test_create_table(table)
    
    res = @db.query('select * from ' + table + ' where 1=0')
    assert_equal([], res.fetch_all)
    
    test_drop_table(table)
  end
  
  # perform an inserstion of 'data' into 'table' and check the resulting 
  # length
  def test_data_integrity(table = 'test_ruby', data = ["Gabriele", 'MODENA'])
        
    values = ""
    data.each do |d| values += '\'' + d.to_s + '\'' + ',' end
    values = values.chop # remove last ',' character
    
    insert = 'INSERT INTO ' + table +  ' VALUES ('  + values + ' )' 
    
    @db.query(insert)
    
    res = @db.query('SELECT * FROM ' + table)
    rows = res.fetch_all
    
    assert_equal(res.num_rows, rows.size)
  end
  
  # DROP TABLE TEST
  def test_drop_table(table='test_ruby')
    res = @db.query('DROP TABLE ' + table)
  end
  
  
  # test TRANSACTION, COMMIT, ROLLBACK and SAVEPOINT in auto_commit=off mode
  # simulates nested transaction via savepoints
  def test_transactions(table = "test_transactions",  columndefs = ['col1 INT', 'col2 VARCHAR(255)'])
    test_drop_table(table)
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
     
    test_drop_table(table)
     
    assert_equal(rows_committed, rows_rolled_back)
  end
  
  # tests on datatypes conversion
  def test_char
     # insert a char, retrieve it and check for consistancy
      char = 'a'
            
      table  = "test_monetdb_char"

      @db.query("CREATE TABLE " + table + " (char_field CHAR(1)) ")

      @db.query("INSERT INTO " + table  + " VALUES ( '" + char  +"' ) ")

      res = @db.query("SELECT char_field FROM " + table + " where char_field = '" + char +"'")
      stored_string = res.fetch_hash
      
      test_drop_table(table)
      
      assert_equal(char, stored_string['char_field'])
  end
  
  def test_smallint      
      original_num = rand(-32768, 32767)
      num = original_num.to_s
            
      table  = "test_monetdb_smallint"

      @db.query("CREATE TABLE " + table + " (int_field SMALLINT) ")

      @db.query("INSERT INTO " + table  + " VALUES ('" + num  +"') ")

      res = @db.query("SELECT int_field FROM " + table + " where int_field = '" + num +"'")

      stored_string = res.fetch_hash
      
      test_drop_table(table)
      
        
      assert_equal(num.to_i, stored_string['int_field'].getInt)
    
  end
 
  def test_int
      #original_num = rand((−2 ** 31), (2 ** 31 -1))
      original_num = rand((2 ** 31 -1))
      num = original_num.to_s

      table  = "test_monetdb_int"

      @db.query("CREATE TABLE " + table + " (int_field INT) ")
      @db.query("INSERT INTO " + table  + " VALUES ('" + num  +"') ")

      res = @db.query("SELECT int_field FROM " + table + " where int_field = '" + num +"'")

      stored_string = res.fetch_hash

      test_drop_table(table)

      assert_equal(original_num, stored_string['int_field'].getInt)
  end

  def test_bigint
      #original_num = rand((−2 ** 31), (2 ** 31 -1))
      original_num = rand((2 ** 63 -1))
      num = original_num.to_s

      table  = "test_monetdb_bigint"
     
      @db.query("CREATE TABLE " + table + " (int_field BIGINT) ")

      @db.query("INSERT INTO " + table  + " VALUES ('" + num  +"') ")

      res = @db.query("SELECT int_field FROM " + table + " where int_field = '" + num +"'")

      stored_string = res.fetch_hash

      test_drop_table(table)

      assert_equal(original_num, stored_string['int_field'].getInt)
   end

   def test_real
     #original_num = rand((−2 ** 31), (2 ** 31 -1))
     #original_num = 3.402 ** 38
     #original_num = rand(1.6065851e+20)
     original_num = 1.6065851e+20
    
     num = original_num.to_s

     table  = "test_monetdb_real"
       
     @db.query("CREATE TABLE " + table + " (float_field REAL) ")
     @db.query("INSERT INTO " + table  + " VALUES ('" + num  +"') ")

     res = @db.query("SELECT float_field FROM " + table + " where float_field = '" + num +"'")

     stored_string = res.fetch_hash

     test_drop_table(table)

     assert_equal(original_num, stored_string['float_field'].getFloat)  
   end

   def test_double
     #original_num = rand((−2 ** 31), (2 ** 31 -1))
     #original_num = 3.402 ** 38
     #original_num = rand(1.6065851e+22)
     original_num = 1.6065851e+22
     
     num = original_num.to_s

     table  = "test_monetdb_double"

     @db.query("CREATE TABLE " + table + " (float_field DOUBLE) ")
     @db.query("INSERT INTO " + table  + " VALUES ('" + num  +"') ")

     res = @db.query("SELECT float_field FROM " + table + " where float_field = '" + num +"'")

     stored_string = res.fetch_hash

     test_drop_table(table)

     assert_equal(original_num, stored_string['float_field'].getFloat)  
   end

   def test_boolean
     original_bool = false
     bool = original_bool.to_s
     
     table  = "test_monetdb_boolean"
     
     @db.query("CREATE TABLE " + table + " (bool_field BOOLEAN) ")
     @db.query("INSERT INTO " + table  + " VALUES ('" + bool  +"') ")
     

     res = @db.query("SELECT bool_field FROM " + table + " where bool_field = '" + bool +"'")

     stored_string = res.fetch_hash

     test_drop_table(table)

     assert_equal(original_bool, stored_string['bool_field'].getBool)
   end
   
   def test_datetime
     table = "test_monetdb_datetime"
     timestamp = "2009-07-01 15:34:33"
     
     date = timestamp.split(' ')[0].split('-')
     time = timestamp.split(' ')[1].split(':')

     dt = Time.gm(date[0], date[1], date[2], time[0], time[1], time[2])     
     
     @db.query("CREATE TABLE " + table + " (dt_field TIMESTAMP) ")
     @db.query("INSERT INTO " + table  + " VALUES ('" + timestamp  +"') ")

     res = @db.query("SELECT dt_field FROM " + table + " where dt_field = '" + timestamp +"'")

     stored_string = res.fetch_hash

     test_drop_table(table)

     assert_equal(dt, stored_string['dt_field'].getDateTime)     
   end
   
   def test_date
     table = "test_monetdb_date"
     timestamp = "2009-07-01"
    
     @db.query("CREATE TABLE " + table + " (dt_field DATE) ")
     @db.query("INSERT INTO " + table  + " VALUES ('" + timestamp  +"') ")

     res = @db.query("SELECT dt_field FROM " + table + " where dt_field = '" + timestamp +"'")

     stored_string = res.fetch_hash

     test_drop_table(table)

     assert_equal(timestamp, stored_string['dt_field'].getDate)     
   end
    
   def test_time
     table = "test_monetdb_time"
     timestamp = "15:34:33"
     
     @db.query("CREATE TABLE " + table + " (dt_field TIME) ")
     @db.query("INSERT INTO " + table  + " VALUES ('" + timestamp  +"') ")

     res = @db.query("SELECT dt_field FROM " + table + " where dt_field = '" + timestamp +"'")

     stored_string = res.fetch_hash

     test_drop_table(table)

     assert_equal(timestamp, stored_string['dt_field'].getTime)     
   end
   
  def test_blob
    blob = '0000000a146f777bb46b8fbd46ad503a54629c5' * 1000
        
    table = "monetdb_test_blob"
     
    test_create_table(table)
     
    @db.query("CREATE TABLE " + table + " (blob_field BLOB) ")
    @db.query("INSERT INTO " + table  + " VALUES ('#{blob}') ")
     
    res = @db.query("SELECT blob_field FROM " + table + " where blob_field = '#{blob}'")

    stored_string = res.fetch_hash
     
    test_drop_table(table) 
    
    assert_equal(blob_hex, stored_string['blob_field'].getBlob)
  end 
   
  def test_utf8
    # insert an utf8 string, retrieve it and check for consistancy
#    string = "€¿®µ¶¹€¿®µ¶¹€¿®µ¶¹" * 1000
    string = ""
    table  = "test_monetdb_utf8"
    coldefs = ["utf8_field varchar(100000)"] 
    
    test_create_table(table, coldefs)

    @db.query("INSERT INTO " + table  + " VALUES ( '" + string  +"' ) ")
        
    res = @db.query("SELECT utf8_field FROM " + table + " where utf8_field = '" + string + "' ")
    stored_string = res.fetch_hash
  
    test_drop_table(table)
  
    assert_equal(string, stored_string['utf8_field'])
  end   
end
