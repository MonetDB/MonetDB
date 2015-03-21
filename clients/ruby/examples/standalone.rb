# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2008-2015 MonetDB B.V.

require 'rubygems'
require 'MonetDB'

db = MonetDB.new

db.connect(user = "monetdb", passwd = "monetdb", lang = "sql", host="127.0.0.1", port = 50000, db_name = "test", auth_type = "SHA1")

# set type_cast=true to enable MonetDB to Ruby type mapping
res = db.query("select * from tables");

puts "Number of rows returned: " + res.num_rows.to_s
puts "Number of fields: " + res.num_fields.to_s


# Get the columns' name
#col_names = res.name_fields

# Get the columns' type
#col_types = res.type_fields

###### Fetch all rows and store them
#puts res.fetch_all


# Iterate over the record set and retrieve on row at a time
while row = res.fetch do
  printf "%s \n", row
end


###### Get all records and hash them by column name
#row = res.fetch_all_hash()

#puts col_names[0] + "\t\t" + col_names[1]
#0.upto(res.num_rows) { |i|
#  puts row['id'][i] + "\t\t" + row['name'][i] 
#}


###### Iterator over columns (on cell at a time), convert the "id" field to a ruby integer value.  

#while row = res.fetch_hash do
#  printf "%s, %i\n", row["name"], row["id"].getInt
#end

###### Transactions

db.query("START TRANSACTION")
db.auto_commit(false)
# create a savepoint
db.save
db.query("SAVEPOINT #{db.transactions} ;")

# Modify the database
db.query('CREATE TABLE test (col1 INT, col2 INT)')

# Rollback to previous savepoint, discard changes

db.query("ROLLBACK TO SAVEPOINT #{db.transactions}")
# Release the save point
db.release

# Switch to auto commit mode
db.auto_commit(true)

# Deallocate memory used for storing the record set
res.free

db.close
