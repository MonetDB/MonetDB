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

require 'MonetDB'

db = MonetDB.new

db.conn({ :user => "monetdb", :passwd => "monetdb", :port => 50000, :language => "sql", :host => "localhost", :database => "ruby_test", :auth_type => "SHA1" })

# set type_cast=true to enable MonetDB to Ruby type mapping
#res = db.query("select * from tables, tables, tables;")

#db.query("DROP TABLE tests2 ")
#db.query(" CREATE TABLE tests2 ( col1  varchar(255), col2 varchar(255)) " )

#puts "Number of rows returned: " + res.num_rows.to_s
#puts "Number of fields: " + res.num_fields.to_s

# Get the columns' name
# print res.name_fields

###### Fetch all rows and store them

#puts res.fetch_all


# Iterate over the record set and retrieve on row at a time
#puts res.fetch
#while row = res.fetch do
#  printf "%s \n", row
#end


###### Get all records and hash them by column name
#row = res.fetch_all_hash()

#puts col_names[0] + "\t\t" + col_names[1]
#0.upto(res.num_rows) { |i|
#  puts row['id'][i]
#}


###### Iterator over columns (on cell at a time)

#while row = res.fetch_hash do
#  printf "%s\n",  row["id"]
#end

# SQL TRANSACTIONS and SAVE POINTS



db.query('DROP TABLE tests2')
db.auto_commit(false)
puts db.auto_commit?
# create a savepoint
db.save
db.query("CREATE TABLE tests2 (col1 VARCHAR(255), col2 VARCHAR(255))")
res = db.query("SAVEPOINT #{db.transactions} ;")
res = db.query("INSERT INTO \"tests2\" VALUES ('€¿®µ¶¹', '€¿®µ¶¹')")
res = db.query("INSERT INTO \"tests2\" VALUES ('€¿®µ¶¹', '€¿®µ¶¹')")
#res = db.query("INSERT INTO \"tests2\" VALUES ('€¿®µ¶¹', '€¿®µ¶¹')")
#res = db.query("INSERT INTO \"tests2\" VALUES ('€¿®µ¶¹', '€¿®µ¶¹')")
#res = db.query("INSERT INTO \"tests2\" VALUES ('€¿®µ¶¹', '€¿®µ¶¹')")
#res = db.query("INSERT INTO \"tests2\" VALUES ('€¿®µ¶¹', '€¿®µ¶¹')")
#res = db.query("INSERT INTO \"tests2\" VALUES ('€¿®µ¶¹', '€¿®µ¶¹')")
#res = db.query("INSERT INTO \"tests2\" VALUES ('€¿®µ¶¹', '€¿®µ¶¹')")
db.query("COMMIT")
db.release

db.save
res = db.query("SAVEPOINT #{db.transactions} ;")
res = db.query("INSERT INTO \"tests2\" VALUES('NAME4', 'SURNAME4')")

res = db.query("ROLLBACK TO SAVEPOINT #{db.transactions};")
db.release

db.auto_commit(true)
puts db.auto_commit?
res = db.query('SELECT * from tests2')
while row = res.fetch do
  printf "%s \n", row
end

res.free

db.close
