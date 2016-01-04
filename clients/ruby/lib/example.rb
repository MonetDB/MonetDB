# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

require_relative 'MonetDB'

def print_result(result, message="")
	puts message
	puts "========"
	result.each_record do |record|
		puts record
	end
	puts "========"
	puts
end

def get_database_connection
	database_connection = MonetDB.new
	database_connection.connect(user = "monetdb", passwd = "monetdb", lang = "sql", host="127.0.0.1", port = 50000, database_connection_name = "testdatabase2", auth_type = "SHA1")
	return database_connection
end

# Connect to the database. This assumes you have a MonetDB server running with a database called testdatabase2
db = get_database_connection

# Add a table and some data to the database
db.query("CREATE TABLE testtable (testnumber INTEGER)")
db.query("INSERT INTO testtable(testnumber) VALUES (1)")
db.query("INSERT INTO testtable(testnumber) VALUES (2)")

# Get some data from the database
result = db.query("SELECT * FROM testtable")
print_result(result, "Showing all data in table")

# Use of autocommit
db.auto_commit(false)
db.query("INSERT INTO testtable(testnumber) VALUES (100)")
db.query("INSERT INTO testtable(testnumber) VALUES (101)")
result = db.query("SELECT * FROM testtable")
print_result(result, "First connection: 100 and 101 have not yet been saved, but are shown because they were added by the same connection")

# Show that the data is not really in the database yet
db2 = get_database_connection
result = db2.query("SELECT * FROM testtable")
print_result(result, "Second connection: 100 and 101 are not shown, because autocommit is off and it is a different connection")

# Save the database data
db.query("COMMIT")
result = db2.query("SELECT * FROM testtable")
print_result(result, "Second connection: 100 and 101 are shown now, because they were committed in db connection 1")
db.auto_commit(true)

# Get the data with other public interface methods
puts "number of rows: #{result.num_rows}"
puts "Fetch all: #{result.fetch_all}"
puts "As column hash: #{result.fetch_all_as_column_hash}"
puts "Iterate one at a time: #{result.fetch}"
result.reset_index # restart the iterator used in the single fetch methods
puts "Iterate one at a time as hash: #{result.fetch_hash}"

# close the connection
db.query("DROP TABLE testtable")
db.close
