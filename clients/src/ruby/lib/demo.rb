require 'MonetDB'

db = MonetDB.new
db.connect(user = "monetdb", passwd = "monetdb", lang = "sql", host="127.0.0.1", port = 50000, db_name = "demo", auth_type = "SHA1")

# set type_cast=true to enable MonetDB to Ruby type mapping
res = db.query("SELECT * from tables;", type_cast = false)

#puts res.debug_columns_type

puts "Number of rows returned: " + res.num_rows.to_s
puts "Number of fields: " + res.num_fields.to_s


# Get the columns' name
#col_names = res.name_fields


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
#  puts row['id'][i] + "\t\t" + row['name'][i] 
#}


###### Iterator over columns (on cell at a time)

#while row = res.fetch_hash do
#  printf "%s, %s\n", row["name"], row["id"]
#end
  
# Deallocate memory used for storing the record set
res.free

db.close
