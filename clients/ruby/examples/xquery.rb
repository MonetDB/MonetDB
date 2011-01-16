require 'MonetDB'

db = MonetDB.new

db.connect(user = "monetdb", passwd = "monetdb", lang = "xquery", host="localhost", port = 50000, db_name = "ruby", auth_type = "SHA1")

res = db.query('1 to 4,"aap",1.0,attribute { "aap" } { "beer" },<aap/>')

while row = res.fetch do
  printf "%s \n", row
end

res.free
