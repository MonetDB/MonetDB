###
# Create four users.
# Drop four users.
###

import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def sql_test_client(user, passwd, input):
    process.client(lang = "sql", user = user, passwd = passwd, communicate = True,
                   stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE,
                   input = input, port = int(os.getenv("MAPIPORT")))

sql_test_client('monetdb', 'monetdb', input = """\
CREATE SCHEMA newSchema;
select u.name, u.fullname, s.name from "sys"."users" u left outer join "sys"."schemas" s on u.default_schema = s.id;

CREATE USER user1 with password '1' name '1st user' schema newSchema;
CREATE USER user2 with password '2' name '2nd user' schema newSchema;
CREATE USER user3 with password '3' name '3rd user' schema newSchema;
CREATE USER user4 with password '4' name '4th user' schema newSchema;

select u.name, u.fullname, s.name from "sys"."users" u left outer join "sys"."schemas" s on u.default_schema = s.id;

DROP USER user1;
DROP USER user2;
DROP USER user3;
DROP USER user4;

select u.name, u.fullname, s.name from "sys"."users" u left outer join "sys"."schemas" s on u.default_schema = s.id;
""")




