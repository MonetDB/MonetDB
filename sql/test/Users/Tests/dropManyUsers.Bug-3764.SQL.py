###
# Create four users.
# Drop four users.
###

from util import sql_test_client

sql_test_client('monetdb', 'monetdb', input = """\
CREATE SCHEMA newSchema;
select * from "sys"."users";

CREATE USER user1 with password '1' name '1st user' schema newSchema;
CREATE USER user2 with password '2' name '2nd user' schema newSchema;
CREATE USER user3 with password '3' name '3rd user' schema newSchema;
CREATE USER user4 with password '4' name '4th user' schema newSchema;

select * from "sys"."users";

DROP USER user1;
DROP USER user2;
DROP USER user3;
DROP USER user4;

select * from "sys"."users";
""")




