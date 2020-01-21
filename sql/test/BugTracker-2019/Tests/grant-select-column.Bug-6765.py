import os
import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process


def client(next_user, next_passwd, input):
    c = process.client('sql', user=next_user, passwd=next_passwd, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)


s = process.server(args=[], stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)

client('monetdb', 'monetdb', '''\
CREATE schema  "myschema";\
CREATE TABLE "myschema"."test" ("id" integer, "name" varchar(20));\
INSERT INTO "myschema"."test" ("id", "name") VALUES (1,'Tom'),(2,'Karen');\
CREATE USER myuser WITH UNENCRYPTED PASSWORD 'Test123' NAME 'Hulk' SCHEMA "myschema";\
GRANT SELECT ON "myschema"."test" TO myuser;
''')

client('myuser', 'Test123', '''\
SELECT "id", "name" FROM "myschema"."test";
''')

client('monetdb', 'monetdb', '''\
REVOKE SELECT ON "myschema"."test" FROM myuser;\
GRANT SELECT ("name") ON "myschema"."test" TO myuser;
''')

client('myuser', 'Test123', '''\
SELECT "id", "name" FROM "myschema"."test"; --error, no permission on column "name"%s\
SELECT "name" FROM "myschema"."test"; --ok
''' % (os.linesep))

client('monetdb', 'monetdb', '''\
DROP USER myuser;\
DROP SCHEMA "myschema" CASCADE;
''')

out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
