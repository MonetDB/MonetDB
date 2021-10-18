import tempfile, os, shutil
from MonetDBtesting.sqltest import SQLTestCase

nexdir = tempfile.mkdtemp()
temp_name = os.path.join(nexdir, 'myp.csv')

with SQLTestCase() as cli:
    cli.connect(username="monetdb", password="monetdb")

    cli.execute("START TRANSACTION;").assertSucceeded()
    cli.execute("""
    CREATE TABLE "t" ("id" INTEGER,"name" VARCHAR(1024),"schema_id" INTEGER,"query" VARCHAR(1048576),"type" SMALLINT,"system" BOOLEAN,"commit_action" SMALLINT,"access" SMALLINT,"temporary" TINYINT);
    """).assertSucceeded()
    cli.execute("""
    COPY SELECT "id","name","schema_id","query","type","system","commit_action","access","temporary" FROM sys.tables LIMIT 100 INTO '%s' DELIMITERS '|';
    """ % (temp_name)).assertSucceeded()
    cli.execute("COPY INTO t FROM '%s' DELIMITERS '|';" % (temp_name)).assertSucceeded()
    cli.execute("DROP TABLE t;").assertSucceeded()
    cli.execute("ROLLBACK;").assertSucceeded()

shutil.rmtree(nexdir)
