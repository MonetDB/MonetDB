import tempfile, os, shutil
from MonetDBtesting.sqltest import SQLTestCase

nexdir = tempfile.mkdtemp()

try:
    temp_name = os.path.join(nexdir, 'myfile.csv').replace("\\", "\\\\")
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
finally:
    if os.path.exists(nexdir):
        shutil.rmtree(nexdir)
