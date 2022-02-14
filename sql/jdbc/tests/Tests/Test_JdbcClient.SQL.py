import os, sys, glob
from subprocess import run, PIPE, CalledProcessError

HOST=os.getenv('HOST')
MAPIPORT=os.getenv('MAPIPORT')
TSTDB=os.getenv('TSTDB')
TSTSRCBASE=os.getenv('TSTSRCBASE')
TSTTRGBASE=os.getenv('TSTTRGBASE')
TSTDIR=os.getenv('TSTDIR')
CLIENT='org.monetdb.client.JdbcClient'
USER='monetdb'
PASSWORD='monetdb'

if __name__ == '__main__':
    with open(os.path.join('.monetdb'), 'w') as f:
        f.write('\n'.join(['user=monetdb', 'password=monetdb']))

    cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '--help']
    try:
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
        pout = str(p.stdout)
        perr = str(p.stderr)
        if perr != "":
            print(perr)
        if pout != "Usage java -jar jdbcclient.jre8.jar\n\t\t[-h host[:port]] [-p port] [-f file] [-u user]\n\t\t[-l language] [-d database] [-e] [-D [table]]\n\t\t[--csvdir /path/to/csvfiles]] [-X<opt>]\n\t\t| [--help] | [--version]\nor using long option equivalents --host --port --file --user --language\n--dump --echo --database.\nArguments may be written directly after the option like -p50000.\n\nIf no host and port are given, localhost and 50000 are assumed.\nAn .monetdb file may exist in the user's home directory.  This file can contain\npreferences to use each time JdbcClient is started.  Options given on the\ncommand line override the preferences file.  The .monetdb file syntax is\n<option>=<value> where option is one of the options host, port, file, mode\ndebug, or password.  Note that the last one is perilous and therefore not\navailable as command line option.\nIf no input file is given using the -f flag, an interactive session is\nstarted on the terminal.\n\nOPTIONS\n-h --host     The hostname of the host that runs the MonetDB database.  A port\n              number can be supplied by use of a colon, i.e. -h somehost:12345.\n-p --port     The port number to connect to.\n-f --file     A file name to use either for reading or writing.  The file will\n              be used for writing when dump mode is used (-D --dump).  In read\n              mode, the file can also be an URL pointing to a plain text file\n              that is optionally gzip compressed.\n-u --user     The username to use when connecting to the database.\n-d --database Try to connect to the given database (only makes sense if\n              connecting to monetdbd).\n-l --language Use the given language, defaults to 'sql'.\n--csvdir      The directory path where csv data files are read or written when\n              using ON CLIENT clause of COPY command.\n--help        This help screen.\n--version     Display driver version and exit.\n-e --echo     Also outputs the contents of the input file, if any.\n-q --quiet    Suppress printing the welcome header.\n-D --dump     Dumps the given table(s), or the complete database if none given.\n-Xoutput      The output mode when dumping.  Default is sql, xml may be used for\n              an experimental XML output.\n-Xhash        Use the given hash algorithm during challenge response. Supported\n              algorithm names: SHA512, SHA384, SHA256 and SHA1.\n-Xdebug       Writes a transmission log to disk for debugging purposes. If a\n              file name is given, it is used, otherwise a file called\n              monet<timestamp>.log is created.  A given file never be\n              overwritten; instead a unique variation of the file is used.\n-Xbatching    Indicates that a batch should be used instead of direct\n              communication with the server for each statement.  If a number is\n              given, it is used as batch size.  i.e. 8000 would execute the\n              contents on the batch after each 8000 statements read.  Batching\n              can greatly speedup the process of restoring a database dump.\n":
            print(pout)
            print("--help difference detected")
    except CalledProcessError as e:
        raise SystemExit(e.stderr)

    cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '-f', os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'JdbcClient_create_tables.sql')]
    try:
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
        pout = str(p.stdout)
        perr = str(p.stderr)
        if perr != "":
            print(perr)
        if pout != "Operation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\n":
            print(pout)
            print("create tables difference detected")
    except CalledProcessError as e:
        print(e.stderr, file=sys.stderr)
        raise SystemExit('ERROR: failed to create tables!')

    cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '-f', os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'JdbcClient_inserts_selects.sql')]
    try:
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
        pout = str(p.stdout)
        perr = str(p.stderr)
        if perr != "":
            print(perr)
        if pout != "Operation successful\n1 affected row\n1 affected row\n1 affected row\n1 affected row\n1 affected row\n1 affected row\n1 affected row\n1 affected row\n1 affected row\n1 affected row\n1 affected row\n1 affected row\n1 affected row\n1 affected row\n+------+---------+-----------+--------+----------+\n| id   | subject | predicate | object | explicit |\n+======+=========+===========+========+==========+\n|    1 |       1 |         1 |      1 | false    |\n|    2 |       1 |         1 |      2 | false    |\n|    3 |       1 |         2 |      1 | false    |\n|    4 |       2 |         1 |      1 | false    |\n|    5 |       1 |         2 |      2 | false    |\n|    6 |       2 |         2 |      1 | false    |\n|    7 |       2 |         2 |      2 | false    |\n+------+---------+-----------+--------+----------+\n7 rows\n+------+---------+-----------+--------+\n| id   | subject | predicate | object |\n+======+=========+===========+========+\n|    1 |       1 |         1 |      1 |\n|    2 |       2 |         2 |      2 |\n|    3 |       1 |         2 |      2 |\n|    4 |       2 |         2 |      1 |\n|    5 |       2 |         1 |      1 |\n|    6 |       1 |         2 |      1 |\n|    7 |       1 |         1 |      2 |\n+------+---------+-----------+--------+\n7 rows\n7 affected rows\n+------+---------+-----------+--------+----------+\n| id   | subject | predicate | object | explicit |\n+======+=========+===========+========+==========+\n|    1 |       1 |         1 |      1 | false    |\n|    2 |       1 |         1 |      2 | false    |\n|    3 |       1 |         2 |      1 | false    |\n|    4 |       2 |         1 |      1 | false    |\n|    5 |       1 |         2 |      2 | false    |\n|    6 |       2 |         2 |      1 | false    |\n|    7 |       2 |         2 |      2 | false    |\n+------+---------+-----------+--------+----------+\n7 rows\n+---------+--------+-----------+-----------+\n| subject | counts | min_value | max_value |\n+=========+========+===========+===========+\n|       1 |      4 |         1 |         1 |\n|       2 |      3 |         2 |         2 |\n+---------+--------+-----------+-----------+\n2 rows\n+-----------+--------+-----------+-----------+\n| predicate | counts | min_value | max_value |\n+===========+========+===========+===========+\n|         1 |      3 |         1 |         1 |\n|         2 |      4 |         2 |         2 |\n+-----------+--------+-----------+-----------+\n2 rows\n+--------+--------+-----------+-----------+\n| object | counts | min_value | max_value |\n+========+========+===========+===========+\n|      1 |      4 |         1 |         1 |\n|      2 |      3 |         2 |         2 |\n+--------+--------+-----------+-----------+\n2 rows\nOperation successful\n":
            print(pout)
            print("inserts selects difference detected")
    except CalledProcessError as e:
        print(e.stderr, file=sys.stderr)
        raise SystemExit('ERROR: failed to insert!')

    cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '-D']
    try:
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
        pout = str(p.stdout)
        perr = str(p.stderr)
        if perr != "":
            print(perr)
        if pout != 'START TRANSACTION;\n\nSET SCHEMA "sys";\n\nCREATE MERGE TABLE "sys"."mt" (\n\t"id" INTEGER       NOT NULL,\n\t"nm" VARCHAR(123)  NOT NULL,\n\tCONSTRAINT "mt_id_pkey" PRIMARY KEY ("id")\n);\n\nCREATE REMOTE TABLE "sys"."remt" (\n\t"id" INTEGER       NOT NULL,\n\t"nm" VARCHAR(123)  NOT NULL,\n\tCONSTRAINT "remt_id_pkey" PRIMARY KEY ("id")\n) ON \'mapi:monetdb://localhost:42001/mdb3\';\n\nCREATE REPLICA TABLE "sys"."replt" (\n\t"id" INTEGER       NOT NULL,\n\t"nm" VARCHAR(123)  NOT NULL,\n\tCONSTRAINT "replt_id_pkey" PRIMARY KEY ("id")\n);\n\nCREATE TABLE "sys"."allnewtriples" (\n\t"id"        INTEGER       NOT NULL,\n\t"subject"   INTEGER       NOT NULL,\n\t"predicate" INTEGER       NOT NULL,\n\t"object"    INTEGER       NOT NULL,\n\t"explicit"  BOOLEAN       NOT NULL,\n\tCONSTRAINT "allnewtriples_id_pkey" PRIMARY KEY ("id"),\n\tCONSTRAINT "unique_key" UNIQUE ("subject", "predicate", "object")\n);\nCREATE INDEX "allnewtriples_object_idx" ON "sys"."allnewtriples" ("object");\nCREATE INDEX "allnewtriples_predicate_idx" ON "sys"."allnewtriples" ("predicate");\nCREATE INDEX "allnewtriples_subject_idx" ON "sys"."allnewtriples" ("subject");\n\nINSERT INTO "sys"."allnewtriples" VALUES (1, 1, 1, 1, false);\nINSERT INTO "sys"."allnewtriples" VALUES (2, 1, 1, 2, false);\nINSERT INTO "sys"."allnewtriples" VALUES (3, 1, 2, 1, false);\nINSERT INTO "sys"."allnewtriples" VALUES (4, 2, 1, 1, false);\nINSERT INTO "sys"."allnewtriples" VALUES (5, 1, 2, 2, false);\nINSERT INTO "sys"."allnewtriples" VALUES (6, 2, 2, 1, false);\nINSERT INTO "sys"."allnewtriples" VALUES (7, 2, 2, 2, false);\n\nCREATE TABLE "sys"."foreign" (\n\t"id"        INTEGER       NOT NULL,\n\t"subject"   INTEGER       NOT NULL,\n\t"predicate" INTEGER       NOT NULL,\n\t"object"    INTEGER       NOT NULL,\n\tCONSTRAINT "foreign_id_fkey" FOREIGN KEY ("id") REFERENCES "sys"."allnewtriples" ("id") ON UPDATE RESTRICT ON DELETE RESTRICT,\n\tCONSTRAINT "foreign_subject_predicate_object_fkey" FOREIGN KEY ("subject", "predicate", "object") REFERENCES "sys"."allnewtriples" ("subject", "predicate", "object") ON UPDATE RESTRICT ON DELETE RESTRICT\n);\n\nINSERT INTO "sys"."foreign" VALUES (1, 1, 1, 1);\nINSERT INTO "sys"."foreign" VALUES (2, 2, 2, 2);\nINSERT INTO "sys"."foreign" VALUES (3, 1, 2, 2);\nINSERT INTO "sys"."foreign" VALUES (4, 2, 2, 1);\nINSERT INTO "sys"."foreign" VALUES (5, 2, 1, 1);\nINSERT INTO "sys"."foreign" VALUES (6, 1, 2, 1);\nINSERT INTO "sys"."foreign" VALUES (7, 1, 1, 2);\n\nCREATE TABLE "sys"."triples" (\n\t"id"        INTEGER       NOT NULL,\n\t"subject"   INTEGER       NOT NULL,\n\t"predicate" INTEGER       NOT NULL,\n\t"object"    INTEGER       NOT NULL,\n\t"explicit"  BOOLEAN       NOT NULL,\n\tCONSTRAINT "triples_subject_predicate_object_unique" UNIQUE ("subject", "predicate", "object")\n);\nCREATE INDEX "triples_object_idx" ON "sys"."triples" ("object");\nCREATE INDEX "triples_predicate_idx" ON "sys"."triples" ("predicate");\nCREATE INDEX "triples_predicate_object_idx" ON "sys"."triples" ("predicate", "object");\nCREATE INDEX "triples_subject_idx" ON "sys"."triples" ("subject");\nCREATE INDEX "triples_subject_object_idx" ON "sys"."triples" ("subject", "object");\nCREATE INDEX "triples_subject_predicate_idx" ON "sys"."triples" ("subject", "predicate");\n\nINSERT INTO "sys"."triples" VALUES (1, 1, 1, 1, false);\nINSERT INTO "sys"."triples" VALUES (2, 1, 1, 2, false);\nINSERT INTO "sys"."triples" VALUES (3, 1, 2, 1, false);\nINSERT INTO "sys"."triples" VALUES (4, 2, 1, 1, false);\nINSERT INTO "sys"."triples" VALUES (5, 1, 2, 2, false);\nINSERT INTO "sys"."triples" VALUES (6, 2, 2, 1, false);\nINSERT INTO "sys"."triples" VALUES (7, 2, 2, 2, false);\n\ncreate or replace view object_stats as select "object", cast(count(*) as bigint) as counts, min("object") as min_value, max("object") as max_value from "triples" group by "object" order by "object"\n;\n\ncreate view predicate_stats as select "predicate", cast(count(*) as bigint) as counts, min("predicate") as min_value, max("predicate") as max_value from "triples" group by "predicate" order by "predicate"\n;\n\ncreate view subject_stats as select "subject", cast(count(*) as bigint) as counts, min("subject") as min_value, max("subject") as max_value from "triples" group by "subject" order by "subject"\n;\n\nCOMMIT;\n':
            print(pout)
            print("-D difference detected")
    except CalledProcessError as e:
        raise SystemExit(e.stderr)

    cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '-f', os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'JdbcClient_drop_tables.sql')]
    try:
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
        pout = str(p.stdout)
        perr = str(p.stderr)
        if perr != "":
            print(perr)
        if pout != "Operation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\nOperation successful\n":
            print(pout)
            print("drop tables difference detected")
    except CalledProcessError as e:
        raise SystemExit(e.stderr)

    try:
        # test the ON CLIENT download/export functionality via JdbcClient with --csvdir argument (to enable the ON CLIENT functionality)
        cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '--csvdir', os.path.join(TSTSRCBASE, TSTDIR, 'Tests'), '-f', os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'OnClientDownloadData.sql')]
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
        pout = str(p.stdout)
        perr = str(p.stderr)
        if perr != "Error on line 12: [22000] Missing file name\nError on line 30: [22000] Requested file compression format .bz2 is not supported. Use .gz instead.\nError on line 32: [22000] Requested file compression format .lz4 is not supported. Use .gz instead.\nError on line 34: [22000] Requested file compression format .xz is not supported. Use .gz instead.\nError on line 36: [22000] Requested file compression format .zip is not supported. Use .gz instead.\n":
            print(perr)
            print("OnClientDownloadData err difference detected")
        if pout != "121 affected rows\n122 affected rows\n123 affected rows\n124 affected rows\n120 affected rows\n":
            print(pout)
            print("OnClientDownloadData out difference detected")

        # test the ON CLIENT upload/import functionality via JdbcClient with --csvdir argument (to enable the ON CLIENT functionality)
        cmd = ['java', CLIENT, '-h', HOST, '-p', MAPIPORT, '-d', TSTDB, '--csvdir', os.path.join(TSTSRCBASE, TSTDIR, 'Tests'), '-f', os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'OnClientUploadData.sql')]
        p = run(cmd, stdout=PIPE, stderr=PIPE, check=True, encoding='utf-8')
        pout = str(p.stdout)
        perr = str(p.stderr)
        if perr != "":
            print(perr)
            print("OnClientUploadData err difference detected")
        if pout != "Operation successful\nOperation successful\n121 affected rows\n+-------+\n| %2    |\n+=======+\n| true  |\n+-------+\n1 row\n121 affected rows\n122 affected rows\n+-------+\n| %2    |\n+=======+\n| true  |\n+-------+\n1 row\n122 affected rows\n20 affected rows\n+-------+\n| %2    |\n+=======+\n| true  |\n+-------+\n1 row\n20 affected rows\n123 affected rows\n+-------+\n| %2    |\n+=======+\n| true  |\n+-------+\n1 row\n123 affected rows\n124 affected rows\n+-------+\n| %2    |\n+=======+\n| true  |\n+-------+\n1 row\n124 affected rows\n120 affected rows\n+-------+\n| %2    |\n+=======+\n| true  |\n+-------+\n1 row\n120 affected rows\n80 affected rows\n+-------+\n| %2    |\n+=======+\n| true  |\n+-------+\n1 row\n80 affected rows\nOperation successful\n":
            print(pout)
            print("OnClientUploadData out difference detected")
    except CalledProcessError as e:
        sys.stderr.write(str(e))
    finally:
        # cleanup created data export files from Tests/OnClientDownloadData.sql
        for tfile in glob.glob(os.path.join(TSTSRCBASE, TSTDIR, 'Tests', 'sys_tables_by_id.*')):
            os.remove(tfile)

#set -e
#
#cat << EOF > .monetdb
#user=monetdb
#password=monetdb
#EOF
#
#java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB --help
#
#java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f $TSTSRCBASE/$TSTDIR/Tests/JdbcClient_create_tables.sql
#java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f $TSTSRCBASE/$TSTDIR/Tests/JdbcClient_inserts_selects.sql
#java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -D
#java org.monetdb.client.JdbcClient -h $HOST -p $MAPIPORT -d $TSTDB -f $TSTSRCBASE/$TSTDIR/Tests/JdbcClient_drop_tables.sql
#
#rm -f .monetdb
