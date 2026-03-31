import pymonetdb
import os

h = os.getenv('MAPIHOST')
p = int(os.getenv('MAPIPORT'))
db = os.getenv('TSTDB')

conn_args = {
    'hostname': h,
    'port': p,
    'database': db,
    'username': 'monetdb',
    'password': 'monetdb',
    'autocommit': False
}

# DDL Operations to test for transaction conflict errors
# Format: (label, setup_sql, op_sql)
OPS = [
    ('DROP',      "CREATE TABLE sch7881.{t} (i int)", "DROP TABLE sch7881.{t}"),
    ('ALTER_RO',  "CREATE TABLE sch7881.{t} (i int)", "ALTER TABLE sch7881.{t} SET READ ONLY"),
    ('ALTER_NN',  "CREATE TABLE sch7881.{t} (i int)", "ALTER TABLE sch7881.{t} ALTER COLUMN i SET NOT NULL"),
    ('ALTER_PK',  "CREATE TABLE sch7881.{t} (i int)", "ALTER TABLE sch7881.{t} ADD PRIMARY KEY (i)"),
    ('ALTER_UQ',  "CREATE TABLE sch7881.{t} (i int)", "ALTER TABLE sch7881.{t} ADD UNIQUE (i)"),
    ('ALTER_FK',  "CREATE TABLE sch7881.{t} (i int)", "ALTER TABLE sch7881.{t} ADD FOREIGN KEY (i) REFERENCES sch7881.ref_table(id)"),
    ('ALTER_ADD', "CREATE TABLE sch7881.{t} (i int)", "ALTER TABLE sch7881.{t} ADD COLUMN j int"),
    ('RENAME',    "CREATE TABLE sch7881.{t} (i int)", "ALTER TABLE sch7881.{t} RENAME TO {t}_renamed"),
    ('CREATE',    "", "CREATE TABLE sch7881.{t}_new (i int)"),
]

def run_test(op1_info, op2_info):
    label1, setup1, sql1 = op1_info
    label2, setup2, sql2 = op2_info
    t1, t2 = "test_a", "test_b"

    # Setup
    conn_setup = pymonetdb.connect(**conn_args)
    cur_setup = conn_setup.cursor()
    cur_setup.execute(f"CREATE SCHEMA IF NOT EXISTS sch7881")

    # Clean up
    for t in [t1, t2, t1+"_new", t2+"_new", t1+"_renamed", t2+"_renamed"]:
        cur_setup.execute(f"DROP TABLE IF EXISTS sch7881.{t} CASCADE")

    # Ensure reference table exists for FK tests
    cur_setup.execute("DROP TABLE IF EXISTS sch7881.ref_table CASCADE")
    cur_setup.execute("CREATE TABLE sch7881.ref_table (id INT PRIMARY KEY)")

    if setup1:
        for s in setup1.split(';'):
            if s.strip(): cur_setup.execute(s.format(t=t1))
    if setup2:
        for s in setup2.split(';'):
            if s.strip(): cur_setup.execute(s.format(t=t2))
    conn_setup.commit()
    conn_setup.close()

    res1, res2, err1, err2 = "PASS", "PASS", "", ""

    # Session 1 and 2 connections
    conn1 = pymonetdb.connect(**conn_args); cur1 = conn1.cursor()
    conn2 = pymonetdb.connect(**conn_args); cur2 = conn2.cursor()

    try:
        cur1.execute(sql1.format(t=t1))
        cur2.execute(sql2.format(t=t2))

        # S1 Commit
        try: conn1.commit()
        except Exception as e: res1 = "FAIL"; err1 = str(e).split('\n')[0]

        # S2 Commit
        try: conn2.commit()
        except Exception as e: res2 = "FAIL"; err2 = str(e).split('\n')[0]

    except Exception as e:
        res1, res2 = "ERROR", "ERROR"
        err1 = "Exec error: " + str(e).split('\n')[0]
    finally:
        conn1.close()
        conn2.close()

    return res1, res2, err1, err2

if __name__ == "__main__":
    print(f"{'Op 1':<10} | {'Op 2':<10} | {'S1 Status':<10} | {'S2 Status':<10} | {'Notes'}")
    print("-" * 130)
    for i in range(len(OPS)):
        for j in range(len(OPS)):
            r1, r2, e1, e2 = run_test(OPS[i], OPS[j])
            notes = (f"S1: {e1} " if r1 == "FAIL" else "") + (f"S2: {e2}" if r2 == "FAIL" else "")
            if r1 == "ERROR": notes = e1
            print(f"{OPS[i][0]:<10} | {OPS[j][0]:<10} | {r1:<10} | {r2:<10} | {notes}")

