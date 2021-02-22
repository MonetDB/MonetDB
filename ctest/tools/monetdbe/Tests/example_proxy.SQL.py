import os, pymonetdb
import subprocess

db = os.getenv("TSTDB")
port = os.getenv("MAPIPORT")

client1 = pymonetdb.connect(database=db, port=port, autocommit=True)
cur1 = client1.cursor()
cur1.execute('''
CREATE TABLE test (x INTEGER, y STRING);
INSERT INTO test VALUES (42, 'Hello'), (NULL, 'World');
''')

cur1.close()
client1.close()

cmd = ['example_proxy', port, db]
results = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

if results.stderr:
    print(results.stderr)

lines = results.stdout.splitlines()

if len(lines) != 3:
    print(results.stdout)
    print("Too many output lines.")
    exit(1)

def test_equal(expected, received):
    if received != expected:
        print("expected:")
        print(expected)
        print("received:")
        print(received)
        exit(1)

expected="Query result with 2 cols and 2 rows"
test_equal(expected, lines[0])

expected="42, Hello"
test_equal(expected, lines[1])

expected="NULL, World"
test_equal(expected, lines[2])
