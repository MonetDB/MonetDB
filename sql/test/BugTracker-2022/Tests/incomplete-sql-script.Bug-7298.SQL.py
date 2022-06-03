from MonetDBtesting.sqltest import SQLTestCase
import tempfile

with SQLTestCase() as tc:

    with tempfile.TemporaryFile('w+') as tf:
        s1 = "select foo"
        tf.write(s1)
        tf.flush()
        tf.seek(0)

        tc.execute(None, '-fraw', client='mclient', stdin=tf).assertFailed(err_code="42000", err_message=['!syntax error, unexpected $end in: "select foo"', '!syntax error in: "select foo"'])
        tf.seek(0)
        tf.truncate(0)

        s1 = "select E'foo"
        tf.write(s1)
        tf.flush()
        tf.seek(0)

        tc.execute(None, '-fraw', client='mclient', stdin=tf).assertFailed(err_code="42000", err_message='!Unexpected end of input')
        tf.seek(0)
        tf.truncate(0)

        s1 = "select foo."
        tf.write(s1)
        tf.flush()
        tf.seek(0)

        tc.execute(None, '-fraw', client='mclient', stdin=tf).assertFailed(err_code="42000", err_message=['!syntax error, unexpected $end, expecting SCOLON in: "select foo."', '!syntax error in: "select foo."'])
        tf.seek(0)
        tf.truncate(0)

        s1 = "select foo-"
        tf.write(s1)
        tf.flush()
        tf.seek(0)

        tc.execute(None, '-fraw', client='mclient', stdin=tf).assertFailed(err_code="42000", err_message=['!syntax error, unexpected $end, expecting SCOLON in: "select foo-"', '!syntax error in: "select foo-"'])
        tf.seek(0)
        tf.truncate(0)

        s1 = "select f001234"
        tf.write(s1)
        tf.flush()
        tf.seek(0)

        tc.execute(None, '-fraw', client='mclient', stdin=tf).assertFailed(err_code="42000", err_message=['!syntax error, unexpected $end in: "select f001234"', '!syntax error in: "select f001234"'])
        tf.seek(0)
        tf.truncate(0)

        s1 = "select \"foo"
        tf.write(s1)
        tf.flush()
        tf.seek(0)

        tc.execute(None, '-fraw', client='mclient', stdin=tf).assertFailed(err_code="42000", err_message='!Unexpected end of input')
        tf.seek(0)
        tf.truncate(0)
