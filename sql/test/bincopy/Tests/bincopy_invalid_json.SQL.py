#!/usr/bin/env python3

from cmath import exp
import os
import pymonetdb

conn = pymonetdb.connect(
    database=os.getenv("TSTDB"),
    port=int(os.getenv("MAPIPORT")))

conn.set_autocommit(False)

CONTENT = dict(
    invalid_utf8=b'"invali\x80\x80"',
    unterminated_object=b'{"foo": "bar',
    valid_json=b'{"foo":42}'
)


class MyUploader(pymonetdb.Uploader):
    def handle_upload(self, upload: pymonetdb.Upload, filename: str, text_mode: bool, skip_amount: int):
        assert text_mode == False
        assert skip_amount == 0
        json = CONTENT[filename]
        bw = upload.binary_writer()
        bw.write(json + b'\x00')


conn.set_uploader(MyUploader())

def run_test(content_name, expected_exception):
    c = conn.cursor()
    try:
        c.execute("DROP TABLE IF EXISTS foo")
        c.execute("CREATE TABLE foo(j JSON)")
        try:
            c.execute("COPY BINARY INTO foo FROM %s ON CLIENT", [content_name])
            if expected_exception:
                content = CONTENT[content_name]
                msg = f"Expected error involving '{expected_exception}' when loading {content!r}"
                raise Exception(msg)
        except pymonetdb.OperationalError as e:
            if expected_exception in str(e):
                conn.rollback()
                return
            raise e
    finally:
        c.close()

run_test('valid_json', None)
run_test('invalid_utf8', 'malformed utf')
run_test('unterminated_object', 'JSONfromString')
