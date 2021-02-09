#!/bin/bash

NAME="$1"
UPPER_NAME="$(echo "$NAME" | tr a-z- A-Z_)"

set -e
set -x

test -n "$NAME"

cat >sql/test/bincopy/Tests/bincopy_${NAME}_on_client.SQL.py <<EOF
#!/usr/bin/env python3

from bincopy_support import run_test
from bincopy_support import $UPPER_NAME as testcode

run_test('client', testcode)
EOF

cat >sql/test/bincopy/Tests/bincopy_${NAME}_on_server.SQL.py <<EOF
#!/usr/bin/env python3

from bincopy_support import run_test
from bincopy_support import $UPPER_NAME as testcode

run_test('server', testcode)
EOF

cat >>sql/test/bincopy/Tests/bincopy_support.py <<EOF

$UPPER_NAME = """
COPY BINARY FROM banana
"""
EOF

cat >>sql/test/bincopy/Tests/All <<EOF
bincopy_${NAME}_on_client
bincopy_${NAME}_on_server
EOF
