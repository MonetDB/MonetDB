#!/bin/bash

monetdb_path=/usr/local/monetdb/bin

case $PATH in
*:$monetdb_path|*:$monetdb_path:*|$monetdb_path:*)
    ;;
*)
    cat >> ~/.profile <<EOF

# Set MonetDB path
export PATH=\$PATH:$monetdb_path
EOF
    ;;
esac
