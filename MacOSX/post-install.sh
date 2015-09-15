#!/bin/bash

monetdb_path=/usr/local/monetdb/bin

if [[ ":$PATH:" != *":$monetdb_path:"* ]]; then
    printf '\n# Set MonetDB path\nexport PATH=$PATH:'$monetdb_path'\n' >> ~/.profile
fi
