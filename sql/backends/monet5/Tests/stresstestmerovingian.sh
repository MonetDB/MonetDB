#!/usr/bin/env bash

# this assumes:
# a) merovingian is running on 50000
# b) foo exists
while /bin/true ; do
	mclient -lsql -dfoo < /dev/null
done
