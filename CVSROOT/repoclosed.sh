#!/bin/sh

echo "\
Pre-commit check failed: this repository is closed for updates.

MonetDB has switched to a Mercurial repository on dev.monetdb.org.
You can check out a read-only copy using
    hg clone http://dev.monetdb.org/hg/MonetDB/

If you have sufficient permissions, you can check out a read-write
copy using
    hg clone ssh://hg@dev.monetdb.org/MonetDB/

This message pertains to the following CVS modules that were housed
here at Sourceforge:

MonetDB, MonetDB4, MonetDB5, clients, sql, pathfinder, buildtools
geom, template, testing

All of these combined will be checked out together using the above
command.

To make the switch from the CVS repository to the Mercurial
repository, and to learn more about how to use the Mercurial
repository, take a look at one of

http://homepages.cwi.nl/~sjoerd/downloads/monetdb/MonetDB-Mercurial.html
http://homepages.cwi.nl/~sjoerd/downloads/monetdb/MonetDB-Mercurial.pdf
"

exit 1
