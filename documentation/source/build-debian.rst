.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

===============
Build on Debian
===============

The following packages (DEBs) are or can be used by MonetDB.

These packages are required.

cmake			# version >= 3.12
bison
gettext
libssl-dev
libxml2-dev
mercurial

These packages are optional, but required when building the MonetDB
.deb packages.

libbz2-dev		# optional: read and write .bz2 compressed files
libcurl4-gnutls-dev	# optional: read remote files with sys.getcontent(url)
libgeos-dev		# optional: required for geom module
liblzma-dev		# optional: read and write .xz compressed files
libpcre3-dev		# optional: use PCRE library, enable some functions
libreadline-dev		# optional, enable editing in mclient
python3-dev		# optional, needed for Python 3 integration
python3-numpy		# optional, needed for Python 3 integration
r-base			# optional, needed for R integration
unixodbc-dev		# optional, needed for ODBC driver
uuid-dev		# optional
zlib1g-dev		# optional: read and write .gz compressed files

These packages are optional.

libcfitsio-dev		# optional: read FITS files
liblz4-dev		# optional: compression in new (unused) MAPI protocol, also used to read and write .lz4 compressed files
libsnappy-dev		# optional: compression in new (unused) MAPI protocol
