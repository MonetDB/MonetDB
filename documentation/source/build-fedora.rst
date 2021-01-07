.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

===============
Build on Fedora
===============

The following packages (RPMs) are or can be used by MonetDB.

These packages are required.

| cmake			# version >= 3.12
| bison
| gettext-devel
| libxml2-devel
| mercurial
| openssl-devel

These packages are optional, but they are required to build RPMs using
the command make rpm.

| bzip2-devel		# optional: read and write .bz2 compressed files
| bzip2			# optional, required to create a tar ball (make dist)
| checkpolicy		# optional, required to create RPMs (make rpm)
| geos-devel		# optional: required for geom module
| libcurl-devel		# optional: read remote files with sys.getcontent(url)
| libuuid-devel		# optional
| pcre-devel		# optional: use PCRE library, enable some functions
| python3-devel		# optional, needed for Python 3 integration
| python3-numpy		# optional, needed for Python 3 integration
| R-core-devel		# optional, needed for R integration
| readline-devel		# optional, enable editing in mclient
| rpm-build		# optional, required to create RPMs (make rpm)
| selinux-policy-devel	# optional, required to create RPMs (make rpm)
| unixODBC-devel		# optional, needed for ODBC driver

These packages are optional.

| cfitsio-devel		# optional: read FITS files
| libasan			# optional: --enable-sanitizer configuration (debug)
| lz4-devel		# optional: compression in new (unused) MAPI protocol, also used to read and write .lz4 compressed files
| netcdf-devel		# optional: read NetCDF files
| proj-devel		# optional, only optionally used in geom module
| snappy-devel		# optional: compression in new (unused) MAPI protocol
| valgrind-devel		# optional: --with-valgrind configuration (debug)
