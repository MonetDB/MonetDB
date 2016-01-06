# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

"""
This is the MonetDB Python API.

The MAPI (MonetDB API) related code is in monetdb.mapi.

The SQL related code is in monetdb.sql.

To set up a connection use monetdb.sql.connect()

"""
from monetdb import sql
from monetdb import mapi
from monetdb import exceptions

__all__ = ["sql", "mapi"]

# for backwards compatability
monetdb_exceptions = exceptions
