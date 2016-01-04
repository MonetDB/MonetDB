# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

"""
Backwards compatible converterts
"""

from monetdb.sql import monetize
from monetdb.sql import pythonize


class Pythonizer:
    """
    backwards compatible class, was used for convertion from
    MonetDB column types to python types. You should use
    monetdb.sql.pythonize now.
    """
    def __init__(self, use_unicode):
        pass

    def convert(self, data, type_code):
        """
        use a type_code defined in monetdb.sql.types
        """
        return pythonize.convert(data, type_code)


class Monetizer:
    """
    backwards compatible class, was used for convertion from
    python types to MonetDB column types. You should use
    monetdb.sql.monetize now.
    """
    def __init__(self):
        pass

    def convert(self, data):
        """
        """
        return monetize.convert(data)
