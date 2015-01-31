# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2015 MonetDB B.V.
# All Rights Reserved.

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
