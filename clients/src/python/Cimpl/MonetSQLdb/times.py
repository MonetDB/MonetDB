# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

""" Times module for MonetSQLdb """

class Timestamp:

    _format_string = "%(year)s-%(month)02d-%(day)02d %(hour)02d:%(minute)02d:%(second)02d"

    def __init__(self, year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second

    def __str__(self):
        x = {}
        x['year'] = self.year
        x['month'] = self.month
        x['day'] = self.day
        x['hour'] = self.hour
        x['minute'] = self.minute
        x['second'] = self.second
        return self._format_string % x

class Date(Timestamp):
    _format_string = "%(year)s-%(month)02d-%(day)02d"
    def __init__(self, year=0, month=0, day=0):
        Timestamp.__init__(self, year, month, day)

class Time(Timestamp):
    _format_string = "%(hour)02d:%(minute)02d:%(second)02d"
    def __init__(self, hour=0, minute=0, second=0):
        Timestamp.__init__(self, 0, 0, 0, hour, minute, second)


def fromTime(x):
    return apply(Time, map(lambda x: int(float(x)), x.split(":")))

def fromDate(x):
    return apply(Date, map(lambda x: int(float(x)), x.split("-")))

def fromTimestamp(x):
    x = x.split(" ")
    return apply(Timestamp,
                 map(lambda x: int(float(x)), x[0].split("-")) +
                 map(lambda x: int(float(x)), x[1].split(":"))
                 )
