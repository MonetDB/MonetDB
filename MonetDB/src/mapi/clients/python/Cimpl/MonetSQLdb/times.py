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
