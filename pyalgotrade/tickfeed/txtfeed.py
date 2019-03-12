
"""
.. moduleauthor:: zkn <zkn@outlook.com>
"""

import datetime

import pytz
import six

from pyalgotrade.utils import dt
from pyalgotrade.tickfeed import memtf
from pyalgotrade import tick
from pyalgotrade import bar


def float_or_string(value):
    try:
        ret = float(value)
    except Exception:
        ret = value
    return ret


# Interface for txt row parsers.
class RowParser(object):
    def parseTick(self, row):
        raise NotImplementedError()

    def getFieldNames(self):
        raise NotImplementedError()

    def getDelimiter(self):
        raise NotImplementedError()


# Interface for tick filters.
class TickFilter(object):
    def includeTick(self, tick_):
        raise NotImplementedError()


class DateRangeFilter(TickFilter):
    def __init__(self, fromDate=None, toDate=None):
        self.__fromDate = fromDate
        self.__toDate = toDate

    def includeTick(self, tick_):
        if self.__toDate and tick_.getDateTime() > self.__toDate:
            return False
        if self.__fromDate and tick_.getDateTime() < self.__fromDate:
            return False
        return True


# US Equities Regular Trading Hours filter
# Monday ~ Friday
# 9:30 ~ 16 (GMT-5)
class USEquitiesRTH(DateRangeFilter):
    timezone = pytz.timezone("US/Eastern")

    def __init__(self, fromDate=None, toDate=None):
        super(USEquitiesRTH, self).__init__(fromDate, toDate)

        self.__fromTime = datetime.time(9, 30, 0)
        self.__toTime = datetime.time(16, 0, 0)

    def includeTick(self, tick_):
        ret = super(USEquitiesRTH, self).includeTick(tick_)
        if ret:
            # Check day of week
            tickDay = tick_.getDateTime().weekday()
            if tickDay > 4:
                return False

            # Check time
            tickTime = dt.localize(tick_.getDateTime(), USEquitiesRTH.timezone).time()
            if tickTime < self.__fromTime:
                return False
            if tickTime > self.__toTime:
                return False
        return ret


class TickFeed(memtf.TickFeed):
    """Base class for TXT file based :class:`pyalgotrade.tickfeed.TickFeed`.

    .. note::
        This is a base class and should not be used directly.
    """

    def __init__(self, frequency, maxLen=1024*10000):
        super(TickFeed, self).__init__(maxLen)

        self.__tickFilter = None
        self.__dailyTime = datetime.time(0, 0, 0)

    def getDailyTickTime(self):
        return self.__dailyTime

    def setDailyTickTime(self, time):
        self.__dailyTime = time

    def getTickFilter(self):
        return self.__tickFilter

    def setTickFilter(self, tickFilter):
        self.__tickFilter = tickFilter

    def addTicksFromTXT(self, instrument, file, rowParser, skipMalformedTicks=False):
        def parse_tick_skip_malformed(row):
            ret = None
            try:
                ret = rowParser.parseTick(row)
            except Exception:
                pass
            return ret

        if skipMalformedTicks:
            parse_tick = parse_tick_skip_malformed
        else:
            parse_tick = rowParser.parseTick

        # Load the txt file
        loadedTicks = []
        with open(file, 'r') as f:
            for row in f:
                row = row.rstrip('\n').rstrip().split(',')
                tick_ = parse_tick(row)
                if tick_ is not None and (self.__tickFilter is None or self.__tickFilter.includeTick(tick_)):
                    loadedTicks.append(tick_)

        self.addTicksFromSequence(instrument, loadedTicks)


class GenericRowParser(RowParser):
    def __init__(self, dateTimeFormat, tickClass=tick.BasicTick):
        self.__dateTimeFormat = dateTimeFormat
        self.__frequency = bar.Frequency.TRADE
        self.__tickClass = tickClass
        # Column names.
        self.__dateTimeColName = "datetime"
        self.__bidColName = "bid"
        self.__askColName = "ask"

    def _parseDate(self, dateString):
        ret = datetime.datetime.strptime(dateString, self.__dateTimeFormat)
        return ret

    def getFieldNames(self):
        # It is expected for the first row to have the field names.
        return None

    def getDelimiter(self):
        return ","

    def parseTick(self, row):
        dateTime = self._parseDate(row[2])
        bid = float(row[0])
        ask = float(row[1])
        return self.__tickClass(
            dateTime, bid, ask
        )


class GenericTickFeed(TickFeed):
    """A TickFeed that loads ticks from CSV files that have the following format:
    ::

        Date Time,Open,High,Low,Close,Volume,Adj Close
        2013-01-01 13:59:00,13.51001,13.56,13.51,13.56,273.88014126,13.51001

    :param frequency: The frequency of the ticks. Check :class:`pyalgotrade.tick.Frequency`.
    :param timezone: The default timezone to use to localize ticks. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.tickds.TickDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    .. note::
        * The CSV file **must** have the column names in the first row.
        * It is ok if the **Adj Close** column is empty.
        * When working with multiple instruments:

         * If all the instruments loaded are in the same timezone, then the timezone parameter may not be specified.
         * If any of the instruments loaded are in different timezones, then the timezone parameter should be set.
    """

    def __init__(self, maxLen=1024*10000):
        super(GenericTickFeed, self).__init__(maxLen)
        self.__tickClass = tick.BasicTick
        self.__dateTimeFormat = "%Y.%m.%d %H:%M:%S"

    def setDateTimeFormat(self, dateTimeFormat):
        """
        Set the format string to use with strptime to parse datetime column.
        """
        self.__dateTimeFormat = dateTimeFormat

    def setTickClass(self, tickClass):
        self.__tickClass = tickClass

    def addTicksFromTXT(self, instrument, file, skipMalformedTicks=False):
        """Loads ticks for a given instrument from a CSV formatted file.
        The instrument gets registered in the tick feed.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param file: The path to the CSV file.
        :type file: string.
        :param skipMalformedTicks: True to skip errors while parsing ticks.
        :type skipMalformedTicks: boolean.
        """
        rowParser = GenericRowParser(
            self.__dateTimeFormat, self.__tickClass
        )
        super(GenericTickFeed, self).addTicksFromTXT(instrument, file, rowParser, skipMalformedTicks=skipMalformedTicks)
