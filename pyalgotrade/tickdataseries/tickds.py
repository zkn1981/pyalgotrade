
"""
.. moduleauthor:: zkn <zkn@outlook.com>
"""

from pyalgotrade import dataseries

import six


class TickDataSeries(dataseries.SequenceDataSeries):
    """A DataSeries of :class:`pyalgotrade.tick.Tick` instances.

    :param maxLen: The maximum number of values to hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the
        opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.
    """

    def __init__(self, maxLen=1024*10000):
        super(TickDataSeries, self).__init__(maxLen)
        self.__bidDS = dataseries.SequenceDataSeries(maxLen)
        self.__askDS = dataseries.SequenceDataSeries(maxLen)

    def append(self, tick):
        self.appendWithDateTime(tick.getDateTime(), tick)

    def appendWithDateTime(self, dateTime, tick):
        assert(dateTime is not None)
        assert(tick is not None)

        super(TickDataSeries, self).appendWithDateTime(dateTime, tick)

        self.__bidDS.appendWithDateTime(dateTime, tick.getBid())
        self.__askDS.appendWithDateTime(dateTime, tick.getAsk())


    def getBidDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the open prices."""
        return self.__bidDS

    def getAskDataSeries(self):
        """Returns a :class:`pyalgotrade.dataseries.DataSeries` with the close prices."""
        return self.__askDS
