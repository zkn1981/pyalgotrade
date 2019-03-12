
"""
.. moduleauthor:: zkn <zkn@outlook.com.com>
"""

import abc

from pyalgotrade import bar
from pyalgotrade.tickdataseries import tickds
from pyalgotrade import feed
from pyalgotrade import dispatchprio


class BaseTickFeed(feed.BaseFeed):
    """Base class for :class:`pyalgotrade.tick.Tick` providing feeds.

    :param frequency: The ticks frequency. Valid values defined in :class:`pyalgotrade.tick.Frequency`.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.tickds.TickDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded
        from the opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    .. note::
        This is a base class and should not be used directly.
    """

    def __init__(self, maxLen=1024*10000):
        super(BaseTickFeed, self).__init__(maxLen)
        self.__frequency = bar.Frequency.TRADE
        self.__defaultInstrument = None
        self.__currentTicks = None
        self.__lastTicks = {}

    def reset(self):
        self.__currentTicks = None
        self.__lastTicks = {}
        super(BaseTickFeed, self).reset()

    # Return the datetime for the current ticks.
    @abc.abstractmethod
    def getCurrentDateTime(self):
        raise NotImplementedError()

    # Subclasses should implement this and return a pyalgotrade.tick.Ticks or None if there are no ticks.
    @abc.abstractmethod
    def getNextTicks(self):
        """Override to return the next :class:`pyalgotrade.tick.Ticks` in the feed or None if there are no ticks.

        .. note::
            This is for BaseTickFeed subclasses and it should not be called directly.
        """
        raise NotImplementedError()

    def createDataSeries(self, key, maxLen):
        ret = tickds.TickDataSeries(maxLen)
        return ret

    def getNextValues(self):
        dateTime = None
        ticks = self.getNextTicks()
        if ticks is not None:
            dateTime = ticks.getDateTime()

            # Check that current tick datetimes are greater than the previous one.
            # if self.__currentTicks is not None and self.__currentTicks.getDateTime() >= dateTime:
            #     raise Exception(
            #         "Tick date times are not in order. Previous datetime was %s and current datetime is %s" % (
            #             self.__currentTicks.getDateTime(),
            #             dateTime
            #         )
            #     )

            # Update self.__currentTicks and self.__lastTicks
            self.__currentTicks = ticks
            for instrument in ticks.getInstruments():
                self.__lastTicks[instrument] = ticks[instrument]
        return (dateTime, ticks)

    def getFrequency(self):
        return bar.Frequency.TRADE

    def isIntraday(self):
        return self.__frequency < bar.Frequency.DAY

    def getCurrentTicks(self):
        """Returns the current :class:`pyalgotrade.tick.Ticks`."""
        return self.__currentTicks

    def getLastTick(self, instrument):
        """Returns the last :class:`pyalgotrade.tick.Tick` for a given instrument, or None."""
        return self.__lastTicks.get(instrument, None)

    def getDefaultInstrument(self):
        """Returns the last instrument registered."""
        return self.__defaultInstrument

    def getRegisteredInstruments(self):
        """Returns a list of registered intstrument names."""
        return self.getKeys()

    def registerInstrument(self, instrument):
        self.__defaultInstrument = instrument
        self.registerDataSeries(instrument)

    def getDataSeries(self, instrument=None):
        """Returns the :class:`pyalgotrade.dataseries.tickds.TickDataSeries` for a given instrument.

        :param instrument: Instrument identifier. If None, the default instrument is returned.
        :type instrument: string.
        :rtype: :class:`pyalgotrade.dataseries.tickds.TickDataSeries`.
        """
        if instrument is None:
            instrument = self.__defaultInstrument
        return self[instrument]

    def getDispatchPriority(self):
        return dispatchprio.TICK_FEED


# This class is used by the optimizer module. The tickfeed is already built on the server side,
# and the ticks are sent back to workers.
class OptimizerTickFeed(BaseTickFeed):
    def __init__(self, instruments, ticks, maxLen=1024*10000):
        super(OptimizerTickFeed, self).__init__(maxLen)
        for instrument in instruments:
            self.registerInstrument(instrument)
        self.__ticks = ticks
        self.__nextPos = 0
        self.__currDateTime = None

    def getCurrentDateTime(self):
        return self.__currDateTime

    def start(self):
        super(OptimizerTickFeed, self).start()

    def stop(self):
        pass

    def join(self):
        pass

    def peekDateTime(self):
        ret = None
        if self.__nextPos < len(self.__ticks):
            ret = self.__ticks[self.__nextPos].getDateTime()
        return ret

    def getNextTicks(self):
        ret = None
        if self.__nextPos < len(self.__ticks):
            ret = self.__ticks[self.__nextPos]
            self.__currDateTime = ret.getDateTime()
            self.__nextPos += 1
        return ret

    def eof(self):
        return self.__nextPos >= len(self.__ticks)
