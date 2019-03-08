
"""
.. moduleauthor:: zkn <zkn@outlook.com>
"""

import six

from pyalgotrade import tickfeed
from pyalgotrade import tick
from pyalgotrade import utils


# A non real-time TickFeed responsible for:
# - Holding ticks in memory.
# - Aligning them with respect to time.
#
# Subclasses should:
# - Forward the call to start() if they override it.

class TickFeed(tickfeed.BaseTickFeed):
    def __init__(self, maxLen=1024*10000):
        super(TickFeed, self).__init__(maxLen)

        self.__ticks = {}
        self.__nextPos = {}
        self.__started = False
        self.__currDateTime = None

    def reset(self):
        self.__nextPos = {}
        for instrument in self.__ticks.keys():
            self.__nextPos.setdefault(instrument, 0)
        self.__currDateTime = None
        super(TickFeed, self).reset()

    def getCurrentDateTime(self):
        return self.__currDateTime

    def start(self):
        super(TickFeed, self).start()
        self.__started = True

    def stop(self):
        pass

    def join(self):
        pass

    def addTicksFromSequence(self, instrument, ticks):
        if self.__started:
            raise Exception("Can't add more ticks once you started consuming ticks")

        self.__ticks.setdefault(instrument, [])
        self.__nextPos.setdefault(instrument, 0)

        # Add and sort the ticks
        self.__ticks[instrument].extend(ticks)
        self.__ticks[instrument].sort(key=lambda b: b.getDateTime())

        self.registerInstrument(instrument)

    def eof(self):
        ret = True
        # Check if there is at least one more tick to return.
        for instrument, ticks in six.iteritems(self.__ticks):
            nextPos = self.__nextPos[instrument]
            if nextPos < len(ticks):
                ret = False
                break
        return ret

    def peekDateTime(self):
        ret = None

        for instrument, ticks in six.iteritems(self.__ticks):
            nextPos = self.__nextPos[instrument]
            if nextPos < len(ticks):
                ret = utils.safe_min(ret, ticks[nextPos].getDateTime())
        return ret

    def getNextTicks(self):
        # All ticks must have the same datetime. We will return all the ones with the smallest datetime.
        smallestDateTime = self.peekDateTime()

        if smallestDateTime is None:
            return None

        # Make a second pass to get all the ticks that had the smallest datetime.
        ret = {}
        for instrument, ticks in six.iteritems(self.__ticks):
            nextPos = self.__nextPos[instrument]
            if nextPos < len(ticks) and ticks[nextPos].getDateTime() == smallestDateTime:
                ret[instrument] = ticks[nextPos]
                self.__nextPos[instrument] += 1

        if self.__currDateTime == smallestDateTime:
            raise Exception("Duplicate ticks found for %s on %s" % (list(ret.keys()), smallestDateTime))

        self.__currDateTime = smallestDateTime
        return tick.Ticks(ret)

    def loadAll(self):
        for dateTime, ticks in self:
            pass
