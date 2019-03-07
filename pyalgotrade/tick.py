"""
.. moduleauthor:: zkn <zkn@outlook.com>
"""

import abc

import six

from pyalgotrade import bar


@six.add_metaclass(abc.ABCMeta)
class Tick(object):
    """A Tick is a summary of the trading activity for a security in a given period.

    .. note::
        This is a base class and should not be used directly.
    """

    @abc.abstractmethod
    def getDateTime(self):
        """Returns the :class:`datetime.datetime`."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getBid(self):
        """Returns the bid price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getAsk(self):
        """Returns the ask price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getFrequency(self):
        """The tick's period."""
        raise NotImplementedError()


class BasicTick(Tick):
    # Optimization to reduce memory footprint.
    __slots__ = (
        '__dateTime',
        '__bid',
        '__ask',
    )

    def __init__(self, dateTime, bid, ask):
        self.__dateTime = dateTime
        self.__open = bid
        self.__close = ask

    def __setstate__(self, state):
        (self.__dateTime,
         self.__bid,
         self.__ask) = state

    def __getstate__(self):
        return (
            self.__dateTime,
            self.__bid,
            self.__ask
        )

    def getDateTime(self):
        return self.__dateTime

    def getBid(self):
        return self.__bid

    def getAsk(self):
        return self.__ask

    def getFrequency(self):
        return bar.Frequency.TRADE


class Ticks(object):
    """A group of :class:`Tick` objects.

    :param tickDict: A map of instrument to :class:`Tick` objects.
    :type tickDict: map.

    .. note::
        All ticks must have the same datetime.
    """

    def __init__(self, tickDict):
        if len(tickDict) == 0:
            raise Exception("No ticks supplied")

        # Check that tick datetimes are in sync
        firstDateTime = None
        firstInstrument = None
        for instrument, currentTick in six.iteritems(tickDict):
            if firstDateTime is None:
                firstDateTime = currentTick.getDateTime()
                firstInstrument = instrument
            elif currentTick.getDateTime() != firstDateTime:
                raise Exception("Tick data times are not in sync. %s %s != %s %s" % (
                    instrument,
                    currentTick.getDateTime(),
                    firstInstrument,
                    firstDateTime
                ))

        self.__tickDict = tickDict
        self.__dateTime = firstDateTime

    def __getitem__(self, instrument):
        """Returns the :class:`pyalgotrade.tick.Tick` for the given instrument.
        If the instrument is not found an exception is raised."""
        return self.__tickDict[instrument]

    def __contains__(self, instrument):
        """Returns True if a :class:`pyalgotrade.tick.Tick` for the given instrument is available."""
        return instrument in self.__tickDict

    def items(self):
        return list(self.__tickDict.items())

    def keys(self):
        return list(self.__tickDict.keys())

    def getInstruments(self):
        """Returns the instrument symbols."""
        return list(self.__tickDict.keys())

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` for this set of ticks."""
        return self.__dateTime

    def getTick(self, instrument):
        """Returns the :class:`pyalgotrade.tick.Tick` for the given instrument or None if the instrument is not found."""
        return self.__tickDict.get(instrument, None)
