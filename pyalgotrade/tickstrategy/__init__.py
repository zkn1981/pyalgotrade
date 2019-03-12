
"""
.. moduleauthor:: zkn <zkn@outlook.com>
"""

import abc
import logging

import six

import pyalgotrade.tickbroker
from pyalgotrade.tickbroker import backtesting
from pyalgotrade import observer
from pyalgotrade import dispatcher
import pyalgotrade.strategy.position
from pyalgotrade import logger


@six.add_metaclass(abc.ABCMeta)
class BaseTickStrategy(object):
    """Base class for strategies.

    :param tickFeed: The tick feed that will supply the ticks.
    :type tickFeed: :class:`pyalgotrade.tickfeed.BaseTickFeed`.
    :param broker: The broker that will handle orders.
    :type broker: :class:`pyalgotrade.broker.Broker`.

    .. note::
        This is a base class and should not be used directly.
    """

    LOGGER_NAME = "tickstrategy"

    def __init__(self, tickFeed, broker):
        self.__tickFeed = tickFeed
        self.__broker = broker
        self.__activePositions = set()
        self.__orderToPosition = {}
        self.__ticksProcessedEvent = observer.Event()
        self.__analyzers = []
        self.__namedAnalyzers = {}
        # self.__resampledTickFeeds = []
        self.__dispatcher = dispatcher.Dispatcher()
        self.__broker.getOrderUpdatedEvent().subscribe(self.__onOrderEvent)
        self.__tickFeed.getNewValuesEvent().subscribe(self.__onTicks)

        # onStart will be called once all subjects are started.
        self.__dispatcher.getStartEvent().subscribe(self.onStart)
        self.__dispatcher.getIdleEvent().subscribe(self.__onIdle)

        # It is important to dispatch broker events before feed events, specially if we're backtesting.
        self.__dispatcher.addSubject(self.__broker)
        self.__dispatcher.addSubject(self.__tickFeed)

        # Initialize logging.
        self.__logger = logger.getLogger(BaseTickStrategy.LOGGER_NAME)

    # Only valid for testing purposes.
    def _setBroker(self, broker):
        self.__broker = broker

    def setUseEventDateTimeInLogs(self, useEventDateTime):
        if useEventDateTime:
            logger.Formatter.DATETIME_HOOK = self.getDispatcher().getCurrentDateTime
        else:
            logger.Formatter.DATETIME_HOOK = None

    def getLogger(self):
        return self.__logger

    def getActivePositions(self):
        return self.__activePositions

    def getOrderToPosition(self):
        return self.__orderToPosition

    def getDispatcher(self):
        return self.__dispatcher

    def getResult(self):
        return self.getBroker().getEquity()

    def getTicksProcessedEvent(self):
        return self.__ticksProcessedEvent

    def registerPositionOrder(self, position, order):
        self.__activePositions.add(position)
        assert(order.isActive())  # Why register an inactive order ?
        self.__orderToPosition[order.getId()] = position

    def unregisterPositionOrder(self, position, order):
        del self.__orderToPosition[order.getId()]

    def unregisterPosition(self, position):
        assert(not position.isOpen())
        self.__activePositions.remove(position)

    def __notifyAnalyzers(self, lambdaExpression):
        for s in self.__analyzers:
            lambdaExpression(s)

    def attachAnalyzerEx(self, strategyAnalyzer, name=None):
        if strategyAnalyzer not in self.__analyzers:
            if name is not None:
                if name in self.__namedAnalyzers:
                    raise Exception("A different analyzer named '%s' was already attached" % name)
                self.__namedAnalyzers[name] = strategyAnalyzer

            strategyAnalyzer.beforeAttach(self)
            self.__analyzers.append(strategyAnalyzer)
            strategyAnalyzer.attached(self)

    def getLastPrice(self, instrument):
        ret = None
        tick = self.getFeed().getLastTick(instrument)
        if tick is not None:
            ret = tick.getPrice()
        return ret

    def getFeed(self):
        """Returns the :class:`pyalgotrade.tickfeed.BaseTickFeed` that this strategy is using."""
        return self.__tickFeed

    def getBroker(self):
        """Returns the :class:`pyalgotrade.broker.Broker` used to handle order executions."""
        return self.__broker

    def getCurrentDateTime(self):
        """Returns the :class:`datetime.datetime` for the current :class:`pyalgotrade.tick.Ticks`."""
        return self.__tickFeed.getCurrentDateTime()

    def marketOrder(self, instrument, quantity, onClose=False, goodTillCanceled=False, allOrNone=False):
        """Submits a market order.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param quantity: The amount of shares. Positive means buy, negative means sell.
        :type quantity: int/float.
        :param onClose: True if the order should be filled as close to the closing price as possible (Market-On-Close order). Default is False.
        :type onClose: boolean.
        :param goodTillCanceled: True if the order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the order should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.broker.MarketOrder` submitted.
        """

        ret = None
        if quantity > 0:
            ret = self.getBroker().createMarketOrder(pyalgotrade.broker.Order.Action.BUY, instrument, quantity, onClose)
        elif quantity < 0:
            ret = self.getBroker().createMarketOrder(pyalgotrade.broker.Order.Action.SELL, instrument, quantity*-1, onClose)
        if ret:
            ret.setGoodTillCanceled(goodTillCanceled)
            ret.setAllOrNone(allOrNone)
            self.getBroker().submitOrder(ret)
        return ret

    def limitOrder(self, instrument, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Submits a limit order.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: The amount of shares. Positive means buy, negative means sell.
        :type quantity: int/float.
        :param goodTillCanceled: True if the order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the order should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.broker.LimitOrder` submitted.
        """

        ret = None
        if quantity > 0:
            ret = self.getBroker().createLimitOrder(pyalgotrade.broker.Order.Action.BUY, instrument, limitPrice, quantity)
        elif quantity < 0:
            ret = self.getBroker().createLimitOrder(pyalgotrade.broker.Order.Action.SELL, instrument, limitPrice, quantity*-1)
        if ret:
            ret.setGoodTillCanceled(goodTillCanceled)
            ret.setAllOrNone(allOrNone)
            self.getBroker().submitOrder(ret)
        return ret

    def stopOrder(self, instrument, stopPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Submits a stop order.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param quantity: The amount of shares. Positive means buy, negative means sell.
        :type quantity: int/float.
        :param goodTillCanceled: True if the order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the order should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.broker.StopOrder` submitted.
        """

        ret = None
        if quantity > 0:
            ret = self.getBroker().createStopOrder(pyalgotrade.broker.Order.Action.BUY, instrument, stopPrice, quantity)
        elif quantity < 0:
            ret = self.getBroker().createStopOrder(pyalgotrade.broker.Order.Action.SELL, instrument, stopPrice, quantity*-1)
        if ret:
            ret.setGoodTillCanceled(goodTillCanceled)
            ret.setAllOrNone(allOrNone)
            self.getBroker().submitOrder(ret)
        return ret

    def stopLimitOrder(self, instrument, stopPrice, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Submits a stop limit order.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: The amount of shares. Positive means buy, negative means sell.
        :type quantity: int/float.
        :param goodTillCanceled: True if the order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the order should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.broker.StopLimitOrder` submitted.
        """

        ret = None
        if quantity > 0:
            ret = self.getBroker().createStopLimitOrder(pyalgotrade.broker.Order.Action.BUY, instrument, stopPrice, limitPrice, quantity)
        elif quantity < 0:
            ret = self.getBroker().createStopLimitOrder(pyalgotrade.broker.Order.Action.SELL, instrument, stopPrice, limitPrice, quantity*-1)
        if ret:
            ret.setGoodTillCanceled(goodTillCanceled)
            ret.setAllOrNone(allOrNone)
            self.getBroker().submitOrder(ret)
        return ret

    def enterLong(self, instrument, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a buy :class:`pyalgotrade.broker.MarketOrder` to enter a long position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return pyalgotrade.strategy.position.LongPosition(self, instrument, None, None, quantity, goodTillCanceled, allOrNone)

    def enterShort(self, instrument, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a sell short :class:`pyalgotrade.broker.MarketOrder` to enter a short position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return pyalgotrade.strategy.position.ShortPosition(self, instrument, None, None, quantity, goodTillCanceled, allOrNone)

    def enterLongLimit(self, instrument, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a buy :class:`pyalgotrade.broker.LimitOrder` to enter a long position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return pyalgotrade.strategy.position.LongPosition(self, instrument, None, limitPrice, quantity, goodTillCanceled, allOrNone)

    def enterShortLimit(self, instrument, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a sell short :class:`pyalgotrade.broker.LimitOrder` to enter a short position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return pyalgotrade.strategy.position.ShortPosition(self, instrument, None, limitPrice, quantity, goodTillCanceled, allOrNone)

    def enterLongStop(self, instrument, stopPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a buy :class:`pyalgotrade.broker.StopOrder` to enter a long position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return pyalgotrade.strategy.position.LongPosition(self, instrument, stopPrice, None, quantity, goodTillCanceled, allOrNone)

    def enterShortStop(self, instrument, stopPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a sell short :class:`pyalgotrade.broker.StopOrder` to enter a short position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return pyalgotrade.strategy.position.ShortPosition(self, instrument, stopPrice, None, quantity, goodTillCanceled, allOrNone)

    def enterLongStopLimit(self, instrument, stopPrice, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a buy :class:`pyalgotrade.broker.StopLimitOrder` order to enter a long position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: Stop price.
        :type stopPrice: float.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return pyalgotrade.strategy.position.LongPosition(self, instrument, stopPrice, limitPrice, quantity, goodTillCanceled, allOrNone)

    def enterShortStopLimit(self, instrument, stopPrice, limitPrice, quantity, goodTillCanceled=False, allOrNone=False):
        """Generates a sell short :class:`pyalgotrade.broker.StopLimitOrder` order to enter a short position.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param stopPrice: The Stop price.
        :type stopPrice: float.
        :param limitPrice: Limit price.
        :type limitPrice: float.
        :param quantity: Entry order quantity.
        :type quantity: int.
        :param goodTillCanceled: True if the entry order is good till canceled. If False then the order gets automatically canceled when the session closes.
        :type goodTillCanceled: boolean.
        :param allOrNone: True if the orders should be completely filled or not at all.
        :type allOrNone: boolean.
        :rtype: The :class:`pyalgotrade.strategy.position.Position` entered.
        """

        return pyalgotrade.strategy.position.ShortPosition(self, instrument, stopPrice, limitPrice, quantity, goodTillCanceled, allOrNone)

    def onEnterOk(self, position):
        """Override (optional) to get notified when the order submitted to enter a position was filled. The default implementation is empty.

        :param position: A position returned by any of the enterLongXXX or enterShortXXX methods.
        :type position: :class:`pyalgotrade.strategy.position.Position`.
        """
        pass

    def onEnterCanceled(self, position):
        """Override (optional) to get notified when the order submitted to enter a position was canceled. The default implementation is empty.

        :param position: A position returned by any of the enterLongXXX or enterShortXXX methods.
        :type position: :class:`pyalgotrade.strategy.position.Position`.
        """
        pass

    # Called when the exit order for a position was filled.
    def onExitOk(self, position):
        """Override (optional) to get notified when the order submitted to exit a position was filled. The default implementation is empty.

        :param position: A position returned by any of the enterLongXXX or enterShortXXX methods.
        :type position: :class:`pyalgotrade.strategy.position.Position`.
        """
        pass

    # Called when the exit order for a position was canceled.
    def onExitCanceled(self, position):
        """Override (optional) to get notified when the order submitted to exit a position was canceled. The default implementation is empty.

        :param position: A position returned by any of the enterLongXXX or enterShortXXX methods.
        :type position: :class:`pyalgotrade.strategy.position.Position`.
        """
        pass

    """Base class for strategies. """
    def onStart(self):
        """Override (optional) to get notified when the strategy starts executing. The default implementation is empty. """
        pass

    def onFinish(self, ticks):
        """Override (optional) to get notified when the strategy finished executing. The default implementation is empty.

        :param ticks: The last ticks processed.
        :type ticks: :class:`pyalgotrade.tick.Ticks`.
        """
        pass

    def onIdle(self):
        """Override (optional) to get notified when there are no events.

       .. note::
            In a pure backtesting scenario this will not be called.
        """
        pass

    @abc.abstractmethod
    def onTicks(self, ticks):
        """Override (**mandatory**) to get notified when new ticks are available. The default implementation raises an Exception.

        **This is the method to override to enter your trading logic and enter/exit positions**.

        :param ticks: The current ticks.
        :type ticks: :class:`pyalgotrade.tick.Ticks`.
        """
        raise NotImplementedError()

    def onOrderUpdated(self, order):
        """Override (optional) to get notified when an order gets updated.

        :param order: The order updated.
        :type order: :class:`pyalgotrade.broker.Order`.
        """
        pass

    def __onIdle(self):
        # Force a resample check to avoid depending solely on the underlying
        # tickfeed events.
        # for resampledTickFeed in self.__resampledTickFeeds:
        #     resampledTickFeed.checkNow(self.getCurrentDateTime())

        # self.onIdle()
        pass

    def __onOrderEvent(self, broker_, orderEvent):
        order = orderEvent.getOrder()
        self.onOrderUpdated(order)

        # Notify the position about the order event.
        pos = self.__orderToPosition.get(order.getId(), None)
        if pos is not None:
            # Unlink the order from the position if its not active anymore.
            if not order.isActive():
                self.unregisterPositionOrder(pos, order)

            pos.onOrderEvent(orderEvent)

    def __onTicks(self, dateTime, ticks):
        # THE ORDER HERE IS VERY IMPORTANT

        # 1: Let analyzers process ticks.
        self.__notifyAnalyzers(lambda s: s.beforeOnTicks(self, ticks))

        # 2: Let the strategy process current ticks and submit orders.
        self.onTicks(ticks)

        # 3: Notify that the ticks were processed.
        self.__ticksProcessedEvent.emit(self, ticks)

    def run(self):
        """Call once (**and only once**) to run the strategy."""
        self.__dispatcher.run()

        if self.__tickFeed.getCurrentTicks() is not None:
            self.onFinish(self.__tickFeed.getCurrentTicks())
        else:
            raise Exception("Feed was empty")

    def stop(self):
        """Stops a running strategy."""
        self.__dispatcher.stop()

    def attachAnalyzer(self, strategyAnalyzer):
        """Adds a :class:`pyalgotrade.stratanalyzer.StrategyAnalyzer`."""
        self.attachAnalyzerEx(strategyAnalyzer)

    def getNamedAnalyzer(self, name):
        return self.__namedAnalyzers.get(name, None)

    def debug(self, msg):
        """Logs a message with level DEBUG on the strategy logger."""
        self.getLogger().debug(msg)

    def info(self, msg):
        """Logs a message with level INFO on the strategy logger."""
        self.getLogger().info(msg)

    def warning(self, msg):
        """Logs a message with level WARNING on the strategy logger."""
        self.getLogger().warning(msg)

    def error(self, msg):
        """Logs a message with level ERROR on the strategy logger."""
        self.getLogger().error(msg)

    def critical(self, msg):
        """Logs a message with level CRITICAL on the strategy logger."""
        self.getLogger().critical(msg)

    def resampleTickFeed(self, frequency, callback):
        """
        Builds a resampled tickfeed that groups ticks by a certain frequency.

        :param frequency: The grouping frequency in seconds. Must be > 0.
        :param callback: A function similar to onTicks that will be called when new ticks are available.
        :rtype: :class:`pyalgotrade.tickfeed.BaseTickFeed`.
        """
        # ret = resampled.ResampledTickFeed(self.getFeed(), frequency)
        # ret.getNewValuesEvent().subscribe(lambda dt, ticks: callback(ticks))
        # self.getDispatcher().addSubject(ret)
        # self.__resampledTickFeeds.append(ret)
        # return ret
        pass


class BacktestingTickStrategy(BaseTickStrategy):
    """Base class for backtesting strategies.

    :param tickFeed: The tick feed to use to backtest the strategy.
    :type tickFeed: :class:`pyalgotrade.tickfeed.BaseTickFeed`.
    :param cash_or_brk: The starting capital or a broker instance.
    :type cash_or_brk: int/float or :class:`pyalgotrade.broker.Broker`.

    .. note::
        This is a base class and should not be used directly.
    """

    def __init__(self, tickFeed, cash_or_brk=1000000):
        # The broker should subscribe to tickFeed events before the strategy.
        # This is to avoid executing orders submitted in the current tick.

        if isinstance(cash_or_brk, pyalgotrade.tickbroker.Broker):
            broker = cash_or_brk
        else:
            broker = backtesting.Broker(cash_or_brk, tickFeed)

        BaseTickStrategy.__init__(self, tickFeed, broker)
        self.setUseEventDateTimeInLogs(True)
        self.setDebugMode(True)

    def setDebugMode(self, debugOn):
        """Enable/disable debug level messages in the strategy and backtesting broker.
        This is enabled by default."""
        level = logging.DEBUG if debugOn else logging.INFO
        self.getLogger().setLevel(level)
        self.getBroker().getLogger().setLevel(level)
