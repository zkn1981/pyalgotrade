
"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

import abc

import six

from pyalgotrade import broker
from pyalgotrade.broker import fillstrategy
from pyalgotrade import logger
import pyalgotrade.tick


######################################################################
# Commission models

@six.add_metaclass(abc.ABCMeta)
class Commission(object):
    """Base class for implementing different commission schemes.

    .. note::
        This is a base class and should not be used directly.
    """

    @abc.abstractmethod
    def calculate(self, order, price, quantity):
        """Calculates the commission for an order execution.

        :param order: The order being executed.
        :type order: :class:`pyalgotrade.broker.Order`.
        :param price: The price for each share.
        :type price: float.
        :param quantity: The order size.
        :type quantity: float.
        :rtype: float.
        """
        raise NotImplementedError()


class NoCommission(Commission):
    """A :class:`Commission` class that always returns 0."""

    def calculate(self, order, price, quantity):
        return 0


class FixedPerTrade(Commission):
    """A :class:`Commission` class that charges a fixed amount for the whole trade.

    :param amount: The commission for an order.
    :type amount: float.
    """
    def __init__(self, amount):
        super(FixedPerTrade, self).__init__()
        self.__amount = amount

    def calculate(self, order, price, quantity):
        ret = 0
        # Only charge the first fill.
        if order.getExecutionInfo() is None:
            ret = self.__amount
        return ret


class TradePercentage(Commission):
    """A :class:`Commission` class that charges a percentage of the whole trade.

    :param percentage: The percentage to charge. 0.01 means 1%, and so on. It must be smaller than 1.
    :type percentage: float.
    """
    def __init__(self, percentage):
        super(TradePercentage, self).__init__()
        assert(percentage < 1)
        self.__percentage = percentage

    def calculate(self, order, price, quantity):
        return price * quantity * self.__percentage


######################################################################
# Orders

class BacktestingOrder(object):
    def __init__(self, *args, **kwargs):
        self.__accepted = None

    def setAcceptedDateTime(self, dateTime):
        self.__accepted = dateTime

    def getAcceptedDateTime(self):
        return self.__accepted

    # Override to call the fill strategy using the concrete order type.
    # return FillInfo or None if the order should not be filled.
    def process(self, broker_, tick_):
        raise NotImplementedError()


class MarketOrder(broker.MarketOrder, BacktestingOrder):
    def __init__(self, action, instrument, quantity, onClose, instrumentTraits):
        super(MarketOrder, self).__init__(action, instrument, quantity, onClose, instrumentTraits)

    def process(self, broker_, tick_):
        return broker_.getFillStrategy().fillMarketOrder(broker_, self, tick_)


class LimitOrder(broker.LimitOrder, BacktestingOrder):
    def __init__(self, action, instrument, limitPrice, quantity, instrumentTraits):
        super(LimitOrder, self).__init__(action, instrument, limitPrice, quantity, instrumentTraits)

    def process(self, broker_, tick_):
        return broker_.getFillStrategy().fillLimitOrder(broker_, self, tick_)


class StopOrder(broker.StopOrder, BacktestingOrder):
    def __init__(self, action, instrument, stopPrice, quantity, instrumentTraits):
        super(StopOrder, self).__init__(action, instrument, stopPrice, quantity, instrumentTraits)
        self.__stopHit = False

    def process(self, broker_, tick_):
        return broker_.getFillStrategy().fillStopOrder(broker_, self, tick_)

    def setStopHit(self, stopHit):
        self.__stopHit = stopHit

    def getStopHit(self):
        return self.__stopHit


# http://www.sec.gov/answers/stoplim.htm
# http://www.interactivebrokers.com/en/trading/orders/stopLimit.php
class StopLimitOrder(broker.StopLimitOrder, BacktestingOrder):
    def __init__(self, action, instrument, stopPrice, limitPrice, quantity, instrumentTraits):
        super(StopLimitOrder, self).__init__(action, instrument, stopPrice, limitPrice, quantity, instrumentTraits)
        self.__stopHit = False  # Set to true when the limit order is activated (stop price is hit)

    def setStopHit(self, stopHit):
        self.__stopHit = stopHit

    def getStopHit(self):
        return self.__stopHit

    def isLimitOrderActive(self):
        # TODO: Deprecated since v0.15. Use getStopHit instead.
        return self.__stopHit

    def process(self, broker_, tick_):
        return broker_.getFillStrategy().fillStopLimitOrder(broker_, self, tick_)


######################################################################
# Broker

class Broker(broker.Broker):
    """Backtesting broker.

    :param cash: The initial amount of cash.
    :type cash: int/float.
    :param tickFeed: The tick feed that will provide the ticks.
    :type tickFeed: :class:`pyalgotrade.tickfeed.TickFeed`
    :param commission: An object responsible for calculating order commissions.
    :type commission: :class:`Commission`
    """

    LOGGER_NAME = "broker.tickbacktesting"

    def __init__(self, cash, tickFeed, commission=None):
        super(Broker, self).__init__()

        assert(cash >= 0)
        self.__cash = cash
        if commission is None:
            self.__commission = NoCommission()
        else:
            self.__commission = commission
        self.__shares = {}
        self.__instrumentPrice = {}  # Used by setShares
        self.__activeOrders = {}
        self.__fillStrategy = fillstrategy.DefaultStrategy()
        self.__logger = logger.getLogger(Broker.LOGGER_NAME)

        # It is VERY important that the broker subscribes to tickfeed events before the strategy.
        tickFeed.getNewValuesEvent().subscribe(self.onTicks)
        self.__tickFeed = tickFeed
        self.__allowNegativeCash = False
        self.__nextOrderId = 1
        self.__started = False

    def _getNextOrderId(self):
        ret = self.__nextOrderId
        self.__nextOrderId += 1
        return ret

    def _getTick(self, ticks, instrument):
        ret = ticks.getTick(instrument)
        if ret is None:
            ret = self.__tickFeed.getLastTick(instrument)
        return ret

    def _registerOrder(self, order):
        assert(order.getId() not in self.__activeOrders)
        assert(order.getId() is not None)
        self.__activeOrders[order.getId()] = order

    def _unregisterOrder(self, order):
        assert(order.getId() in self.__activeOrders)
        assert(order.getId() is not None)
        del self.__activeOrders[order.getId()]

    def getLogger(self):
        return self.__logger

    def setAllowNegativeCash(self, allowNegativeCash):
        self.__allowNegativeCash = allowNegativeCash

    def getCash(self, includeShort=True):
        ret = self.__cash
        if not includeShort and self.__tickFeed.getCurrentTicks() is not None:
            ticks = self.__tickFeed.getCurrentTicks()
            for instrument, shares in six.iteritems(self.__shares):
                if shares < 0:
                    instrumentPrice = self._getTick(ticks, instrument).getPrice()
                    ret += instrumentPrice * shares
        return ret

    def setCash(self, cash):
        self.__cash = cash

    def getCommission(self):
        """Returns the strategy used to calculate order commissions.

        :rtype: :class:`Commission`.
        """
        return self.__commission

    def setCommission(self, commission):
        """Sets the strategy to use to calculate order commissions.

        :param commission: An object responsible for calculating order commissions.
        :type commission: :class:`Commission`.
        """

        self.__commission = commission

    def setFillStrategy(self, strategy):
        """Sets the :class:`pyalgotrade.tickbroker.fillstrategy.FillStrategy` to use."""
        self.__fillStrategy = strategy

    def getFillStrategy(self):
        """Returns the :class:`pyalgotrade.tickbroker.fillstrategy.FillStrategy` currently set."""
        return self.__fillStrategy

    def getActiveOrders(self, instrument=None):
        if instrument is None:
            ret = list(self.__activeOrders.values())
        else:
            ret = [order for order in self.__activeOrders.values() if order.getInstrument() == instrument]
        return ret

    def _getCurrentDateTime(self):
        return self.__tickFeed.getCurrentDateTime()

    def getInstrumentTraits(self, instrument):
        return broker.IntegerTraits()

    def getShares(self, instrument):
        return self.__shares.get(instrument, 0)

    def setShares(self, instrument, quantity, price):
        """
        Set existing shares before the strategy starts executing.

        :param instrument: Instrument identifier.
        :param quantity: The number of shares for the given instrument.
        :param price: The price for each share.
        """

        assert not self.__started, "Can't setShares once the strategy started executing"
        self.__shares[instrument] = quantity
        self.__instrumentPrice[instrument] = price

    def getPositions(self):
        return self.__shares

    def getActiveInstruments(self):
        return [instrument for instrument, shares in six.iteritems(self.__shares) if shares != 0]

    def _getPriceForInstrument(self, instrument):
        ret = None

        # Try gettting the price from the last tick first.
        lastTick = self.__tickFeed.getLastTick(instrument)
        if lastTick is not None:
            ret = lastTick.getBid()
        else:
            # Try using the instrument price set by setShares if its available.
            ret = self.__instrumentPrice.get(instrument)

        return ret

    def getEquity(self):
        """Returns the portfolio value (cash + shares * price)."""

        ret = self.getCash()
        for instrument, shares in six.iteritems(self.__shares):
            instrumentPrice = self._getPriceForInstrument(instrument)
            assert instrumentPrice is not None, "Price for %s is missing" % instrument
            ret += instrumentPrice * shares
        return ret

    # Tries to commit an order execution.
    def commitOrderExecution(self, order, dateTime, fillInfo):
        price = fillInfo.getPrice()
        quantity = fillInfo.getQuantity()

        if order.isBuy():
            cost = price * quantity * -1
            assert(cost < 0)
            sharesDelta = quantity
        elif order.isSell():
            cost = price * quantity
            assert(cost > 0)
            sharesDelta = quantity * -1
        else:  # Unknown action
            assert(False)

        commission = self.getCommission().calculate(order, price, quantity)
        cost -= commission
        resultingCash = self.getCash() + cost

        # Check that we're ok on cash after the commission.
        if resultingCash >= 0 or self.__allowNegativeCash:

            # Update the order before updating internal state since addExecutionInfo may raise.
            # addExecutionInfo should switch the order state.
            orderExecutionInfo = broker.OrderExecutionInfo(price, quantity, commission, dateTime)
            order.addExecutionInfo(orderExecutionInfo)

            # Commit the order execution.
            self.__cash = resultingCash
            updatedShares = order.getInstrumentTraits().roundQuantity(
                self.getShares(order.getInstrument()) + sharesDelta
            )
            if updatedShares == 0:
                del self.__shares[order.getInstrument()]
            else:
                self.__shares[order.getInstrument()] = updatedShares

            # Let the strategy know that the order was filled.
            self.__fillStrategy.onOrderFilled(self, order)

            # Notify the order update
            if order.isFilled():
                self._unregisterOrder(order)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.FILLED, orderExecutionInfo))
            elif order.isPartiallyFilled():
                self.notifyOrderEvent(
                    broker.OrderEvent(order, broker.OrderEvent.Type.PARTIALLY_FILLED, orderExecutionInfo)
                )
            else:
                assert(False)
        else:
            self.__logger.debug("Not enough cash to fill %s order [%s] for %s share/s" % (
                order.getInstrument(),
                order.getId(),
                order.getRemaining()
            ))

    def submitOrder(self, order):
        if order.isInitial():
            order.setSubmitted(self._getNextOrderId(), self._getCurrentDateTime())
            self._registerOrder(order)
            # Switch from INITIAL -> SUBMITTED
            order.switchState(broker.Order.State.SUBMITTED)
            self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.SUBMITTED, None))
        else:
            raise Exception("The order was already processed")

    # Return True if further processing is needed.
    def __preProcessOrder(self, order, tick_):
        ret = True

        # For non-GTC orders we need to check if the order has expired.
        if not order.getGoodTillCanceled():
            expired = tick_.getDateTime().date() > order.getAcceptedDateTime().date()

            # Cancel the order if it is expired.
            if expired:
                ret = False
                self._unregisterOrder(order)
                order.switchState(broker.Order.State.CANCELED)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "Expired"))

        return ret

    def __postProcessOrder(self, order, tick_):
        # For non-GTC orders and daily (or greater) ticks we need to check if orders should expire right now
        # before waiting for the next tick.
        if not order.getGoodTillCanceled():
            expired = False
            if self.__tickFeed.getFrequency() >= pyalgotrade.tick.Frequency.DAY:
                expired = tick_.getDateTime().date() >= order.getAcceptedDateTime().date()

            # Cancel the order if it will expire in the next tick.
            if expired:
                self._unregisterOrder(order)
                order.switchState(broker.Order.State.CANCELED)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "Expired"))

    def __processOrder(self, order, tick_):
        if not self.__preProcessOrder(order, tick_):
            return

        # Double dispatch to the fill strategy using the concrete order type.
        fillInfo = order.process(self, tick_)
        if fillInfo is not None:
            self.commitOrderExecution(order, tick_.getDateTime(), fillInfo)

        if order.isActive():
            self.__postProcessOrder(order, tick_)

    def __onTicksImpl(self, order, ticks):
        # IF WE'RE DEALING WITH MULTIPLE INSTRUMENTS WE SKIP ORDER PROCESSING IF THERE IS NO TICK FOR THE ORDER'S
        # INSTRUMENT TO GET THE SAME BEHAVIOUR AS IF WERE BE PROCESSING ONLY ONE INSTRUMENT.
        tick_ = ticks.getTick(order.getInstrument())
        if tick_ is not None:
            # Switch from SUBMITTED -> ACCEPTED
            if order.isSubmitted():
                order.setAcceptedDateTime(tick_.getDateTime())
                order.switchState(broker.Order.State.ACCEPTED)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.ACCEPTED, None))

            if order.isActive():
                # This may trigger orders to be added/removed from __activeOrders.
                self.__processOrder(order, tick_)
            else:
                # If an order is not active it should be because it was canceled in this same loop and it should
                # have been removed.
                assert(order.isCanceled())
                assert(order not in self.__activeOrders)

    def onTicks(self, dateTime, ticks):
        # Let the fill strategy know that new ticks are being processed.
        self.__fillStrategy.onTicks(self, ticks)

        # This is to froze the orders that will be processed in this event, to avoid new getting orders introduced
        # and processed on this very same event.
        ordersToProcess = list(self.__activeOrders.values())

        for order in ordersToProcess:
            # This may trigger orders to be added/removed from __activeOrders.
            self.__onTicksImpl(order, ticks)

    def start(self):
        super(Broker, self).start()
        self.__started = True

    def stop(self):
        pass

    def join(self):
        pass

    def eof(self):
        # If there are no more events in the tickfeed, then there is nothing left for us to do since all processing took
        # place while processing tickfeed events.
        return self.__tickFeed.eof()

    def dispatch(self):
        # All events were already emitted while handling tickfeed events.
        pass

    def peekDateTime(self):
        return None

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
        # In order to properly support market-on-close with intraday feeds I'd need to know about different
        # exchange/market trading hours and support specifying routing an order to a specific exchange/market.
        # Even if I had all this in place it would be a problem while paper-trading with a live feed since
        # I can't tell if the next tick will be the last tick of the market session or not.
        if onClose is True and self.__tickFeed.isIntraday():
            raise Exception("Market-on-close not supported with intraday feeds")

        return MarketOrder(action, instrument, quantity, onClose, self.getInstrumentTraits(instrument))

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        return LimitOrder(action, instrument, limitPrice, quantity, self.getInstrumentTraits(instrument))

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        return StopOrder(action, instrument, stopPrice, quantity, self.getInstrumentTraits(instrument))

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        return StopLimitOrder(action, instrument, stopPrice, limitPrice, quantity, self.getInstrumentTraits(instrument))

    def cancelOrder(self, order):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")

        self._unregisterOrder(activeOrder)
        activeOrder.switchState(broker.Order.State.CANCELED)
        self.notifyOrderEvent(
            broker.OrderEvent(activeOrder, broker.OrderEvent.Type.CANCELED, "User requested cancellation")
        )
