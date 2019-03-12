from pyalgotrade import tickstrategy
from pyalgotrade.tickfeed import txtfeed


class MyStrategy(tickstrategy.BacktestingTickStrategy):
    def __init__(self, feed, instrument):
        super(MyStrategy, self).__init__(feed)
        self.__instrument = instrument

    def onTicks(self, ticks):
        tick = ticks[self.__instrument]
        self.info(tick.getBid())


# Load the bar feed from the CSV file
feed = txtfeed.GenericTickFeed()
feed.addTicksFromTXT("AUDUSD", "D:/github/AUDUSD.txt")

# Evaluate the strategy with the feed's bars.
myStrategy = MyStrategy(feed, "AUDUSD")
myStrategy.run()
