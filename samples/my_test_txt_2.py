from pyalgotrade import tickplotter
from pyalgotrade.tickfeed import txtfeed
from pyalgotrade.tickstratanalyzer import returns
import sma_crossover2

feed = txtfeed.GenericTickFeed()
feed.addTicksFromTXT("AUDUSD", "D:/putty/AUDUSD.txt")


# Evaluate the strategy with the feed's bars.
myStrategy = sma_crossover2.SMACrossOver(feed, "AUDUSD", 20)

# Attach a returns analyzers to the strategy.
returnsAnalyzer = returns.Returns()
myStrategy.attachAnalyzer(returnsAnalyzer)

# Attach the plotter to the strategy.
plt = tickplotter.StrategyPlotter(myStrategy)
# Include the SMA in the instrument's subplot to get it displayed along with the closing prices.
plt.getInstrumentSubplot("AUDUSD").addDataSeries("SMA", myStrategy.getSMA())
# Plot the simple returns on each bar.
plt.getOrCreateSubplot("returns").addDataSeries("Simple returns", returnsAnalyzer.getReturns())

# Run the strategy.
myStrategy.run()
myStrategy.info("Final portfolio value: $%.2f" % myStrategy.getResult())

# Plot the strategy.
plt.plot()
