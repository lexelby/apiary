"""Classes for tracking statistics in Apairy.

Each statistic is represented as an object of type Tally, Level, or Series.
When apiary gets a data point for a statistic, it calls the add() method.
Periodically, apairy calls report() to retrieve information from the statistic.

Statistics should aggregate information between calls to report().  They should
also produce information about the change in each value since the last call
to report().

Types of stats:

    tally  - Tracks the total number of a certain kind of event.  Reports the
            number of events in each interval, and the total number of
            events.
    level  - Tracks a quantity that increments and decrements.  Reports the
            current level and the high/low/median/mean during the interval.
    series - Tracks a value as it changes over time.  Reports the mean/median/
            min/max/stdev in the interval.
"""

import numpy
import math
from collections import OrderedDict
from .table import ALIGN_LEFT, ALIGN_CENTER, ALIGN_RIGHT


class Statistic(object):
    def __init__(self):
        self._last = {}

    def add(self, *args, **kwargs):
        """Consume a value and store it for a later calculate() call."""
        raise NotImplementedError()

    def calculate(self):
        """Crunch numbers received since the last reset() call.

        Produce a dict of named aggregate stats.  Value type for each aggregate
        stat is expected to be the same between calls to calculate().
        """
        raise NotImplementedError()

    def reset(self):
        """Clear all stored values."""
        raise NotImplementedError()

    def format_number(self, value):
        raise NotImplementedError()

    def format_change(self, value):
        raise NotImplementedError()

    def report(self):
        current = self.calculate()
        self.reset()

        row = []

        for name, value in current.iteritems():
            row.append((ALIGN_RIGHT, name.lower() + ":"))
            row.append((ALIGN_RIGHT, self.format_number(value)))

            if name in self._last:
                row.append((ALIGN_LEFT, self.format_change(value - self._last[name])))
            else:
                row.append((ALIGN_LEFT, ""))

        self._last = current

        return row


class IntegerStatistic(Statistic):
    def format_number(self, value):
        return "%d" % value

    def format_change(self, value):
        if value == 0:
            return ""
        else:
            return "(%+d)" % value


class FloatStatistic(Statistic):
    def _get_precision(self, f, significant_figures, min_precision=3):
        # I don't want to print in scientific notation.  It's harder to
        # quickly visually compare numbers with exponents.  Instead, I'd like to
        # show a minimum number of significant figures, and print
        # as many digits as are necessary to make this happen.

        if f == 0:
            # just protecting against taking the log of 0 here, so comparing
            # a float to 0 is okay
            exponent = 0
        else:
            exponent = int(math.ceil(math.log10(abs(f))))
        return max(significant_figures - exponent, min_precision)

    def format_number(self, value):
        return "%.*f" % (self._get_precision(value, 6), value)

    def format_change(self, value):
        return "(%+.3g)" % value


class Tally(IntegerStatistic):
    def __init__(self):
        super(IntegerStatistic,self).__init__()

        self._grand_total = 0
        self._total = 0

    def add(self):
        self._total += 1

    def calculate(self):
        self._grand_total += self._total

        stats = OrderedDict()

        stats["Current"] = self._total
        stats["Total"] = self._grand_total

        return stats

    def reset(self):
        self._total = 0

class Level(IntegerStatistic):
    def __init__(self):
        super(Level,self).__init__()

        self._level = 0
        self._levels = [0]

    def add(self, direction):
        if direction == "+":
            self._level += 1
        elif direction == "-":
            self._level -= 1

        self._levels.append(self._level)

    def reset(self):
        self._levels = []

    def calculate(self):
        stats = OrderedDict()

        stats['Current'] = self._level

        if self._levels:
            add_series_stats(stats, self._levels)

        return stats


class Series(FloatStatistic):
    def __init__(self):
        super(Series, self).__init__()

        self._values = []

    def add(self, value):
        self._values.append(value)

    def reset(self):
        self._values = []

    def calculate(self):
        stats = OrderedDict()

        if self._values:
            stats['Current'] = self._values[-1]
            add_series_stats(stats, self._values)

        return stats


def add_series_stats(stats, series):
    series = numpy.array(series)

    stats['Min'] = numpy.min(series)
    stats['Max'] = numpy.max(series)
    stats['Median'] = numpy.median(series)
    stats['Mean'] = numpy.mean(series)
    stats['Stdev'] = numpy.std(series)
