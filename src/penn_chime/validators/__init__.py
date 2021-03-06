"""the callable validator design pattern"""

from .validators import EPSILON, OptionalValue, Bounded, OptionalBounded, Rate, Date, OptionalDate, ValDisposition, StrictlyPositiveRate, List

OptionalValue = OptionalValue()
OptionalStrictlyPositive = OptionalBounded(lower_bound=EPSILON)
StrictlyPositive = Bounded(lower_bound=EPSILON)
Positive = Bounded(lower_bound=-EPSILON)
Rate = Rate()  # type: ignore
StrictlyPositiveRate = StrictlyPositiveRate()  # type: ignore
Date = Date()  # type: ignore
OptionalDate = OptionalDate()  # type: ignore
ValDisposition = ValDisposition()
List = List
# # rolling a custom validator for doubling time in case DS wants to add upper bound
# DoublingTime = OptionalBounded(lower_bound=0-EPSILON, upper_bound=None)
