"""design pattern via https://youtu.be/S_ipdVNSFlo?t=2153"""

from typing import Optional
from datetime import date, datetime

from .base import Validator

EPSILON = 1.e-7

class OptionalValue(Validator):
    """Any value at all"""
    def __init__(self) -> None:
        pass

    def validate(self, value):
        pass

class Bounded(Validator):
    """A bounded number."""
    def __init__(
            self,
            lower_bound: Optional[float] = None,
            upper_bound: Optional[float] = None) -> None:
        assert lower_bound is not None or upper_bound is not None, "Do not use this object to create an unbounded validator."
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.message = {
            (lower_bound, upper_bound): f"in ({self.lower_bound}, {self.upper_bound})",
            (None, upper_bound): f"less than {self.upper_bound}",
            (lower_bound, None): f"greater than {self.lower_bound}",
        }

    def validate(self, value):
        """This method implicitly validates isinstance(value, (float, int)) because it will throw a TypeError on comparison"""
        if value is None:
            raise ValueError(f"This parameter must be set")
        if (self.upper_bound is not None and value > self.upper_bound) \
           or (self.lower_bound is not None and value < self.lower_bound):
            raise ValueError(f"{value} needs to be {self.message[(self.lower_bound, self.upper_bound)]}.")


class OptionalBounded(Bounded):
    """a bounded number or a None."""
    def __init__(
            self,
            lower_bound: Optional[float] = None,
            upper_bound: Optional[float] = None) -> None:
        super().__init__(lower_bound=lower_bound, upper_bound=upper_bound)

    def validate(self, value):
        if value is None:
            return None
        super().validate(value)

class Rate(Validator):
    """A rate in [0,1]."""
    def __init__(self) -> None:
        pass
   
    def validate(self, value):
        if value is None:
            raise ValueError(f"This parameter must be set")
        if 0 > value or value > 1:
            raise ValueError(f"{value} needs to be a rate (i.e. in [0,1]).")

class StrictlyPositiveRate(Rate):
    """A rate in (0,1]."""
    def __init__(self) -> None:
        pass

    def validate(self, value):
        Rate.validate(self, value)
        if value < 0:
            raise ValueError(f"{value} needs to be a rate greater than zero.")

class Date(Validator):
    """A date of some sort."""
    def __init__(self) -> None:
        pass

    def validate(self, value):
        if value is None:
            raise ValueError(f"This parameter must be set")
        if not isinstance(value, (date, datetime)):
            raise (ValueError(f"{value} must be a date or datetime object."))

class OptionalDate(Date):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, value):
        if value is None:
            return None
        super().validate(value)

class ValDisposition(Validator):
    def __init__(self) -> None:
        pass

    def validate(self, value):
        if value is None:
            raise ValueError(f"This parameter must be set")
        Bounded(lower_bound=EPSILON)(value=value.days)
        Rate()(value=value.rate)

class List(Validator):
    def __init__(self, *member_validators) -> None:
        self.member_validators = member_validators

    def validate(self, value_list):
        for value_tuple in value_list:
            if len(value_tuple) != len(self.member_validators):
                raise ValueError(f"Tuple length doesn't match expected.")
            for i in range(len(value_tuple)):
                mv = self.member_validators[i]
                value = value_tuple[i]
                mv(value=value)

