# vim: et ts=8 sts=4 sw=4
"""Parameters.

Changes affecting results or their presentation should also update
constants.py `change_date``.
"""

from collections import namedtuple
from datetime import date, datetime
from typing import Optional
import sys, json

PRINT_PARAMS = False

from .validators import (
    OptionalValue, Positive, OptionalStrictlyPositive, StrictlyPositive, Rate, StrictlyPositiveRate, Date, OptionalDate, ValDisposition, List
    )

# Parameters for each disposition (hospitalized, icu, ventilated)
#   The rate of disposition within the population of infected
#   The average number days a patient has such disposition

# Hospitalized:
#   2.5 percent of the infected population are hospitalized: hospitalized.rate is 0.025
#   Average hospital length of stay is 7 days: hospitalized.days = 7

# ICU:
#   0.75 percent of the infected population are in the ICU: icu.rate is 0.0075
#   Average number of days in an ICU is 9 days: icu.days = 9

# Ventilated:
#   0.5 percent of the infected population are on a ventilator: ventilated.rate is 0.005
#   Average number of days on a ventilator: ventilated.days = 10

# Be sure to multiply by 100 when using the parameter as a default to a percent widget!


Disposition = namedtuple("Disposition", ("rate", "days"))


class Regions:
    """Arbitrary regions to sum population."""

    def __init__(self, **kwargs):
        population = 0
        for key, value in kwargs.items():
            setattr(self, key, value)
            population += value
        self.population = population


def cast_date(string):
    return datetime.strptime(string, '%Y-%m-%d').date()

def cast_list(*member_cast_functions):
    def cast_item_tuple(item_tuple):
        if len(item_tuple) != len(member_cast_functions):
            raise ValueError("Tuple doesn't match expected length.")
        return tuple(
            member_cast_functions[i](item_tuple[i])
            for i in range(len(item_tuple))
        )
    def cast_function(items):
        return [ cast_item_tuple(item) for item in items ]
    return cast_function

# Dictionary from parameter names to Tuples containing (validator, default value, cast function, help text)
ACCEPTED_PARAMETERS = {
    "current_hospitalized": (Positive, None, int, "Currently hospitalized COVID-19 patients (>= 0)"),
    "current_date": (OptionalDate, None, cast_date, "Date on which the forecast should be based"),
    "date_first_hospitalized": (
        OptionalDate, None, cast_date,
        "Date the first patient was hospitalized"
    ),
    "doubling_time": (
        OptionalStrictlyPositive, None, float,
        "Doubling time before social distancing (days)"
    ),
    "mitigation_stages": (
        List(Date, Rate), None, cast_list(cast_date, float),
        "Multiple mitigation dates with associated relative contact rates."
    ),
    "relative_contact_rate": (
        Rate, None, float, "Social distancing reduction rate: 0.0 - 1.0"
    ),
    "mitigation_date": (
        OptionalDate, None, cast_date, "Date on which social distancing measures too effect"
    ),
    "infectious_days": (
        StrictlyPositive, 14, int, "Infectious days"
    ),
    "market_share": (StrictlyPositiveRate, 1.0, float, "Hospital market share (0.00001 - 1.0)"),
    "max_y_axis": (OptionalStrictlyPositive, None, int, None),
    "n_days": (StrictlyPositive, 100, int, "Number of days to project >= 0"),
    "recovered": (Positive, 0, int, "Number of patients already recovered (not yet implemented)"),
    "population": (OptionalStrictlyPositive, None, int, "Regional population >= 1"),
    "region": (OptionalValue, None, None, "No help available"),

    "hospitalized": (ValDisposition, None, None, None),
    "icu": (ValDisposition, None, None, None),
    "ventilated": (ValDisposition, None, None, None),
}


class Parameters:
    """Parameters."""

    def __init__(self, **kwargs):
        passed_and_default_parameters = {}
        for key, value in kwargs.items():
            if key not in ACCEPTED_PARAMETERS:
                raise ValueError(f"Unexpected parameter {key}")
            passed_and_default_parameters[key] = value

        for key, (validator, default_value, cast, help) in ACCEPTED_PARAMETERS.items():
            if key not in passed_and_default_parameters:
                passed_and_default_parameters[key] = default_value

        for key, value in passed_and_default_parameters.items():
            validator = ACCEPTED_PARAMETERS[key][0]
            try:
                validator(value=value)
            except (TypeError, ValueError) as ve:
                raise ValueError(f"For parameter '{key}', with value '{value}', validation returned error \"{ve}\"")
            setattr(self, key, value)

        if self.region is  None and self.population is None:
            raise AssertionError('population or regions must be provided.')

        if self.current_date is None:
            self.current_date = date.today()
        Date(value=self.current_date)

        self.labels = {
            "hospitalized": "Hospitalized",
            "icu": "ICU",
            "ventilated": "Ventilated",
            "day": "Day",
            "date": "Date",
            "susceptible": "Susceptible",
            "infected": "Infected",
            "recovered": "Recovered",
        }

        self.dispositions = {
            "hospitalized": self.hospitalized,
            "icu": self.icu,
            "ventilated": self.ventilated,
        }

        if PRINT_PARAMS:
            self.print_params()

    def to_dict(self):
        d = {
            "hospitalized": self.hospitalized,
            "current_hospitalized": self.current_hospitalized,
            "icu": self.icu,
            "current_date": self.current_date.isoformat(),
            "date_first_hospitalized": self.date_first_hospitalized,
            "doubling_time": self.doubling_time,
            "infectious_days": self.infectious_days,
            "market_share": self.market_share,
            "max_y_axis": self.max_y_axis,
            "n_days": self.n_days,
            "recovered": self.recovered,
            "relative_contact_rate": self.relative_contact_rate,
            "hospitalized": self.hospitalized,
            "icu": self.icu,
            "ventilated": self.ventilated,
            "region": self.region,
            "population": self.population,
        }
        return d

    def print_params(self, to_file = sys.stderr):
        d = self.to_dict()
        print("Parameters {", file=to_file)
        for k in d.keys():
            print(" ", k, ":", d[k], file=to_file)
        print("}", file=to_file)

