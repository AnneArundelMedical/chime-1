#!/usr/bin/python3
# vim: et ts=8 sts=4 sw=4

from aamc import interpolate_dates
from aamc import *

from penn_chime.parameters import Parameters, Disposition, Regions

import datetime
import sys, json, re, os, os.path
import functools, itertools, traceback, hashlib

def _percent_range(lo_bound, hi_bound, step):
    return [ r/100.0 for r in range(lo_bound, hi_bound + 1, step) ]

def get_varying_params(report_date, interpolated_days_count: int, use_future_divergence: bool):

    fixed_dates = [
        (datetime.date(2020, 4, 1), [.2]),
        (datetime.date(2020, 4, 10), [.35, .40]),
        (datetime.date(2020, 4, 20), [.40, .45, .50]),
        (datetime.date(2020, 4, 30), [.40, .45, .50]),
        (datetime.date(2020, 5, 7), _percent_range(40, 60, 5)),
        #(datetime.date(2020, 5, 10), [.60]),
    ]
    last_week = report_date - datetime.timedelta(days=7)
    last_week_rates = _percent_range(55-3*3, 55+3*3, 3)
    last_fixed_date = fixed_dates[-1][0]
    interpolated_dates = \
        interpolate_dates(last_fixed_date, last_week, interpolated_days_count)

    past_stages = (
        fixed_dates
        + list(zip(interpolated_dates, [_percent_range(25, 55, 5)] * len(interpolated_dates)))
        + [ (last_week, last_week_rates) ]
    )

    future_stages = {}
    delta = 0.05
    for last_week_rate in last_week_rates:
        fs = []
        inc = 1.0 + delta
        dec = 1.0 - delta
        fs.append((last_week_rate, last_week_rate, last_week_rate))
        fs.append((last_week_rate * inc, last_week_rate * inc, last_week_rate * inc))
        fs.append((last_week_rate * inc, last_week_rate * inc**2, last_week_rate * inc**2))
        fs.append((last_week_rate * inc, last_week_rate * inc**2, last_week_rate * inc**3))
        fs.append((last_week_rate * dec, last_week_rate * dec, last_week_rate * dec))
        fs.append((last_week_rate * dec, last_week_rate * dec**2, last_week_rate * dec**2))
        fs.append((last_week_rate * dec, last_week_rate * dec**2, last_week_rate * dec**3))
        fs = [ tuple( round(r, 4) for r in rs ) for rs in fs ]
        future_stages[last_week_rate] = fs

    past_combinations = list(itertools.product(*[ r for (d, r) in past_stages ]))

    # It's useful to print these when debugging, but for real runs they get too
    # large for us to want to print them.
    #print(list( r for (d, r) in past_stages ))
    #print(past_combinations)
    #print(future_stages)

    if not use_future_divergence:
        combinations = list(past_combinations)
    else:
        combinations = []
        for pc in past_combinations:
            last_week_rate = pc[-1]
            for fs in future_stages[last_week_rate]:
                combined = pc + fs
                combinations.append(combined)

    dates = [ d for (d, r) in past_stages ]
    if use_future_divergence:
        dates = dates + [
            report_date + datetime.timedelta(days=n) for n in [0,3,6,9]
        ]

    for (d1, d2) in zip(dates[:-1], dates[1:]):
        assert d1 < d2

    mitigation_stages = [
        list(zip(dates, combo))
        for combo in combinations
    ]

    return {

        "doubling_time":
            list( dt/10.0 for dt in range(14, 26+1, 2) ),

        "relative_contact_rate":
            [.30],
            #list( rcr/100.0 for rcr in range(50, 81, 10) ),

        "mitigation_stages": mitigation_stages,

        "mitigation_date": [ datetime.date(2020, 4, 10), ],

        "hospitalized": list(
            Disposition(pct/1000.0, days) for (pct, days)
            in itertools.product(
                #range(5, 15 + 1, 5),
                [10],
                [8] #range(5, 8, 1),
            )
        ),

        "relative_icu_rate": [
            #pct/100.0 for pct in range(20, 50 + 1, 15)
            pct/100.0 for pct in [24]
        ],

        "relative_vent_rate": [
            #pct/100.0 for pct in range(70, 90 + 1, 10)
            pct/100.0 for pct in [80]
        ],

        "end_date_days_back": [ 0 ],

    }

# Population from www.census.gov, 2019-07-01 estimate (V2019).
# Example link: https://www.census.gov/quickfacts/annearundelcountymaryland
# The "excluded" regions are predicted separately but not included in totals
# because they duplicate counts already accounted for in other region rows.

#REGION_INCLUDED_FIELDS = [ "region_name", "population", "market_share", ]

_BASE_REGIONS = [
    { "region_name": "Anne Arundel", "region_group": 1,
     "population": 597234, "market_share": .30, "pat_share": 0.5686 },
    #{ "region_name": "Queen Anne's",
    # "population": 50381, "market_share": .40, "patient_share": 0 },
    #{ "region_name": "Talbot",
    # "population": 37181, "market_share": .09, "patient_share": 0 },
    { "region_name": "Prince George's",
     "population": 909327, "market_share": .07, "patient_share": 0.4314 },
]

_DERIVED_REGIONS = [
    { "region_name": "DCMC", "region_derived_from": "Prince George's",
     "market_share": .23,
     },
]

def get_regions():
    all_regions_population = 0
    all_regions_market_share_population = 0
    regions = list(_BASE_REGIONS)
    for r in base_regions:
        market_share_population = r["population"] * r["market_share"]
        all_regions_population = \
            all_regions_population + r["population"]
        all_regions_market_share_population = \
            all_regions_market_share_population + market_share_population
    derived_regions = list(_DERIVED_REGIONS)
    for r in derived_regions:
        derived_from = r["region_derived_from"]
        base_region = next( br for br in regions if br["region_name"] == derived_from )
        r["population"] = base_region["population"]
    regions += derived_regions
    for r in regions:
        market_share_population = r["population"] * r["market_share"]
        r["hosp_pop_share"] = \
            market_share_population / all_regions_market_share_population

BASE_PARAMS = {
    #"current_hospitalized": 14,
    # rates for whole pop
    #"hospitalized": Disposition(.044, 10), # 6 or maybe 5
    #"icu": Disposition(.013, 10), # ICU LOS 8
    "relative_contact_rate": .30,
    #"ventilated": Disposition(.007, 10),
    #"current_date": datetime.date.today(),
    "date_first_hospitalized": datetime.date.fromisoformat("2020-03-12"),
    #"doubling_time": 3.0,
    "infectious_days": 14,
    #market_share: 1.0,
    #max_y_axis: Optional[int] = None,
    "n_days": 14,
    #population: Optional[int] = None,
    "recovered": 0,
    #region: Optional[Regions] = None,
    #"relative_contact_rate": .30,
    "icu_days": 10,
}

def generate_param_permutations(
    use_doubling_time,
    base_params, regions, doubling_times,
    relative_contact_rates, mitigation_dates,
    hospitalized, icu_rate, vent_rate,
    end_date_days_back,
    mitigation_stages,
):
    param_set_id = 0
    #params = []
    # Important: regions must be the innermost loop because we compile
    # results from all regions on each iteration.
    #for dt in doubling_times:
    if use_doubling_time:
        combinations = itertools.product(
            relative_contact_rates,
            doubling_times,
            [0], hospitalized, icu_rate, vent_rate,
            end_date_days_back,
            mitigation_stages,
            regions, )
    else:
        combinations = itertools.product(
            relative_contact_rates,
            [0],
            [datetime.date(2000, 1, 1)], hospitalized, icu_rate, vent_rate,
            end_date_days_back,
            mitigation_stages,
            regions, )
    combo_count = 0
    for combo in combinations:
        combo_count = combo_count + 1
        param_set_id = param_set_id + 1
        # None takes the place of dt (doubling time)
        p = _combine_params(use_doubling_time, param_set_id, base_params, *combo)
        #params.append(p)
        yield p
    print("Number of parameter combinations:", combo_count)
    #return params

def _combine_params(
    use_doubling_time,
    param_set_id, base_params,
    relative_contact_rate, doubling_time,
    mitigation_date, hospitalized,
    relative_icu_rate, relative_vent_rate,
    end_date_days_back,
    mitigation_stages,
    region,
):
    p = { **base_params, **region, }
    p["param_set_id"] = param_set_id
    if use_doubling_time:
        p["doubling_time"] = doubling_time
        del p["date_first_hospitalized"]
    p["relative_contact_rate"] = relative_contact_rate
    p["mitigation_date"] = mitigation_date
    p["hospitalized"] = hospitalized
    p["relative_icu_rate"] = relative_icu_rate
    p["relative_vent_rate"] = relative_vent_rate
    p["end_date_days_back"] = end_date_days_back
    p["mitigation_stages"] = mitigation_stages
    return p

def _lists_equal(a, b):
    if len(a) != len(b):
        return False
    a = list(a)
    b = list(b)
    for ai in range(len(a)):
        for bi in range(len(b)):
            print(ai, bi)
            ax = a[ai]
            bx = b[bi]
            if a[ai] == b[bi]:
                del b[bi]
                break
        else:
            raise ValueError("Unmatched index: %d" % ai)
    return True

def get_model_params(parameters, region_results):
    #print("PARAMETERS PRINT", parameters)
    p = { **parameters }
    del p["param_set_id"]
    days_back = p["end_date_days_back"]
    del p["end_date_days_back"]
    p["current_date"] = \
        p["current_date"] - datetime.timedelta(days=days_back)
    p["region"] = Regions(**{ p["region_name"]: p["population"] })
    #print(p["hosp_census_lookback"])
    derived_from = p.get("region_derived_from")
    if derived_from:
        _derived_region_setup(p, derived_from, region_results)
    else:
        _base_region_setup(p)
    del p["hosp_census_lookback"]
    for k in p:
        if k.startswith("region_"):
            del p[k] # delete region_name, etc.
    del p["hosp_pop_share"]
    if "exclude_pop_from_total" in p:
        del p["exclude_pop_from_total"]
    icu_rate = round(p["relative_icu_rate"] * p["hospitalized"].rate, 4)
    vent_rate = round(p["relative_vent_rate"] * icu_rate, 4)
    p["icu"] = Disposition(icu_rate, p["icu_days"])
    p["ventilated"] = Disposition(vent_rate, p["icu_days"])
    del p["relative_icu_rate"]
    del p["relative_vent_rate"]
    del p["icu_days"]
    return p

def _derived_region_setup(p, derived_from, region_results):
    base_region_results = region_results["region_derived_from"]
    base_region_params = region_derived_from["params"]
    base_region_curr_hosp = base_region_params["current_hospitalized"]
    base_region_mkt_share = base_region_params["market_share"]
    extrapolated_region_census = base_region_curr_hosp / base_regions
    region_market_share = p["market_share"]
    p["current_hospitalized"] = round(extrapolated_region_census * region_market_share)

def _base_region_setup(p):
    curr_hosp = p["hosp_census_lookback"][days_back]
    p["current_hospitalized"] = round(curr_hosp * p["patient_share"])

