#!/usr/bin/python3
# vim: et ts=8 sts=4 sw=4

import pandas
from penn_chime.settings import DEFAULTS
from penn_chime.parameters import Parameters, Disposition
from penn_chime.models import SimSirModel
from datetime import date

# Population from www.census.gov, 2019-07-01 estimate (V2019).
# Example link: https://www.census.gov/quickfacts/annearundelcountymaryland
regions = [
    { "region_name": "Anne Arundel", "population": 597234, "market_share": 30, },
    { "region_name": "Prince George's", "population": 909327, "market_share": 7, },
    { "region_name": "Queen Anne's", "population": 50381, "market_share": 40, },
    { "region_name": "Talbot", "population": 37181, "market_share": 9, },
];

doubling_times = [ 3.0, 3.5, 4.0 ]

relative_contact_rates = [ .25, .30, .40 ]

base_params = {
    "current_hospitalized": 14,
    # rates for whole pop
    "hospitalized": Disposition(.044, 10),
    "icu": Disposition(.013, 10),
    "relative_contact_rate": .30,
    "ventilated": Disposition(.007, 10),
    "current_date": date.today(),
    #"date_first_hospitalized": date.fromisoformat("2020-03-12"),
    "doubling_time": 3.0,
    "infectious_days": 14,
    #market_share: 1.0,
    #max_y_axis: Optional[int] = None,
    "n_days": 90,
    #population: Optional[int] = None,
    "recovered": 0,
    #region: Optional[Regions] = None,
    "relative_contact_rate": .30,
}

param_sets = []

for region in regions:
    for dt in doubling_times:
        for rcr in relative_contact_rates:
            p = dict(base_params)
            p.update(region)
            p["doubling_time"] = dt
            p["relative_contact_rate"] = rcr
            param_sets.append(p)

for p in param_sets:
    region_name = p["region_name"]
    del p["region_name"]
    params = Parameters(**p)
    m = SimSirModel(params)
    charts = [
        ["admits", m.admits_df],
        ["census", m.census_df],
        ["simsir", m.sim_sir_w_date_df]
    ]
    for chart_name, df in charts:
        int_doubling_time = int(100*p["doubling_time"])
        int_rcr = int(100*p["relative_contact_rate"])
        filename = ("penn_model_%s_%s_dt%d_rc%d.csv" % (
                chart_name, region_name, int_doubling_time, int_rcr))
        df.to_csv(filename)

