#!/usr/bin/python3
# vim: et ts=8 sts=4 sw=4

import pandas as pd
from penn_chime.settings import DEFAULTS
from penn_chime.parameters import Parameters, Disposition
from penn_chime.models import SimSirModel
from sklearn.metrics import mean_squared_error
from datetime import datetime, date
import sys, json, re, os, os.path

OUTPUT_DIR = "output"
INPUT_DIR = "input"

HOSP_DATA_COLNAME_DATE = "[Census.CalculatedValue]"
HOSP_DATA_COLNAME_TOTAL_PATS = "Total Patients"
HOSP_DATA_COLNAME_TESTRESULT = "Current Order Status"

PENNMODEL_COLNAME_DATE = "date"
PENNMODEL_COLNAME_HOSPITALIZED = "hospitalized"

def input_file_path(file_date):
    filename = "CovidTestedCensus_%s.csv" % file_date.isoformat()
    return os.path.join(INPUT_DIR, filename)

# Population from www.census.gov, 2019-07-01 estimate (V2019).
# Example link: https://www.census.gov/quickfacts/annearundelcountymaryland
regions = [
    { "region_name": "Anne Arundel", "population": 597234, "market_share": .30, },
    { "region_name": "Prince George's", "population": 909327, "market_share": .07, },
    { "region_name": "Queen Anne's", "population": 50381, "market_share": .40, },
    { "region_name": "Talbot", "population": 37181, "market_share": .09, },
];

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

def generate_param_permutations(base_params, regions, doubling_times, relative_contact_rates):
    for region in regions:
        for dt in doubling_times:
            for rcr in relative_contact_rates:
                p = combine_params(base_params, region, dt, rcr)
                yield p

def combine_params(base_params, region, doubling_time, relative_contact_rate):
    p = dict(base_params)
    p.update(region)
    p["doubling_time"] = dt
    p["relative_contact_rate"] = rcr
    return p

def load_hospital_census_data(report_date):
    print("load_hospital_census_data")
    data_path = input_file_path(report_date)
    """
    #date_parser = lambda dt: pd.datetime.strptime(dt, "%Y-%M-%D %H:%M:%S.%U")
    census_df = pd.read_csv(data_path, parse_dates=True) # , date_parser=date_parser)
    """
    census_df = pd.read_csv(data_path, dtype=pd.StringDtype())
    d = census_df[HOSP_DATA_COLNAME_DATE].str.rstrip("0").rstrip(".").to_datetime()
    census_df[HOSP_DATA_COLNAME_DATE] = census_df[HOSP_DATA_COLNAME_DATE].str.rstrip("0").rstrip(".").to_datetime()
    """
    census_df = pd.read_csv(data_path).astype({
        HOSP_DATA_COLNAME_DATE: "datetime64",
        HOSP_DATA_COLNAME_TESTRESULT: pd.StringDtype(),
        HOSP_DATA_COLNAME_TOTAL_PATS: "int32"
    })
    """
    is_positive = census_df[HOSP_DATA_COLNAME_TESTRESULT] == "POSITIVE"
    positive_df = census_df[is_positive]
    grouped_df = positive_df.set_index(HOSP_DATA_COLNAME_DATE).resample("D")
    max_pos_df = grouped_df[HOSP_DATA_COLNAME_TOTAL_PATS].max()
    return max_pos_df

def original_variations():
    """This is the original report that I did on the first day."""
    doubling_times = [ 3.0, 3.5, 4.0 ]
    relative_contact_rates = [ .25, .30, .40 ]
    param_set = (base_params, regions, doubling_times, relative_contact_rates)
    write_model_outputs_for_permutations(*param_set)

def data_based_variations():
    print("data_based_variations")
    today = date.today()
    hosp_census_df = load_hospital_census_data(today)
    patients_today = hosp_census_df[hosp_census_df[HOSP_DATA_COLNAME_DATE] == today.isoformat()].iloc[0]["Total Patients"]
    base = dict(base_params)
    base["current_hospitalized"] = patients_today
    doubling_times = ( dt/10.0 for dt in range(28, 41) )
    relative_contact_rates = ( rcr/100.0 for rcr in range(20, 51) )
    param_set = (base_params, regions, doubling_times, relative_contact_rates)
    write_model_outputs_for_permutations(*param_set)
    find_best_fitting_params(hosp_census_df, *param_set)

def write_model_outputs_for_permutations(
            base_params, regions, doubling_times, relative_contact_rates):
    print("write_model_outputs_for_permutations")
    for p in generate_param_permutations(
            base_params, regions, doubling_times, relative_contact_rates
            ):
        write_model_outputs(p)

def find_best_fitting_params(hosp_census_df,
            base_params, regions, doubling_times, relative_contact_rates):
    print("find_best_fitting_params")
    best_score = 1e10
    best_params = None
    hosp_dates = hosp_census_df[HOSP_DATA_COLNAME_DATE]
    for region in regions:
        for p in generate_param_permutations(
                base_params, [region], doubling_times, relative_contact_rates
                ):
            m = get_model_from_params(p)
            census_df = m.census_df
            matched_pred_census_df = census_df[census_df[PENNMODEL_COLNAME_DATE].isin(hosp_dates)]
            mse = mean_squared_error(
                hosp_census_df[HOSP_DATA_COLNAME_TOTAL_PATS], 
                matched_pred_census_df[PENNMODEL_COLNAME_HOSPITALIZED]
                )
            if mse < best_score:
                best_score = mse
                best_params = p
        write_model_outputs(best_params, "Best")
        params_filename = "PennModel_%s_%s_bestparams.json" % (date.today().isoformat(), region)
        with open(params_filename, "w") as f:
            json.dump({ "params": p, "mse": best_score }, f)

def rounded_percent(pct):
    return int(100 * pct)

def get_model_from_params(parameters):
    p = dict(parameters)
    del p["region_name"]
    params_obj = Parameters(**p)
    m = SimSirModel(params_obj)
    return m

def write_model_outputs(parameters, filename_annotation = None):
    p = parameters
    m = get_model_from_params(p)
    charts = [
        ["admits", m.admits_df],
        ["census", m.census_df],
        ["simsir", m.sim_sir_w_date_df]
    ]
    if filename_annotation is None:
        label = ""
    else:
        label = filename_annotation + "_"
    for chart_name, df in charts:
        int_doubling_time = rounded_percent(p["doubling_time"])
        int_rcr = rounded_percent(p["relative_contact_rate"])
        filename = ("PennModel_%s_%s_%s%s_dt%d_rc%d.csv" % (
                today.isoformat(), p["region_name"], label, chart_name, int_doubling_time, int_rcr))
        path = os.path.join(OUTPUT_DIR, filename)
        df.to_csv(path)

if __name__ == "__main__":
    #original_variations()
    data_based_variations()

