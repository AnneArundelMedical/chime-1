#!/usr/bin/python3
# vim: et ts=8 sts=4 sw=4

import pandas as pd
#from penn_chime.settings import get_defaults
from penn_chime.parameters import Parameters, Disposition, Regions
from penn_chime.models import SimSirModel
from sklearn.metrics import mean_squared_error
from datetime import datetime, date
import sys, json, re, os, os.path

OUTPUT_DIR = "output"
INPUT_DIR = "input"

HOSP_DATA_COLNAME_DATE = "[Census.CalculatedValue]"
HOSP_DATA_COLNAME_TOTAL_PATS = "Total Patients"
HOSP_DATA_COLNAME_TESTRESULT = "Current Order Status"
HOSP_DATA_COLNAME_TRUE_DATETIME = "TRUE_DATETIME"

PENNMODEL_COLNAME_DATE = "date"
PENNMODEL_COLNAME_HOSPITALIZED = "census_hospitalized"

def input_file_path(file_date):
    filename = "CovidTestedCensus_%s.csv" % file_date.isoformat()
    return os.path.join(INPUT_DIR, filename)

def output_file_path(main_label, sub_label, chart_name, parameters):
    p = parameters
    if sub_label is None:
        sub_label_ext = ""
    else:
        sub_label_ext = sub_label + "_"
    region_name = p["region_name"]
    region_name_tidy = re.sub(r"[ ']", "", region_name)
    int_doubling_time = rounded_percent(p["doubling_time"])
    int_rcr = rounded_percent(p["relative_contact_rate"])
    filename = ("%s_%s_%s_%s%s_dt%d_rc%d.csv" % (
        main_label,
        p["current_date"].isoformat(),
        region_name_tidy,
        sub_label_ext,
        chart_name,
        int_doubling_time,
        int_rcr
        ))
    path = os.path.join(OUTPUT_DIR, filename)
    return path

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

ITERATION_COUNT = 0

def generate_param_permutations(base_params, current_date, regions, doubling_times, relative_contact_rates):
    perms = []
    for region in regions:
        for dt in doubling_times:
            for rcr in relative_contact_rates:
                p = combine_params(base_params, current_date, region, dt, rcr)
                perms.append(p)
    return perms

def combine_params(base_params, current_date, region, doubling_time, relative_contact_rate):
    p = dict(base_params)
    p.update(region)
    p["current_date"] = current_date
    #print(region)
    p["region"] = Regions(**{ region["region_name"]: region["population"] })
    p["doubling_time"] = doubling_time
    p["relative_contact_rate"] = relative_contact_rate
    return p

def load_hospital_census_data(report_date):
    print("load_hospital_census_data")
    data_path = input_file_path(report_date)
    census_df = pd.read_csv(data_path)
    #print(census_df)
    datetime_column = pd.to_datetime(census_df[HOSP_DATA_COLNAME_DATE], format="%Y-%m-%d %H:%M:%S")
    #print(datetime_column)
    census_df[HOSP_DATA_COLNAME_TRUE_DATETIME] = datetime_column
    is_positive = census_df[HOSP_DATA_COLNAME_TESTRESULT] == "POSITIVE"
    positive_df = census_df[is_positive]
    grouped_df = positive_df.set_index(HOSP_DATA_COLNAME_TRUE_DATETIME).resample("D")
    max_pats_series = grouped_df[HOSP_DATA_COLNAME_TOTAL_PATS].max()
    print("MAX PATS DF")
    print(max_pats_series)
    hosp_census_today_series = max_pats_series.filter([report_date])
    try:
        hosp_census_today = hosp_census_today_series[0]
    except IndexError:
        raise Exception("Input file does not contain the report date. Date=%s, File=%s" % (
                report_date.isoformat(), data_path))
    print("Patients on report date: %s" % hosp_census_today)
    print("to_frame")
    max_pats_df = max_pats_series.to_frame()
    print(max_pats_df)
    #print("FINAL")
    #print(max_pats_series.iloc[:,[0]])
    #print("/FINAL")
    return max_pats_df, hosp_census_today

def original_variations(day):
    """This is the original report that I did on the first day."""
    doubling_times = [ 3.0, 3.5, 4.0 ]
    relative_contact_rates = [ .25, .30, .40 ]
    param_set = (base_params, regions, doubling_times, relative_contact_rates)
    write_model_outputs_for_permutations(*param_set)

def data_based_variations(day):
    print("data_based_variations")
    hosp_census_df, patients_today = load_hospital_census_data(day)
    print("LOAD COMPLETE")
    print(hosp_census_df)
    print(hosp_census_df.dtypes)
    print("Patients today: %s" % patients_today)
    base = dict(base_params)
    base["current_hospitalized"] = patients_today
    doubling_times = ( dt/10.0 for dt in range(28, 41) )
    relative_contact_rates = ( rcr/100.0 for rcr in range(20, 51) )
    param_set = (base_params, day, regions, doubling_times, relative_contact_rates)
    #write_model_outputs_for_permutations(*param_set)
    find_best_fitting_params(hosp_census_df, *param_set)

def write_model_outputs_for_permutations(
            base_params, current_date, regions, doubling_times, relative_contact_rates):
    print("write_model_outputs_for_permutations")
    for p in generate_param_permutations(
            base_params, current_date, regions, doubling_times, relative_contact_rates
            ):
        write_model_outputs(p)

def find_best_fitting_params(hosp_census_df,
            base_params, day, regions, doubling_times, relative_contact_rates):
    print("find_best_fitting_params")
    best_score = 1e10
    best_params = None
    hosp_dates = hosp_census_df.index
    print("hosp_dates\n", hosp_dates)
    #print(list(doubling_times))
    #print(list(relative_contact_rates))
    #print(list(generate_param_permutations(base_params, day, regions, doubling_times, relative_contact_rates)))
    for p in generate_param_permutations(
            base_params, day, regions, list(doubling_times), list(relative_contact_rates)
                ):
        m = get_model_from_params(p)
        census_df = m.census_df
        #print("census_df\n", census_df)
        census_df.to_csv(os.path.join(OUTPUT_DIR, "debug-model-census-dump.csv"))
        census_df = census_df.dropna() # remove rows with NaN values
        census_df = census_df.set_index(PENNMODEL_COLNAME_DATE);
        #print("census_df\n", census_df)
        #print("census_df.dtypes\n", census_df.dtypes)
        dates_intersection = census_df.index.intersection(hosp_dates)
        matched_pred_census_df = census_df.loc[dates_intersection]
        #print("matched_pred_census_df\n", matched_pred_census_df)
        matched_hosp_census_df = hosp_census_df.loc[dates_intersection]
        #print("matched_hosp_census_df\n", matched_hosp_census_df)
        mse = mean_squared_error(
            matched_hosp_census_df[HOSP_DATA_COLNAME_TOTAL_PATS], 
            matched_pred_census_df[PENNMODEL_COLNAME_HOSPITALIZED]
            )
        if mse < best_score:
            best_score = mse
            best_params = p
            #print("*" * 60)
            #print("BEST SCORE: %g" % mse)
        write_fit_rows(p, m.census_df, mse)
    try:
        print("BEST PARAMS:", best_params)
    except Exception as e:
        print("(printing error: %s)" % str(e))
    #write_model_outputs(best_params, "Best")
    #params_filename = "PennModel_%s_%s_bestparams.json" % (day.isoformat(), region)
    #with open(params_filename, "w") as f:
    #    json.dump({ "params": p, "mse": best_score }, f)

def write_fit_rows(p, census_df, mse):
    df = census_df.dropna().set_index(PENNMODEL_COLNAME_DATE)
    df["region_name"] = p["region_name"]
    df["population"] = p["population"]
    df["market_share"] = p["market_share"]
    df["relative_contact_rate"] = p["relative_contact_rate"]
    df["doubling_time"] = p["doubling_time"]
    df["mse"] = mse
    """
    df["hospitalized_rate"] = p["hospitalized"].rate
    df["hospitalized_days"] = p["hospitalized"].days
    df["icu_rate"] = p["icu"].rate
    df["icu_days"] = p["icu"].days
    df["ventilated_rate"] = p["ventilated"].rate
    df["ventilated_days"] = p["ventilated"].days
    """
    path = output_file_path("PennModelFit", None, "census", p)
    df.to_csv(path)

def rounded_percent(pct):
    return int(100 * pct)

def get_model_from_params(parameters):
    print("PARMETERS PRINT", parameters)
    p = dict(parameters)
    del p["region_name"]
    params_obj = Parameters(**p)
    m = SimSirModel(params_obj)
    return m

def write_model_outputs(parameters, filename_annotation = None):
    p = parameters
    day = p["current_date"]
    m = get_model_from_params(p)
    charts = [
        ["admits", m.admits_df],
        ["census", m.census_df],
        ["simsir", m.sim_sir_w_date_df]
    ]
    for chart_name, df in charts:
        path = output_file_path("PennModel", filename_annotation, chart_name, p)
        df.to_csv(path)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        today_override = date.fromisoformat(sys.argv[1])
    else:
        today_override = date.today()
    print("Pandas version:", pd.__version__)
    #original_variations()
    data_based_variations(today_override)

