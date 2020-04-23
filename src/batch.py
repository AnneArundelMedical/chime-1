#!/usr/bin/python3
# vim: et ts=8 sts=4 sw=4

import pandas as pd
#from penn_chime.settings import get_defaults
import penn_chime.parameters
from penn_chime.parameters import Parameters, Disposition, Regions
import penn_chime.models
from sklearn.metrics import mean_squared_error
import datetime
import sys, json, re, os, os.path
import logging
import functools, itertools

OUTPUT_DIR = "output"
INPUT_DIR = "input"

ERRORS_FILE = "ERRORS.txt"


penn_chime.parameters.PRINT_PARAMS = False
penn_chime.models.logger.setLevel(logging.CRITICAL)

VARYING_PARAMS = {
    "doubling_time":
        list( dt/10.0 for dt in range(20, 41, 2) ),
    "relative_contact_rate":
        list( rcr/100.0 for rcr in range(10, 81, 10) ),
    "mitigation_date":
        [
            datetime.date(2020, 3, 24),
            datetime.date(2020, 4, 1),
            #datetime.date(2020, 4, 8),
        ],
        #[ datetime.date(2020, 3, 23) ]
        #list( datetime.date(2020, 3, 15) + datetime.timedelta(n)
        #      for n in range(0, 17, 2) ),
    "hospitalized": list(
        Disposition(pct/1000, days) for (pct, days)
        in itertools.product(
            range(5, 41, 5),
            range(5, 8, 1),
        )
    ),
    "icu": list(
        Disposition(.013, days) for days in range(7, 11, 1)
    ),
}

HOSP_DATA_OLD_COLNAME_DATE = "[Census.CalculatedValue]"
HOSP_DATA_OLD_COLNAME_TOTAL_PATS = "Total Patients"
HOSP_DATA_OLD_COLNAME_TESTRESULT = "Current Order Status"

HOSP_DATA_COLNAME_TRUE_DATETIME = "TRUE_DATETIME"

HOSP_DATA_COLNAME_DATE = "CensusDate"
HOSP_DATA_COLNAME_TESTRESULT = "OrderStatus"
HOSP_DATA_COLNAME_TESTRESULTCOUNT = "OrderStatusCount"

HOSP_DATA_FILE_COLUMN_NAMES = [
    HOSP_DATA_COLNAME_DATE,
    HOSP_DATA_COLNAME_TESTRESULT,
    HOSP_DATA_COLNAME_TESTRESULTCOUNT,
]

PENNMODEL_COLNAME_DATE = "date"
PENNMODEL_COLNAME_HOSPITALIZED = "census_hospitalized"

def input_file_path(file_date):
    filename = "CovidTestedCensus_%s.csv" % file_date.isoformat()
    return os.path.join(INPUT_DIR, filename)

def input_file_path_newstyle(file_date):
    filename = "CovidDailyCensus_%s.txt" % file_date.isoformat()
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
    str_mitigation_date = p["mitigation_date"].isoformat().replace("-", "")
    filename = ("%s_%s_%s_%s%s_dt%d_rc%d_md%s.csv" % (
        main_label,
        p["current_date"].isoformat(),
        region_name_tidy,
        sub_label_ext,
        chart_name,
        int_doubling_time,
        int_rcr,
        str_mitigation_date,
        ))
    path = os.path.join(OUTPUT_DIR, filename)
    return path

# Population from www.census.gov, 2019-07-01 estimate (V2019).
# Example link: https://www.census.gov/quickfacts/annearundelcountymaryland
REGIONS = [
    { "region_name": "Anne Arundel", "population": 597234, "market_share": .30, },
    { "region_name": "Prince George's", "population": 909327, "market_share": .07, },
    { "region_name": "Queen Anne's", "population": 50381, "market_share": .40, },
    { "region_name": "Talbot", "population": 37181, "market_share": .09, },
];

BASE_PARAMS = {
    "current_hospitalized": 14,
    # rates for whole pop
    "hospitalized": Disposition(.044, 10), # 6 or maybe 5
    "icu": Disposition(.013, 10), # ICU LOS 8
    "relative_contact_rate": .30,
    "ventilated": Disposition(.007, 10),
    #"current_date": datetime.date.today(),
    "date_first_hospitalized": datetime.date.fromisoformat("2020-03-12"),
    #"doubling_time": 3.0,
    "infectious_days": 14,
    #market_share: 1.0,
    #max_y_axis: Optional[int] = None,
    "n_days": 90,
    #population: Optional[int] = None,
    "recovered": 0,
    #region: Optional[Regions] = None,
    "relative_contact_rate": .30,
}

def parsedate(ds):
    return datetime.date.fromisoformat(ds)

def generate_param_permutations(
        base_params, current_date, regions, doubling_times,
        relative_contact_rates, mitigation_dates,
        hospitalized, icu,
        ):
    param_set_id = 0
    params = []
    # Important: regions must be the innermost loop because we compile
    # results from all regions on each iteration.
    #for dt in doubling_times:
    combinations = itertools.product(
        relative_contact_rates, mitigation_dates, hospitalized, icu,
        regions, 
    )
    combo_count = 0
    for combo in combinations:
        combo_count = combo_count + 1
        param_set_id = param_set_id + 1
        # None takes the place of dt (doubling time)
        p = combine_params(param_set_id, base_params, current_date, *combo)
        params.append(p)
    print("Number of parameter combinations:", combo_count)
    return params

def combine_params(param_set_id, base_params, current_date, 
                   relative_contact_rate, mitigation_date, hospitalized, icu,
                   region,
                   ):
    p = { **base_params, **region }
    p["param_set_id"] = param_set_id
    p["current_date"] = current_date
    #if doubling_time:
    #    p["doubling_time"] = doubling_time
    p["relative_contact_rate"] = relative_contact_rate
    p["mitigation_date"] = mitigation_date
    p["hospitalized"] = hospitalized
    p["icu"] = icu
    return p

def test_params():
    TEST_DATE = parsedate("2020-02-01")
    base_params = {
        "current_hospitalized": 14,
        "hospitalized": Disposition(.044, 10),
        "relative_contact_rate": .30,
        "current_date": datetime.date.today(),
        "doubling_time": 3.0,
        "relative_contact_rate": .30,
    }
    current_date = TEST_DATE
    regions = [ REGIONS[0] ]
    doubling_times = [ 3.0, 4.0 ]
    relative_contact_rates = [ .30, .40 ]
    mitigation_dates = [ parsedate(d) for d in ['2020-02-05', '2020-02-10'] ]
    params = generate_param_permutations(
        base_params, current_date, regions, doubling_times,
        relative_contact_rates, mitigation_dates,
    )

    print(params)

    assert len(params) == 8

    expected = [
        { "current_date": TEST_DATE, **REGIONS[0], "region": REGIONS[0],
          "hospitalized": Disposition(.044, 10), "current_hospitalized": 14,
          "mitigation_date": parsedate("2020-02-05"),
          "doubling_time": 3.0, "relative_contact_rate": .30, },
        { "current_date": TEST_DATE, **REGIONS[0], "region": REGIONS[0],
          "hospitalized": Disposition(.044, 10), "current_hospitalized": 14,
          "mitigation_date": parsedate("2020-02-05"),
          "doubling_time": 4.0, "relative_contact_rate": .30, },
        { "current_date": TEST_DATE, **REGIONS[0], "region": REGIONS[0],
          "hospitalized": Disposition(.044, 10), "current_hospitalized": 14,
          "mitigation_date": parsedate("2020-02-05"),
          "doubling_time": 3.0, "relative_contact_rate": .40, },
        { "current_date": TEST_DATE, **REGIONS[0], "region": REGIONS[0],
          "hospitalized": Disposition(.044, 10), "current_hospitalized": 14,
          "mitigation_date": parsedate("2020-02-05"),
          "doubling_time": 4.0, "relative_contact_rate": .40, },
        { "current_date": TEST_DATE, **REGIONS[0], "region": REGIONS[0],
          "hospitalized": Disposition(.044, 10), "current_hospitalized": 14,
          "mitigation_date": parsedate("2020-02-10"),
          "doubling_time": 3.0, "relative_contact_rate": .30, },
        { "current_date": TEST_DATE, **REGIONS[0], "region": REGIONS[0],
          "hospitalized": Disposition(.044, 10), "current_hospitalized": 14,
          "mitigation_date": parsedate("2020-02-10"),
          "doubling_time": 4.0, "relative_contact_rate": .30, },
        { "current_date": TEST_DATE, **REGIONS[0], "region": REGIONS[0],
          "hospitalized": Disposition(.044, 10), "current_hospitalized": 14,
          "mitigation_date": parsedate("2020-02-10"),
          "doubling_time": 3.0, "relative_contact_rate": .40, },
        { "current_date": TEST_DATE, **REGIONS[0], "region": REGIONS[0],
          "hospitalized": Disposition(.044, 10), "current_hospitalized": 14,
          "mitigation_date": parsedate("2020-02-10"),
          "doubling_time": 4.0, "relative_contact_rate": .40, },
    ]

    assert len(expected) == len(params)
    assert lists_equal(expected, expected)
    assert lists_equal(expected, params)

def lists_equal(a, b):
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

def load_hospital_census_data(report_date):
    print("load_hospital_census_data")
    data_path = input_file_path(report_date)
    census_df = pd.read_csv(data_path)
    #print(census_df)
    datetime_column = pd.to_datetime(census_df[HOSP_DATA_OLD_COLNAME_DATE], format="%Y-%m-%d %H:%M:%S")
    #print(datetime_column)
    census_df[HOSP_DATA_COLNAME_TRUE_DATETIME] = datetime_column
    is_positive = census_df[HOSP_DATA_OLD_COLNAME_TESTRESULT] == "POSITIVE"
    positive_df = census_df[is_positive]
    grouped_df = positive_df.set_index(HOSP_DATA_COLNAME_TRUE_DATETIME).resample("D")
    max_pats_series = grouped_df[HOSP_DATA_OLD_COLNAME_TOTAL_PATS].max()
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
    # Rename columns to match new data file format
    max_pats_df.columns = HOSP_DATA_FILE_COLUMN_NAMES
    print("RENAMED COLUMNS", max_pats_df)
    return max_pats_df, hosp_census_today

def load_newstyle_hospital_census_data(report_date):
    print("load_newstyle_hospital_census_data")
    data_path = input_file_path_newstyle(report_date)
    census_df = pd.read_csv(data_path,
                            sep="\t",
                            names=HOSP_DATA_FILE_COLUMN_NAMES,
                            parse_dates=[0],
                            index_col=[0])
    print(census_df)
    positive_census_today_series = (
        census_df[HOSP_DATA_COLNAME_TESTRESULTCOUNT]
        .filter([report_date]))
    positive_census_today = positive_census_today_series[0]
    print("TODAY'S POSITIVE COUNT:", positive_census_today)
    return census_df, positive_census_today

def original_variations(day):
    """This is the original report that I did on the first day."""
    doubling_times = [ 3.0, 3.5, 4.0 ]
    relative_contact_rates = [ .25, .30, .40 ]
    param_set = (BASE_PARAMS, REGIONS, doubling_times, relative_contact_rates)
    write_model_outputs_for_permutations(*param_set)

def data_based_variations(day, old_style_inputs):
    print("data_based_variations")
    if old_style_inputs:
        hosp_census_df, patients_today = load_hospital_census_data(day)
    else:
        hosp_census_df, patients_today = load_newstyle_hospital_census_data(day)
    print("LOAD COMPLETE")
    print(hosp_census_df)
    print(hosp_census_df.dtypes)
    print("Patients today: %s" % patients_today)
    base = dict(BASE_PARAMS)
    base["current_hospitalized"] = patients_today
    doubling_times = VARYING_PARAMS["doubling_time"]
    relative_contact_rates = VARYING_PARAMS["relative_contact_rate"]
    mitigation_dates = VARYING_PARAMS["mitigation_date"]
    hospitalized = VARYING_PARAMS["hospitalized"]
    icu = VARYING_PARAMS["icu"]
    param_set = (
        BASE_PARAMS, day, REGIONS, doubling_times,
        relative_contact_rates, mitigation_dates,
        hospitalized, icu,
    )
    #write_model_outputs_for_permutations(*param_set)
    start_time = datetime.datetime.now()
    print("Beginning fit: %s" % start_time.isoformat())
    find_best_fitting_params(hosp_census_df, *param_set)
    compl_time = datetime.datetime.now()
    print("Completed fit: %s" % compl_time.isoformat())
    print("Elapsed time:", (compl_time - start_time).total_seconds())

def write_model_outputs_for_permutations(
            base_params, current_date, regions, doubling_times,
            relative_contact_rates, mitigation_dates
            ):
    print("write_model_outputs_for_permutations")
    for p in generate_param_permutations(
            base_params, current_date, regions, doubling_times,
            relative_contact_rates, mitigation_dates,
            hospitalized, icu,
            ):
        write_model_outputs(p)

def find_best_fitting_params(
    hosp_census_df,
    base_params, day, regions, doubling_times,
    relative_contact_rates, mitigation_dates,
    hospitalized, icu,
):
    print("find_best_fitting_params")
    best = {}
    for region in regions:
        region_name = region["region_name"]
        best[region_name] = { "score": 1e10, "params": None }
    hosp_dates = hosp_census_df.dropna().index
    print("hosp_dates\n", hosp_dates)
    params_list = generate_param_permutations(
        base_params, day, regions, list(doubling_times),
        list(relative_contact_rates), mitigation_dates,
        hospitalized, icu,
        )
    with open("PARAMS.txt", "w") as f:
        for p in params_list:
            print(p, file=f)
    is_first_batch = True
    output_file_path = os.path.join(OUTPUT_DIR, "PennModelFit_Combined_%s.csv" % day.isoformat())
    print("Writing to file:", output_file_path)
    with open(output_file_path, "w") as output_file:
        region_results = {}
        for p in params_list:
            region_name = p["region_name"]
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
            #print("mse")
            #print(matched_hosp_census_df[HOSP_DATA_COLNAME_TOTAL_PATS])
            #print(matched_pred_census_df[PENNMODEL_COLNAME_HOSPITALIZED])
            if region_name in region_results:
                predict_for_all_regions(region_results, is_first_batch, output_file)
                is_first_batch = False
                region_results = {}
            region_results[region_name] = {
                "model_census_df": m.census_df,
                "matched_actual_census_df": matched_hosp_census_df,
                "matched_predict_census_df": matched_pred_census_df,
                "params": p,
            }
    print("Closed file:", output_file_path)

def concat_dataframes(dataframes):
    return functools.reduce(lambda a, b: a.append(b), dataframes)

def common_params(params_list):
    common = {}
    #print(params_list)
    for k in params_list[0].keys():
        v = params_list[0][k]
        all_match = True
        for p2 in params_list[1:]:
            if p2[k] != v:
                all_match = False
                break
        if all_match:
            common[k] = v
    return common

def predict_for_all_regions(region_results, is_first_batch, output_file):
    region_results_list = list(region_results.values())
    combined_actual_df = concat_dataframes(
        [ r["matched_actual_census_df"] for r in region_results_list ])
    combined_predict_df = concat_dataframes(
        [ r["matched_predict_census_df"] for r in region_results_list ])
    combined_model_census_df_list = \
        [ r["model_census_df"] for r in region_results_list ]
    params_list = \
        [ r["params"] for r in region_results_list ]
    for i in range(len(region_results_list)):
        for prop_name in ["param_set_id", "region_name", "population", "market_share"]:
            combined_model_census_df_list[i][prop_name] = params_list[i][prop_name]
    combined_model_census_df = concat_dataframes(combined_model_census_df_list)
    group_param_set_id = min(
        [ r["params"]["param_set_id"] for r in region_results_list ])
    combined_model_census_df["group_param_set_id"] = group_param_set_id
    actual_df = (
        combined_actual_df
        .resample("D")[HOSP_DATA_COLNAME_TESTRESULTCOUNT]
        .max()
    )
    #print("actual_df", actual_df)
    #print("compiled_results_df[2]", compiled_results_df[2])
    predict_df = (
        combined_predict_df
        #.set_index("date")
        .resample("D")[PENNMODEL_COLNAME_HOSPITALIZED]
        .sum()
    )
    #print("predict_df", predict_df)
    mse = mean_squared_error(actual_df, predict_df)
    #print(region_results)
    print("MSE:", mse)
    midpoint_index = int(actual_df.size/2);
    print("SIZE", actual_df.size)
    actual_endpoints = [
        actual_df.iloc[1],
        actual_df.iloc[midpoint_index],
        actual_df.iloc[-1],
    ]
    predict_endpoints = [
        predict_df.iloc[1],
        predict_df.iloc[midpoint_index],
        predict_df.iloc[-1],
    ]
    mse_endpoints = mean_squared_error(actual_endpoints, predict_endpoints)
    #print(actual_df)
    print(actual_endpoints, predict_endpoints, mse_endpoints)
    write_fit_rows(
        common_params(params_list),
        combined_model_census_df,
        mse,
        is_first_batch,
        output_file)

"""
    if mse < best[region_name]["score"]:
        best[region_name]["score"] = mse
        best[region_name]["params"] = p
        #print("*" * 60)
        #print("BEST SCORE: %g" % mse)
"""

ITERS = 0

def write_fit_rows(p, census_df, mse, is_first_batch, output_file):
    try:
        df = census_df.dropna().set_index(PENNMODEL_COLNAME_DATE)
        df["relative_contact_rate"] = p["relative_contact_rate"]
        #df["doubling_time"] = p["doubling_time"]
        df["mitigation_date"] = p["mitigation_date"]
        df["hospitalized_rate"] = p["hospitalized"].rate
        df["mitigation_date"] = p["mitigation_date"]
        df["mse"] = mse
        df["run_date"] = p["current_date"]
        df["hospitalized_rate"] = p["hospitalized"].rate
        df["hospitalized_days"] = p["hospitalized"].days
        df["icu_rate"] = p["icu"].rate
        df["icu_days"] = p["icu"].days
        df["ventilated_rate"] = p["ventilated"].rate
        df["ventilated_days"] = p["ventilated"].days
    except KeyError as e:
        print("EXCEPTION IN WRITE:", e, file=sys.stderr)
        with open(ERRORS_FILE, "a") as f:
            print(datetime.datetime.now().isoformat(), file=f)
            print(e, file=f)
    #path = output_file_path("PennModelFit", None, "census", p)
    df.to_csv(output_file, header=is_first_batch)
    #print("Printing batch:", p["param_set_id"], "Count:", len(df))
    global ITERS
    ITERS = ITERS + 1
    if ITERS == 1:
        #raise Exception("STOPPING TO DEBUG")
        pass

#{'current_hospitalized': 14, 'hospitalized': Disposition(rate=0.044, days=10), 'icu': Disposition(rate=0.013, days=10), 'relative_contact_rate': 0.2, 'ventilated': Disposition(rate=0.007, days=10), 'current_date': datetime.date(2020, 4, 8), 'doubling_time': 2.8, 'infectious_days': 14, 'n_days': 90, 'recovered': 0, 'region_name': 'Anne Arundel', 'population': 597234, 'market_share': 0.3, 'mitigation_date': datetime.date(2020, 3, 1)}

def rounded_percent(pct):
    return int(100 * pct)

def get_model_from_params(parameters):
    #print("PARAMETERS PRINT", parameters)
    p = { **parameters }
    p["region"] = Regions(**{ p["region_name"]: p["population"] })
    del p["region_name"]
    del p["param_set_id"]
    params_obj = Parameters(**p)
    m = penn_chime.models.SimSirModel(params_obj)
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
    if os.path.exists(ERRORS_FILE):
        os.remove(ERRORS_FILE)
    old_style_inputs = False
    if len(sys.argv) > 1:
        args = sys.argv[1:]
        if args[0] == "--old-style-inputs":
            args.pop(0)
            old_style_inputs = True
        today_override = datetime.date.fromisoformat(args[0])
    else:
        today_override = datetime.date.today()
    print("Pandas version:", pd.__version__)
    #original_variations()
    data_based_variations(today_override, old_style_inputs)
    if os.path.exists(ERRORS_FILE):
        with open(ERRORS_FILE, "r") as f:
            sys.stderr.write(f.read())

