#!/usr/bin/python3
# vim: et ts=8 sts=4 sw=4

import pandas as pd
#from penn_chime.settings import get_defaults
import penn_chime.parameters
from penn_chime.parameters import Parameters, Disposition, Regions
import penn_chime.models
from sklearn.metrics import mean_squared_error
import datetime
import sys, json, re, os, os.path, shutil
import logging
import functools, itertools, traceback, hashlib

OUTPUT_DIR = "output"
INPUT_DIR = "input"
COPY_PATH = "//aamcvepcndw01/D$/".replace("/", os.sep)

ERRORS_FILE = "ERRORS.txt"

USE_DOUBLING_TIME = False


penn_chime.parameters.PRINT_PARAMS = False
penn_chime.models.logger.setLevel(logging.CRITICAL)

def percent_range(lo_bound, hi_bound, step):
    return [ r/100.0 for r in range(lo_bound, hi_bound + 1, step) ]

def get_varying_params(report_date):

    relative_contact_rates = percent_range(10, 90, 10)

    april_1 = datetime.date(2020, 4, 1)
    last_week = report_date - datetime.timedelta(days=7)
    midpoint_days_back = int((report_date - april_1).days / 2)
    midpoint = last_week - datetime.timedelta(days=midpoint_days_back)
    last_week_rates = percent_range(30, 70, 2)

    past_stages = (
        (april_1, percent_range(10, 30, 10)),
        (midpoint, percent_range(20, 50, 15)),
        (last_week, last_week_rates),
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

    #print(list( r for (d, r) in past_stages ))
    print(past_combinations)
    #print(future_stages)

    combinations = []
    for pc in past_combinations:
        last_week_rate = pc[-1]
        for fs in future_stages[last_week_rate]:
            combined = pc + fs
            combinations.append(combined)

    dates = [ d for (d, r) in past_stages ] + [
        report_date + datetime.timedelta(days=n) for n in [3,6,9]
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
                range(5, 15 + 1, 5),
                [5] #range(5, 8, 1),
            )
        ),

        "relative_icu_rate": [
            pct/100.0 for pct in range(20, 50 + 1, 15)
        ],

        "relative_vent_rate": [
            pct/100.0 for pct in range(70, 90 + 1, 10)
        ],

        "end_date_days_back": [ 0 ],

    }

HOSP_DATA_OLD_COLNAME_DATE = "[Census.CalculatedValue]"
HOSP_DATA_OLD_COLNAME_TOTAL_PATS = "Total Patients"
HOSP_DATA_OLD_COLNAME_TESTRESULT = "Current Order Status"

HOSP_DATA_COLNAME_TRUE_DATETIME = "TRUE_DATETIME"

HOSP_DATA_COLNAME_DATE = "CensusDate"
HOSP_DATA_COLNAME_TESTRESULT = "OrderStatus"
HOSP_DATA_COLNAME_TESTRESULTCOUNT = "OrderStatusCount"
HOSP_DATA_COLNAME_ICU_COUNT = "IcuCount"
HOSP_DATA_COLNAME_CUMULATIVE_COUNT = "CumCount"

HOSP_DATA_FILE_COLUMN_NAMES = [
    HOSP_DATA_COLNAME_DATE,
    HOSP_DATA_COLNAME_TESTRESULT,
    HOSP_DATA_COLNAME_TESTRESULTCOUNT,
]

PENNMODEL_COLNAME_DATE = "date"

PENNMODEL_COLNAME_HOSPITALIZED = "census_hospitalized"
PENNMODEL_COLNAME_ICU = "census_icu"

PENNMODEL_COLNAME_EVER_HOSP = "ever_hospitalized"
PENNMODEL_COLNAME_EVER_ICU  = "ever_icu"
PENNMODEL_COLNAME_EVER_VENT = "ever_ventilated"

PENNMODEL_COLNAME_ADMITS_HOSP = "admits_hospitalized"
PENNMODEL_COLNAME_ADMITS_ICU  = "admits_icu"
PENNMODEL_COLNAME_ADMITS_VENT = "admits_ventilated"

PENNMODEL_COLNAME_CENSUS_HOSP = "census_hospitalized"
PENNMODEL_COLNAME_CENSUS_ICU  = "census_icu"
PENNMODEL_COLNAME_CENSUS_VENT = "census_ventilated"

def input_file_path(file_date):
    filename = "CovidTestedCensus_%s.csv" % file_date.isoformat()
    return os.path.join(INPUT_DIR, filename)

def make_path(base_name, file_date):
    for bn in base_name:
        for ext in ["csv", "txt"]:
            filename = "%s_%s.%s" % (bn, file_date.isoformat(), ext)
            path = os.path.join(INPUT_DIR, filename)
            if os.path.exists(path):
                return (path, "," if ext == "csv" else "\t")
    raise FileNotFoundError(
        "Unable to find file with base name one of %s for date '%s'"
        % ( str(base_name), file_date.isoformat() ))

def input_file_path_newstyle(file_date, version_number):
    base_name = "CovidDailyCensus"
    if version_number:
        base_name = "%sV%d" % (base_name, version_number)
    return make_path(["CovidTested_Census", base_name], file_date)

def input_file_path_icu(file_date):
    return make_path(["CovidTested_Icu", "CovidDailyCensusIcu"], file_date)

def input_file_path_cumulative(file_date):
    return make_path(["CovidTested_Cumulative", "CovidDailyCumulative"], file_date)

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

def add_region_share():
    all_regions_population = 0
    all_regions_market_share_population = 0
    for r in REGIONS:
        market_share_population = r["population"] * r["market_share"]
        all_regions_population = \
            all_regions_population + r["population"]
        all_regions_market_share_population = \
            all_regions_market_share_population + market_share_population
    for r in REGIONS:
        market_share_population = r["population"] * r["market_share"]
        r["hosp_pop_share"] = \
            market_share_population / all_regions_market_share_population
add_region_share()

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
    "n_days": 90,
    #population: Optional[int] = None,
    "recovered": 0,
    #region: Optional[Regions] = None,
    #"relative_contact_rate": .30,
    "icu_days": 8,
}

def parsedate(ds):
    return datetime.date.fromisoformat(ds)

def generate_param_permutations(
    base_params, regions, doubling_times,
    relative_contact_rates, mitigation_dates,
    hospitalized, icu_rate, vent_rate,
    end_date_days_back,
    mitigation_stages,
):
    param_set_id = 0
    params = []
    # Important: regions must be the innermost loop because we compile
    # results from all regions on each iteration.
    #for dt in doubling_times:
    if USE_DOUBLING_TIME:
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
        p = combine_params(param_set_id, base_params, *combo)
        params.append(p)
    print("Number of parameter combinations:", combo_count)
    return params

def combine_params(
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
    if USE_DOUBLING_TIME:
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
    hosp_census_list = max_pats_series.tolist()
    hosp_census_lookback = list(reversed(hosp_census_list))
    hosp_census_today_series = max_pats_series.filter([report_date])
    try:
        hosp_census_today = hosp_census_today_series[0]
    except IndexError:
        raise Exception("Input file does not contain the report date. Date=%s, File=%s" % (
                report_date.isoformat(), data_path))
    assert hosp_census_today == hosp_census_lookback[0]
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
    return max_pats_df, hosp_census_lookback

def load_newstyle_hospital_census_data(report_date):
    print("load_newstyle_hospital_census_data")
    print("REPORT DATE:", report_date)
    data_path, sep = input_file_path_newstyle(report_date, 2)
    print("DATA SOURCE FILE:", data_path)
    census_df = pd.read_csv(data_path,
                            sep=sep,
                            names=[HOSP_DATA_COLNAME_DATE,
                                   HOSP_DATA_COLNAME_TESTRESULTCOUNT],
                            parse_dates=[0],
                            index_col=[0])
    print(census_df)
    print(census_df.dtypes)
    icu_path, sep = input_file_path_icu(report_date)
    icu_df = pd.read_csv(icu_path,
                         sep=sep,
                         names=[HOSP_DATA_COLNAME_DATE,
                                HOSP_DATA_COLNAME_ICU_COUNT],
                         parse_dates=[0],
                         index_col=[0])
    print(icu_df)
    print(icu_df.dtypes)
    cum_path, sep = input_file_path_cumulative(report_date)
    cum_df = pd.read_csv(cum_path,
                         sep=sep,
                         names=[HOSP_DATA_COLNAME_DATE,
                                HOSP_DATA_COLNAME_CUMULATIVE_COUNT],
                         parse_dates=[0],
                         index_col=[0])
    print(cum_df)
    print(cum_df.dtypes)
    #print("INDEX", census_df.index)
    #census_df.index.astype("datetime64", copy = False)
    print("Sizes:", {"census": census_df.size, "icu": icu_df.size, "cum": cum_df.size })
    assert census_df.size == icu_df.size
    assert census_df.size == cum_df.size
    census_df[HOSP_DATA_COLNAME_ICU_COUNT] = icu_df[HOSP_DATA_COLNAME_ICU_COUNT]
    census_df[HOSP_DATA_COLNAME_CUMULATIVE_COUNT] = cum_df[HOSP_DATA_COLNAME_CUMULATIVE_COUNT]
    print(census_df)
    pos_cen_today_df = census_df[HOSP_DATA_COLNAME_TESTRESULTCOUNT]
    print(pos_cen_today_df)
    positive_census_today_series = pos_cen_today_df.filter([report_date])
    hosp_census_lookback = list(reversed(pos_cen_today_df.tolist()))
    print(positive_census_today_series)
    positive_census_today = positive_census_today_series[0]
    print("TODAY'S POSITIVE COUNT:", positive_census_today)
    assert hosp_census_lookback[0] == positive_census_today
    return census_df, hosp_census_lookback

def data_based_variations(report_date, old_style_inputs):
    print("data_based_variations")
    if old_style_inputs:
        hosp_census_df, hosp_census_lookback = load_hospital_census_data(report_date)
    else:
        hosp_census_df, hosp_census_lookback = load_newstyle_hospital_census_data(report_date)
    print("LOAD COMPLETE")
    print(hosp_census_df)
    print(hosp_census_df.dtypes)
    print("Patients today: %s" % hosp_census_lookback[0])
    base = dict(BASE_PARAMS)
    base["hosp_census_lookback"] = hosp_census_lookback
    base["current_date"] = report_date
    varying_params_lists = get_varying_params(report_date)
    varying_params = [
        varying_params_lists[k] for k in [
            "doubling_time", "relative_contact_rate", "mitigation_date",
            "hospitalized", "relative_icu_rate", "relative_vent_rate",
            "end_date_days_back", "mitigation_stages",
        ]
    ]
    param_set = (
        base, REGIONS, *varying_params
    )
    start_time = datetime.datetime.now()
    print("Beginning fit: %s" % start_time.isoformat())
    output_file_path = os.path.join(OUTPUT_DIR, "PennModelFit_Combined_%s_%s.csv" % (
        report_date.isoformat(), now_timestamp()))
    find_best_fitting_params(output_file_path, hosp_census_df, *param_set)
    compl_time = datetime.datetime.now()
    print("Completed fit: %s" % compl_time.isoformat())
    elapsed_time_secs = (compl_time - start_time).total_seconds()
    print("Elapsed time: %d:%02d (%s secs)" % (
        int(elapsed_time_secs % 60), int(elapsed_time_secs / 60),
         str(elapsed_time_secs)))
    if os.path.exists(COPY_PATH):
        copy_file(output_file_path, COPY_PATH)

def copy_file(from_path, to_path):
    with open(to_path, "w") as to_file:
        with open(from_path, "r") as from_file:
            for line in from_file.readlines():
                to_file.write(line)

def find_best_fitting_params(
    output_file_path,
    hosp_census_df,
    base_params, regions, doubling_times,
    relative_contact_rates, mitigation_dates,
    hospitalized, rel_icu_rate, rel_vent_rate,
    end_date_days_back,
    mitigation_stages,
):
    print("find_best_fitting_params")
    best = {}
    for region in regions:
        region_name = region["region_name"]
        best[region_name] = { "score": 1e10, "params": None }
    hosp_dates = hosp_census_df.dropna().index
    print("hosp_dates\n", hosp_dates)
    params_list = generate_param_permutations(
        base_params, regions, list(doubling_times),
        list(relative_contact_rates), mitigation_dates,
        hospitalized, rel_icu_rate, rel_vent_rate,
        end_date_days_back,
        mitigation_stages,
        )
    with open("PARAMS.txt", "w") as f:
        for p in params_list:
            print(p, file=f)
    is_first_batch = True
    print("Writing to file:", output_file_path)
    with open(output_file_path, "w") as output_file:
        region_results = {}
        for p in params_list:
            try:
                region_name = p["region_name"]
                #print("PARAMS PASSED TO MODEL:", p)
                m, final_p = get_model_from_params(p)
                predict_df = m.raw_df
                #print("predict_df\n", predict_df)
                #predict_df.to_csv(os.path.join(OUTPUT_DIR, "debug-model-census-dump.csv"))
                predict_df = predict_df.dropna() # remove rows with NaN values
                predict_df = predict_df.set_index(PENNMODEL_COLNAME_DATE);
                #print("predict_df\n", predict_df)
                #print("predict_df.dtypes\n", predict_df.dtypes)
                dates_intersection = predict_df.index.intersection(hosp_dates)
                matched_pred_census_df = predict_df.loc[dates_intersection]
                #print("matched_pred_census_df\n", matched_pred_census_df)
                matched_hosp_census_df = hosp_census_df.loc[dates_intersection]
                #print("matched_hosp_census_df\n", matched_hosp_census_df)
                #print("mse")
                #print(matched_hosp_census_df[HOSP_DATA_COLNAME_TOTAL_PATS])
                #print(matched_pred_census_df[PENNMODEL_COLNAME_HOSPITALIZED])
                if region_name in region_results:
                    #print("CURRENT POP:", final_p["current_hospitalized"])
                    predict_for_all_regions(region_results, is_first_batch, output_file)
                    is_first_batch = False
                    region_results = {}
                region_results[region_name] = {
                    "model_predict_df": m.raw_df,
                    "matched_actual_census_df": matched_hosp_census_df,
                    "matched_predict_census_df": matched_pred_census_df,
                    "params": p,
                    "final_params": final_p,
                }
            except Exception as e:
                print("ERROR:")
                traceback.print_exc()
                with open(ERRORS_FILE, "a") as errfile:
                    print("Errors in param set:", p, file=errfile)
                    traceback.print_exc(file=errfile)
                sys.exit(1) # FIXME: REMOVE THIS LINE!!!!!!!!!!!!!!!!!!
    print("Closed file:", output_file_path.replace("\\", "/"))

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
    first_region_params = region_results_list[0]["params"]
    final_params = region_results_list[0]["final_params"]
    combined_actual_df = concat_dataframes(
        [ r["matched_actual_census_df"] for r in region_results_list ])
    combined_predict_df = concat_dataframes(
        [ r["matched_predict_census_df"] for r in region_results_list ])
    combined_model_predict_df_list = \
        [ r["model_predict_df"] for r in region_results_list ]
    params_list = \
        [ r["params"] for r in region_results_list ]
    for i in range(len(region_results_list)):
        for prop_name in ["param_set_id", "region_name", "population", "market_share"]:
            combined_model_predict_df_list[i][prop_name] = params_list[i][prop_name]
    combined_model_predict_df = concat_dataframes(combined_model_predict_df_list)
    group_param_set_id = min(
        [ r["params"]["param_set_id"] for r in region_results_list ])
    combined_model_predict_df["group_param_set_id"] = group_param_set_id
    actual_df = (
        combined_actual_df
        .resample("D")[[
            HOSP_DATA_COLNAME_TESTRESULTCOUNT,
            HOSP_DATA_COLNAME_ICU_COUNT,
            HOSP_DATA_COLNAME_CUMULATIVE_COUNT,
        ]]
        .max()
    )
    #print("actual_df", actual_df)
    predict_df = (
        combined_predict_df
        .resample("D")[[
            PENNMODEL_COLNAME_CENSUS_HOSP,
            PENNMODEL_COLNAME_CENSUS_ICU,
            PENNMODEL_COLNAME_EVER_HOSP,
        ]]
        .sum()
    )
    #print("predict_df", predict_df)
    mse, mse_icu, mse_cum = [
        round(mean_squared_error(actual_df[actual_col], predict_df[predict_col]), 2)
        for actual_col, predict_col in [
            (HOSP_DATA_COLNAME_TESTRESULTCOUNT, PENNMODEL_COLNAME_CENSUS_HOSP),
            (HOSP_DATA_COLNAME_ICU_COUNT, PENNMODEL_COLNAME_CENSUS_ICU),
            (HOSP_DATA_COLNAME_CUMULATIVE_COUNT, PENNMODEL_COLNAME_EVER_HOSP),
        ]
    ]
    print("MSE = %s, ICU MSE = %s, CUM MSE = %s" % (str(mse), str(mse_icu), str(mse_cum)))
    data_len = len(actual_df.index)
    midpoint_index = int(data_len / 2);
    indices = sorted(set([
        int(x)
        for x in (0, data_len/2, data_len-1,
                  data_len - first_region_params["end_date_days_back"] - 1)
    ]))
    #print("INDICES:", indices, "; LEN: %d" % data_len)
    #print(predict_df)
    #print(predict_df[PENNMODEL_COLNAME_CENSUS_HOSP])
    #for i in indices:
    #    print("[%d]=" % i, end="")
    #    print(actual_df[HOSP_DATA_COLNAME_TESTRESULTCOUNT].iloc[i])
    #with open("FITDATA.txt", "w") as f:
    #    print(actual_df, file=f)
    #    print(actual_df.dtypes, file=f)
    #    print(predict_df, file=f)
    #    print(predict_df.dtypes, file=f)
    #    print("INDICES:", indices, file=f)
    actual_endpoints = []
    for x in indices:
        #print("INDEX:", x, file=f)
        actual_df_value = actual_df[HOSP_DATA_COLNAME_TESTRESULTCOUNT].iloc[x]
        #print("VALUE:", actual_df_value, file=f)
        actual_endpoints.append(actual_df_value)
    #actual_endpoints = [
    #    actual_df[HOSP_DATA_COLNAME_TESTRESULTCOUNT].iloc[x]
    #    for x in indices ]
    predict_endpoints = [
        predict_df[PENNMODEL_COLNAME_CENSUS_HOSP].iloc[x]
        for x in indices ]
    mse_endpoints = mean_squared_error(actual_endpoints, predict_endpoints)
    print(actual_endpoints, predict_endpoints, mse_endpoints)
    write_fit_rows(
        common_params(params_list),
        final_params,
        combined_model_predict_df,
        mse, mse_icu, mse_cum,
        is_first_batch,
        output_file)

ITERS = 0

def write_fit_rows(
    p, final_p, predict_df,
    mse, mse_icu, mse_cum,
    is_first_batch, output_file,
):
    try:
        df = predict_df.dropna().set_index(PENNMODEL_COLNAME_DATE)
        mitigation_policy_serialized = str(p["mitigation_stages"])
        md5 = hashlib.md5()
        md5.update(mitigation_policy_serialized.encode())
        mitigation_policy_hash = int.from_bytes(md5.digest()[:8], byteorder="big")
        df["mitigation_policy_hash"] = mitigation_policy_hash
        for i in range(3):
            n = str(i + 1)
            try:
                df["mitigation_date_" + n] = p["mitigation_stages"][i][0]
                df["relative_contact_rate_" + n] = p["mitigation_stages"][i][1]
            except IndexError:
                df["mitigation_date_" + n] = None
                df["relative_contact_rate_" + n] = None
        df["hospitalized_rate"] = p["hospitalized"].rate
        if USE_DOUBLING_TIME:
            df["doubling_time"] = p["doubling_time"]
        df["mse"] = mse
        df["mse_icu"] = mse_icu
        df["mse_cum"] = mse_cum
        df["run_date"] = p["current_date"]
        df["end_date_days_back"] = p["end_date_days_back"]
        df["hospitalized_rate"] = p["hospitalized"].rate
        df["hospitalized_days"] = p["hospitalized"].days
        df["icu_rate"] = final_p["icu"].rate
        df["icu_days"] = final_p["icu"].days
        df["ventilated_rate"] = final_p["ventilated"].rate
        df["ventilated_days"] = final_p["ventilated"].days
        df["current_hospitalized"] = final_p["current_hospitalized"]
    except KeyError as e:
        print("EXCEPTION IN WRITE:", e, file=sys.stderr)
        with open(ERRORS_FILE, "a") as errfile:
            print(datetime.datetime.now().isoformat(), file=errfile)
            traceback.print_exc(file=errfile)
        sys.exit(1) # FIXME: REMOVE THIS LINE!!!!!!!!!!!!!!!!!!
        return
    df.to_csv(output_file, header=is_first_batch)
    global ITERS
    ITERS = ITERS + 1
    if ITERS == 1:
        #raise Exception("STOPPING TO DEBUG")
        pass

def rounded_percent(pct):
    return int(100 * pct)

def now_timestamp():
    ts = datetime.datetime.now().isoformat()
    ts = re.sub(r"[^\d]", "", ts)
    return ts[:12]

def get_model_from_params(parameters):
    #print("PARAMETERS PRINT", parameters)
    p = { **parameters }
    del p["param_set_id"]
    days_back = p["end_date_days_back"]
    del p["end_date_days_back"]
    p["current_date"] = \
        p["current_date"] - datetime.timedelta(days=days_back)
    p["region"] = Regions(**{ p["region_name"]: p["population"] })
    #print(p["hosp_census_lookback"])
    curr_hosp = p["hosp_census_lookback"][days_back]
    del p["hosp_census_lookback"]
    p["current_hospitalized"] = round(curr_hosp * p["hosp_pop_share"])
    del p["region_name"]
    del p["hosp_pop_share"]
    icu_rate = round(p["relative_icu_rate"] * p["hospitalized"].rate, 4)
    vent_rate = round(p["relative_vent_rate"] * icu_rate, 4)
    p["icu"] = Disposition(icu_rate, p["icu_days"])
    p["ventilated"] = Disposition(vent_rate, p["icu_days"])
    del p["relative_icu_rate"]
    del p["relative_vent_rate"]
    del p["icu_days"]
    print(p)
    params_obj = Parameters(**p)
    m = penn_chime.models.SimSirModel(params_obj)
    return m, p

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
    data_based_variations(today_override, old_style_inputs)
    if os.path.exists(ERRORS_FILE):
        with open(ERRORS_FILE, "r") as f:
            sys.stderr.write(f.read())

