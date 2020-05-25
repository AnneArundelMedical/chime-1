#!/usr/bin/python3
# vim: et ts=8 sts=4 sw=4

import pandas as pd
import numpy as np
import datetime
import sys, json, re, os, os.path, shutil
import logging, configparser
import functools, itertools, traceback, hashlib
import csv

OUTPUT_DIR_DEFAULT = "output"
INPUT_DIR = "input"
QLIK_EXPORT_DATA_PATH = "//dataviz.aahs.org/L$/CovidLogs/".replace("/", os.sep)
QLIK_EXPORT_DATA_FILENAME = "CovidCensusSnapshot.csv"
QLIK_EXPORT_DATA_SEP = ","
COPY_PATH = "//aamcvepcndw01/D$/".replace("/", os.sep)
DIRCONFIG_FILENAME = "dirconfig.ini"

ERRORS_FILE = "ERRORS.txt"

def get_output_dir():
    try:
        conf = configparser.ConfigParser()
        conf.read(DIRCONFIG_FILENAME)
        return conf["OUTPUT_DIRECTORY"]
    except:
        return OUTPUT_DIR_DEFAULT

HOSP_DATA_OLD_COLNAME_DATE = "[Census.CalculatedValue]"
HOSP_DATA_OLD_COLNAME_TOTAL_PATS = "Total Patients"
HOSP_DATA_OLD_COLNAME_TESTRESULT = "Current Order Status"

HOSP_DATA_COLNAME_TRUE_DATETIME = "TRUE_DATETIME"

HOSP_DATA_COLNAME_DATE = "CensusDate"
HOSP_DATA_COLNAME_TESTRESULT = "OrderStatus"
HOSP_DATA_COLNAME_TESTRESULTCOUNT = "OrderStatusCount"
HOSP_DATA_COLNAME_ICU_COUNT = "IcuCount"
HOSP_DATA_COLNAME_COUNTY = "County"
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
    path = os.path.join(get_output_dir(), filename)
    return path

def parsedate(ds):
    ds = ds.replace(" ", "T")
    return datetime.datetime.fromisoformat(ds).date()

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

def load_qlik_exported_data(report_date):
    print("load_qlik_exported_data")
    print("REPORT DATE:", report_date if report_date else "(default)")
    data_path = None
    data_path_candidates = [ QLIK_EXPORT_DATA_PATH, INPUT_DIR ]
    for p in data_path_candidates:
        path = os.path.join(p, QLIK_EXPORT_DATA_FILENAME)
        if os.path.exists(path):
            data_path = path
            break
    else:
        raise ValueError("File '" + QLIK_EXPORT_DATA_FILENAME
                         + "' not found in candidate paths: " + str(data_path_candidates))
    sep = QLIK_EXPORT_DATA_SEP
    column_names = [
        HOSP_DATA_COLNAME_DATE,
        HOSP_DATA_COLNAME_TESTRESULTCOUNT,
        HOSP_DATA_COLNAME_CUMULATIVE_COUNT,
        HOSP_DATA_COLNAME_ICU_COUNT,
        #HOSP_DATA_COLNAME_COUNTY,
    ]
    print("DATA SOURCE FILE:", data_path)
    with open(data_path) as f:
        f.readline()
        csv_reader = csv.reader(f)
        csv_rows = [[np.datetime64(parsedate(d)),
                     int(cen),
                     int(icu),
                     int(vent),
                     #county,
                    ]
                    for (
                            d,
                            cen,
                            icu,
                            vent,
                            #county,
                        ) in csv_reader
                    ]
        print("Column names:", column_names)
        for row in csv_rows:
            json.dump(row, sys.stdout, default=lambda x: str(x))
            print()
            assert len(row) == len(column_names)
        census_df = pd.DataFrame(csv_rows, columns=column_names)
    print(census_df)
    print("Setting index.")
    census_df = census_df.groupby(["CensusDate"]).sum() # add counties
    #census_df = census_df.set_index(HOSP_DATA_COLNAME_DATE)
    census_df = census_df.sort_index()
    print(census_df)
    print("INDEX:", census_df.index.dtype, type(census_df.index).__name__)
    print(census_df.dtypes)
    pos_cen_today_df = census_df[HOSP_DATA_COLNAME_TESTRESULTCOUNT]
    print(pos_cen_today_df)
    if report_date:
        report_date = census_df.index.max().date()
    positive_census_today_series = pos_cen_today_df.filter([report_date])
    if positive_census_today_series.size == 0:
        raise Exception("Report date not in census data: %s" % report_date.isoformat())
    hosp_census_lookback = list(reversed(pos_cen_today_df.tolist()))
    print(positive_census_today_series)
    positive_census_today = positive_census_today_series[0]
    print("TODAY'S POSITIVE COUNT:", positive_census_today)
    assert hosp_census_lookback[0] == positive_census_today
    print("Load complete.")
    return census_df, hosp_census_lookback, report_date

def copy_file(from_path, to_path):
    print("COPYING:")
    print("'%s'" % from_path)
    print("TO")
    print("'%s'" % to_path)
    shutil.copy(from_path, to_path)

def rounded_percent(pct):
    return int(100 * pct)

