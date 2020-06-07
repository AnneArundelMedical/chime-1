#!/usr/bin/python3
# vim: et ts=8 sts=4 sw=4

from aamc import interpolate_dates
from aamc import *

import pandas as pd
#from penn_chime.settings import get_defaults
import penn_chime.parameters
from penn_chime.parameters import Parameters, Disposition, Regions
import penn_chime.models
from sklearn.metrics import mean_squared_error
import datetime
import sys, json, re, os, os.path, shutil
import logging, configparser
import functools, itertools, traceback, hashlib

USE_DOUBLING_TIME = False
USE_FUTURE_DIVERGENCE = True
INTERPOLATED_DATES_COUNT = 0
MITIGATION_DATE_LISTING_COUNT = 3

start_time = None

penn_chime.parameters.PRINT_PARAMS = False
penn_chime.models.logger.setLevel(logging.CRITICAL)

def data_based_variations(report_date, old_style_inputs):
    print("data_based_variations")
    hosp_census_df, hosp_census_lookback, report_date = \
        load_qlik_exported_data(report_date)
    print("LOAD COMPLETE")
    print(hosp_census_df)
    print(hosp_census_df.dtypes)
    print("Patients today: %s" % hosp_census_lookback[0])
    base = dict(BASE_PARAMS)
    base["hosp_census_lookback"] = hosp_census_lookback
    base["current_date"] = report_date
    varying_params_lists = \
        get_varying_params(report_date, INTERPOLATED_DATES_COUNT, USE_FUTURE_DIVERGENCE)
    varying_params = [
        varying_params_lists[k] for k in [
            "doubling_time", "relative_contact_rate", "mitigation_date",
            "hospitalized", "relative_icu_rate", "relative_vent_rate",
            "end_date_days_back", "mitigation_stages",
        ]
    ]
    param_set = (
        base, get_regions(), *varying_params
    )
    global start_time
    start_time = datetime.datetime.now()
    print("Beginning fit: %s" % start_time.isoformat())
    output_file_path = os.path.join(get_output_dir(), "PennModelFit_Combined_%s_%s.csv" % (
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
    print("OUTPUT FILE BASENAME: %s" % os.path.basename(output_file_path))

def find_best_fitting_params(
    output_file_path,
    hosp_census_df,
    base_params, regions, doubling_times,
    relative_contact_rates, mitigation_dates,
    hospitalized, rel_icu_rate, rel_vent_rate,
    end_date_days_back,
    mitigation_stages,
):
    #print("find_best_fitting_params")
    best = {}
    for region in regions:
        region_name = region["region_name"]
        best[region_name] = { "score": 1e10, "params": None }
    hosp_dates = hosp_census_df.dropna().index
    print("hosp_dates\n", hosp_dates)
    generate_param_arguments = (
        base_params, regions, list(doubling_times),
        list(relative_contact_rates), mitigation_dates,
        hospitalized, rel_icu_rate, rel_vent_rate,
        end_date_days_back,
        mitigation_stages,
        )
    params_count = 0
    with open("PARAMS.txt", "w") as f:
        for p in generate_param_permutations(USE_DOUBLING_TIME, *generate_param_arguments):
            print(p, file=f)
            params_count += 1
        print("PARAMETER COUNT:", params_count)
        print("PARAMETER COUNT:", params_count, file=f)
    #print("EXIT EARLY")
    #sys.exit(0)
    is_first_batch = True
    print("Writing to file:", output_file_path)
    params_progress_count = 0
    with open(output_file_path, "w") as output_file:
        region_results = {}
        for p in generate_param_permutations(USE_DOUBLING_TIME, *generate_param_arguments):
            params_progress_count += 1
            record_progress(params_progress_count, params_count)
            try:
                current_region_results = predict_one_region(
                    p, region_results, hosp_dates, hosp_census_df)
                # When we see the same region a second time, we know that we've seen an
                # entire cycle and we can record the results and start the next cycle.
                region_name = p["region_name"]
                if region_name in region_results:
                    predict_for_all_regions(
                        region_results, is_first_batch, output_file)
                    is_first_batch = False
                    region_results.clear()
                    print("CLEAR REGION RESULTS")
                region_results[region_name] = current_region_results
                print("Added region results:", region_name)
            except Exception as e:
                print("ERROR:")
                traceback.print_exc()
                with open(ERRORS_FILE, "a") as errfile:
                    print("Errors in param set:", p, file=errfile)
                    traceback.print_exc(file=errfile)
                sys.exit(1) # FIXME: REMOVE THIS LINE!!!!!!!!!!!!!!!!!!
    output_path_display = output_file_path.replace("\\", "/")
    print("Closed file:", output_path_display)
    with open("OUTPUT_PATH.txt", "w") as f:
        print(output_path_display, file=f)

def predict_one_region(p, region_results, hosp_dates, hosp_census_df):
    # The prediction happens here.
    m, final_p = get_model_from_params(p, region_results)
    # DataFrame raw_df holds the results of the model's prediction.
    predict_df = m.raw_df
    predict_df = predict_df.dropna() # remove rows with NaN values
    predict_df = predict_df.set_index(PENNMODEL_COLNAME_DATE);
    dates_intersection = predict_df.index.intersection(hosp_dates)
    matched_pred_census_df = predict_df.loc[dates_intersection]
    matched_hosp_census_df = hosp_census_df.loc[dates_intersection]
    current_region_results = {
        "model_predict_df": m.raw_df,
        "matched_actual_census_df": matched_hosp_census_df,
        "matched_predict_census_df": matched_pred_census_df,
        "params": p,
        "final_params": final_p,
        "is_derived": bool(p.get("region_derived_from"))
    }
    return current_region_results

def record_progress(params_progress_count, params_count):
    global start_time
    curr_time = datetime.datetime.now()
    elapsed_time_secs = int((curr_time - start_time).total_seconds())
    percent_done = params_progress_count / params_count
    total_time_est = int(elapsed_time_secs / percent_done)
    remaining_time_est = total_time_est - elapsed_time_secs
    progress_message = (
        ("PROGRESS: %d/%d (%s%%)"
        % (params_progress_count, params_count, str(round(100 * percent_done, 2))))
        +
        (" (time: %d secs elapsed, est %d/%d remaining)"
        % (elapsed_time_secs, remaining_time_est, total_time_est))
    )
    print(progress_message)
    with open("PROGRESS.txt", "w") as f:
        print(progress_message, file=f)

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
    #print("predict_for_all_regions")
    region_results_list = list(region_results.values())
    add_actual_share_census(region_results)
    region_results_nonderived = [
        r for r in region_results_list
        if r["is_derived"] == False 
    ]
    actual_df, predict_df, mse, mse_icu, mse_cum = \
        compute_error(region_results_nonderived)
    print("MSE = %s, ICU MSE = %s, CUM MSE = %s" % (str(mse), str(mse_icu), str(mse_cum)))
    params_list = [ r["params"] for r in region_results_list ]
    combined_model_predict_df = combine_model_predictions(region_results_list, params_list)
    first_region = region_results_list[0]
    display_fit_estimates(actual_df, predict_df, first_region["params"])
    write_fit_rows(
        common_params(params_list),
        first_region["final_params"],
        combined_model_predict_df,
        mse, mse_icu, mse_cum,
        is_first_batch,
        output_file)

def combine_model_predictions(region_results_list, params_list):
    combined_model_predict_df_list = \
        [ r["model_predict_df"] for r in region_results_list ]
    for i in range(len(region_results_list)):
        for prop_name in ["param_set_id", "region_name", "population", "market_share"]:
            combined_model_predict_df_list[i][prop_name] = params_list[i][prop_name]
    combined_model_predict_df = concat_dataframes(combined_model_predict_df_list)
    group_param_set_id = min(
        [ r["params"]["param_set_id"] for r in region_results_list ])
    combined_model_predict_df["group_param_set_id"] = group_param_set_id
    combined_model_predict_df["future_divergence_set_id"] = \
            int(group_param_set_id / get_future_divergence_set_size())
    return combined_model_predict_df

def add_actual_share_census(region_results):
    for rr in region_results.values():
        if not rr["is_derived"]:
            actual_df = rr["matched_actual_census_df"]
            actual_df = rr["params"]["region_patient_share"] * actual_df
            rr["actual_share_census_df"] = actual_df
        else:
            derived_from = rr["params"]["region_derived_from"]
            base_region_results = region_results[derived_from]
            derived_scale = rr["params"]["region_derived_scale"]
            base_region_actual_df = base_region_results["actual_share_census_df"]
            derived_actual_df = derived_scale * base_region_actual_df
            rr["actual_share_census_df"] = derived_actual_df

def compute_error(region_results_nonderived):
    combined_actual_df = concat_dataframes(
        [r["matched_actual_census_df"] for r in region_results_nonderived ])
    combined_predict_included_df = concat_dataframes(
        [r["matched_predict_census_df"] for r in region_results_nonderived ])
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
        combined_predict_included_df
        #.region_name.isin(region_names_included)
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
    return actual_df, predict_df, mse, mse_icu, mse_cum

def display_fit_estimates(actual_df, predict_df, first_region_params):
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

def write_fit_rows(
    p, final_p, predict_df,
    mse, mse_icu, mse_cum,
    is_first_batch, output_file,
):
    #print("write_fit_rows")
    try:
        df = predict_df.dropna().set_index(PENNMODEL_COLNAME_DATE)
        mitigation_policy_summ = \
            summarize_mitigation_policy(p["current_date"], p["mitigation_stages"])
        for key, val in mitigation_policy_summ:
            df[key] = val
        #df["mitigation_policy_hash"] = mitigation_policy_hash
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
        # Leaving this stop here for now because it's not getting hit.
        # Looks like the sporadic errors we had before are fixed now.
        return
    df.to_csv(output_file, header=is_first_batch)
    increment_iters()

ITERS = 0

def increment_iters():
    global ITERS
    ITERS = ITERS + 1
    if ITERS == 1:
        #raise Exception("STOPPING TO DEBUG")
        pass
    print("ITERATIONS:", ITERS)
    #if ITERS == 10: sys.exit() # FIXME
    sys.stdout.flush()

def summarize_mitigation_policy(report_date, mitigation_stages):
    past_policy = [ ms for ms in mitigation_stages if ms[0] <= report_date ]
    future_policy = [ ms for ms in mitigation_stages if ms[0] > report_date ]
    complete_str, past_str, future_str = [
        mitigation_policy_tostring(mp)
        for mp in [ mitigation_stages, past_policy, future_policy ]
    ]
    hash_function = lambda x: md5(x, 8)
    summaries = [
        [ "policy_str", complete_str ],
        [ "policy_hash", hash_function(complete_str) ],
        [ "past_str", past_str ],
        [ "past_policy_hash", hash_function(past_str) ],
        [ "future_str", future_str ],
        [ "future_policy_hash", hash_function(future_str) ],
    ]
    return summaries
    """
    partial_listing = []
    for i in range(MITIGATION_DATE_LISTING_COUNT):
        n = str(i + 1)
        try:
            partial_listing += [
                ("mitigation_date_" + n, "mitigation_stages"[i][0]),
                ("relative_contact_rate_" + n, "mitigation_stages"[i][1]),
            ]
        except IndexError:
            partial_listing += [
                ("mitigation_date_" + n, None),
                ("relative_contact_rate_" + n, None),
            ]
    return summaries + partial_listing
    """

def mitigation_policy_tostring(mitigation_policy):
    s = [ "%s:%f" % (d.isoformat(), r) for (d, r) in mitigation_policy ]
    return ";".join(s)

def get_model_from_params(parameters, region_results):
    p = get_model_params(parameters, region_results)
    print(p)
    params_obj = Parameters(**p)
    m = penn_chime.models.SimSirModel(params_obj)
    return m, p

def delete_old_errors():
    if os.path.exists(ERRORS_FILE):
        os.remove(ERRORS_FILE)

def print_errors():
    if os.path.exists(ERRORS_FILE):
        with open(ERRORS_FILE, "r") as f:
            sys.stderr.write(f.read())
        return True
    return False
