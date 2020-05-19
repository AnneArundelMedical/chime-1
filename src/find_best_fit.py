#!/usr/bin/env python3

# This code seems right but for big files it runs "forever".
# (That is, it ran for hours and I killed it.)
# Probably should stop using pandas for parsing if we want it to run faster.

import pandas as pd
import sys, os, os.path, io

def main():
    _, filename = sys.argv
    inf = float("inf")
    minimums = {
        "mse": (inf, 0, None), "mse_cum": (inf, 0, None), "mse_icu": (inf, 0, None),
    }
    with open(filename) as f:
        first_lines = ""
        for i in range(2):
            first_lines += f.readline()
        df = pd.read_csv(io.StringIO(first_lines))
        columns = list(df.columns)
        line_number = 1
        while True:
            line_number += 1
            line = f.readline()
            if not line:
                break
            line_df = pd.read_csv(io.StringIO(line), names=columns)
            for k in minimums:
                (min_value, _, _) = minimums[k]
                cur_value = line_df.iloc[0][k]
                if cur_value < min_value:
                    minimums[k] = (cur_value, line_number, line_df)
            #print(line_df)
            print(line_number)
    for k in minimums:
        (min_value, line_number, min_df) = minimums[k]
        print("Field '%s': line %d" % (k, min_value))
        print(min_df)
    print(minimums)
    #idxmin = df["mse"].idxmin()
    #print("Row with best fit:", idxmin)
    #print(df.iloc[idxmin])

if __name__ == "__main__":
    main()

