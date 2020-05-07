#!/usr/bin/env python3

import datetime

def interpolate_dates(start_date, end_date, interpolated_days_count):
    difference_days = (end_date - start_date).days
    interpolated_days = [int((d+1) * difference_days / (interpolated_days_count+1))
                         for d in range(interpolated_days_count)]
    interpolated_days = sorted(set(interpolated_days))
    interpolated_dates = [ start_date + datetime.timedelta(days=d) for d in interpolated_days ]
    return interpolated_dates

def test_interpolate_dates():
    s, e = datetime.date(2020, 4, 1), datetime.date(2020, 4, 10)
    for n in range(9):
        dates = interpolate_dates(s, e, n+1)
        assert(len(dates) == n+1)

if __name__ == "__main__":
    pass

