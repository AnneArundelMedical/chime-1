#!/usr/bin/python3
# vim: et ts=8 sts=4 sw=4

from aamc import *

def get_date_override():
    if len(sys.argv) > 1:
        args = sys.argv[1:]
        today_override = datetime.date.fromisoformat(args[0])
    else:
        today_override = datetime.date.today()
    return today_override

if __name__ == "__main__":
    delete_old_errors()
    today_override = get_date_override()
    data_based_variations(today_override, False)
    print_errors()

